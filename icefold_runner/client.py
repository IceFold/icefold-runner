"""Reverse WebSocket client for icefold-runner.

Dials out to ``<server>/v1/ws/worker``, authenticates with the shared token
(also the XOR keystream), then serves leaf ``node_exec`` jobs concurrently.
Reconnects with jittered exponential backoff; an auth rejection is fatal.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import platform
import random
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit

import websockets

# The auth token rides in a header (not the URL query) so it can't be captured
# by server access logs / proxies. websockets renamed the client header kwarg
# (legacy ``extra_headers`` → ``additional_headers`` in the newer asyncio
# client), so pick whichever this installed version exposes.
try:
    _WS_HEADERS_KW = (
        "additional_headers"
        if "additional_headers" in inspect.signature(websockets.connect).parameters
        else "extra_headers"
    )
except (TypeError, ValueError):  # pragma: no cover - exotic websockets builds
    _WS_HEADERS_KW = "extra_headers"

import uuid

from icefold.crypto import xor_bytes
from icefold.exceptions import MissingDependencyError
from icefold.wire import (
    SRV_CANCEL,
    SRV_NODE_CALLBACK_RESULT,
    SRV_NODE_EXEC,
    SRV_PING,
    WKR_HELLO,
    WKR_NODE_DONE,
    WKR_PONG,
    WKR_PING,
    make_missing_dep,
    make_node_callback,
)
from icefold_runner import __version__ as VERSION
from icefold_runner.runner import NodeRunner
from icefold import log_error, log_info, log_warning

_BACKOFF_MIN = 1.0
_BACKOFF_MAX = 30.0


def _keepalive_s() -> float:
    """Ping cadence (and ping timeout) for the worker link, in seconds.

    Must stay comfortably under any proxy/CDN WebSocket idle timeout in front of
    the server (Cloudflare's is ~100 s) so the connection never goes idle and
    half-opens. Tunable via ``ICEFOLD_RUNNER_KEEPALIVE_S`` for aggressive
    front-ends; clamped to [5, 90] s. Defaults to 20 s."""
    raw = os.environ.get("ICEFOLD_RUNNER_KEEPALIVE_S", "")
    try:
        return min(90.0, max(5.0, float(raw))) if raw else 20.0
    except ValueError:
        return 20.0


_KEEPALIVE_S = _keepalive_s()
_MAX_FRAME = 8 * 1024 * 1024
# A connection that stayed up at least this long counts as "healthy": its drop
# is a fresh incident, not part of a tight reconnect storm, so we reset the
# backoff and reconnect fast instead of waiting out the escalated ceiling.
_HEALTHY_UPTIME_S = 5.0
# WebSocket close codes the SERVER sends on an orderly, expected teardown:
# 1012 "service restart" (the backend's uvicorn going down for a deploy/restart)
# and 1001 "going away". The runner did nothing wrong and reconnects within a
# second, so these are routine reconnects — logged at info, not error, so genuine
# faults aren't drowned out by deploy churn (every push to main restarts prod).
_GRACEFUL_CLOSE_CODES = frozenset({1001, 1012})


class AuthError(Exception):
    """Server rejected our credentials — fatal, retrying won't help."""


def _log(level: str, msg: str, **kw) -> None:
    {"warn": log_warning, "error": log_error}.get(level, log_info)("worker", msg, **kw)


def _server_close_code(e: Exception) -> Optional[int]:
    """The close code the server sent us, if the drop was a clean WS closure.

    ``websockets`` raises ``ConnectionClosed*`` carrying the received close frame
    on ``.rcvd``; transport-level failures (DNS / TCP / handshake) have no
    ``.rcvd``, so this returns ``None`` and the drop is treated as a real error.
    """
    return getattr(getattr(e, "rcvd", None), "code", None)


class WorkerClient:
    def __init__(
        self,
        *,
        server: str,
        token: str,
        worker_id: str,
        http_base: Optional[str] = None,
        staged_retention_s: float = 7 * 86400,
    ) -> None:
        self.server = server
        self.token = token
        self.worker_id = worker_id
        self.xor_key = token.encode("utf-8") if token else b""
        self.http_base = (http_base or self._derive_http_base(server)).rstrip("/")
        self.runner = NodeRunner(
            self.http_base, token, _log, staged_retention_s=staged_retention_s,
        )
        self._tasks: Dict[str, asyncio.Task] = {}
        # Bundle-host callback bookkeeping: bundle code reaches back into the
        # server via ``ctx.progress(...)`` / ``ctx.llm.text(...)``; those land
        # here as outbound ``node_callback`` frames keyed by ``req_id`` and
        # we await the server's matching ``node_callback_result`` to resolve
        # the bundle's future.
        # Keyed by req_id → (call_id, future). The call_id scoping lets a
        # finishing node fail ONLY its own pending callbacks, never a
        # concurrently-running node's (multiple node_exec run at once).
        self._pending_callbacks: Dict[str, Tuple[str, "asyncio.Future[dict]"]] = {}
        # Monotonic timestamp of the most recent successful connect, or None
        # while disconnected. Drives the healthy-drop backoff reset.
        self._connected_at: Optional[float] = None

    # ── URL helpers ──

    @staticmethod
    def _derive_http_base(server: str) -> str:
        parts = urlsplit(server)
        scheme = {"ws": "http", "wss": "https"}.get(parts.scheme, parts.scheme or "http")
        return urlunsplit((scheme, parts.netloc, "", "", ""))

    def _ws_url(self) -> str:
        parts = urlsplit(self.server)
        scheme = {"http": "ws", "https": "wss"}.get(parts.scheme, parts.scheme or "ws")
        path = parts.path.rstrip("/")
        if not path.endswith("/v1/ws/worker"):
            path = path + "/v1/ws/worker"
        # Only worker_id in the URL — the token goes in the ``X-Worker-Token``
        # header (see ``_run_once``) so it never lands in an access log. No
        # user_id: the token encodes + signs it; the server derives the identity
        # from the token so this runner can't claim another account.
        query = urlencode({"worker_id": self.worker_id})
        return urlunsplit((scheme, parts.netloc, path, query, ""))

    # ── main loop ──

    async def run_forever(self) -> None:
        _log("info", f"icefold-runner {VERSION} starting",
             server=self.server, worker_id=self.worker_id)
        backoff = _BACKOFF_MIN
        while True:
            self._connected_at = None
            try:
                await self._run_once()
                backoff = _BACKOFF_MIN
                _log("info", "connection closed; will reconnect")
            except AuthError as e:
                _log("error", f"authentication failed; exiting: {e}")
                return
            except Exception as e:  # noqa: BLE001
                # A connection that was actually established and stayed up is a
                # healthy session that happened to drop — not the server being
                # down. Reset the backoff so we reconnect promptly instead of
                # serving out a ceiling we only escalated to during real outages.
                if self._was_healthy():
                    backoff = _BACKOFF_MIN
                code = _server_close_code(e)
                if code in _GRACEFUL_CLOSE_CODES:
                    # Server closed the link cleanly on its own (e.g. it was
                    # restarted by a deploy). Reconnecting is the expected next
                    # step, not a fault on our end — don't cry wolf at error.
                    _log("info", f"server closed link for restart ({code}); reconnecting",
                         next_retry=round(backoff, 1))
                else:
                    _log("error", f"connection failed: {e}", next_retry=round(backoff, 1))
            sleep = backoff + random.uniform(0, backoff / 4)
            await asyncio.sleep(sleep)
            backoff = min(backoff * 2, _BACKOFF_MAX)

    def _was_healthy(self) -> bool:
        """True if the last dial reached a connection that stayed up a while."""
        return (
            self._connected_at is not None
            and (time.monotonic() - self._connected_at) >= _HEALTHY_UPTIME_S
        )

    async def _run_once(self) -> None:
        url = self._ws_url()
        _log("info", "dialing", url=url)  # token is in a header now, not the URL
        try:
            ws = await websockets.connect(
                url, max_size=_MAX_FRAME, open_timeout=15,
                ping_interval=_KEEPALIVE_S, ping_timeout=_KEEPALIVE_S,
                **{_WS_HEADERS_KW: {"X-Worker-Token": self.token}},
            )
        except Exception as e:  # noqa: BLE001
            if self._is_auth_rejection(e):
                raise AuthError(str(e))
            raise
        async with ws:
            await self._send(ws, {
                "type": WKR_HELLO,
                "worker_id": self.worker_id,
                "version": VERSION,
                # Shown in Settings → Runners ("Linux" / "Darwin" / "Windows").
                "os": platform.system(),
                "capabilities": ["builtin"],
            })
            self._connected_at = time.monotonic()
            _log("info", "connected", worker_id=self.worker_id)
            keepalive = asyncio.create_task(self._keepalive(ws))
            try:
                async for raw in ws:
                    msg = self._decode(raw)
                    if msg is not None:
                        await self._handle(ws, msg)
            finally:
                keepalive.cancel()
                for t in list(self._tasks.values()):
                    t.cancel()
                self._tasks.clear()

    async def _keepalive(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(_KEEPALIVE_S)
                await self._send(ws, {"type": WKR_PING})
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001
            pass

    # ── frame codec ──

    def _decode(self, raw) -> Optional[dict]:
        try:
            if isinstance(raw, (bytes, bytearray)):
                data = xor_bytes(bytes(raw), self.xor_key) if self.xor_key else bytes(raw)
                return json.loads(data.decode("utf-8"))
            return json.loads(raw)
        except Exception as e:  # noqa: BLE001
            _log("warn", f"bad frame from server: {e}")
            return None

    async def _send(self, ws, msg: dict) -> None:
        payload = json.dumps(msg).encode("utf-8")
        if self.xor_key:
            await ws.send(xor_bytes(payload, self.xor_key))
        else:
            await ws.send(payload.decode("utf-8"))

    # ── dispatch ──

    async def _handle(self, ws, msg: dict) -> None:
        mtype = msg.get("type", "")
        if mtype == SRV_NODE_EXEC:
            call_id = msg.get("call_id", "")
            if not call_id:
                return
            self._tasks[call_id] = asyncio.create_task(self._run_node(ws, msg))
        elif mtype == SRV_CANCEL:
            task = self._tasks.get(msg.get("call_id", ""))
            if task is not None:
                task.cancel()
        elif mtype == SRV_NODE_CALLBACK_RESULT:
            # Server is replying to a callback the bundle issued via
            # ctx.progress(...) / ctx.llm.text(...). Look up the awaiter
            # by req_id and feed it the result; the bundle's coroutine
            # resumes inside the node's task.
            req_id = msg.get("req_id", "")
            entry = self._pending_callbacks.pop(req_id, None)
            if entry is not None:
                _cid, fut = entry
                if not fut.done():
                    fut.set_result(msg)
        elif mtype == SRV_PING:
            await self._send(ws, {"type": WKR_PONG})

    async def _run_node(self, ws, msg: dict) -> None:
        call_id = msg["call_id"]
        node_type = msg.get("node_type", "")
        try:
            _log("info", f"running node {node_type}", call_id=call_id)
            send_callback = self._make_send_callback(ws, call_id)
            output = await self.runner.run(msg, send_callback=send_callback)
            await self._send(ws, {
                "type": WKR_NODE_DONE, "call_id": call_id,
                "output": output, "err": "", "killed": False,
            })
            _log("info", f"node done {node_type}", call_id=call_id)
        except MissingDependencyError as dep:
            # Bundle pre-flight detected a missing native/python dep. Send the
            # typed reply (not node_done) so the server can surface a
            # user-actionable "install X via …" notification.
            _log(
                "warn",
                f"node {node_type} skipped: missing deps "
                f"binaries={list(dep.missing_binaries)} python={list(dep.missing_python)}",
                call_id=call_id,
            )
            await self._safe_send(ws, make_missing_dep(
                call_id=call_id,
                missing_binaries=dep.missing_binaries,
                missing_python=dep.missing_python,
                install_hint=dep.install_hint,
            ))
        except asyncio.TimeoutError:
            await self._safe_send(ws, {
                "type": WKR_NODE_DONE, "call_id": call_id,
                "output": None, "err": "remote node timed out", "killed": True,
            })
        except asyncio.CancelledError:
            # Server asked us to cancel (or we're tearing down). The server's
            # awaiting future is already cancelled, so no node_done is needed.
            raise
        except Exception as e:  # noqa: BLE001
            import traceback
            # ``repr`` (not ``str``) so a message-less exception — a bare
            # ``AssertionError``/``RuntimeError()`` — still reports its class
            # instead of a blank; the traceback pins where it came from.
            _log(
                "error",
                f"node failed {node_type}: {e!r}\n{traceback.format_exc()}",
                call_id=call_id,
            )
            await self._safe_send(ws, {
                "type": WKR_NODE_DONE, "call_id": call_id,
                "output": None, "err": str(e) or repr(e), "killed": False,
            })
        finally:
            self._tasks.pop(call_id, None)
            self._fail_pending_callbacks(call_id)

    def _fail_pending_callbacks(self, call_id: str) -> None:
        """Fail + drop only THIS node's still-pending callbacks (e.g. it was
        cancelled mid-LLM-call) so its awaiter doesn't hang — without touching
        a concurrently-running node's callbacks, which a flat sweep would
        wrongly abort (spurious failure + dropped result on the other node)."""
        for req_id, (cid, fut) in list(self._pending_callbacks.items()):
            if cid != call_id:
                continue
            if not fut.done():
                fut.set_result({
                    "type": SRV_NODE_CALLBACK_RESULT,
                    "call_id": call_id, "req_id": req_id,
                    "ok": False, "result": None,
                    "error": "node ended before callback resolved",
                })
            self._pending_callbacks.pop(req_id, None)

    def _make_send_callback(self, ws, call_id: str):
        """Return the bundle-host callback sender bound to one node_exec.

        Bundles only ever see this closure (never the raw WS). It allocates
        a ``req_id``, queues a ``node_callback`` frame, and awaits the
        server's matching ``node_callback_result``. Result frames where
        ``ok=False`` are translated into ``RuntimeError`` so the bundle can
        catch them like any synchronous failure.
        """
        loop = asyncio.get_event_loop()

        async def _send(kind: str, payload: dict):
            req_id = uuid.uuid4().hex
            fut: "asyncio.Future[dict]" = loop.create_future()
            self._pending_callbacks[req_id] = (call_id, fut)
            try:
                await self._send(ws, make_node_callback(
                    call_id=call_id, req_id=req_id, kind=kind, payload=payload,
                ))
            except Exception:
                self._pending_callbacks.pop(req_id, None)
                raise
            reply = await fut
            if not reply.get("ok"):
                raise RuntimeError(reply.get("error") or f"callback {kind!r} failed")
            return reply.get("result")

        return _send

    async def _safe_send(self, ws, msg: dict) -> None:
        try:
            await self._send(ws, msg)
        except Exception:  # noqa: BLE001
            pass

    # ── error classification ──

    @staticmethod
    def _is_auth_rejection(e: Exception) -> bool:
        resp = getattr(e, "response", None)
        status = getattr(resp, "status_code", None) or getattr(e, "status_code", None)
        return status in (401, 403)


if __name__ == "__main__":
    import asyncio as _asyncio

    async def _smoke() -> None:
        # A finishing node must fail ONLY its own pending callbacks, never a
        # concurrently-running node's — the flat sweep used to abort every
        # node's callbacks (spurious failures + dropped results).
        client = WorkerClient(server="wss://x", token="t", worker_id="w")

        # Security: the token rides in the X-Worker-Token header, never the URL
        # (so it can't be captured by access logs / proxies).
        _url = client._ws_url()
        assert "token=" not in _url, f"token must not be in the WS URL: {_url}"
        assert "worker_id=w" in _url, _url

        loop = _asyncio.get_running_loop()
        fa = loop.create_future()
        fb = loop.create_future()
        client._pending_callbacks["ra"] = ("A", fa)
        client._pending_callbacks["rb"] = ("B", fb)

        client._fail_pending_callbacks("A")
        assert fa.done() and fa.result()["ok"] is False, "A's callback must fail"
        assert "ra" not in client._pending_callbacks, "A's callback must drop"
        assert not fb.done(), "B's in-flight callback must NOT be touched"
        assert client._pending_callbacks.get("rb") == ("B", fb), "B's callback must remain"

        client._fail_pending_callbacks("B")
        assert fb.done() and "rb" not in client._pending_callbacks

        # A server-initiated 1012 "service restart" (uvicorn going down on a
        # deploy) is a routine reconnect, not an error — classify it as graceful.
        from websockets.exceptions import ConnectionClosedError
        from websockets.frames import Close
        restart = ConnectionClosedError(Close(1012, "service restart"), None, None)
        assert _server_close_code(restart) == 1012, "must read the server's close code"
        assert _server_close_code(restart) in _GRACEFUL_CLOSE_CODES, "1012 must be graceful"
        # A transport-level failure carries no close frame → treated as a real error.
        assert _server_close_code(OSError("connection refused")) is None
        assert _server_close_code(OSError("x")) not in _GRACEFUL_CLOSE_CODES

        print("icefold_runner.client: OK")

    _asyncio.run(_smoke())

"""Reverse WebSocket client for icefold-runner.

Dials out to ``<server>/v1/ws/worker``, authenticates with the shared token
(also the XOR keystream), then serves leaf ``node_exec`` jobs concurrently.
Reconnects with jittered exponential backoff; an auth rejection is fatal.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from typing import Dict, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

import websockets

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


class AuthError(Exception):
    """Server rejected our credentials — fatal, retrying won't help."""


def _log(level: str, msg: str, **kw) -> None:
    {"warn": log_warning, "error": log_error}.get(level, log_info)("worker", msg, **kw)


class WorkerClient:
    def __init__(
        self,
        *,
        server: str,
        token: str,
        worker_id: str,
        http_base: Optional[str] = None,
    ) -> None:
        self.server = server
        self.token = token
        self.worker_id = worker_id
        self.xor_key = token.encode("utf-8") if token else b""
        self.http_base = (http_base or self._derive_http_base(server)).rstrip("/")
        self.runner = NodeRunner(self.http_base, token, _log)
        self._tasks: Dict[str, asyncio.Task] = {}
        # Bundle-host callback bookkeeping: bundle code reaches back into the
        # server via ``ctx.progress(...)`` / ``ctx.llm.text(...)``; those land
        # here as outbound ``node_callback`` frames keyed by ``req_id`` and
        # we await the server's matching ``node_callback_result`` to resolve
        # the bundle's future.
        self._pending_callbacks: Dict[str, "asyncio.Future[dict]"] = {}
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
        # No user_id: the token encodes + signs it; the server derives the
        # identity from the token so this runner can't claim another account.
        query = urlencode({
            "token": self.token,
            "worker_id": self.worker_id,
        })
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
        _log("info", "dialing", url=self._redact(url))
        try:
            ws = await websockets.connect(
                url, max_size=_MAX_FRAME, open_timeout=15,
                ping_interval=_KEEPALIVE_S, ping_timeout=_KEEPALIVE_S,
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
            fut = self._pending_callbacks.pop(req_id, None)
            if fut is not None and not fut.done():
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
            _log("error", f"node failed {node_type}: {e}", call_id=call_id)
            await self._safe_send(ws, {
                "type": WKR_NODE_DONE, "call_id": call_id,
                "output": None, "err": str(e), "killed": False,
            })
        finally:
            self._tasks.pop(call_id, None)
            # Fail any still-pending callbacks (e.g. the bundle was cancelled
            # mid-LLM-call) so the bundle's awaiter doesn't hang on shutdown.
            for req_id, fut in list(self._pending_callbacks.items()):
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
            self._pending_callbacks[req_id] = fut
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

    @staticmethod
    def _redact(url: str) -> str:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "token=<redacted>", ""))

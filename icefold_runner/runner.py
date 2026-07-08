"""Run one leaf node-exec job on this machine.

Each ``node_exec`` frame is a single, already-sliced variant (the server did
all variant planning) and carries a **bundle hash** — the server has already
rendered the node into a self-contained ``.py``. The runner ships no node
implementations of its own and never compiles user source.

Per call:

  1. fetch (cache-aware) ``/v1/bundles/<hash>`` into ``runner_work_dir/bundles/``
  2. exec the bundle in a fresh module namespace — it self-declares
     ``__icefold_python_deps__`` / ``__icefold_binary_deps__`` plus the
     ``async def __icefold_run__(inputs, ctx_dict) -> Any`` entry point
  3. pre-flight the declared deps (``shutil.which`` + ``import_module``);
     surface ``MissingDependencyError`` so the client wraps a structured
     ``missing_dep`` reply instead of ``node_done``
  4. download ``/files/`` & ``/tmp/`` input refs to a staging dir and
     rewrite them to local paths
  5. await ``__icefold_run__(local_inputs, ctx_dict)``
  6. upload product files back to the server and rewrite the output to the
     server-canonical paths it hands back

Output that isn't a file (text, numbers, None) passes through untouched.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import os
import shutil
import sys
import tempfile
import time
from types import ModuleType
from typing import Any, Dict, List, Tuple

import httpx

# Bundled node SDK (importable via the CLI's _sdk sys.path entry). DATA_DIR
# reflects this runner's --work-dir because the CLI sets ICEFOLD_PROJECT_ROOT
# before these imports, so executors write products under our download dir.
from icefold.config import DATA_DIR
from icefold.exceptions import MissingDependencyError
from icefold.wire import OUTPUT_UPLOAD_PATH, binary_install_hint

_STAGED_DIR = os.path.join(DATA_DIR, "staged")
_BUNDLES_DIR = os.path.join(DATA_DIR, "bundles")


def _is_server_ref(value: Any) -> bool:
    # Server file-plane mounts the worker GETs its inputs from: ``/files`` (the
    # persistent Library store) and ``/tmp`` (ephemeral scratch). A ``/tmp/``
    # ref is a SERVER URL path to fetch, never a runner-local absolute path.
    return isinstance(value, str) and (
        value.startswith("/files/") or value.startswith("/tmp/")
    )


def _ext_from_ref(ref: str) -> str:
    ext = os.path.splitext(ref.split("?", 1)[0])[1].lower()
    if not ext or len(ext) > 12 or not ext[1:].isalnum():
        return ""
    return ext


class NodeRunner:
    """Stateless per-worker runner; one instance shared across jobs."""

    def __init__(
        self, http_base: str, token: str, log, *, staged_retention_s: float = 7 * 86400,
    ) -> None:
        self._http_base = http_base.rstrip("/")
        self._token = token
        self._log = log
        # Staged inputs are reaped by AGE (``_sweep_staged``), not per-run: a
        # per-run rmtree could delete a file a still-running subprocess needs.
        self._staged_retention_s = staged_retention_s
        # Cache of bundle modules keyed by bundle hash. A bundle is a
        # self-contained .py; once exec'd we keep the module around for the
        # lifetime of this runner process.
        self._bundles: Dict[str, ModuleType] = {}
        # Per-hash locks so concurrent first-time jobs for the same bundle
        # fetch+import it once instead of each racing through the cache miss
        # (last-writer-wins duplicate work). Distinct hashes take distinct
        # locks, so they never serialize against each other; get-or-create is
        # await-free, so it's race-safe under the single-threaded event loop.
        # Bounded by the bundle set, same as ``_bundles`` above.
        self._bundle_locks: Dict[str, asyncio.Lock] = {}
        os.makedirs(_STAGED_DIR, exist_ok=True)
        os.makedirs(_BUNDLES_DIR, exist_ok=True)
        self._sweep_staged()

    def _sweep_staged(self) -> None:
        """Delete staged dirs older than the retention window.

        Reaping by age — rather than an end-of-run ``rmtree`` — keeps a run from
        ever deleting a staged file that another run, or a subprocess that
        outlived its awaiting task (a cancelled/timed-out ffmpeg or stable-ts
        keeps running in its worker thread), is still reading. Safe as long as
        the window exceeds the node timeout, so an in-flight dir (mtime≈now) is
        never swept out from under its own run.
        """
        try:
            names = os.listdir(_STAGED_DIR)
        except OSError:
            return
        cutoff = time.time() - self._staged_retention_s
        for name in names:
            path = os.path.join(_STAGED_DIR, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    shutil.rmtree(path, ignore_errors=True)
            except OSError:
                continue

    async def run(self, msg: dict, *, send_callback=None) -> Any:
        """Execute one ``node_exec`` frame against a server-rendered bundle.

        ``send_callback(kind, payload) -> awaitable`` (optional) is the
        host-injected seam the bundle uses to reach back into the server for
        capabilities the runner can't fulfil locally — ``progress`` (session
        notifications) and ``llm.*`` (the server owns the provider keys and
        accounting). The runner client wires this so the same callable
        correlates replies via ``req_id``. ``None`` means no host is wired
        (e.g. self-check), and the bundle's callback methods raise instead of
        silently no-op'ing.
        """
        bundle_hash = msg.get("bundle_hash") or ""
        if not bundle_hash:
            node_type = msg.get("node_type") or msg.get("node_id", "")
            raise RuntimeError(
                f"node_exec for {node_type!r} arrived without bundle_hash; "
                "the server must render a bundle via codegen before dispatch"
            )
        # ``or`` (not the .get default) so an explicit ``timeout_ms: null`` / 0
        # from any caller of this public framework falls back to the default
        # instead of raising ``TypeError`` on ``None / 1000``.
        timeout = max(1.0, (msg.get("timeout_ms") or 1800_000) / 1000.0)

        # Per-call staging dir for downloaded inputs. NOT removed at end-of-run:
        # a per-run ``rmtree`` could pull a staged file out from under a
        # subprocess that outlived the awaiting task (a cancelled/timed-out
        # ffmpeg or stable-ts keeps running in its thread), which stranded
        # concurrent ComposeVideo variants with "No such file". Reap by age
        # instead — swept here before staging, so a fresh dir (mtime≈now) is
        # never caught mid-run, while old dirs from finished runs still get
        # collected before the disk fills (ENOSPC). Off the event loop: the
        # listdir/getmtime/rmtree are blocking and a large reap backlog would
        # otherwise stall every concurrent node's awaits + the WS keepalive.
        await asyncio.to_thread(self._sweep_staged)
        stage_dir = os.path.join(_STAGED_DIR, os.urandom(8).hex())
        os.makedirs(stage_dir, exist_ok=True)
        async with httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as http:
            local_inputs = await self._download_inputs(http, msg.get("inputs") or {}, stage_dir)
            output = await asyncio.wait_for(
                self._run_bundle(http, bundle_hash, msg, local_inputs, send_callback),
                timeout=timeout,
            )
            return await self._upload_outputs(http, output, msg.get("session_id", ""))

    # ── bundle path ──

    async def _run_bundle(
        self,
        http: httpx.AsyncClient,
        bundle_hash: str,
        msg: dict,
        local_inputs: Any,
        send_callback,
    ) -> Any:
        """Fetch + pre-flight + exec a server-rendered self-contained bundle."""
        mod = self._bundles.get(bundle_hash)
        if mod is None:
            mod = await self._ensure_bundle(http, bundle_hash, msg)

        # Pre-flight declared deps (binary first, then python). Raise a typed
        # exception so the client wraps a ``missing_dep`` reply instead of
        # ``node_done``.
        self._preflight_deps(
            tuple(getattr(mod, "__icefold_binary_deps__", ()) or ()),
            tuple(getattr(mod, "__icefold_python_deps__", ()) or ()),
        )

        ctx_dict = {
            "node_id": msg.get("node_id", msg.get("node_type", "")),
            "node_config": msg.get("node_config") or {},
            "user_id": msg.get("user_id", ""),
            "session_id": msg.get("session_id") or None,
            "space_name": msg.get("space_name") or None,
            "variant": msg.get("variant") or {},
            # Dimension defs (id / values / tags) travel in the frame so the
            # bundle's NodeContext can answer variant_has_tag / resolve_by_tag
            # the same as an in-process run.
            "dims": msg.get("dims") or [],
            "raw_inputs": local_inputs if isinstance(local_inputs, dict) else {},
            "provider": msg.get("provider") or {},
            "model": msg.get("model", ""),
        }
        # Bundle-host callback seam: bundles call this via the embedded
        # NodeContext's ``progress`` / ``llm.text`` methods. The runner
        # client wires ``send_callback(kind, payload)`` so it correlates
        # the reply via ``req_id`` and resolves the bundle's awaiter.
        if send_callback is not None:
            ctx_dict["_send_callback"] = send_callback

        entry = getattr(mod, "__icefold_run__", None)
        if entry is None:
            raise RuntimeError(
                f"bundle {bundle_hash[:8]} is missing __icefold_run__ entry point"
            )
        return await entry(local_inputs if isinstance(local_inputs, dict) else {}, ctx_dict)

    async def _ensure_bundle(
        self, http: httpx.AsyncClient, bundle_hash: str, msg: dict,
    ) -> ModuleType:
        """Fetch+import a bundle exactly once across concurrent jobs.

        A per-hash lock collapses a thundering herd of first-time jobs for the
        same bundle into a single fetch+import: without it, every job that
        slipped past the cache miss before the winner repopulated the cache
        would re-fetch and re-exec the module. The double-check inside the lock
        hands the now-cached module to the jobs that queued behind the winner.
        Distinct hashes take distinct locks, so they never wait on each other.
        """
        lock = self._bundle_locks.setdefault(bundle_hash, asyncio.Lock())
        async with lock:
            mod = self._bundles.get(bundle_hash)
            if mod is None:
                bundle_path = await self._fetch_bundle(
                    http, bundle_hash, msg.get("bundle_url") or "",
                )
                mod = self._import_bundle(bundle_hash, bundle_path)
                self._bundles[bundle_hash] = mod
            return mod

    async def _fetch_bundle(
        self, http: httpx.AsyncClient, bundle_hash: str, bundle_url: str,
    ) -> str:
        """Cache-aware bundle fetch. Returns the on-disk path."""
        path = os.path.join(_BUNDLES_DIR, f"{bundle_hash}.py")
        if os.path.isfile(path):
            return path
        url = bundle_url or f"{self._http_base}/v1/bundles/{bundle_hash}"
        self._log("info", f"pulling bundle {bundle_hash[:8]}")
        headers = {"X-Worker-Token": self._token} if self._token else {}
        async with http.stream("GET", url, headers=headers) as resp:
            resp.raise_for_status()
            # Unique temp file per writer. A shared, deterministic ``<hash>.py.part``
            # races whenever two runner processes share this cache dir (a second
            # runner instance, or an old + new container briefly overlapping across
            # a restart): both open the same ``.part``, the first ``os.replace``
            # moves it to ``<hash>.py``, and the second then hits ENOENT renaming a
            # part file that's already gone. ``mkstemp`` gives each writer its own
            # file; ``os.replace`` still publishes atomically, so the last writer
            # wins with byte-identical content and neither sees ENOENT.
            fd, tmp = tempfile.mkstemp(
                dir=_BUNDLES_DIR, prefix=f"{bundle_hash}.", suffix=".py.part"
            )
            try:
                with os.fdopen(fd, "wb") as fh:
                    async for chunk in resp.aiter_bytes(64 * 1024):
                        fh.write(chunk)
                os.replace(tmp, path)
            except BaseException:
                # Don't leak the unique temp if the download or rename fails.
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        # Sanity: re-hash + compare so a corrupted download can't silently exec.
        with open(path, "rb") as fh:
            got = hashlib.sha256(fh.read()).hexdigest()
        if got != bundle_hash:
            os.unlink(path)
            raise RuntimeError(
                f"bundle hash mismatch: expected {bundle_hash[:8]}, got {got[:8]}"
            )
        return path

    @staticmethod
    def _import_bundle(bundle_hash: str, path: str) -> ModuleType:
        """exec the bundle in a fresh module namespace. No sys.modules pollution."""
        mod_name = f"_icefold_bundle_{bundle_hash[:16]}"
        spec = importlib.util.spec_from_file_location(mod_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to create import spec for bundle {bundle_hash[:8]}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception:
            sys.modules.pop(mod_name, None)
            raise
        return mod

    def _preflight_deps(
        self, binary_deps: Tuple[str, ...], python_deps: Tuple[str, ...],
    ) -> None:
        """Surface a structured ``MissingDependencyError`` when any dep is absent."""
        missing_bin: List[str] = [b for b in binary_deps if b and shutil.which(b) is None]
        missing_py: List[str] = []
        for pkg in python_deps:
            if not pkg:
                continue
            try:
                __import__(pkg.split(".")[0])
            except ImportError:
                missing_py.append(pkg)
        if not (missing_bin or missing_py):
            return
        # Build a platform-aware install hint covering both categories.
        plat = sys.platform if sys.platform in ("linux", "darwin", "win32") else "linux"
        lines: List[str] = []
        for b in missing_bin:
            lines.append(f"  · {b} (binary) → {binary_install_hint(b, plat)}")
        for p in missing_py:
            lines.append(f"  · {p} (python) → pip install {p}")
        hint = "Install the following on this runner host:\n" + "\n".join(lines)
        raise MissingDependencyError(
            missing_binaries=tuple(missing_bin),
            missing_python=tuple(missing_py),
            install_hint=hint,
        )

    # ── input staging (download) ──

    async def _download_inputs(self, http: httpx.AsyncClient, inputs: Any, stage_dir: str) -> Any:
        if isinstance(inputs, str):
            if _is_server_ref(inputs):
                return await self._download_one(http, inputs, stage_dir)
            return inputs
        if isinstance(inputs, dict):
            return {k: await self._download_inputs(http, v, stage_dir) for k, v in inputs.items()}
        if isinstance(inputs, (list, tuple)):
            return [await self._download_inputs(http, v, stage_dir) for v in inputs]
        return inputs

    async def _download_one(self, http: httpx.AsyncClient, ref: str, stage_dir: str) -> str:
        url = self._http_base + ref
        dest = os.path.join(stage_dir, f"{os.urandom(8).hex()}{_ext_from_ref(ref)}")
        self._log("info", f"pulling input {ref}")
        async with http.stream("GET", url) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                async for chunk in resp.aiter_bytes(1024 * 1024):
                    fh.write(chunk)
        return dest

    # ── output staging (upload) ──

    async def _upload_outputs(self, http: httpx.AsyncClient, output: Any, session_id: str) -> Any:
        if isinstance(output, str):
            # Containment check with a separator boundary — a bare ``startswith``
            # would treat a sibling like ``<DATA_DIR>-tmp/x`` as inside DATA_DIR.
            abs_data = os.path.abspath(DATA_DIR)
            abs_out = os.path.abspath(output) if output else ""
            within = abs_out == abs_data or abs_out.startswith(abs_data + os.sep)
            if output and within and os.path.isfile(output):
                return await self._upload_one(http, output, session_id)
            return output
        if isinstance(output, dict):
            return {k: await self._upload_outputs(http, v, session_id) for k, v in output.items()}
        if isinstance(output, (list, tuple)):
            return [await self._upload_outputs(http, v, session_id) for v in output]
        return output

    async def _upload_one(self, http: httpx.AsyncClient, path: str, session_id: str) -> str:
        url = self._http_base + OUTPUT_UPLOAD_PATH
        self._log("info", f"pushing product {os.path.basename(path)}")
        headers = {"X-Worker-Token": self._token} if self._token else {}
        with open(path, "rb") as fh:
            resp = await http.post(
                url,
                headers=headers,
                data={"session_id": session_id or ""},
                files={"file": (os.path.basename(path), fh, "application/octet-stream")},
            )
        resp.raise_for_status()
        server_path = resp.json().get("path")
        if not server_path:
            raise RuntimeError("server did not return a stored path for output")
        return server_path


if __name__ == "__main__":
    import asyncio as _asyncio
    import tempfile

    async def _smoke() -> None:
        # Staged dirs are reaped by AGE, not per-run: a fresh dir must survive its
        # own run (an end-of-run rmtree once yanked staged files out from under a
        # still-running subprocess); only dirs older than the window are swept.
        globals()["_STAGED_DIR"] = tempfile.mkdtemp()
        runner = NodeRunner("http://x", "tok", lambda *a, **k: None, staged_retention_s=3600)
        captured: dict = {}

        async def _fake_download(http, inputs, stage_dir):
            captured["stage_dir"] = stage_dir
            assert os.path.isdir(stage_dir)
            with open(os.path.join(stage_dir, "in.bin"), "wb") as fh:
                fh.write(b"x")
            return inputs

        async def _fake_run_bundle(http, bundle_hash, msg, local_inputs, send_callback):
            assert os.path.isdir(captured["stage_dir"]), "stage dir must live during run"
            return "out"

        async def _fake_upload(http, output, session_id):
            return output

        runner._download_inputs = _fake_download   # type: ignore[assignment]
        runner._run_bundle = _fake_run_bundle      # type: ignore[assignment]
        runner._upload_outputs = _fake_upload      # type: ignore[assignment]

        out = await runner.run({"bundle_hash": "abc", "inputs": {"a": "/files/x"}})
        assert out == "out"
        # NOT removed at end-of-run — the fresh dir (mtime≈now) survives.
        assert os.path.isdir(captured["stage_dir"]), "fresh stage dir must survive its run"

        # A dir older than the retention window is reaped on the next run's sweep,
        # while that run's own fresh dir is kept.
        stale = os.path.join(_STAGED_DIR, "stale")
        os.makedirs(stale, exist_ok=True)
        os.utime(stale, (time.time() - 7200, time.time() - 7200))  # 2h old, window 1h
        out2 = await runner.run({"bundle_hash": "abc", "inputs": {}})
        assert out2 == "out"
        assert not os.path.isdir(stale), "stale stage dir must be reaped by age"
        assert os.path.isdir(captured["stage_dir"]), "the just-run dir must survive the sweep"

        # A bundle error still propagates (no swallowing).
        async def _boom(http, bundle_hash, msg, local_inputs, send_callback):
            raise RuntimeError("boom")

        runner._run_bundle = _boom  # type: ignore[assignment]
        try:
            await runner.run({"bundle_hash": "abc", "inputs": {}})
        except RuntimeError:
            pass
        else:
            raise AssertionError("bundle error must propagate")

        # Concurrent first-time jobs for the SAME bundle hash must fetch+import
        # it once, not once-per-job: the per-hash lock collapses the herd. The
        # sleep(0) forces all three to reach the lock before the winner caches.
        deduped = NodeRunner("http://x", "tok", lambda *a, **k: None)
        calls = {"fetch": 0, "import": 0}

        async def _counting_fetch(http, bundle_hash, bundle_url):
            calls["fetch"] += 1
            await _asyncio.sleep(0)
            return "/bundles/x.py"

        def _counting_import(bundle_hash, path):
            calls["import"] += 1
            return ModuleType(f"m_{bundle_hash}")

        deduped._fetch_bundle = _counting_fetch    # type: ignore[assignment]
        deduped._import_bundle = _counting_import  # type: ignore[assignment]

        mods = await _asyncio.gather(
            *(deduped._ensure_bundle(None, "samehash", {}) for _ in range(3))
        )
        assert calls["fetch"] == 1, f"one fetch for concurrent jobs, got {calls['fetch']}"
        assert calls["import"] == 1, f"one import for concurrent jobs, got {calls['import']}"
        assert all(m is mods[0] for m in mods), "all jobs must share the one module"

        # Distinct hashes must NOT serialize on one lock: each gets its own.
        d2 = NodeRunner("http://x", "tok", lambda *a, **k: None)
        d2._fetch_bundle = _counting_fetch    # type: ignore[assignment]
        d2._import_bundle = _counting_import  # type: ignore[assignment]
        calls["fetch"] = calls["import"] = 0
        await _asyncio.gather(
            d2._ensure_bundle(None, "h1", {}), d2._ensure_bundle(None, "h2", {})
        )
        assert calls["fetch"] == 2 and calls["import"] == 2, "distinct hashes load independently"
        assert len(d2._bundle_locks) == 2, "one lock per distinct hash"

        # Cross-process fetch race: when two runner processes share the bundles
        # cache dir, a fixed ``<hash>.py.part`` name made the loser hit ENOENT on
        # os.replace (the winner had already moved the part file away). Drive
        # concurrent _fetch_bundle calls directly (bypassing the per-hash lock,
        # as separate processes would) and assert all succeed with a valid,
        # hash-matching file and no leaked temp files.
        class _FakeResp:
            def __init__(self, data: bytes) -> None:
                self._data = data

            def raise_for_status(self) -> None:
                pass

            async def aiter_bytes(self, n: int):
                for i in range(0, len(self._data), n):
                    await _asyncio.sleep(0)  # interleave the concurrent writers
                    yield self._data[i : i + n]

        class _FakeStream:
            def __init__(self, data: bytes) -> None:
                self._data = data

            async def __aenter__(self):
                await _asyncio.sleep(0)
                return _FakeResp(self._data)

            async def __aexit__(self, *a):
                return False

        class _FakeHttp:
            def __init__(self, data: bytes) -> None:
                self._data = data

            def stream(self, method, url, headers=None):
                return _FakeStream(self._data)

        globals()["_BUNDLES_DIR"] = tempfile.mkdtemp()
        payload = b"print('bundle')\n" * 5000  # multi-chunk so writers interleave
        real_hash = hashlib.sha256(payload).hexdigest()
        fetcher = NodeRunner("http://x", "tok", lambda *a, **k: None)
        paths = await _asyncio.gather(
            *(fetcher._fetch_bundle(_FakeHttp(payload), real_hash, "") for _ in range(3))
        )
        final = os.path.join(_BUNDLES_DIR, f"{real_hash}.py")
        assert all(p == final for p in paths), paths
        with open(final, "rb") as fh:
            assert hashlib.sha256(fh.read()).hexdigest() == real_hash, "cached bundle corrupt"
        leftovers = [f for f in os.listdir(_BUNDLES_DIR) if f.endswith(".part")]
        assert not leftovers, f"temp files leaked: {leftovers}"

        print("icefold_runner.runner: OK")

    _asyncio.run(_smoke())

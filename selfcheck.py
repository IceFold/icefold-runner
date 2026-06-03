"""No-network self-check for icefold-runner.

Proves, with zero network, that:
  * the installed ``icefold`` (slim runner-side shell) imports;
  * ``icefold_runner`` imports and the CLI is token-only (built-in server);
  * the runner can load + execute a server-rendered bundle through its real
    code path — the ``async def __icefold_run__(inputs, ctx_dict)`` ABI.

Post-strip the runner no longer compiles node source; the server renders each
node into a self-contained bundle and the runner imports it on demand. This
check drives ``NodeRunner.run()`` against a pre-cached bundle (so the fetch is
a local cache hit) with plain string I/O (so input download / output upload
pass through) — exercising the real loader without touching the network.

Run:  python selfcheck.py   (after `pip install -e .`, which pulls in icefold-sdk)
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile


def main() -> int:
    # Must precede any icefold/icefold_runner import so DATA_DIR (hence the
    # bundle cache dir) resolves under this throwaway work dir.
    work = tempfile.mkdtemp(prefix="icefold-runner-selfcheck-")
    os.environ["ICEFOLD_PROJECT_ROOT"] = work

    # 1) SDK slim shell imports (the post-strip surface).
    import icefold  # noqa: F401
    from icefold import get_file_id, run_blocking, write_text, log_info  # noqa: F401

    fid = get_file_id()
    assert fid and len(fid) == 36 and fid.count("-") == 4, f"bad file id: {fid!r}"
    print("icefold imports OK")

    # 2) Runner imports + token-only CLI (no --server / --user-id).
    from icefold_runner.client import WorkerClient  # noqa: F401
    from icefold_runner.runner import NodeRunner, _BUNDLES_DIR
    from icefold_runner.__main__ import DEFAULT_SERVER, _parse_args

    assert DEFAULT_SERVER.startswith("wss://"), DEFAULT_SERVER
    assert _parse_args(["--token", "ifr_demo"]).token == "ifr_demo"
    try:
        _parse_args([])  # --token is required
    except SystemExit:
        pass
    else:
        raise AssertionError("CLI must require --token")
    print("icefold_runner imports + token-only CLI OK")

    # 3) Bundle ABI: a minimal self-contained bundle, pre-cached, run through
    #    the real NodeRunner path (cache hit + plain I/O ⇒ no network).
    bundle_src = (
        "async def __icefold_run__(inputs, ctx_dict):\n"
        "    return (inputs.get('t') or '').upper()\n"
    )
    bundle_hash = hashlib.sha256(bundle_src.encode("utf-8")).hexdigest()
    os.makedirs(_BUNDLES_DIR, exist_ok=True)
    with open(os.path.join(_BUNDLES_DIR, f"{bundle_hash}.py"), "w", encoding="utf-8") as fh:
        fh.write(bundle_src)

    runner = NodeRunner(http_base="http://localhost:0", token="ifr_demo", log=lambda *a, **k: None)
    out = asyncio.run(
        runner.run(
            {
                "bundle_hash": bundle_hash,
                "node_id": "n1",
                "node_type": "Demo",
                "node_config": {},
                "inputs": {"t": "hello"},
            }
        )
    )
    assert out == "HELLO", f"bundle run returned {out!r}"
    print("bundle load + run OK -> HELLO")

    print("icefold-runner selfcheck: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

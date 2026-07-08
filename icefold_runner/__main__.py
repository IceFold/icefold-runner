"""CLI entrypoint:  icefold-runner --token <token>

Run IceFold nodes on this machine. The runner reverse-connects to IceFold and
serves the account the token belongs to — the token (generated in the IceFold
app, Nodes ▸ Connect a runner) encodes + signs your user id, so there's no
server URL or user id to pass.

Bootstrap order matters: we point ``ICEFOLD_PROJECT_ROOT`` at the runner's
``--work-dir`` *before* importing ``icefold``, so the SDK's ``DATA_DIR``
(hence where ffmpeg writes products) resolves under this runner's own dir.
``icefold`` itself is an installed dependency (``pip install icefold-sdk``).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket

# Built-in server. Self-hosters / dev can override via the ICEFOLD_RUNNER_SERVER
# env var (intentionally not a CLI flag — the normal user never sets it).
DEFAULT_SERVER = "wss://api.icefold.com"


_DEFAULT_ROTATION = "7d"


def _parse_duration(text: str, *, default: float) -> float:
    """Parse ``30d`` / ``12h`` / ``90m`` / ``3600s`` (or a bare seconds number)
    into seconds; fall back to ``default`` on anything unparseable."""
    text = (text or "").strip().lower()
    if not text:
        return default
    unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(text[-1])
    try:
        return max(0.0, float(text[:-1]) * unit if unit is not None else float(text))
    except ValueError:
        return default


def _parse_args(argv):
    p = argparse.ArgumentParser(
        prog="icefold-runner",
        description="Run IceFold nodes on this machine. "
                    "Get a token from the IceFold app (Nodes ▸ Connect a runner).",
    )
    p.add_argument("--token", default=os.environ.get("ICEFOLD_RUNNER_TOKEN", ""),
                   help="Runner token from the IceFold app. env: ICEFOLD_RUNNER_TOKEN")
    p.add_argument("--runner-id", default=os.environ.get("ICEFOLD_RUNNER_ID", "") or socket.gethostname(),
                   help="Stable id for this runner (default: hostname). env: ICEFOLD_RUNNER_ID")
    p.add_argument("--work-dir",
                   default=os.environ.get("ICEFOLD_RUNNER_DIR", "") or os.path.abspath("./icefold-runner-data"),
                   help="Scratch dir for staged inputs + ffmpeg products. env: ICEFOLD_RUNNER_DIR")
    p.add_argument("--rotation",
                   default=os.environ.get("ICEFOLD_RUNNER_STAGED_ROTATION", "") or _DEFAULT_ROTATION,
                   help="How long to keep staged input scratch before reaping it by "
                        "age (e.g. 30d/12h/90m). Must exceed the longest node run. "
                        f"env: ICEFOLD_RUNNER_STAGED_ROTATION (default: {_DEFAULT_ROTATION})")
    p.add_argument("--concurrency", type=int,
                   default=int(os.environ.get("ICEFOLD_RUNNER_CONCURRENCY", "") or 4),
                   help="Max nodes to execute at once; excess queue. Keep low for "
                        "GPU-bound work (subtitle stable-ts) — 1 avoids VRAM thrashing; "
                        "raise it for CPU-bound built-ins. "
                        "env: ICEFOLD_RUNNER_CONCURRENCY (default: 4)")
    args = p.parse_args(argv)

    if not args.token:
        p.error("missing required argument: --token "
                "(generate one in the IceFold app: Nodes ▸ Connect a runner)")
    return args


def main(argv=None) -> int:
    args = _parse_args(argv)

    # Built-in server; ICEFOLD_RUNNER_SERVER overrides for self-host / dev.
    server = os.environ.get("ICEFOLD_RUNNER_SERVER", "").strip() or DEFAULT_SERVER

    work_dir = os.path.abspath(args.work_dir)
    # ``tmp`` = where a node writes its products (icefold.config.TMP_BASE_DIR);
    # ``staged`` = where fetched input files land before a run.
    os.makedirs(os.path.join(work_dir, "data", "tmp"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "data", "staged"), exist_ok=True)

    # Must precede any icefold import so DATA_DIR resolves under work_dir.
    os.environ["ICEFOLD_PROJECT_ROOT"] = work_dir

    from icefold_runner.client import WorkerClient

    # The staged-reap window MUST exceed the longest node run, else _sweep_staged
    # (which runs at the start of each run, before the new stage dir is created)
    # could delete a concurrently-running sibling's stage dir. Floor it so a
    # mistyped tiny/0 --rotation can't re-enable that "No such file" race.
    retention = _parse_duration(args.rotation, default=7 * 86400)
    min_retention = 3600.0
    if retention < min_retention:
        print(f"icefold-runner: --rotation {args.rotation!r} is below the "
              f"{int(min_retention)}s floor; using {int(min_retention)}s")
        retention = min_retention

    client = WorkerClient(
        server=server,
        token=args.token,
        worker_id=args.runner_id,
        staged_retention_s=retention,
        concurrency=args.concurrency,
    )
    try:
        asyncio.run(client.run_forever())
    except KeyboardInterrupt:
        print("\nicefold-runner stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

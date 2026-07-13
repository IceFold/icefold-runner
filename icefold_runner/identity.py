"""Who this runner is, and how wide its lanes are.

Deliberately free of any ``icefold`` (SDK) import: ``__main__`` reads these to
build its argparse defaults, and it must do that *before* pointing
``ICEFOLD_PROJECT_ROOT`` at the work dir — importing the SDK any earlier would
resolve ``DATA_DIR`` against the wrong root.
"""

from __future__ import annotations

import os
import uuid


# Upper bound on the CPU lane's default width. Cores past this stop buying
# throughput for ffmpeg — which is already internally threaded and takes a good
# chunk of the box for a single encode — and start costing memory and I/O
# contention. A 32-core machine defaulting to 32 concurrent encodes would be a
# pessimisation dressed as a speedup. Raise it explicitly (``--concurrency``) if
# a workload proves otherwise.
MAX_DEFAULT_CPU_LANE = 8


def default_cpu_lane() -> int:
    """CPU-lane width for this machine: cores, capped, never below 1."""
    return max(1, min(MAX_DEFAULT_CPU_LANE, os.cpu_count() or 1))


def new_runner_id() -> str:
    """A fresh id for THIS runner process.

    The server keys its registry on ``(user, runner_id)``, and a second
    connection under a LIVE id evicts the first. The old default was the
    hostname — so two runners on one machine (the obvious way to use a big box)
    presented the same id and kicked each other off forever, each eviction
    failing whatever node the loser had in flight. A per-process id makes them
    peers; the machine they share rides along as ``host`` in the hello, which is
    the label a human actually reads.

    Fresh per process, deliberately NOT persisted: a runner that crashed and
    restarted must not reclaim the id of the phantom connection it is still
    racing — the server can take ~40 s to reap a half-open socket, and inheriting
    its id would evict... itself.
    """
    return uuid.uuid4().hex[:12]


if __name__ == "__main__":
    lane = default_cpu_lane()
    assert 1 <= lane <= MAX_DEFAULT_CPU_LANE, lane

    # The whole point: two processes on one machine must NOT collide. (The old
    # hostname default gave them the same id, and they evicted each other.)
    ids = {new_runner_id() for _ in range(1000)}
    assert len(ids) == 1000, "runner ids must be unique per process"
    assert all(i and i.isalnum() for i in ids)

    print("icefold_runner.identity: OK")

"""icefold-runner — a self-hosted execution runner for IceFold nodes.

Like a GitHub self-hosted CI runner: you start it on your own machine, it
reverse-connects to an IceFold server, receives node-execution jobs, and runs
them locally — pulling input media over HTTP and pushing products back.

The runner is a *generic execution framework*: it ships no node implementations
of its own. The server renders each node into a self-contained ``.py`` bundle;
the runner fetches the bundle on demand, pre-flights its declared dependencies,
and runs it. So upgrading or adding nodes on the server never requires updating
the runner.
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    # Single source of truth: the installed package metadata (pyproject version).
    # Reading it here means the version can never drift from the wheel that's
    # actually running — which a hardcoded literal silently does across releases.
    __version__ = _pkg_version("icefold-runner")
except PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0.0.0+dev"

__all__ = ["__version__", "WorkerClient", "NodeRunner"]


def __getattr__(name):  # lazy so importing the package is cheap
    if name == "WorkerClient":
        from icefold_runner.client import WorkerClient
        return WorkerClient
    if name == "NodeRunner":
        from icefold_runner.runner import NodeRunner
        return NodeRunner
    raise AttributeError(name)

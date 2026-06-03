# icefold-runner

A self-hosted execution runner for [IceFold](https://icefold.com) nodes — like
a GitHub self-hosted CI runner. You start it on your own machine; it
**reverse-connects** to an IceFold server (so it works behind NAT with no
inbound ports, no public IP, no tunnel), receives node-execution jobs, runs them
locally, and streams results back.

It is the place where **your uploaded node code runs** — on your hardware, with
full `subprocess` / `ffmpeg` / GPU / any-dependency access — instead of inside
the server's restricted sandbox.

## How it works

```
   your machine (private, behind NAT)              IceFold server (public)
 ┌──────────────────────────────────┐  reverse WSS  ┌───────────────────────────┐
 │ icefold-runner                    │ ───────────► │ /v1/ws/worker?token         │
 │  • dials out, token auth          │  node_exec ◄─│ routes node runs (per user) │
 │  • reconnect + keepalive          │  node_done ─►│                             │
 │  • bundle runner:                 │              │                             │
 │    GET /v1/bundles/<hash>         │   HTTP pull  │ /upload  /download          │
 │    import bundle + preflight deps │ ◄──────────► │ /v1/workers/output          │
 │    await __icefold_run__          │              │                             │
 └──────────────────────────────────┘   HTTP push  └───────────────────────────┘
```

- **Control plane** rides the reverse WebSocket (`node_exec` / `cancel` →
  `node_status` / `node_done` / `missing_dep`), JSON frames XOR-obfuscated with
  the token (TLS still does the real protection). Each `node_exec` frame only
  carries a `bundle_hash` and a single already-sliced variant — no source.
- **Bulk media + bundles** ride plain HTTP: the runner GETs inputs from the
  server's `/upload` & `/download` and node bundles from `/v1/bundles/<hash>`
  (sha256-addressed, cached locally as `runner_work_dir/bundles/<hash>.py`,
  re-hashed on every download), runs the bundle, POSTs products back to
  `/v1/workers/output` (which returns server-canonical paths).
- **The runner ships no node implementations and never compiles user source.**
  The IceFold server renders every node (your custom ones *and* the platform's
  built-in ones) into a self-contained `.py` bundle, with `python_deps` /
  `binary_deps` declared in the bundle header. The runner imports the bundle,
  pre-flights the deps (sending back a structured `missing_dep` reply with
  platform-aware install hints if anything is absent), and awaits
  `__icefold_run__(inputs, ctx_dict)`. So when the server adds or upgrades
  nodes, **you never have to upgrade the runner.**
- Variant planning / dimension & provider resolution all stay on the server;
  each job is a single already-sliced leaf call.

## Install

Requires **Python ≥ 3.11**, **ffmpeg/ffprobe** on `PATH` (for media nodes), and
whatever third-party packages your custom nodes import.

```bash
pip install icefold-runner          # pulls in icefold-sdk
```

From source:

```bash
git clone <this-repo> icefold-runner
cd icefold-runner
python -m venv .venv && . .venv/bin/activate
pip install -e .
```

## Run

Generate a token in the IceFold app (**Nodes ▸ Connect a runner**), then:

```bash
icefold-runner --token <your-token>
```

That's it — the token (GitHub-CI style) encodes + signs your IceFold user id, so
there's no server URL or user id to pass. The server is built in.

Every flag also reads an env var (see [`.env.example`](.env.example)):

| flag | env | meaning |
|---|---|---|
| `--token` | `ICEFOLD_RUNNER_TOKEN` | runner token from the IceFold app |
| `--runner-id` | `ICEFOLD_RUNNER_ID` | stable id (default: hostname) |
| `--work-dir` | `ICEFOLD_RUNNER_DIR` | scratch for staged inputs + products |

The runner honors standard proxy env vars (`HTTPS_PROXY`, …) for reaching the
server. It reconnects automatically with backoff; an auth rejection is fatal.

> Self-hosting / dev: point the runner at a different server with the
> `ICEFOLD_RUNNER_SERVER` env var (e.g. `ws://127.0.0.1:7000`).

### Run as a service (systemd)

```ini
# /etc/systemd/system/icefold-runner.service
[Unit]
Description=IceFold runner
After=network-online.target

[Service]
EnvironmentFile=/etc/icefold-runner.env
ExecStart=/opt/icefold-runner/.venv/bin/icefold-runner
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Layout

```
icefold_runner/      the runner agent (connection, file staging, bundle exec)
  client.py            reverse-WS client: dial / auth / reconnect / keepalive
  runner.py            fetch /v1/bundles/<hash>, preflight deps, await __icefold_run__
  __main__.py          CLI entrypoint (icefold-runner)
```

The runner imports the bundle on demand; the bundle is **self-contained** and
already inlines whatever it needs (the author's function body, the
`Inputs` / `Output` dataclasses, and a minimal `NodeContext` shim). The only
runtime dependency on `icefold-sdk` is the wire protocol + a small helper kit
(`get_file_id` / `run_blocking` / `write_text`), used by the runner agent
itself, not by node code.

## Security model

- Node code runs **unsandboxed** here — it's your machine, your risk. That's the
  point: code the server sandbox forbids (subprocess/ffmpeg/native deps) runs on
  the runner instead. The runner downloads each bundle from the server and
  executes it; it verifies the bundle's sha256 matches the requested hash, but
  the bundle itself is whatever the server you authenticated to sends. Only
  point a runner at a server you trust.
- The runner only talks to the one server you point it at, authenticated by the
  shared token; it pulls input files and pushes products over HTTP to that host.

## Self-check

A no-network sanity check that `icefold` is importable and the bundle execution
path (fetch + import + run `__icefold_run__`) works against a locally-rendered
bundle:

```bash
python selfcheck.py
```

# Minimal container for the IceFold runner.
#
# Holds only what the runner strictly needs: a Python interpreter, ffmpeg /
# ffprobe (media nodes), CA roots (outbound WSS to the IceFold server), and
# the ``icefold-runner`` package (which pulls ``icefold-sdk`` in as a dep).
# At runtime the container only sees its own scratch volume and the
# outbound network for the reverse WSS to the server — no host source,
# no host data, no backend env.
#
# Build:
#   podman build -t icefold-runner:local - < Containerfile
#   # pin a release: --build-arg ICEFOLD_RUNNER_VERSION=0.1.0
#
# Run (token from the IceFold app ▸ Nodes ▸ Connect a runner):
#   podman run --rm -it --read-only --tmpfs /tmp:rw,size=1g \
#     --cap-drop=ALL --security-opt=no-new-privileges \
#     -v icefold-runner-data:/data \
#     -e ICEFOLD_RUNNER_TOKEN=<token> icefold-runner:local
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Both packages come from PyPI. Leave the version blank for latest; pin via
# --build-arg ICEFOLD_RUNNER_VERSION=<x.y.z> for reproducible images.
ARG ICEFOLD_RUNNER_VERSION=
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir "icefold-runner${ICEFOLD_RUNNER_VERSION:+==$ICEFOLD_RUNNER_VERSION}"

# Rootfs is mounted --read-only at runtime, so silence .pyc writes (would
# otherwise try to land alongside the installed source in /usr/local).
# ICEFOLD_RUNNER_DIR points at /data, which is the named-volume mount.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/data \
    ICEFOLD_RUNNER_DIR=/data

WORKDIR /data
ENTRYPOINT ["icefold-runner"]

#!/usr/bin/env bash
# Phase 3A Part B: fails loudly, naming the exact missing tool, instead
# of letting a later step fail with an unrelated-looking error.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh

log "checking prerequisites..."

for tool in docker kind kubectl node npm; do
  command -v "$tool" >/dev/null 2>&1 || fail "required tool '$tool' not found on PATH"
done

docker info >/dev/null 2>&1 || fail "Docker daemon is not reachable (is Docker Desktop running?)"

KIND_VERSION="$(kind version 2>&1 | awk '{print $2}')"
KUBECTL_VERSION="$(kubectl version --client 2>/dev/null | grep -o 'v[0-9]*\.[0-9]*\.[0-9]*' | head -1 || echo "unknown")"
NODE_VERSION="$(node --version)"
DOCKER_VERSION="$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "unknown")"

log "docker server:  $DOCKER_VERSION"
log "kind:           $KIND_VERSION (expected v0.33.0)"
log "kubectl client: $KUBECTL_VERSION"
log "node (host):    $NODE_VERSION (engines.node requires >=24.0.0 -- see package.json)"

if [[ "$NODE_VERSION" != v24.* ]]; then
  log "WARNING: host Node is $NODE_VERSION, not a v24.x -- the project's own package.json 'engines' requires >=24.0.0."
  log "         The scripts in this directory will still run (they only need a modern Node for tsx), but see"
  log "         CLAUDE.md 'Runtime and dependency strategy' for how this project tracks host-Node-version claims."
fi

log "prerequisites OK"

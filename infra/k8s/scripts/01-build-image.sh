#!/usr/bin/env bash
# Phase 3A Part B: builds the app image and loads it DIRECTLY into the
# kind node's containerd image store -- no registry involved, and
# imagePullPolicy: IfNotPresent in the Deployments means it is never
# re-pulled from anywhere else.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh

log "building $IMAGE_TAG from $REPO_ROOT (context) using infra/app-image/Dockerfile ..."
docker build -f "$REPO_ROOT/infra/app-image/Dockerfile" -t "$IMAGE_TAG" "$REPO_ROOT"

IMAGE_ID="$(docker image inspect "$IMAGE_TAG" --format '{{.Id}}')"
log "built image id: $IMAGE_ID"

require_cluster_exists
log "loading $IMAGE_TAG into kind cluster '$CLUSTER_NAME' ..."
kind load docker-image "$IMAGE_TAG" --name "$CLUSTER_NAME"
log "image loaded"

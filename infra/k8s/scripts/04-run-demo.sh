#!/usr/bin/env bash
# Phase 3A Part B: the bounded smoke/demo scenario. Assumes 02-up.sh,
# 03-wait-ready.sh, and start-port-forwards.sh have already succeeded.
#
# Runs the EXISTING host A->B Git-worker handoff (src/worker-main.ts /
# src/worker/handoff.ts via tests/integration/demo-handoff.ts, UNMODIFIED)
# against the cluster-local MySQL, plus a new smoke test proving the real
# in-cluster publisher/consumer pods carry a command end to end. Captures
# host/pod Node versions, image identities, epoch transition, and
# matching restored Git identities, as required by Part B section 6.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh
require_cluster_exists
# shellcheck disable=SC1090
source "$SECRETS_FILE"

if [ ! -f "$PORT_FORWARD_PID_FILE" ]; then
  fail "no port-forwards running -- run start-port-forwards.sh first."
fi

log "=== confirming the port-forward actually reaches the CLUSTER database, not infra/compose.yaml's dev MySQL ==="
assert_targeting_cluster_mysql 127.0.0.1 "$K8S_MYSQL_HOST_PORT" "$MYSQL_APP_USER" "$MYSQL_APP_PASSWORD" relaylab

log "=== identity evidence ==="
log "host node version: $(node --version)"
log "pod (publisher) node version: $(kctl exec deploy/relaylab-publisher -- node --version)"
log "pod (consumer) node version:  $(kctl exec deploy/relaylab-consumer -- node --version)"
LOCAL_IMAGE_ID="$(docker image inspect "$IMAGE_TAG" --format '{{.Id}}')"
POD_PUBLISHER_IMAGE_ID="$(kctl get pod -l app=relaylab-publisher -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')"
POD_CONSUMER_IMAGE_ID="$(kctl get pod -l app=relaylab-consumer -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')"
log "locally built image id:        $LOCAL_IMAGE_ID"
log "publisher pod's running image: $POD_PUBLISHER_IMAGE_ID"
log "consumer pod's running image:  $POD_CONSUMER_IMAGE_ID"

log "=== step 1: existing host A->B Git-worker handoff against cluster-local MySQL (tests/integration/demo-handoff.ts, UNMODIFIED) ==="
(cd "$REPO_ROOT" && \
  RELAYLAB_MYSQL_HOST=127.0.0.1 RELAYLAB_MYSQL_PORT="$K8S_MYSQL_HOST_PORT" RELAYLAB_MYSQL_DATABASE=relaylab \
  RELAYLAB_MYSQL_USER="$MYSQL_APP_USER" RELAYLAB_MYSQL_PASSWORD="$MYSQL_APP_PASSWORD" \
  RELAYLAB_OTEL_DISABLED=1 \
  npm run demo:handoff)

log "=== step 2: real in-cluster publisher+consumer pods carry a command end to end (tests/integration/demo-k8s-messaging.ts) ==="
(cd "$REPO_ROOT" && \
  RELAYLAB_MYSQL_HOST=127.0.0.1 RELAYLAB_MYSQL_PORT="$K8S_MYSQL_HOST_PORT" RELAYLAB_MYSQL_DATABASE=relaylab \
  RELAYLAB_MYSQL_USER="$MYSQL_APP_USER" RELAYLAB_MYSQL_PASSWORD="$MYSQL_APP_PASSWORD" \
  RELAYLAB_OTEL_DISABLED=1 RELAYLAB_K8S_DEMO_MODE=seed \
  npm run demo:k8s-messaging)

log "=== demo complete: both scenarios PASSED ==="

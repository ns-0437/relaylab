#!/usr/bin/env bash
# Phase 3A Part B: creates (or reuses) the dedicated kind cluster and
# namespace, generates local dev secrets, and applies every manifest.
# Idempotent: safe to re-run. Never touches any cluster other than the
# one explicitly named below, and never touches the user's default
# kubeconfig (~/.kube/config) -- everything goes through the dedicated
# $KUBECONFIG_FILE set in lib/common.sh.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "kind cluster '$CLUSTER_NAME' already exists -- reusing it, not recreating."
else
  log "creating kind cluster '$CLUSTER_NAME' (pinned node image, see kind-config.yaml) ..."
  kind create cluster --name "$CLUSTER_NAME" --config "$K8S_DIR/kind-config.yaml" --kubeconfig "$KUBECONFIG_FILE"
fi

log "applying namespace ..."
kctl apply -f "$K8S_DIR/manifests/00-namespace.yaml" >/dev/null

# Phase 3A.1: never blindly (re-)applies a Secret over the cluster's
# existing one -- see ensure_mysql_secret's own header comment for the
# exact incompatible-credential-rotation risk this closes.
ensure_mysql_secret
# shellcheck disable=SC1090
source "$SECRETS_FILE"

log "pre-loading vendor images (mysql:8.4, nats:2.10-alpine) into the kind node ..."
load_vendor_image "mysql:8.4"
load_vendor_image "nats:2.10-alpine"

log "applying ConfigMap, MySQL, NATS manifests ..."
kctl apply -f "$K8S_DIR/manifests/30-configmap.yaml" >/dev/null
kctl apply -f "$K8S_DIR/manifests/10-mysql.yaml" >/dev/null
kctl apply -f "$K8S_DIR/manifests/20-nats.yaml" >/dev/null

log "building and loading the application image (publisher/consumer manifests are applied by 03-wait-ready.sh, AFTER migration succeeds) ..."
"$K8S_DIR/scripts/01-build-image.sh"

log "infra manifests applied. Run 03-wait-ready.sh next (waits for MySQL/NATS readiness, runs the migration step, then applies and waits for publisher/consumer)."

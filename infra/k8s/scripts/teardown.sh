#!/usr/bin/env bash
# Phase 3A Part B: the ONLY script that deletes anything persistent.
# Never called automatically by any other script in this directory.
# Deletes the kind cluster itself -- which is also the only thing that
# removes the MySQL/NATS PV data (kind's default local-path-provisioner
# backs PVs with the node container's own filesystem, so destroying the
# node destroys that storage; see docs/KUBERNETES.md "persistence").
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh

read -r -p "This will DELETE the kind cluster '$CLUSTER_NAME' and ALL persistent data in it (MySQL, NATS JetStream). Type 'yes' to continue: " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
  log "aborted -- nothing was deleted."
  exit 1
fi

if [ -f "$PORT_FORWARD_PID_FILE" ]; then
  log "stopping project-owned port-forwards first ..."
  "$K8S_DIR/scripts/stop-port-forwards.sh"
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "deleting kind cluster '$CLUSTER_NAME' ..."
  kind delete cluster --name "$CLUSTER_NAME" --kubeconfig "$KUBECONFIG_FILE"
else
  log "kind cluster '$CLUSTER_NAME' does not exist -- nothing to delete."
fi

rm -f "$KUBECONFIG_FILE"
log "teardown complete. Local secrets file ($SECRETS_FILE) was LEFT IN PLACE -- delete it by hand if you also want a fresh set of generated credentials next time."

#!/usr/bin/env bash
# Phase 3A Part B: bounded readiness waits, PLUS the explicit, observable
# migration step. Order matters: MySQL/NATS must be Ready, THEN migration
# must succeed, THEN (and only then) are the publisher/consumer
# Deployments applied and waited on -- a failed migration here makes this
# script exit nonzero and NEVER applies the app Deployments, so the setup
# never proceeds as if the application were ready (the one explicit
# requirement this ordering exists to satisfy).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh
require_cluster_exists
# shellcheck disable=SC1090
source "$SECRETS_FILE"

READY_TIMEOUT="180s"

log "waiting for MySQL pod to become Ready (bounded: $READY_TIMEOUT) ..."
wait_ready_or_diagnose "app=relaylab-mysql" "$READY_TIMEOUT"

log "waiting for NATS pod to become Ready (bounded: $READY_TIMEOUT) ..."
wait_ready_or_diagnose "app=relaylab-nats" "$READY_TIMEOUT"

log "running the migration step against the cluster MySQL via a temporary port-forward ..."
MIGRATE_LOCAL_PORT=33061
MIGRATE_PF_RECORD="$K8S_DIR/.pf-migrate.record"
require_port_free "$MIGRATE_LOCAL_PORT" "relaylab migration port-forward"
# --address 127.0.0.1 explicit -- see start-port-forwards.sh's header
# comment for why the default (localhost, tries 127.0.0.1 AND ::1) can
# silently succeed on the wrong address family when 127.0.0.1 is taken.
# po_start (process-ownership.sh) captures real process identity and
# fails loudly (rather than hanging or leaving an orphan) if the
# port-forward does not start cleanly -- see docs/KUBERNETES.md "Safe
# process termination."
MIGRATE_MARKER="svc/relaylab-mysql-lb ${MIGRATE_LOCAL_PORT}:3306"
po_start "$MIGRATE_PF_RECORD" /tmp/relaylab-migrate-port-forward.log /tmp/relaylab-migrate-port-forward.err.log \
  kubectl "$(kctl_args port-forward --address 127.0.0.1 svc/relaylab-mysql-lb "${MIGRATE_LOCAL_PORT}:3306")" \
  "$MIGRATE_MARKER"
# Covers EVERY exit path from here on, including a `fail` call (which is
# just `exit 1`) raised by the readiness loop below -- the temporary
# migration forward is never left running on failure, not just on the
# success path. po_stop verifies identity before terminating; `|| true`
# here because by the time this trap fires the port-forward may already
# have been stopped on the success path below.
trap 'po_stop "$MIGRATE_PF_RECORD" || true' EXIT

# Bounded wait for the forwarded port to actually accept connections
# before handing it to the migration CLI (a cold port-forward can take a
# moment to establish).
for i in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/127.0.0.1/${MIGRATE_LOCAL_PORT}") 2>/dev/null; then exec 3>&-; break; fi
  sleep 0.5
  if [ "$i" -eq 30 ]; then fail "port-forward to relaylab-mysql-lb never became reachable on 127.0.0.1:${MIGRATE_LOCAL_PORT}"; fi
done

assert_targeting_cluster_mysql 127.0.0.1 "$MIGRATE_LOCAL_PORT" "$MYSQL_APP_USER" "$MYSQL_APP_PASSWORD" relaylab

if ! (cd "$REPO_ROOT" && RELAYLAB_MYSQL_HOST=127.0.0.1 RELAYLAB_MYSQL_PORT="$MIGRATE_LOCAL_PORT" RELAYLAB_MYSQL_DATABASE=relaylab \
     RELAYLAB_MYSQL_USER="$MYSQL_APP_USER" RELAYLAB_MYSQL_PASSWORD="$MYSQL_APP_PASSWORD" \
     npx tsx src/db/migrate-cli.ts); then
  fail "migration failed -- refusing to apply/wait for the publisher/consumer Deployments. The cluster is NOT ready."
fi

po_stop "$MIGRATE_PF_RECORD"
trap - EXIT
log "migration succeeded"

log "applying publisher/consumer manifests ..."
kctl apply -f "$K8S_DIR/manifests/40-publisher.yaml" >/dev/null
kctl apply -f "$K8S_DIR/manifests/50-consumer.yaml" >/dev/null

# Stamps the actually-built image's resolved ID onto each pod template --
# see stamp_and_rollout's comment in lib/common.sh for why this is what
# makes a rebuilt image (same floating tag, new content) actually roll
# out, instead of `kubectl apply` silently being a no-op against the
# already-running old pods.
CURRENT_IMAGE_ID="$(docker image inspect "$IMAGE_TAG" --format '{{.Id}}')"
log "stamping resolved image id ($CURRENT_IMAGE_ID) onto publisher/consumer pod templates ..."
stamp_and_rollout relaylab-publisher "$CURRENT_IMAGE_ID"
stamp_and_rollout relaylab-consumer "$CURRENT_IMAGE_ID"

log "waiting for publisher rollout to complete (bounded: $READY_TIMEOUT) ..."
kctl rollout status deployment/relaylab-publisher --timeout="$READY_TIMEOUT" || { kctl describe deployment/relaylab-publisher | tail -n 20; fail "publisher rollout did not complete."; }

log "waiting for consumer rollout to complete (bounded: $READY_TIMEOUT) ..."
kctl rollout status deployment/relaylab-consumer --timeout="$READY_TIMEOUT" || { kctl describe deployment/relaylab-consumer | tail -n 20; fail "consumer rollout did not complete."; }

log "cluster is ready: MySQL + NATS persistent, schema migrated, publisher + consumer Ready."

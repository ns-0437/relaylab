#!/usr/bin/env bash
# Phase 3A Part B: exercises ONE stateless service pod replacement (the
# consumer) and proves: a genuinely new pod UID becomes Ready, durable
# state (task_consumer_state.event_count) survives, processing resumes
# for a NEW command, and no duplicate/extra completion is recorded.
#
# Deliberately port-forwards directly to the OLD POD'S NAME (not the
# Service) for ONE illustrative check, specifically to demonstrate and
# correctly classify the failure this causes when that pod is deleted --
# "Account for port-forward termination when its target pod is replaced.
# Do not confuse a dead host forwarding process with failed application
# recovery." Every OTHER check in this script goes through the resilient
# Service-based port-forward already started by start-port-forwards.sh,
# which is never affected by this pod replacement.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh
require_cluster_exists
# shellcheck disable=SC1090
source "$SECRETS_FILE"

if [ ! -f "$PORT_FORWARD_PID_FILE" ]; then
  fail "no port-forwards running -- run start-port-forwards.sh first."
fi

MYSQL_ENV=(RELAYLAB_MYSQL_HOST=127.0.0.1 "RELAYLAB_MYSQL_PORT=$K8S_MYSQL_HOST_PORT" RELAYLAB_MYSQL_DATABASE=relaylab
           "RELAYLAB_MYSQL_USER=$MYSQL_APP_USER" "RELAYLAB_MYSQL_PASSWORD=$MYSQL_APP_PASSWORD" RELAYLAB_OTEL_DISABLED=1)

OLD_POD="$(kctl get pod -l app=relaylab-consumer -o jsonpath='{.items[0].metadata.name}')"
OLD_UID="$(kctl get pod -l app=relaylab-consumer -o jsonpath='{.items[0].metadata.uid}')"
log "current consumer pod: $OLD_POD (uid=$OLD_UID)"

log "=== illustrative step: a port-forward targeting this POD BY NAME (not the Service) ==="
log "starting pod-direct port-forward localhost:19470 -> pod/$OLD_POD:9470 ..."
kctl port-forward --address 127.0.0.1 "pod/$OLD_POD" 19470:9470 >/tmp/relaylab-pf-pod-direct.log 2>&1 &
POD_PF_PID=$!
sleep 2
if curl -sf "http://127.0.0.1:19470/healthz/ready" >/dev/null 2>&1; then
  log "pod-direct port-forward confirmed working against $OLD_POD"
else
  log "WARNING: pod-direct port-forward did not respond as expected before the kill; continuing anyway"
fi

log "=== seeding one command, waiting for the real in-cluster consumer to process it ==="
SEED_OUT="$(cd "$REPO_ROOT" && env "${MYSQL_ENV[@]}" RELAYLAB_K8S_DEMO_MODE=seed npm run --silent demo:k8s-messaging)"
echo "$SEED_OUT"
TASK_ID="$(echo "$SEED_OUT" | grep '^TASK_ID=' | cut -d= -f2-)"
OWNER_ID="$(echo "$SEED_OUT" | grep '^OWNER_ID=' | cut -d= -f2-)"
EPOCH="$(echo "$SEED_OUT" | grep '^EPOCH=' | cut -d= -f2-)"
[ -n "$TASK_ID" ] || fail "could not parse TASK_ID from demo-k8s-messaging seed output"
log "seeded task=$TASK_ID owner=$OWNER_ID epoch=$EPOCH, event_count=1 confirmed"

log "=== deleting the consumer pod (normal delete, respects terminationGracePeriodSeconds) ==="
kctl delete pod "$OLD_POD"

log "checking the pod-direct port-forward NOW (expected to be broken -- its target pod is gone) ..."
if curl -sf --max-time 3 "http://127.0.0.1:19470/healthz/ready" >/dev/null 2>&1; then
  log "NOTE: the pod-direct port-forward still answered once more (race with the delete) -- not conclusive either way"
else
  log "CONFIRMED (expected): the pod-direct port-forward is now broken, because its target pod ($OLD_POD) no longer exists."
  log "This is a dead HOST FORWARDING PROCESS, not an application failure -- the application's own recovery is verified"
  log "below via the Service-based port-forward (svc/relaylab-mysql-lb), which was never affected by this pod's deletion."
fi
kill_pid_robust "$POD_PF_PID"
log "stopped the pod-direct port-forward process (it would otherwise sit dead forever, pointed at a pod that no longer exists)"

log "=== waiting for the Deployment to produce a NEW, Ready consumer pod with a DIFFERENT uid ==="
NEW_POD=""
for i in $(seq 1 60); do
  CANDIDATE="$(kctl get pod -l app=relaylab-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  CANDIDATE_UID="$(kctl get pod -l app=relaylab-consumer -o jsonpath='{.items[0].metadata.uid}' 2>/dev/null || true)"
  if [ -n "$CANDIDATE_UID" ] && [ "$CANDIDATE_UID" != "$OLD_UID" ]; then
    NEW_POD="$CANDIDATE"
    NEW_UID="$CANDIDATE_UID"
    break
  fi
  sleep 2
done
[ -n "$NEW_POD" ] || fail "no replacement consumer pod with a new uid appeared within 120s"
log "new candidate pod: $NEW_POD (uid=$NEW_UID) -- waiting for it to become Ready ..."
kctl wait --for=condition=Ready "pod/$NEW_POD" --timeout=120s
log "CONFIRMED: new pod uid ($NEW_UID) is DIFFERENT from the old one ($OLD_UID), and is Ready."

log "=== confirming durable state survived the replacement (read-only check, BEFORE applying anything new) ==="
(cd "$REPO_ROOT" && env "${MYSQL_ENV[@]}" RELAYLAB_K8S_DEMO_MODE=check "RELAYLAB_K8S_DEMO_TASK_ID=$TASK_ID" RELAYLAB_K8S_DEMO_EXPECTED_COUNT=1 npm run --silent demo:k8s-messaging)

log "=== confirming processing RESUMES: applying ONE more command for the SAME task, expecting event_count=2 exactly (no duplicate, no loss) ==="
(cd "$REPO_ROOT" && env "${MYSQL_ENV[@]}" RELAYLAB_K8S_DEMO_MODE=continue \
  "RELAYLAB_K8S_DEMO_TASK_ID=$TASK_ID" "RELAYLAB_K8S_DEMO_OWNER_ID=$OWNER_ID" "RELAYLAB_K8S_DEMO_EPOCH=$EPOCH" \
  RELAYLAB_K8S_DEMO_SEQ=1 RELAYLAB_K8S_DEMO_EXPECTED_COUNT=2 \
  npm run --silent demo:k8s-messaging)

log "=== PASS: pod replacement proof complete -- new uid Ready, durable state preserved, processing resumed, exactly one new logical result (no duplicate) ==="

#!/usr/bin/env bash
# Phase 3A Part B/3A.1/3A.2: starts project-owned host port-forwards to
# the MySQL and NATS SERVICES (not pod names) so they survive a pod
# replacement transparently -- Kubernetes Services already solve "the
# pod behind this name may change," which is exactly why every
# port-forward EXCEPT the one deliberately used to demonstrate pod
# replacement (05-pod-replacement-demo.sh) targets a Service here.
# NOTE: in practice a kubectl port-forward (Service- or pod-targeted)
# still resolves to one specific pod at connection time and does not
# follow a LATER replacement of that pod -- see docs/KUBERNETES.md
# "Port-forwards do NOT follow a replaced pod."
#
# Host ports are DELIBERATELY DIFFERENT from infra/compose.yaml's (3306,
# 24222) -- see lib/common.sh's comment on K8S_MYSQL_HOST_PORT/
# K8S_NATS_HOST_PORT. Binds `--address 127.0.0.1` explicitly: the
# default (`--address localhost`) tries BOTH 127.0.0.1 and ::1 and only
# FAILS if NEITHER is free -- which is exactly how an earlier session's
# collision went undetected (compose held 127.0.0.1:3306, so kubectl
# silently fell back to ::1:3306 alone and reported success). Binding
# 127.0.0.1 explicitly makes an actual collision FAIL LOUDLY instead of
# silently degrading to the wrong address family.
#
# Phase 3A.2: process tracking/termination goes through
# infra/k8s/scripts/lib/process-ownership.sh (po_start/po_stop) -- see
# docs/KUBERNETES.md "Safe process termination" for why the OLD
# kill_pid_robust/kill_port_robust mechanism was replaced, not patched.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh
require_cluster_exists

if [ -f "$MYSQL_PF_RECORD" ] || [ -f "$NATS_PF_RECORD" ]; then
  fail "port-forwards already appear to be tracked ($MYSQL_PF_RECORD / $NATS_PF_RECORD). Run stop-port-forwards.sh first."
fi

# Catches an ORPHANED process from an earlier run that the record-file
# check above cannot see (see require_port_free's own comment).
require_port_free "$K8S_MYSQL_HOST_PORT" "relaylab mysql port-forward"
require_port_free "$K8S_NATS_HOST_PORT" "relaylab nats port-forward"

MYSQL_MARKER="svc/relaylab-mysql-lb ${K8S_MYSQL_HOST_PORT}:3306"
po_start "$MYSQL_PF_RECORD" /tmp/relaylab-pf-mysql.log /tmp/relaylab-pf-mysql.err.log \
  kubectl "$(kctl_args port-forward --address 127.0.0.1 svc/relaylab-mysql-lb "${K8S_MYSQL_HOST_PORT}:3306")" \
  "$MYSQL_MARKER"
log "started and tracked port-forward: 127.0.0.1:${K8S_MYSQL_HOST_PORT} -> svc/relaylab-mysql-lb:3306"

NATS_MARKER="svc/relaylab-nats-lb ${K8S_NATS_HOST_PORT}:4222"
po_start "$NATS_PF_RECORD" /tmp/relaylab-pf-nats.log /tmp/relaylab-pf-nats.err.log \
  kubectl "$(kctl_args port-forward --address 127.0.0.1 svc/relaylab-nats-lb "${K8S_NATS_HOST_PORT}:4222")" \
  "$NATS_MARKER"
log "started and tracked port-forward: 127.0.0.1:${K8S_NATS_HOST_PORT} -> svc/relaylab-nats-lb:4222"

for i in $(seq 1 30); do
  mysql_up=0; nats_up=0
  (exec 3<>"/dev/tcp/127.0.0.1/${K8S_MYSQL_HOST_PORT}") 2>/dev/null && { exec 3>&-; mysql_up=1; }
  (exec 3<>"/dev/tcp/127.0.0.1/${K8S_NATS_HOST_PORT}") 2>/dev/null && { exec 3>&-; nats_up=1; }
  if [ "$mysql_up" -eq 1 ] && [ "$nats_up" -eq 1 ]; then
    log "both port-forwards are reachable on 127.0.0.1 (tracked in $MYSQL_PF_RECORD / $NATS_PF_RECORD)"
    exit 0
  fi
  sleep 0.5
done
# Clean up on our own failure path too -- never leave a half-started pair
# running. po_stop verifies identity before terminating, same as a
# normal stop-port-forwards.sh run.
po_stop "$MYSQL_PF_RECORD" || true
po_stop "$NATS_PF_RECORD" || true
fail "port-forwards did not become reachable within 15s -- see /tmp/relaylab-pf-mysql.log and /tmp/relaylab-pf-nats.log"

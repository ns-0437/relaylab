#!/usr/bin/env bash
# Phase 3A Part B/3A.1: starts project-owned host port-forwards to the
# MySQL and NATS SERVICES (not pod names) so they survive a pod
# replacement transparently -- Kubernetes Services already solve "the
# pod behind this name may change," which is exactly why every
# port-forward EXCEPT the one deliberately used to demonstrate pod
# replacement (05-pod-replacement-demo.sh) targets a Service here.
#
# Host ports are DELIBERATELY DIFFERENT from infra/compose.yaml's (3306,
# 24222) -- see lib/common.sh's comment on K8S_MYSQL_HOST_PORT/
# K8S_NATS_HOST_PORT. Phase 3A.1 ALSO binds `--address 127.0.0.1`
# explicitly: `kubectl port-forward`'s default (`--address localhost`)
# tries BOTH 127.0.0.1 and ::1 and only FAILS if NEITHER is free -- which
# is exactly how the prior session's collision went undetected (compose
# held 127.0.0.1:3306, so kubectl silently fell back to ::1:3306 alone
# and reported success). Binding 127.0.0.1 explicitly makes an actual
# collision FAIL LOUDLY instead of silently degrading to the wrong
# address family.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh
require_cluster_exists

if [ -f "$PORT_FORWARD_PID_FILE" ]; then
  fail "port-forwards already appear to be running (found $PORT_FORWARD_PID_FILE). Run stop-port-forwards.sh first."
fi

# Catches an ORPHANED process from an earlier run that this PID file
# check above cannot see (see require_port_free's own comment).
require_port_free "$K8S_MYSQL_HOST_PORT" "relaylab mysql port-forward"
require_port_free "$K8S_NATS_HOST_PORT" "relaylab nats port-forward"

# Phase 3A.1: detects a bind/startup failure directly (the PID exiting
# almost immediately) rather than only inferring success from the port
# eventually answering -- a port that answers could, in principle, be
# answering FOR A DIFFERENT REASON; a dead PID is unambiguous.
start_one_port_forward() {
  local log_file="$1"; shift
  kctl port-forward --address 127.0.0.1 "$@" >"$log_file" 2>&1 &
  local pid=$!
  sleep 0.3
  if ! kill -0 "$pid" 2>/dev/null; then
    log "port-forward process exited immediately -- log follows:"
    cat "$log_file" >&2 || true
    fail "port-forward ($*) failed to start -- see log above ($log_file)."
  fi
  echo "$pid"
}

MYSQL_PF_PID="$(start_one_port_forward /tmp/relaylab-pf-mysql.log svc/relaylab-mysql-lb "${K8S_MYSQL_HOST_PORT}:3306")"
log "started port-forward: 127.0.0.1:${K8S_MYSQL_HOST_PORT} -> svc/relaylab-mysql-lb:3306 (pid $MYSQL_PF_PID)"

NATS_PF_PID="$(start_one_port_forward /tmp/relaylab-pf-nats.log svc/relaylab-nats-lb "${K8S_NATS_HOST_PORT}:4222")"
log "started port-forward: 127.0.0.1:${K8S_NATS_HOST_PORT} -> svc/relaylab-nats-lb:4222 (pid $NATS_PF_PID)"

echo "$MYSQL_PF_PID $NATS_PF_PID" > "$PORT_FORWARD_PID_FILE"

for i in $(seq 1 30); do
  mysql_up=0; nats_up=0
  (exec 3<>"/dev/tcp/127.0.0.1/${K8S_MYSQL_HOST_PORT}") 2>/dev/null && { exec 3>&-; mysql_up=1; }
  (exec 3<>"/dev/tcp/127.0.0.1/${K8S_NATS_HOST_PORT}") 2>/dev/null && { exec 3>&-; nats_up=1; }
  if [ "$mysql_up" -eq 1 ] && [ "$nats_up" -eq 1 ]; then
    log "both port-forwards are reachable on 127.0.0.1 (recorded in $PORT_FORWARD_PID_FILE)"
    exit 0
  fi
  if ! kill -0 "$MYSQL_PF_PID" 2>/dev/null; then fail "mysql port-forward process died while waiting -- see /tmp/relaylab-pf-mysql.log"; fi
  if ! kill -0 "$NATS_PF_PID" 2>/dev/null; then fail "nats port-forward process died while waiting -- see /tmp/relaylab-pf-nats.log"; fi
  sleep 0.5
done
# Clean up on our own failure path too -- never leave a half-started pair running.
kill_pid_robust "$MYSQL_PF_PID"; kill_pid_robust "$NATS_PF_PID"
rm -f "$PORT_FORWARD_PID_FILE"
fail "port-forwards did not become reachable within 15s -- see /tmp/relaylab-pf-mysql.log and /tmp/relaylab-pf-nats.log"

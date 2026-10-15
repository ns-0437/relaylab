#!/usr/bin/env bash
# Phase 3A Part B: kills EXACTLY the PIDs this project's own
# start-port-forwards.sh recorded -- never a blind `pkill kubectl`, which
# could kill an unrelated port-forward the user has running for something
# else entirely.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh

if [ ! -f "$PORT_FORWARD_PID_FILE" ]; then
  log "no $PORT_FORWARD_PID_FILE found -- nothing to stop."
  exit 0
fi

read -r -a PIDS < "$PORT_FORWARD_PID_FILE"
for pid in "${PIDS[@]}"; do
  # kill_pid_robust (lib/common.sh): plain `kill` alone has repeatedly
  # left the underlying kubectl.exe process running as an orphan on this
  # host -- always also tries taskkill, unconditionally.
  kill_pid_robust "$pid"
  log "stopped port-forward process $pid (by pid)"
done
# Phase 3A.1: ALSO kill by the known project ports directly -- see
# kill_port_robust's own comment for why PID-based killing alone was
# observed to miss in practice. Project-owned ports only, never a blind
# process sweep.
kill_port_robust "$K8S_MYSQL_HOST_PORT"
kill_port_robust "$K8S_NATS_HOST_PORT"
log "also force-cleared ports ${K8S_MYSQL_HOST_PORT}/${K8S_NATS_HOST_PORT} directly, in case the tracked PIDs did not match the actual listening process"
rm -f "$PORT_FORWARD_PID_FILE"

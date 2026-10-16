#!/usr/bin/env bash
# Phase 3A Part B/3A.2: stops EXACTLY the processes this project's own
# start-port-forwards.sh started AND whose identity still matches what
# was recorded -- never a blind `pkill kubectl`, and never a port-based
# sweep (see docs/KUBERNETES.md "Safe process termination" for why the
# old kill_port_robust/kill_pid_robust mechanism was replaced). If
# identity cannot be confirmed for either record, that one is reported
# as a failure and LEFT RUNNING -- po_stop never guesses.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source lib/common.sh

status=0
po_stop "$MYSQL_PF_RECORD" || status=1
po_stop "$NATS_PF_RECORD" || status=1

if [ "$status" -ne 0 ]; then
  log "one or more port-forwards could not be safely stopped -- see the messages above and $PO_RECOVERY_LOG."
fi
exit "$status"

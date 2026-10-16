#!/usr/bin/env bash
# Phase 3A.2: replaces Phase 3A.1's kill_pid_robust/kill_port_robust.
#
# The problem with the old mechanism, found on inspection: kill_port_robust
# looked up WHATEVER PID netstat reported listening on a configured port
# and taskkilled it -- a port number proves nothing about ownership (any
# other process, including one the user cares about, could be bound
# there by the time cleanup runs). kill_pid_robust trusted a recorded
# numeric PID the same way -- PIDs are reused by the OS, so a PID alone
# does not establish identity either, and this project's own evidence
# already showed stale PID files pointing at processes that were no
# longer the ones that were started.
#
# This module never kills by port, and never kills by PID alone. It
# captures the ACTUAL Win32 process identity (PID + creation timestamp +
# executable path) the moment a process is started, via
# process-ownership.ps1's `start` action (Start-Process -PassThru is
# the authoritative source of the real PID here -- see that script's
# header comment for why Git Bash's own `$!` was tried first and found
# unreliable: backgrounding a native process from MSYS bash sometimes
# yields an intermediate wrapper's PID instead of the target's, and that
# wrapper can already have exited by the time anything queries it).
# Before ever terminating a recorded process, it re-reads that process's
# CURRENT identity and requires an EXACT match on creation timestamp
# (the one thing that changes if the PID has been reused by something
# else) and confirms the current command line still contains the
# expected marker substring (the port-forward target this record was
# made for). If either check fails, or if termination is issued but not
# confirmed within a bounded wait, the process is LEFT RUNNING, a
# human-actionable recovery file is written (append-only, never
# overwritten), and the function returns failure -- it never silently
# reports success for an action it could not verify.
set -euo pipefail

PROCESS_OWNERSHIP_PS1="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/process-ownership.ps1"
# Shared by every record this directory writes; append-only, kept across
# runs so a human can see the full history of anything this tooling
# could not safely clean up on its own.
PO_RECOVERY_LOG="${K8S_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}/.process-ownership-recovery.log"

_po_log() { echo "[process-ownership] $*"; }
_po_fail() { echo "[process-ownership] ERROR: $*" >&2; return 1; }

# Phase 3A.2, a real hang found and fixed while testing this very
# module: `result="$(powershell.exe ... 2>&1 | tr -d '\r')"` hung
# INDEFINITELY, even though the exact same `powershell.exe ...`
# invocation returned in under a second when its output was NOT piped
# into a bash command substitution. Root cause, confirmed by isolating
# each piece: `Start-Process` inside the .ps1 script launches a
# DETACHED grandchild (the actual tracked process, e.g. `kubectl
# port-forward`, which keeps running long after this script itself
# exits); that grandchild was inheriting the OUTER powershell.exe
# invocation's own stdout/stderr pipe write-end on this host, so the
# pipe's read end (bash's `$(...)`) never saw EOF -- it kept waiting for
# ALL holders of the pipe to close it, including a process that was
# NEVER going to exit on its own (that is the whole point of starting
# it). Redirecting to a FILE instead of a pipe, then reading the file
# with `cat` (which has no children of its own to inherit anything),
# sidesteps the inheritance entirely. This is why every call into
# process-ownership.ps1 below goes through this helper rather than
# plain command substitution.
_po_run_ps1() {
  local out_file
  out_file="$(mktemp)"
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$PROCESS_OWNERSHIP_PS1" "$@" > "$out_file" 2>&1
  local result
  result="$(cat "$out_file" | tr -d '\r')"
  rm -f "$out_file"
  printf '%s' "$result"
}

# Never passed credentials: callers of this module only ever start
# `kubectl port-forward` (see start-port-forwards.sh/03-wait-ready.sh/
# 05-pod-replacement-demo.sh), whose arguments are a kubeconfig path,
# context/namespace names, and a Service/pod name + port pair -- never a
# secret. This function logs $* (the arguments) directly on that basis;
# it must never be reused for a command whose arguments could contain one.
_po_record_recovery() {
  {
    echo "---"
    echo "time: $(date -Iseconds)"
    echo "reason: $1"
    echo "record_file: $2"
  } >> "$PO_RECOVERY_LOG"
}

# Starts $filepath with $arguments (ONE pre-joined string, not an array
# -- PowerShell's Start-Process -ArgumentList was found to mis-split a
# bash-sourced array into a single comma-joined token; a plain string
# is split the same way a shell command line is, which is what every
# actual caller here already produces) as a detached, tracked process.
# $expected_marker is a substring that MUST appear in the process's own
# reported command line (a positive, independent confirmation that the
# process WMI found is actually the one just launched, not a PID
# collision with something that happened to start in the same instant).
# Writes $record_file (PID=/CREATED=/EXE=/MARKER=) on success. Never
# silently continues if the process cannot be identified: a process that
# exits immediately, or whose command line does not contain the marker,
# is reported as a failure (with a `_po_record_recovery` entry if the
# unidentified process may still be running, so it is not simply
# forgotten).
po_start() {
  local record_file="$1" stdout_log="$2" stderr_log="$3" filepath="$4" arguments="$5" expected_marker="$6"
  local result
  result="$(_po_run_ps1 -Action start -FilePath "$filepath" -Arguments "$arguments" \
    -StdOutLog "$stdout_log" -StdErrLog "$stderr_log")"

  case "$result" in
    EXITED_IMMEDIATELY)
      _po_log "the process exited immediately after starting -- see $stderr_log"
      cat "$stderr_log" >&2 2>/dev/null || true
      _po_fail "could not start a tracked process (exited immediately)"
      return 1
      ;;
    ERROR:*)
      _po_fail "could not start a tracked process: ${result#ERROR:}"
      return 1
      ;;
    OK:*)
      local pid created exe cmdline
      IFS='|' read -r pid created exe cmdline <<< "${result#OK:}"
      case "$cmdline" in
        *"$expected_marker"*) ;;
        *)
          # The process DID start and IS identifiable, but does not look
          # like what we asked for -- do not track it as ours (we have
          # no basis to), and do not kill it either (same reason). Make
          # its existence visible rather than losing track of it.
          _po_record_recovery "started process pid=$pid did not contain the expected marker in its own command line -- NOT tracked, NOT killed, left exactly as the OS started it" "$record_file"
          _po_fail "pid $pid started but its own reported command line did not contain the expected marker -- refusing to track an unverified process. See $PO_RECOVERY_LOG."
          return 1
          ;;
      esac
      {
        echo "PID=$pid"
        echo "CREATED=$created"
        echo "EXE=$exe"
        echo "MARKER=$expected_marker"
      } > "$record_file"
      _po_log "started and verified pid=$pid (created=$created)"
      return 0
      ;;
    *)
      _po_fail "unexpected response starting a tracked process: $result"
      return 1
      ;;
  esac
}

# Stops the process recorded in $record_file, but ONLY after confirming
# its CURRENT identity still matches what was recorded (creation
# timestamp, exact) and that its CURRENT command line still contains the
# recorded marker. If the recorded pid is already gone, this is a
# (logged) success -- nothing to do. If identity cannot be confirmed, or
# termination is not confirmed within the bound, this LEAVES THE PROCESS
# RUNNING, appends to the recovery log, and returns failure; it never
# reports success for something it could not verify.
po_stop() {
  local record_file="$1"
  if [ ! -f "$record_file" ]; then
    _po_log "no record at $record_file -- nothing tracked, nothing to stop."
    return 0
  fi

  local recorded_pid recorded_created recorded_exe recorded_marker
  recorded_pid="$(grep '^PID=' "$record_file" | cut -d= -f2-)"
  recorded_created="$(grep '^CREATED=' "$record_file" | cut -d= -f2-)"
  recorded_exe="$(grep '^EXE=' "$record_file" | cut -d= -f2-)"
  recorded_marker="$(grep '^MARKER=' "$record_file" | cut -d= -f2-)"
  if [ -z "$recorded_pid" ]; then
    _po_fail "record file $record_file is malformed (no PID= line) -- refusing to act on it"
    return 1
  fi

  local result
  result="$(_po_run_ps1 -Action identity -ProcessId "$recorded_pid")"

  if [ "$result" = "NOTFOUND" ]; then
    _po_log "pid $recorded_pid already exited -- nothing to stop (treated as success)."
    rm -f "$record_file"
    return 0
  fi

  local cur_pid cur_created cur_exe cur_cmdline
  case "$result" in
    OK:*) IFS='|' read -r cur_pid cur_created cur_exe cur_cmdline <<< "${result#OK:}" ;;
    *)
      _po_record_recovery "could not read current identity for recorded pid=$recorded_pid (unexpected response: $result) -- LEFT RUNNING, not killed" "$record_file"
      _po_fail "could not confirm pid $recorded_pid's current identity -- leaving it running. See $PO_RECOVERY_LOG."
      return 1
      ;;
  esac

  if [ "$cur_created" != "$recorded_created" ]; then
    _po_record_recovery "pid=$recorded_pid creation time changed (recorded=$recorded_created now=$cur_created) -- this PID has been REUSED by a different process. LEFT RUNNING, not killed." "$record_file"
    _po_fail "pid $recorded_pid's creation time no longer matches what was recorded -- this is a DIFFERENT process now (PID reuse). Leaving it running. See $PO_RECOVERY_LOG."
    return 1
  fi
  case "$cur_cmdline" in
    *"$recorded_marker"*) ;;
    *)
      _po_record_recovery "pid=$recorded_pid creation time still matches but its command line no longer contains the recorded marker '$recorded_marker' -- LEFT RUNNING, not killed." "$record_file"
      _po_fail "pid $recorded_pid no longer looks like the process we started (command line mismatch) -- leaving it running. See $PO_RECOVERY_LOG."
      return 1
      ;;
  esac

  _po_log "identity confirmed for pid $recorded_pid (creation time + command line both match) -- terminating."
  result="$(_po_run_ps1 -Action stop -ProcessId "$recorded_pid" -TimeoutMs 3000)"

  case "$result" in
    ALREADY_GONE|STOPPED)
      _po_log "pid $recorded_pid terminated and confirmed gone."
      rm -f "$record_file"
      return 0
      ;;
    STILL_RUNNING)
      _po_record_recovery "pid=$recorded_pid: termination was issued but NOT confirmed within the bound -- may still be running." "$record_file"
      _po_fail "termination of pid $recorded_pid was issued but could not be confirmed within the timeout -- treat as NOT stopped. See $PO_RECOVERY_LOG."
      return 1
      ;;
    *)
      _po_record_recovery "pid=$recorded_pid: unexpected stop response: $result" "$record_file"
      _po_fail "unexpected response stopping pid $recorded_pid: $result"
      return 1
      ;;
  esac
}

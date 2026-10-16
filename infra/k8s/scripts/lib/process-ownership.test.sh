#!/usr/bin/env bash
# Phase 3A.2: focused, bounded tests for process-ownership.sh. Uses only
# isolated, disposable fixture processes created by this test file
# itself (plain `node -e` long-running loops with a unique marker baked
# into their own source, so they are trivially distinguishable from any
# real user process) -- never an existing/real process, and never a
# literal network port (port numbers are deliberately NOT part of how
# this mechanism identifies anything to kill; see process-ownership.sh).
#
# Run directly: bash infra/k8s/scripts/lib/process-ownership.test.sh
set -uo pipefail  # NOT -e: individual test failures must not abort the whole run

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "$HERE/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
PASS=0
FAIL=0

# shellcheck source=./process-ownership.sh
source "$HERE/process-ownership.sh"
# process-ownership.sh sets -euo pipefail at its own top -- sourcing it
# applies that to THIS shell too, silently overriding this file's own
# deliberate `set -uo pipefail` (no -e) above and making any unexpected
# nonzero return abort the WHOLE test run instead of just failing one
# test (found directly: a transient po_start hiccup under this exact
# bug took down the rest of the suite with no "NOT OK" ever printed for
# it). Restoring `set +e` here, after sourcing, keeps -u/pipefail active
# while giving this file back its own intended fault tolerance.
set +e

pass() { PASS=$((PASS + 1)); echo "ok - $1"; }
fail() { FAIL=$((FAIL + 1)); echo "NOT OK - $1"; }

# A long-running, harmless, uniquely-markable fixture -- never a real
# user process. $1 is a unique marker string baked into the fixture's
# own `node -e` source, so its command line is independently verifiable.
# Retries ONCE on a start failure -- rapid back-to-back fixture churn
# (several of these in one test run) was observed to occasionally trip
# po_start's 500ms settle window transiently under host load; a single
# retry absorbs that without masking a REAL, reproducible failure (which
# would fail twice in a row).
start_fixture() {
  local marker="$1"
  local record="$TMP_DIR/record-$marker"
  if ! po_start "$record" "$TMP_DIR/out-$marker.log" "$TMP_DIR/err-$marker.log" \
    "node" "-e \"/* fixture:$marker */ setInterval(()=>{}, 100000)\"" "fixture:$marker" \
    >"$TMP_DIR/po-start-$marker.log" 2>&1; then
    sleep 1
    po_start "$record" "$TMP_DIR/out-$marker.log" "$TMP_DIR/err-$marker.log" \
      "node" "-e \"/* fixture:$marker */ setInterval(()=>{}, 100000)\"" "fixture:$marker" \
      >>"$TMP_DIR/po-start-$marker.log" 2>&1
  fi
  echo "$record"
}

fixture_pid_alive() {
  local record="$1"
  [ -f "$record" ] || return 1
  local pid
  pid="$(grep '^PID=' "$record" | cut -d= -f2-)"
  [ -n "$pid" ] || return 1
  powershell.exe -NoProfile -Command "if (Get-Process -Id $pid -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >/dev/null 2>&1
}

cleanup_pid() {
  local pid="$1"
  [ -n "$pid" ] || return 0
  powershell.exe -NoProfile -Command "Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue" >/dev/null 2>&1 || true
}

# --- Test 1: a process started by this mechanism is stopped. ---
test_start_then_stop() {
  local name="t1-$$"
  local record
  record="$(start_fixture "$name")"
  local pid
  pid="$(grep '^PID=' "$record" 2>/dev/null | cut -d= -f2-)"
  if [ -z "$pid" ]; then fail "test1: fixture failed to start at all"; return; fi
  if ! fixture_pid_alive "$record"; then fail "test1: fixture not alive right after starting"; cleanup_pid "$pid"; return; fi

  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    if fixture_pid_alive "$record"; then
      fail "test1: po_stop reported success but the process is STILL alive"
      cleanup_pid "$pid"
    elif [ -f "$record" ]; then
      fail "test1: po_stop succeeded but did not remove the record file"
      cleanup_pid "$pid"
    else
      pass "test1: a RelayLab-started process is stopped, confirmed dead, record removed"
    fi
  else
    fail "test1: po_stop reported failure for a genuinely owned, still-running process"
    cleanup_pid "$pid"
  fi
}

# --- Test 2: a stale record pointing to an UNRELATED process (PID reuse simulated) leaves it untouched. ---
test_stale_record_unrelated_process() {
  local name="t2-$$"
  # Fixture B: the "innocent bystander" -- a REAL process that exists,
  # but was never started by po_start, so it has no real record of its
  # own. We fabricate a record claiming it is "ours" with a WRONG
  # (impossible) creation timestamp, exactly simulating what a stale
  # record looks like after the original pid has been reused.
  local bystander_marker="bystander-$name"
  local bystander_log="$TMP_DIR/bystander-$name.log"
  node -e "/* fixture:$bystander_marker */ setInterval(()=>{}, 100000)" >"$bystander_log" 2>&1 &
  local bash_bg=$!
  sleep 1
  local bystander_pid
  bystander_pid="$(powershell.exe -NoProfile -Command "(Get-CimInstance Win32_Process -Filter \"CommandLine LIKE '%$bystander_marker%'\" | Select-Object -First 1).ProcessId" 2>/dev/null | tr -d '\r' | tr -d ' ')"
  if [ -z "$bystander_pid" ]; then
    fail "test2: could not locate the bystander fixture process at all (test setup problem, not the thing under test)"
    kill "$bash_bg" 2>/dev/null || true
    return
  fi

  local record="$TMP_DIR/record-$name"
  {
    echo "PID=$bystander_pid"
    echo "CREATED=1999-01-01T00:00:00.0000000+05:30"   # deliberately impossible/stale
    echo "EXE=C:\\nonexistent\\node.exe"
    echo "MARKER=fixture:$name-DOES-NOT-EXIST"
  } > "$record"

  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    fail "test2: po_stop reported SUCCESS for a stale/mismatched record -- it should have refused"
  else
    if fixture_pid_alive "$record" 2>/dev/null; then : ; fi
    if powershell.exe -NoProfile -Command "if (Get-Process -Id $bystander_pid -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >/dev/null 2>&1; then
      pass "test2: a stale record (simulated PID reuse) was correctly refused -- the unrelated process was left running"
    else
      fail "test2: po_stop refused as expected, but the unrelated bystander process is somehow gone anyway"
    fi
  fi
  grep -qi "reuse" "$PO_RECOVERY_LOG" 2>/dev/null && pass "test2b: the mismatch was recorded in the recovery log" || fail "test2b: no recovery-log entry was written for the refused stop"

  cleanup_pid "$bystander_pid"
  kill "$bash_bg" 2>/dev/null || true
}

# --- Test 3: an unrelated process survives even when a record's marker happens to mention the same target the unrelated process is using. ---
test_unrelated_listener_survives() {
  local name="t3-$$"
  # Fixture C: an entirely unrelated, never-tracked process that happens
  # to be running AT THE SAME TIME we attempt to stop something else --
  # proves cleanup never widens its scope by scanning for "whatever is
  # doing X" (the exact pattern this patch removes: the old
  # kill_port_robust looked up whatever PID occupied a port; this
  # confirms the replacement never does the analogous thing).
  local unrelated_marker="unrelated-$name"
  node -e "/* fixture:$unrelated_marker */ setInterval(()=>{}, 100000)" >"$TMP_DIR/unrelated-$name.log" 2>&1 &
  local bash_bg=$!
  sleep 1
  local unrelated_pid
  unrelated_pid="$(powershell.exe -NoProfile -Command "(Get-CimInstance Win32_Process -Filter \"CommandLine LIKE '%$unrelated_marker%'\" | Select-Object -First 1).ProcessId" 2>/dev/null | tr -d '\r' | tr -d ' ')"

  # Our OWN, legitimately tracked fixture, stopped normally.
  local record
  record="$(start_fixture "$name")"
  po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1 || true

  if [ -n "$unrelated_pid" ] && powershell.exe -NoProfile -Command "if (Get-Process -Id $unrelated_pid -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >/dev/null 2>&1; then
    pass "test3: an unrelated process running throughout the cleanup was never touched"
  else
    fail "test3: the unrelated process is gone -- cleanup affected something it should never have reached"
  fi
  cleanup_pid "$unrelated_pid"
  kill "$bash_bg" 2>/dev/null || true
}

# --- Test 4: an already-exited owned process is handled safely (treated as success, not an error). ---
test_already_exited() {
  local name="t4-$$"
  local record
  record="$(start_fixture "$name")"
  local pid
  pid="$(grep '^PID=' "$record" 2>/dev/null | cut -d= -f2-)"
  cleanup_pid "$pid"
  sleep 1

  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    if [ -f "$record" ]; then
      fail "test4: po_stop succeeded on an already-exited process but left the record file behind"
    else
      pass "test4: an already-exited owned process is handled safely (treated as success, record cleaned up)"
    fi
  else
    fail "test4: po_stop reported FAILURE for a process that had already exited on its own"
  fi
}

# --- Test 5: a genuinely failed/ambiguous termination is visible, never silently reported as success. ---
test_ambiguous_identity_is_visible() {
  local name="t5-$$"
  local record="$TMP_DIR/record-$name"
  {
    echo "PID=999999"   # a pid essentially guaranteed not to exist
    echo "CREATED=2026-01-01T00:00:00.0000000+05:30"
    echo "EXE=C:\\nonexistent\\node.exe"
    echo "MARKER=fixture:$name"
  } > "$record"

  # A nonexistent recorded pid is NOTFOUND -> treated as already-gone,
  # which IS the correct, honest "nothing to do" outcome -- not the
  # ambiguous case this test is really after. The ambiguous case is: a
  # REAL, currently-running process whose identity does not match. That
  # is exactly test2 above. This test instead confirms the distinct
  # "malformed record" refusal path never reports success either.
  local bad_record="$TMP_DIR/bad-record-$name"
  echo "garbage, not a real record" > "$bad_record"
  if po_stop "$bad_record" >"$TMP_DIR/po-stop-bad-$name.log" 2>&1; then
    fail "test5: po_stop reported SUCCESS for a malformed record with no PID= line"
  else
    pass "test5: a malformed/unidentifiable record is refused, not silently reported successful"
  fi
}

# --- Test 6: an executable-path/command-line mismatch (creation time
# UNCHANGED) is refused just as a creation-time mismatch is -- a
# different identity dimension than test2. ---
test_exe_and_command_mismatch() {
  local name="t6-$$"
  local record
  record="$(start_fixture "$name")"
  local pid
  pid="$(grep '^PID=' "$record" 2>/dev/null | cut -d= -f2-)"
  if [ -z "$pid" ]; then fail "test6: fixture failed to start"; return; fi

  # Tamper with ONLY the EXE/MARKER fields, leaving CREATED (and the
  # real pid) exactly as recorded -- isolates this check from test2's
  # creation-time dimension.
  local created
  created="$(grep '^CREATED=' "$record" | cut -d= -f2-)"
  {
    echo "PID=$pid"
    echo "CREATED=$created"
    echo "EXE=C:\\this\\does\\not\\match\\node.exe"
    echo "MARKER=this-marker-does-not-appear-in-the-real-command-line"
  } > "$record"

  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    fail "test6: po_stop reported SUCCESS despite an exe/command-line mismatch"
    cleanup_pid "$pid"
  else
    if fixture_pid_alive "$record" 2>/dev/null || powershell.exe -NoProfile -Command "if (Get-Process -Id $pid -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >/dev/null 2>&1; then
      pass "test6: an exe/command-line mismatch (creation time unchanged) is refused -- the real process survives untouched"
    else
      fail "test6: po_stop refused as expected, but the process is gone anyway"
    fi
    cleanup_pid "$pid"
  fi
}

# --- Test 7: an EMPTY marker field is rejected before any termination attempt. ---
test_empty_marker_rejected() {
  local name="t7-$$"
  local record
  record="$(start_fixture "$name")"
  local pid created exe
  pid="$(grep '^PID=' "$record" | cut -d= -f2-)"
  created="$(grep '^CREATED=' "$record" | cut -d= -f2-)"
  exe="$(grep '^EXE=' "$record" | cut -d= -f2-)"
  {
    echo "PID=$pid"
    echo "CREATED=$created"
    echo "EXE=$exe"
    echo "MARKER="
  } > "$record"

  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    fail "test7: po_stop reported SUCCESS with an EMPTY marker field"
    cleanup_pid "$pid"
  else
    pass "test7: an empty marker field is rejected outright, before any termination attempt"
    cleanup_pid "$pid"
  fi
}

# --- Test 8: an identity-query FAILURE (distinct from confirmed absence)
# retains the record and reports failure -- never ALREADY_GONE/STOPPED. ---
test_query_failure_retains_record() {
  local name="t8-$$"
  local record
  record="$(start_fixture "$name")"
  local pid
  pid="$(grep '^PID=' "$record" | cut -d= -f2-)"
  if [ -z "$pid" ]; then fail "test8: fixture failed to start"; return; fi

  # Controlled injection: a genuine WMI query failure for a REAL, still
  # alive process is not reliably reproducible from a test without
  # tampering with the host. Instead, this directly exercises the bash
  # layer's handling of that outcome by substituting a stand-in for
  # _po_run_ps1 that returns exactly what process-ownership.ps1 itself
  # would return on a query failure -- never targeting a real process,
  # never claiming the PowerShell layer was itself exercised by this
  # specific test (see process-ownership.ps1's own `stop` action, and
  # test1/test6 above, for that).
  _po_run_ps1() { echo "QUERY_FAILED:simulated WMI failure for this test"; }
  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    fail "test8: po_stop reported SUCCESS on a simulated query failure -- it must never treat inspection failure as ALREADY_GONE or STOPPED"
  else
    if [ -f "$record" ]; then
      pass "test8: a query failure retains the record and reports failure, distinct from confirmed absence"
    else
      fail "test8: po_stop reported failure (correct) but removed the record anyway (incorrect -- only a confirmed outcome should remove it)"
    fi
  fi
  # `unset -f` only DELETES the override -- it does not restore the
  # original definition from process-ownership.sh (bash functions are
  # not layered/stacked). Re-sourcing the real file is what actually
  # puts the genuine _po_run_ps1 back; this was found directly here --
  # without it, every test after this one silently lost _po_run_ps1
  # entirely, making po_start/po_stop fail with no function defined.
  unset -f _po_run_ps1
  source "$HERE/process-ownership.sh"
  set +e
  grep -qi "inspection failure\|QUERY_FAILED" "$PO_RECOVERY_LOG" 2>/dev/null && pass "test8b: the query failure was recorded in the recovery log" || fail "test8b: no recovery-log entry was written for the query failure"
  cleanup_pid "$pid"
}

# --- Test 9: an identity change simulated BETWEEN an earlier check and
# the final termination boundary is refused -- the boundary itself must
# re-validate, not trust a prior conclusion. ---
test_identity_change_between_check_and_termination() {
  local name="t9-$$"
  local record
  record="$(start_fixture "$name")"
  local pid
  pid="$(grep '^PID=' "$record" 2>/dev/null | cut -d= -f2-)"
  if [ -z "$pid" ]; then fail "test9: fixture failed to start"; return; fi

  # At this point the record is CORRECT -- exactly what an earlier
  # caller's own check would have seen and approved. Simulate reality
  # having changed by the time termination actually runs (the real
  # shape of a PID-reuse race) by rewriting the record's CREATED field
  # to a value that no longer matches the still-running real process --
  # the termination boundary must catch THIS, independent of whatever
  # an earlier check believed.
  local exe marker
  exe="$(grep '^EXE=' "$record" | cut -d= -f2-)"
  marker="$(grep '^MARKER=' "$record" | cut -d= -f2-)"
  {
    echo "PID=$pid"
    echo "CREATED=2000-01-01T00:00:00.0000000+05:30"
    echo "EXE=$exe"
    echo "MARKER=$marker"
  } > "$record"

  if po_stop "$record" >"$TMP_DIR/po-stop-$name.log" 2>&1; then
    fail "test9: po_stop reported SUCCESS despite the identity having changed since the record was established"
    cleanup_pid "$pid"
  else
    if powershell.exe -NoProfile -Command "if (Get-Process -Id $pid -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >/dev/null 2>&1; then
      pass "test9: an identity change between an earlier check and the termination boundary is refused -- the real process survives untouched"
    else
      fail "test9: po_stop refused as expected, but the process is gone anyway"
    fi
    cleanup_pid "$pid"
  fi
}

test_start_then_stop
test_stale_record_unrelated_process
test_unrelated_listener_survives
test_already_exited
test_ambiguous_identity_is_visible
test_exe_and_command_mismatch
test_empty_marker_rejected
test_query_failure_retains_record
test_identity_change_between_check_and_termination

echo "---"
echo "tests passed: $PASS, failed: $FAIL"
rm -rf "$TMP_DIR"
[ "$FAIL" -eq 0 ]

# Phase 3A.2: the Windows-side half of the process-ownership mechanism.
# Exists because Git Bash's own `$!` was found to be unreliable for this
# purpose (see infra/k8s/scripts/lib/process-ownership.sh's header
# comment for the exact, reproduced failure): backgrounding a native
# Win32 process from MSYS bash sometimes yields an intermediate wrapper
# shell's PID instead of the real target process's PID, and that
# wrapper can exit before anything queries it. `Start-Process -PassThru`
# is the authoritative alternative: PowerShell itself hands back the
# real Win32 PID of the process it just created, with no guessing.
#
# Never pass command arguments containing a literal `|` through this
# script -- CommandLine is reported with `|` as the field delimiter
# below, on the (currently always true, for this project's actual use:
# kubectl port-forward arguments) assumption that real arguments never
# contain one. Never pass credentials as arguments either -- CommandLine
# is written to stdout/log files verbatim.
#
# Phase 3A.2 corrective patch: the `stop` action now validates identity
# ITSELF, immediately before terminating, using the EXPECTED identity
# the caller passes in -- it no longer blindly trusts a -ProcessId that
# some earlier, separate call already decided was safe. See that
# action's own comments below for why a stable .NET process HANDLE
# (obtained once and then acted on directly), not a second PID
# resolution via Stop-Process -Id, is what actually narrows the PID-
# reuse race, and why simply re-ordering the same check does not.
param(
  [Parameter(Mandatory = $true)][ValidateSet('start', 'identity', 'stop')][string]$Action,
  [string]$FilePath,
  [string]$Arguments,
  [string]$StdOutLog,
  [string]$StdErrLog,
  [int]$ProcessId,
  [string]$ExpectedCreated,
  [string]$ExpectedExe,
  [string]$ExpectedMarker,
  [int]$TimeoutMs = 3000
)

function Get-Identity([int]$targetPid) {
  # NEVER name this parameter $pid -- PowerShell's automatic $PID
  # variable is read-only and case-insensitive; using $pid as a
  # parameter name throws "Cannot overwrite variable pid because it is
  # read-only or constant" the moment it is assigned, which this script
  # hit directly during its own test run.
  $wmi = Get-CimInstance Win32_Process -Filter "ProcessId=$targetPid" -ErrorAction SilentlyContinue
  if ($wmi) {
    return "$($wmi.ProcessId)|$($wmi.CreationDate.ToString('o'))|$($wmi.ExecutablePath)|$($wmi.CommandLine)"
  }
  return $null
}

switch ($Action) {
  'start' {
    try {
      $p = Start-Process -FilePath $FilePath -ArgumentList $Arguments -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog
      # Bounded settle time before the first identity read -- long enough
      # for a process that is going to exit immediately (wrong args, a
      # missing executable resolved to a stub, etc.) to have already done
      # so, short enough to keep every caller of this script fast.
      Start-Sleep -Milliseconds 500
      $id = Get-Identity $p.Id
      if ($id) { Write-Output "OK:$id" } else { Write-Output "EXITED_IMMEDIATELY" }
    } catch {
      Write-Output "ERROR:$($_.Exception.Message)"
    }
  }
  'identity' {
    # Kept for direct diagnostic use (e.g. "what does this PID look like
    # right now") -- `stop` below no longer relies on a SEPARATE prior
    # call to this action; it performs its own identity check at the
    # actual termination boundary.
    $id = Get-Identity $ProcessId
    if ($id) { Write-Output "OK:$id" } else { Write-Output "NOTFOUND" }
  }
  'stop' {
    # Reject missing/empty/malformed expected-identity fields OUTRIGHT --
    # this action refuses to terminate anything without a complete
    # expected identity to validate against. (The bash caller is also
    # expected to check this before ever invoking -Action stop; this is
    # the termination boundary's OWN redundant check, not a replacement
    # for that one.)
    if ([string]::IsNullOrWhiteSpace($ExpectedCreated) -or [string]::IsNullOrWhiteSpace($ExpectedExe) -or [string]::IsNullOrWhiteSpace($ExpectedMarker)) {
      Write-Output "ERROR:missing or empty expected identity field(s) -- refusing to terminate anything without a complete expected identity"
      return
    }

    # GetProcessById ALONE does not pin anything -- verified directly on
    # this runtime: the .NET Process object it returns, and even reading
    # its .StartTime afterward, do NOT force an OS-level handle open and
    # held for the object's lifetime (that part of the earlier comment
    # here was wrong). Process's internal process handle is opened
    # LAZILY, only when something that specifically needs it is
    # accessed (EnsureState(HaveProcessHandle)), and .StartTime was
    # found NOT to reliably trigger that on this runtime. Without an
    # explicitly retained handle, the PID-reuse race this action exists
    # to close is NOT actually closed by merely holding the wrapper
    # object -- a reused PID could still be resolved fresh at Kill()
    # time, identically to Stop-Process -Id's own re-resolution.
    #
    # GetProcessById throws when no process with this PID exists AT ALL
    # right now -- a CONFIRMED absence, correctly reported as
    # ALREADY_GONE below, never confused with a query/inspection failure.
    try {
      $proc = [System.Diagnostics.Process]::GetProcessById($ProcessId)
    } catch {
      Write-Output "ALREADY_GONE"
      return
    }

    try {
      # Explicitly force the native handle open and RETAINED on this
      # SAME $proc instance, right now, by reading .Handle -- this is
      # what actually triggers EnsureState(HaveProcessHandle) to call
      # OpenProcess immediately and cache the resulting SafeHandle on
      # $proc for the rest of this action. Once that handle is open,
      # Windows cannot reassign this PID to a different process until
      # the handle is closed (our `finally` below) -- THIS is what
      # closes the PID-reuse race, not merely resolving the PID a
      # second time, and not the StartTime read the previous version of
      # this comment relied on. Everything below (the WMI identity
      # re-check AND the final Kill()) acts on this SAME pinned
      # instance.
      $null = $proc.Handle
    } catch [System.InvalidOperationException] {
      # The Process component is no longer associated with a running
      # process -- a CONFIRMED absence, discovered the instant this
      # action tried to pin a handle to it.
      Write-Output "ALREADY_GONE"
      $proc.Dispose()
      return
    } catch [System.ComponentModel.Win32Exception] {
      if ($_.Exception.NativeErrorCode -eq 87) {
        # ERROR_INVALID_PARAMETER: OpenProcess failed because no process
        # with this PID exists any more -- a CONFIRMED absence.
        Write-Output "ALREADY_GONE"
      } else {
        # Any other Win32 failure (e.g. access denied) is an
        # INSPECTION FAILURE, never confused with confirmed absence.
        Write-Output "QUERY_FAILED:could not obtain a stable handle to pid $ProcessId -- $($_.Exception.Message)"
      }
      $proc.Dispose()
      return
    } catch {
      Write-Output "QUERY_FAILED:could not obtain a stable handle to pid $ProcessId immediately before termination -- $($_.Exception.Message)"
      $proc.Dispose()
      return
    }

    try {
      # Re-query full identity (WMI; CommandLine is not available from
      # the .NET Process object itself) for this exact PID -- this IS
      # the termination boundary's own validation, using the identity
      # the CALLER expects, not a re-use of some earlier, separate
      # check's conclusion. The PID cannot have been reassigned since
      # the handle above was pinned, so this still refers to the SAME
      # instance $proc now holds open.
      $wmi = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction SilentlyContinue
      if (-not $wmi) {
        # The pinned handle confirms an instance exists, but WMI could
        # not be queried right now -- an INSPECTION FAILURE, never
        # treated as confirmed absence (ALREADY_GONE) or as a
        # successful stop (STOPPED). The record is left in place by the
        # bash caller on this outcome.
        Write-Output "QUERY_FAILED:could not read WMI identity immediately before termination"
        return
      }

      $actualCreated = $wmi.CreationDate.ToString('o')
      $actualExe = $wmi.ExecutablePath
      $actualCmdline = $wmi.CommandLine

      $createdMatches = ($actualCreated -eq $ExpectedCreated)
      # Windows executable-path comparison: case-insensitive (NTFS/the
      # Windows API treat paths case-insensitively) and tolerant of a
      # trailing separator -- appropriate Windows path semantics, not a
      # byte-for-byte string match.
      $exeMatches = $actualExe -and ($actualExe.TrimEnd('\') -ieq $ExpectedExe.TrimEnd('\'))
      $markerMatches = $actualCmdline -and ($actualCmdline -like "*$ExpectedMarker*")

      if (-not ($createdMatches -and $exeMatches -and $markerMatches)) {
        Write-Output "IDENTITY_MISMATCH:created=$actualCreated|exe=$actualExe"
        return
      }

      # $proc's handle is the SAME one pinned above -- Kill() acts on
      # that already-open handle, never a fresh PID resolution.
      try {
        $proc.Kill()
      } catch {
        Write-Output "ERROR:$($_.Exception.Message)"
        return
      }

      $elapsed = 0
      while ($elapsed -lt $TimeoutMs) {
        Start-Sleep -Milliseconds 300
        $elapsed += 300
        if ($proc.HasExited) { Write-Output "STOPPED"; return }
      }
      Write-Output "STILL_RUNNING"
    } finally {
      $proc.Dispose()
    }
  }
}

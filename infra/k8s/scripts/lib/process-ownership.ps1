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

    # Obtain a STABLE HANDLE to the process INSTANCE first, via .NET's
    # own GetProcessById -- NOT Stop-Process -Id, which re-resolves the
    # PID number at the moment it runs and would act on WHATEVER process
    # currently holds that PID, including one that reused it after the
    # original exited. A .NET Process object returned by GetProcessById
    # is bound to the specific process INSTANCE the OS associated with
    # this PID at the moment of the call; everything below (the WMI
    # identity re-check AND the final Kill()) acts on or alongside THAT
    # SAME object, not a PID number resolved a second time. Merely
    # moving an identity check to run immediately before Stop-Process
    # -Id would NOT close this gap -- Stop-Process -Id still re-resolves
    # the PID itself, independent of whatever was checked a moment
    # earlier. Using the handle is what actually does.
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

    # Re-query full identity (WMI; CommandLine is not available from the
    # .NET Process object itself) for this exact PID, as close to the
    # handle acquisition above as this script can get -- this IS the
    # termination boundary's own validation, using the identity the
    # CALLER expects, not a re-use of some earlier, separate check's
    # conclusion.
    $wmi = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction SilentlyContinue
    if (-not $wmi) {
      # The .NET handle says an instance with this PID exists, but WMI
      # could not be queried right now -- an INSPECTION FAILURE, never
      # treated as confirmed absence (ALREADY_GONE) or as a successful
      # stop (STOPPED). The record is left in place by the bash caller
      # on this outcome.
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

    # One more, independent corroboration via the HANDLE's own StartTime
    # (a different API/code path than the WMI query above) immediately
    # before Kill() -- if the original instance has exited in the
    # instant between the WMI check and here, this throws, and that is
    # reported as a query failure (not a confirmed stop), never silently
    # ignored.
    try {
      $null = $proc.StartTime
    } catch {
      Write-Output "QUERY_FAILED:could not confirm the process handle was still valid immediately before termination"
      return
    }

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
  }
}

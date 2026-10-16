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
param(
  [Parameter(Mandatory = $true)][ValidateSet('start', 'identity', 'stop')][string]$Action,
  [string]$FilePath,
  [string]$Arguments,
  [string]$StdOutLog,
  [string]$StdErrLog,
  [int]$ProcessId,
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
    $id = Get-Identity $ProcessId
    if ($id) { Write-Output "OK:$id" } else { Write-Output "NOTFOUND" }
  }
  'stop' {
    # Re-reads identity immediately before killing -- the caller (bash)
    # is responsible for comparing this against the RECORDED identity
    # and deciding whether to proceed; this script only ever kills the
    # exact -ProcessId it is given, once already told to.
    $id = Get-Identity $ProcessId
    if (-not $id) { Write-Output "ALREADY_GONE"; return }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
    $elapsed = 0
    while ($elapsed -lt $TimeoutMs) {
      Start-Sleep -Milliseconds 300
      $elapsed += 300
      if (-not (Get-Identity $ProcessId)) { Write-Output "STOPPED"; return }
    }
    Write-Output "STILL_RUNNING"
  }
}

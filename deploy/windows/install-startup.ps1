<#
.SYNOPSIS
  Configure the Windows collector agent to start at logon (per-user Startup folder, no admin required).

.DESCRIPTION
  1. Generates agent/52_push_panel.cmd (token, python path, restart loop, log redirect)
  2. Drops a launcher into the user's Startup folder
  3. Starts it once (skip with -NoStart)

.PARAMETER Token
  Must match PANEL_TOKEN in the Linux gateway gateway.env.

.PARAMETER Receiver
  Gateway base URL, e.g. http://192.168.0.198:18082

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File deploy\windows\install-startup.ps1 -Token <token> -Receiver http://192.168.0.198:18082
#>
param(
  [Parameter(Mandatory = $true)][string]$Token,
  [string]$Receiver = "http://127.0.0.1:18082",
  [string]$TdxPyPlugins = "C:\new_tdx64\PYPlugins\user",
  [string]$Python = "python",
  [string]$RepoRoot = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)),
  [string]$StartupDir = "",
  [switch]$Force,
  [switch]$NoStart
)

$ErrorActionPreference = "Stop"
$agent = Join-Path $RepoRoot "agent\52_push_panel.py"
if (-not (Test-Path $agent)) { throw "agent script not found: $agent" }

$cmdPath = Join-Path $RepoRoot "agent\52_push_panel.cmd"
if ((Test-Path $cmdPath) -and -not $Force) {
  throw "already exists: $cmdPath (use -Force to overwrite)"
}

$reports = Join-Path $RepoRoot "reports"
New-Item -ItemType Directory -Force -Path $reports | Out-Null
$logPath = Join-Path $reports "panel_push.log"

$runLine = '"{0}" "{1}" --receiver {2} --market 5 --include-markets 9,31 --interval 6 --depth-market 24 --depth-limit 500 --minute-market 24 --minute-limit 500 --daily-auto --daily-markets 5,31,9 --daily-codes 000001.SH,000002.SH,399107.SZ --daily-count 30 --no-seed >> "{3}" 2>&1' -f '%PY%', $agent, $Receiver, $logPath

$lines = @(
  '@echo off',
  "set TDX_PYPLUGINS=$TdxPyPlugins",
  "set PANEL_TOKEN=$Token",
  'set PYTHONIOENCODING=utf-8',
  'set PYTHONUNBUFFERED=1',
  "set PY=$Python",
  ':loop',
  $runLine,
  'timeout /t 10 /nobreak >nul',
  'goto loop'
)
Set-Content -Path $cmdPath -Value ($lines -join "`r`n") -Encoding ASCII

$startup = if ($StartupDir) { $StartupDir } else { [Environment]::GetFolderPath("Startup") }
New-Item -ItemType Directory -Force -Path $startup | Out-Null
$launcher = Join-Path $startup "tdx-panel-push.cmd"
$launcherLines = @(
  '@echo off',
  ('start "" /min "{0}"' -f $cmdPath)
)
Set-Content -Path $launcher -Value ($launcherLines -join "`r`n") -Encoding ASCII

Write-Host "generated: $cmdPath"
Write-Host "startup  : $launcher"
Write-Host "log      : $logPath"

if (-not $NoStart) {
  Start-Process -FilePath $launcher -WindowStyle Hidden
  Write-Host "started; first batch (7483 symbols seed) arrives in about 60 seconds."
}

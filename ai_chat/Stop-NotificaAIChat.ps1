# --- Notifica AI Chat: STOP (robust, PS 5.1+ compatible) ---
[CmdletBinding()]
param(
  # Eén of meer poorten die je gebruikt voor de chat (http/https)
  [int[]]$Ports = @(8000, 8443),

  # Projectmap zodat we processen kunnen herkennen in fallback
  [string]$ProjectDir = "C:\Projects\DataNavigator\ai_chat",

  # Sluit ook het aparte PowerShell-venster dat door het startscript is geopend
  [switch]$CloseConsole
)

Write-Host "Stopping Notifica AI Chat ..." -ForegroundColor Yellow

# --- Admin check (voor Get-NetTCPConnection en proces-kills) ---
try {
  $isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
             ).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
} catch { $isAdmin = $false }
if (-not $isAdmin) {
  Write-Host "  [WAARSCHUWING] Geen Administrator-rechten; listeners/kills kunnen onvolledig zijn." -ForegroundColor Yellow
}

function Get-ListeningPidsForPorts {
  param([int[]]$PortList)
  $pids = @()

  foreach ($p in $PortList) {
    try {
      $listeners = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction Stop
      if ($listeners) {
        $lpids = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
        $pids += $lpids
        Write-Host ("  • Luisterend op poort {0}: {1}" -f $p, ($lpids -join ", "))
      } else {
        Write-Host ("  • Geen listener op poort {0}" -f $p)
      }
    } catch {
      Write-Host ("  • Geen listener op poort {0} (of geen rechten om te lezen)" -f $p)
    }
  }
  return ($pids | Select-Object -Unique)
}

function Get-ChildrenPids {
  param([int]$ParentPid, [hashtable]$IndexByParent)
  $all = @()
  $kids = @()
  if ($IndexByParent.ContainsKey($ParentPid)) { $kids = $IndexByParent[$ParentPid] }
  foreach ($k in $kids) {
    $all += $k
    $all += Get-ChildrenPids -ParentPid $k -IndexByParent $IndexByParent
  }
  return $all
}

function Stop-ProcessTree {
  param([int]$RootPid, [hashtable]$IndexByParent)

  $tree = @()
  $tree += $RootPid
  $tree += Get-ChildrenPids -ParentPid $RootPid -IndexByParent $IndexByParent
  $tree = $tree | Select-Object -Unique

  foreach ($pid in ($tree | Sort-Object -Descending)) {
    try {
      $proc = Get-Process -Id $pid -ErrorAction Stop
      Stop-Process -Id $pid -Force -ErrorAction Stop
      Write-Host ("  ✓ Gestopt: {0} (PID {1})" -f $proc.ProcessName, $pid) -ForegroundColor Green
    } catch {
      # kan al gestopt/ontoegankelijk zijn
    }
  }
}

# 1) Stop via poorten
$pids = Get-ListeningPidsForPorts -PortList $Ports
$pids = $pids | Select-Object -Unique

# Bouw één keer een parent->children index (voor snelle tree-kill & fallback)
$procIndex = @{}
try {
  $allProcs = Get-CimInstance Win32_Process
  foreach ($p in $allProcs) {
    $ppid = [int]$p.ParentProcessId
    if (-not $procIndex.ContainsKey($ppid)) { $procIndex[$ppid] = @() }
    $procIndex[$ppid] += [int]$p.ProcessId
  }
} catch {
  $allProcs = @()
}

if ($pids.Count -gt 0) {
  foreach ($pid in $pids) {
    Stop-ProcessTree -RootPid $pid -IndexByParent $procIndex
  }
}

# 2) Fallback: kill python/uvicorn processen die vanuit jouw project-dir draaien
try {
  $projNorm = $ProjectDir.TrimEnd('\')
  $candidates = @()

  foreach ($c in $allProcs) {
    $cmd = [string]$c.CommandLine
    $exe = [string]$c.ExecutablePath
    if ([string]::IsNullOrEmpty($cmd) -and [string]::IsNullOrEmpty($exe)) { continue }

    $looksLikePython = $exe -like "*\python.exe" -or $cmd -match '\bpython(\.exe)?\b'
    $looksLikeUvicorn = $cmd -match '\buvicorn\b' -or $cmd -match 'app:app'

    if (($looksLikePython -or $looksLikeUvicorn) -and ($cmd -like "*$projNorm*" -or $exe -like "*$projNorm*")) {
      $candidates += [int]$c.ProcessId
    }
  }

  if ($candidates.Count -gt 0) {
    # vermijd dubbele kills (al gestopt via poorten)
    $candidates = $candidates | Where-Object { $pids -notcontains $_ } | Select-Object -Unique
    if ($candidates.Count -gt 0) {
      Write-Host "Fallback: stop processen op basis van projectpad ..." -ForegroundColor Yellow
      foreach ($pid in $candidates) {
        Stop-ProcessTree -RootPid $pid -IndexByParent $procIndex
      }
    }
  }
} catch {
  # best effort
}

# 3) Optioneel: sluit het aparte PowerShell-venster met titel “Notifica AI Chat …”
if ($CloseConsole) {
  try {
    $ps = Get-Process | Where-Object {
      $_.ProcessName -in @("powershell","pwsh") -and $_.MainWindowTitle -like "*Notifica AI Chat*"
    }
    foreach ($p in $ps) {
      try {
        Stop-Process -Id $p.Id -Force -ErrorAction Stop
        Write-Host ("  ✓ Console gesloten: PID {0} '{1}'" -f $p.Id, $p.MainWindowTitle) -ForegroundColor Green
      } catch { }
    }
  } catch { }
}

# 4) Controle
Start-Sleep -Milliseconds 300
$left = Get-ListeningPidsForPorts -PortList $Ports
if ($left.Count -eq 0) {
  Write-Host "Notifica AI Chat: niets meer luistert op poorten ($($Ports -join ', '))." -ForegroundColor Cyan
} else {
  Write-Host ("[WAARSCHUWING] Nog luisterende PID(s): {0}" -f ($left -join ", ")) -ForegroundColor Yellow
}

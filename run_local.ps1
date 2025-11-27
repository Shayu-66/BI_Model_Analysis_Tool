#!/usr/bin/env pwsh
# run_local.ps1 - simple, ASCII-only PowerShell script to prepare venv and start Streamlit

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "Changing to script directory..."
if ($PSScriptRoot) {
    Set-Location $PSScriptRoot
} else {
    Set-Location (Split-Path -Path $MyInvocation.MyCommand.Definition -Parent)
}

function Find-Python {
    $py = Get-Command python -ErrorAction SilentlyContinue
    if ($py) { return $py.Source }
    $py3 = Get-Command python3 -ErrorAction SilentlyContinue
    if ($py3) { return $py3.Source }
    return $null
}

$pythonExe = Find-Python
if (-not $pythonExe) {
    Write-Error "Python executable not found. Please install Python and add it to PATH."
    Write-Host "Run: where python   or   Get-Command python"
    pause
    exit 1
}
Write-Host "Using Python: $pythonExe"

$venvPaths = @('venv', '.venv')
$venvFound = $null
foreach ($p in $venvPaths) {
    if (Test-Path (Join-Path $p 'Scripts\Activate.ps1')) {
        $venvFound = (Resolve-Path $p).ProviderPath
        break
    }
}

if (-not $venvFound) {
    Write-Host "No venv detected. Creating venv and installing requirements if available..."
    & $pythonExe -m venv venv
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create virtual environment. Check Python installation and permissions."
        pause
        exit 1
    }
    $venvFound = (Resolve-Path 'venv').ProviderPath
}

Write-Host "Activating venv at: $venvFound"
. (Join-Path $venvFound 'Scripts\Activate.ps1')

Write-Host "Upgrading pip and installing requirements (if any)..."
& $pythonExe -m pip install --upgrade pip
if (Test-Path 'requirements.txt') {
    & $pythonExe -m pip install -r requirements.txt
}

Write-Host "Starting Streamlit in a new window..."
Start-Process -FilePath $pythonExe -ArgumentList '-m','streamlit','run','streamlit_app.py','--server.port','8501','--server.headless','true' -WindowStyle Normal

Write-Host "Waiting 4 seconds then opening http://localhost:8501 ..."
Start-Sleep -Seconds 4
Start-Process 'http://localhost:8501'

Write-Host "Done. If there are errors above, please copy them and send to me. Press any key to exit."
pause

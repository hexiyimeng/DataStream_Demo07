# scripts/install_gpu.ps1
# BrainFlow GPU/Cellpose optional dependency installer - Windows PowerShell
# 用法: .\scripts\install_gpu.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$ReqFile = Join-Path $BackendDir "requirements-gpu.txt"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow GPU Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Test-Path $ReqFile)) {
    Write-Warning "requirements-gpu.txt not found at $ReqFile"
    Write-Host "This file should be in the backend/ directory." -ForegroundColor Yellow
    exit 0
}

$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-Error "Python venv not found at $VenvDir. Run .\scripts\setup.ps1 first."
    exit 1
}

Write-Host "Installing GPU/Cellpose dependencies..." -ForegroundColor Yellow
& $PythonExe -m pip install -r $ReqFile
Write-Host ""
Write-Host "GPU deps installed." -ForegroundColor Green
Write-Host ""
Write-Host "Note: Restart the backend (\.\scripts\start.ps1) if it is running." -ForegroundColor Cyan
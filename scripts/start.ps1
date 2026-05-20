# scripts/start.ps1
# BrainFlow backend startup script - Windows PowerShell
# 用法: .\scripts\start.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$DistIndex = Join-Path $BackendDir "dist\index.html"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Backend" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check venv
if (-not (Test-Path $PythonExe)) {
    Write-Error "Python not found at $PythonExe"
    Write-Host "Run .\scripts\setup.ps1 first." -ForegroundColor Yellow
    exit 1
}

# Check dist
if (-not (Test-Path $DistIndex)) {
    Write-Warning "backend/dist/index.html not found."
    Write-Host "The backend will start but the frontend UI will not be available." -ForegroundColor Yellow
    Write-Host "Run .\scripts\build_frontend.ps1 to build the frontend." -ForegroundColor Yellow
    Write-Host ""
}

Write-Host "Starting backend on http://localhost:8000" -ForegroundColor Green
Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
Write-Host ""

# Single-process uvicorn via main.py
& $PythonExe (Join-Path $BackendDir "main.py")
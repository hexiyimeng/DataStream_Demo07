# scripts/update.ps1
# BrainFlow update script - pull latest code and rebuild - Windows PowerShell
# 用法: .\scripts\update.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$FrontendDir = Join-Path $ProjectRoot "frontend"
$ReqFile = Join-Path $BackendDir "requirements.txt"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Update" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Git pull
Write-Host "[1/4] Pulling latest code..." -ForegroundColor Yellow
git pull
if ($LASTEXITCODE -ne 0) { Write-Warning "git pull had issues, continuing anyway..." }

# 2. Update backend deps
Write-Host "[2/4] Updating backend dependencies..." -ForegroundColor Yellow
if (Test-Path $ReqFile) {
    & $PythonExe -m pip install -r $ReqFile --upgrade --quiet
    Write-Host "  Backend deps updated." -ForegroundColor Green
} else {
    Write-Warning "  requirements.txt not found, skipping."
}

# 3. Update frontend deps
Write-Host "[3/4] Updating frontend dependencies..." -ForegroundColor Yellow
if (Test-Path $FrontendDir) {
    Push-Location $FrontendDir
    try {
        npm install
        Write-Host "  Frontend deps updated." -ForegroundColor Green
    } finally {
        Pop-Location
    }
} else {
    Write-Warning "  frontend/ not found, skipping."
}

# 4. Rebuild frontend
Write-Host "[4/4] Rebuilding frontend..." -ForegroundColor Yellow
& (Join-Path $ProjectRoot "scripts\build_frontend.ps1")

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Update complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Please restart the backend:" -ForegroundColor Cyan
Write-Host "  .\scripts\start.ps1" -ForegroundColor Gray
Write-Host ""
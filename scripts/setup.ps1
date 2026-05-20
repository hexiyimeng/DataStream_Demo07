# scripts/setup.ps1
# BrainFlow 基础安装脚本 - Windows PowerShell
# 用法: .\scripts\setup.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$VenvDir = Join-Path $ProjectRoot ".venv"
$FrontendDir = Join-Path $ProjectRoot "frontend"
$BackendDir = Join-Path $ProjectRoot "backend"
$ReqFile = Join-Path $BackendDir "requirements.txt"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Python venv
Write-Host "[1/5] Setting up Python virtual environment..." -ForegroundColor Yellow
if (Test-Path $VenvDir) {
    Write-Host "  .venv already exists, skipping creation."
} else {
    Write-Host "  Creating .venv..."
    python -m venv $VenvDir
}
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

# 2. Upgrade pip
Write-Host "[2/5] Upgrading pip..." -ForegroundColor Yellow
& $PythonExe -m pip install --upgrade pip --quiet

# 3. Install backend requirements
Write-Host "[3/5] Installing backend dependencies..." -ForegroundColor Yellow
if (Test-Path $ReqFile) {
    & $PythonExe -m pip install -r $ReqFile --quiet
    Write-Host "  Backend deps installed." -ForegroundColor Green
} else {
    Write-Warning "  requirements.txt not found at $ReqFile, skipping."
}

# 4. Install frontend deps
Write-Host "[4/5] Installing frontend dependencies..." -ForegroundColor Yellow
if (Test-Path $FrontendDir) {
    Push-Location $FrontendDir
    try {
        $LockFile = Join-Path $FrontendDir "package-lock.json"
        if (Test-Path $LockFile) {
            Write-Host "  Using npm ci (package-lock.json found)..."
            npm ci 2>$err
        } else {
            Write-Host "  No package-lock.json, using npm install..."
            npm install 2>$err
        }
        Write-Host "  Frontend deps installed." -ForegroundColor Green
    } catch {
        Write-Warning "  npm install/ci failed: $_"
    } finally {
        Pop-Location
    }
} else {
    Write-Warning "  frontend/ not found at $FrontendDir, skipping."
}

# 5. Build frontend
Write-Host "[5/5] Building frontend..." -ForegroundColor Yellow
$BuildScript = Join-Path $ProjectRoot "scripts\build_frontend.ps1"
if (Test-Path $BuildScript) {
    & $BuildScript
} else {
    Write-Warning "  build_frontend.ps1 not found, skipping build."
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Setup complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  .\scripts\start.ps1          # Start backend"
Write-Host "  .\scripts\install_gpu.ps1    # (Optional) Install GPU/Cellpose deps"
Write-Host ""
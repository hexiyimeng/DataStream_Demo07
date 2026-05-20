# scripts/dev.ps1
# BrainFlow development mode launcher - Windows PowerShell
# 用法: .\scripts\dev.ps1
# Opens both backend and frontend dev servers in separate windows.
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$FrontendDir = Join-Path $ProjectRoot "frontend"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Dev Mode" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Test-Path $PythonExe)) {
    Write-Error "Python venv not found at $VenvDir. Run .\scripts\setup.ps1 first."
    exit 1
}
if (-not (Test-Path (Join-Path $FrontendDir "package.json"))) {
    Write-Error "frontend/ not found at $FrontendDir"
    exit 1
}

Write-Host "Starting two terminals:" -ForegroundColor Yellow
Write-Host ""
Write-Host "  [Terminal 1] Backend:  .venv\Scripts\python.exe backend\main.py" -ForegroundColor Gray
Write-Host "  [Terminal 2] Frontend: cd frontend; npm run dev" -ForegroundColor Gray
Write-Host ""
Write-Host "Frontend dev server connects to http://localhost:8000" -ForegroundColor Cyan
Write-Host ""

# Start backend in new window
$BackendJob = Start-Process powershell -ArgumentList `
    "-NoExit", "-Command",
    "Write-Host 'BrainFlow Backend' -ForegroundColor Cyan; Write-Host ''; & '$PythonExe' '$BackendDir\main.py'" `
    -PassThru -WindowStyle Normal

# Start frontend in new window
$FrontendJob = Start-Process powershell -ArgumentList `
    "-NoExit", "-Command",
    "Write-Host 'BrainFlow Frontend (Vite Dev Server)' -ForegroundColor Cyan; Write-Host ''; Write-Host 'Connecting to http://localhost:8000'; Write-Host ''; cd '$FrontendDir'; npm run dev" `
    -PassThru -WindowStyle Normal

Write-Host "Backend window PID: $($BackendJob.Id)" -ForegroundColor Gray
Write-Host "Frontend window PID: $($FrontendJob.Id)" -ForegroundColor Gray
Write-Host ""
Write-Host "Close these windows or stop the processes to exit dev mode." -ForegroundColor Yellow
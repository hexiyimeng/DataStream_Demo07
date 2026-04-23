$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$frontendPath = Join-Path $projectRoot "frontend"
$backendPath = Join-Path $projectRoot "backend"
$backendPython = Join-Path $backendPath "venv\Scripts\python.exe"

if (-not (Test-Path $frontendPath)) {
    Write-Error "Frontend directory not found: $frontendPath"
    exit 1
}

if (-not (Test-Path $backendPath)) {
    Write-Error "Backend directory not found: $backendPath"
    exit 1
}

if (-not (Test-Path $backendPython)) {
    Write-Error "Backend Python not found: $backendPython"
    exit 1
}

$frontendCommand = "Set-Location `"$frontendPath`"; npm run dev"
$backendCommand = "Set-Location `"$backendPath`"; & `"$backendPython`" main.py"

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-Command", $frontendCommand
)

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-Command", $backendCommand
)

Write-Host "Frontend and backend are starting in separate windows..."

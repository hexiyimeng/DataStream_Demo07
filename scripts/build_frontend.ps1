# scripts/build_frontend.ps1
# BrainFlow frontend build script - Windows PowerShell
# 用法: .\scripts\build_frontend.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$FrontendDir = Join-Path $ProjectRoot "frontend"
$DistDir = Join-Path $ProjectRoot "backend\dist"

Write-Host ""
Write-Host "[Frontend] Building..." -ForegroundColor Yellow

if (-not (Test-Path $FrontendDir)) {
    Write-Error "frontend/ directory not found at $FrontendDir"
    exit 1
}

Push-Location $FrontendDir
try {
    npm run build
} catch {
    Write-Error "npm run build failed: $_"
    exit 1
} finally {
    Pop-Location
}

# Verify output
$IndexHtml = Join-Path $DistDir "index.html"
if (-not (Test-Path $IndexHtml)) {
    Write-Error "Build succeeded but backend/dist/index.html not found at $IndexHtml"
    Write-Error "Check frontend/vite.config.ts build.outDir setting."
    exit 1
}

Write-Host ""
Write-Host "Frontend built successfully." -ForegroundColor Green
Write-Host "Output: $IndexHtml" -ForegroundColor Cyan
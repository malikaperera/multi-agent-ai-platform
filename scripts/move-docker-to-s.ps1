# Move Docker Desktop WSL data from C:\Users\amali\Documents\Docker to S:\Docker
# Run as Administrator in PowerShell

$ErrorActionPreference = "Stop"

$src      = "$env:USERPROFILE\Documents\Docker\DockerDesktopWSL"
$dst      = "S:\Docker\DockerDesktopWSL"
$settings = "$env:APPDATA\Docker\settings-store.json"

Write-Host "=== Docker WSL Data Migration ===" -ForegroundColor Cyan
Write-Host "From: $src"
Write-Host "To:   $dst"
Write-Host ""

# 1. Quit Docker Desktop
Write-Host "Stopping Docker Desktop..." -ForegroundColor Yellow
Get-Process "Docker Desktop" -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 5

# Also stop the WSL distros Docker uses
wsl --terminate docker-desktop      2>$null
wsl --terminate docker-desktop-data 2>$null
Start-Sleep -Seconds 3
Write-Host "  Stopped." -ForegroundColor Green

# 2. Move the data
Write-Host "Moving $src -> $dst (26 GB, may take a few minutes)..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
Move-Item -Path $src -Destination $dst -Force
Write-Host "  Moved." -ForegroundColor Green

# 3. Patch settings-store.json
Write-Host "Updating Docker Desktop settings..." -ForegroundColor Yellow
$json = Get-Content $settings -Raw | ConvertFrom-Json
$json.CustomWslDistroDir = $dst
$json | ConvertTo-Json -Depth 20 | Set-Content $settings -Encoding UTF8
Write-Host "  CustomWslDistroDir = $dst" -ForegroundColor Green

# 4. Start Docker Desktop
Write-Host "Starting Docker Desktop..." -ForegroundColor Yellow
$exe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
if (Test-Path $exe) {
    Start-Process $exe
    Write-Host "  Started. Wait ~30s for Docker to come up, then run: docker compose up -d" -ForegroundColor Green
} else {
    Write-Host "  Could not find Docker Desktop.exe — start it manually." -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Cyan
Write-Host "C drive freed: ~26 GB"

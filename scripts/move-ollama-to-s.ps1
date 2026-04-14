# Move Ollama model storage from C drive to S drive
# Run as: powershell -ExecutionPolicy Bypass -File scripts\move-ollama-to-s.ps1
# Run this from an elevated PowerShell (Run as Administrator)

$ErrorActionPreference = "Stop"

$src  = "$env:USERPROFILE\.ollama\models"
$dst  = "S:\AI\ollama\models"
$dstParent = "S:\AI\ollama"

Write-Host "=== Ollama Model Migration ===" -ForegroundColor Cyan
Write-Host "From: $src"
Write-Host "To:   $dst"
Write-Host ""

# 1. Stop Ollama
Write-Host "Stopping Ollama..." -ForegroundColor Yellow
try {
    Stop-Process -Name "ollama" -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Write-Host "  Ollama stopped." -ForegroundColor Green
} catch {
    Write-Host "  Ollama was not running (ok)." -ForegroundColor Gray
}

# 2. Create destination
Write-Host "Creating destination: $dstParent" -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path $dstParent | Out-Null
Write-Host "  Done." -ForegroundColor Green

# 3. Move models
if (Test-Path $src) {
    $sizeMB = [math]::Round((Get-ChildItem $src -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB)
    Write-Host "Moving $sizeMB MB of models (this may take a few minutes)..." -ForegroundColor Yellow
    Move-Item -Path $src -Destination $dst -Force
    Write-Host "  Models moved." -ForegroundColor Green
} else {
    Write-Host "  No models directory found at $src — nothing to move." -ForegroundColor Gray
    New-Item -ItemType Directory -Force -Path $dst | Out-Null
}

# 4. Set OLLAMA_MODELS environment variable (user-level, persists across reboots)
Write-Host "Setting OLLAMA_MODELS environment variable..." -ForegroundColor Yellow
[System.Environment]::SetEnvironmentVariable("OLLAMA_MODELS", $dst, "User")
$env:OLLAMA_MODELS = $dst
Write-Host "  OLLAMA_MODELS=$dst" -ForegroundColor Green

# 5. Restart Ollama
Write-Host "Starting Ollama..." -ForegroundColor Yellow
$ollamaExe = "$env:LOCALAPPDATA\Programs\Ollama\ollama.exe"
if (Test-Path $ollamaExe) {
    Start-Process $ollamaExe
    Start-Sleep -Seconds 3
    Write-Host "  Ollama started." -ForegroundColor Green
} else {
    Write-Host "  Could not find ollama.exe at $ollamaExe — start it manually." -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Cyan
Write-Host "Models are now at: $dst"
Write-Host "Environment variable OLLAMA_MODELS set permanently."
Write-Host ""
Write-Host "Verify with: ollama list" -ForegroundColor Gray

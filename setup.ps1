# PowerShell Setup Script for Observability Gap Reproduction
# Uses script-owned disposable repositories inside .repro/
$ErrorActionPreference = "Stop"

$TOKIO_SHA = "b2636752450484955e7ad334bac678424d51bc4a"
$DIAL9_SHA = "33b2d780628b42251047909ff2b88fdb97e3c28b"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ReproDir = Join-Path $ScriptDir ".repro"

if (-not (Test-Path $ReproDir)) {
    New-Item -ItemType Directory -Force -Path $ReproDir | Out-Null
}

Write-Host "=== 1. Setting up .repro/tokio with ground-truth patch ==="
$TokioDir = Join-Path $ReproDir "tokio"
if (-not (Test-Path $TokioDir)) {
    Write-Host "Cloning Tokio repository at $TOKIO_SHA..."
    git clone https://github.com/tokio-rs/tokio.git $TokioDir
}

Push-Location $TokioDir
Write-Host "Checking out exact tested SHA: $TOKIO_SHA..."
git checkout -f $TOKIO_SHA
git clean -fd

Write-Host "Applying minimal ground-truth instrumentation patch..."
$PatchPath = Join-Path $ScriptDir "patches\tokio-ground-truth.patch"
git apply $PatchPath
Pop-Location

Write-Host "=== 2. Setting up .repro/dial9 ==="
$Dial9Dir = Join-Path $ReproDir "dial9"
if (-not (Test-Path $Dial9Dir)) {
    Write-Host "Cloning Dial9 repository at $DIAL9_SHA..."
    git clone https://github.com/dial9-rs/dial9.git $Dial9Dir
}

Push-Location $Dial9Dir
Write-Host "Checking out exact tested SHA: $DIAL9_SHA..."
git checkout -f $DIAL9_SHA
git clean -fd
Pop-Location

Write-Host "=== 3. Verifying tokio-test-app build ==="
Push-Location $ScriptDir
cargo check
Pop-Location

Write-Host "=== Setup complete! ==="
Write-Host "Run the test suite with: cargo run --release"

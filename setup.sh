#!/usr/bin/env bash
set -euo pipefail

# Tokio and Dial9 Setup Script for Observability Gap Reproduction
# Uses script-owned disposable repositories inside .repro/
TOKIO_SHA="b2636752450484955e7ad334bac678424d51bc4a"
DIAL9_SHA="33b2d780628b42251047909ff2b88fdb97e3c28b"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPRO_DIR="${SCRIPT_DIR}/.repro"
mkdir -p "${REPRO_DIR}"

echo "=== 1. Setting up .repro/tokio with ground-truth patch ==="
if [ ! -d "${REPRO_DIR}/tokio" ]; then
    echo "Cloning Tokio repository at ${TOKIO_SHA}..."
    git clone https://github.com/tokio-rs/tokio.git "${REPRO_DIR}/tokio"
fi

cd "${REPRO_DIR}/tokio"
echo "Checking out exact tested SHA: ${TOKIO_SHA}..."
git checkout -f "${TOKIO_SHA}"
git clean -fd

echo "Applying minimal ground-truth instrumentation patch..."
git apply "${SCRIPT_DIR}/patches/tokio-ground-truth.patch"

echo "=== 2. Setting up .repro/dial9 ==="
if [ ! -d "${REPRO_DIR}/dial9" ]; then
    echo "Cloning Dial9 repository at ${DIAL9_SHA}..."
    git clone https://github.com/dial9-rs/dial9.git "${REPRO_DIR}/dial9"
fi

cd "${REPRO_DIR}/dial9"
echo "Checking out exact tested SHA: ${DIAL9_SHA}..."
git checkout -f "${DIAL9_SHA}"
git clean -fd

echo "=== 3. Verifying tokio-test-app build ==="
cd "${SCRIPT_DIR}"
if ! command -v cargo >/dev/null 2>&1 && [ -f "${HOME}/.cargo/env" ]; then
    # Source Rust toolchain environment if installed via rustup
    . "${HOME}/.cargo/env"
fi
cargo check

echo "=== Setup complete! ==="
echo "Run the test suite with: cargo run --release"

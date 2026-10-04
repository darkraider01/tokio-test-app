#!/usr/bin/env bash
set -euo pipefail

# Tokio and Dial9 Setup Script for Observability Gap Reproduction
# Exact tested commits:
TOKIO_SHA="b2636752450484955e7ad334bac678424d51bc4a"
DIAL9_SHA="33b2d780628b42251047909ff2b88fdb97e3c28b"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== 1. Setting up tokio-probe with ground-truth patch ==="
if [ ! -d "${PARENT_DIR}/tokio-probe" ]; then
    echo "Cloning Tokio repository at ${TOKIO_SHA}..."
    git clone https://github.com/tokio-rs/tokio.git "${PARENT_DIR}/tokio-probe"
fi

cd "${PARENT_DIR}/tokio-probe"
echo "Checking out exact tested SHA: ${TOKIO_SHA}..."
git checkout -f "${TOKIO_SHA}"
git clean -fd

echo "Applying minimal ground-truth instrumentation patch..."
git apply "${SCRIPT_DIR}/patches/tokio-ground-truth.patch"

echo "=== 2. Setting up Dial9 ==="
if [ ! -d "${PARENT_DIR}/dial9" ]; then
    echo "Cloning Dial9 repository at ${DIAL9_SHA}..."
    git clone https://github.com/dial9-ai/dial9.git "${PARENT_DIR}/dial9"
fi

cd "${PARENT_DIR}/dial9"
echo "Checking out exact tested SHA: ${DIAL9_SHA}..."
git checkout -f "${DIAL9_SHA}"

echo "=== 3. Verifying tokio-test-app build ==="
cd "${SCRIPT_DIR}"
cargo check

echo "=== Setup complete! ==="
echo "Run the test suite with: cargo run --release"

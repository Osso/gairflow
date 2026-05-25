#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
cargo install --path . --locked

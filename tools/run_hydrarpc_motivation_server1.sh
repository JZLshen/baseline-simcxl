#!/usr/bin/env bash
set -euo pipefail

SERVER_TAG="server1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_motivation_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_dedicated_respsize 8192
run_dedicated_respsize 8
run_dedicated_sparse32 20 15
run_dedicated_sparse32 16 15
run_dedicated_sparse32 4 30
run_dedicated_sparse32 4 8

print_motivation_footer

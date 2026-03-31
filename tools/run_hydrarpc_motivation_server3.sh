#!/usr/bin/env bash
set -euo pipefail

SERVER_TAG="server3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_motivation_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_dedicated_respsize 4096
run_dedicated_respsize 256
run_dedicated_sparse32 24 15
run_dedicated_sparse32 20 8
run_dedicated_sparse32 8 30

print_motivation_footer

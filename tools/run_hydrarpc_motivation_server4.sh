#!/usr/bin/env bash
set -euo pipefail

SERVER_TAG="server4"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_motivation_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image
ensure_coherent_image

run_dedicated_cc_staging_pow2
run_dedicated_respsize 1024
run_dedicated_sparse32 28 15
run_dedicated_sparse32 28 8
run_dedicated_sparse32 4 15
run_dedicated_sparse32 8 8

print_motivation_footer

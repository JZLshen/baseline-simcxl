#!/usr/bin/env bash
set -euo pipefail

SERVER_TAG="server5"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_motivation_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_dedicated_noncc_direct_pow2
run_dedicated_noncc_staging_pow2
run_dedicated_sparse32 24 30
run_dedicated_sparse32 16 30
run_dedicated_sparse32 16 8

print_motivation_footer

#!/usr/bin/env bash
set -euo pipefail

SERVER_TAG="server2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_motivation_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_shared_image
ensure_dedicated_image

run_shared_competition_pow2
run_dedicated_sparse32 28 30
run_dedicated_sparse32 20 30
run_dedicated_sparse32 24 8
run_dedicated_sparse32 8 15

print_motivation_footer

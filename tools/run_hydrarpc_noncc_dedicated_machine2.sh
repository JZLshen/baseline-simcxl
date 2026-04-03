#!/usr/bin/env bash
set -euo pipefail

MACHINE_TAG="machine2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_noncc_dedicated_shard_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_case c16_overall_req38_resp230 \
  --client-count 16 \
  --num-cpus 18 \
  --checkpoint-id atomic_n18 \
  --req-bytes 38 \
  --resp-bytes 230

run_case c16_noncc_direct_req64_resp64 \
  --client-count 16 \
  --num-cpus 18 \
  --checkpoint-id atomic_n18 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct

run_case c32_overall_req38_resp230 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 38 \
  --resp-bytes 230

run_case c32_reqsize_req8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 8 \
  --resp-bytes 64

run_case c32_ringsize_s128 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slot-count 128

run_case c32_sparse_sc4_sq8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 4 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc20_sq8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 20 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000

print_footer

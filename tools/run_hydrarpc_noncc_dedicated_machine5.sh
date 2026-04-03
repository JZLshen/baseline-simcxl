#!/usr/bin/env bash
set -euo pipefail

MACHINE_TAG="machine5"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_noncc_dedicated_shard_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_case c4_overall_req64_resp64 \
  --client-count 4 \
  --num-cpus 6 \
  --checkpoint-id atomic_n6 \
  --req-bytes 64 \
  --resp-bytes 64

run_case c4_overall_req1530_resp315 \
  --client-count 4 \
  --num-cpus 6 \
  --checkpoint-id atomic_n6 \
  --req-bytes 1530 \
  --resp-bytes 315

run_case c4_overall_req38_resp230 \
  --client-count 4 \
  --num-cpus 6 \
  --checkpoint-id atomic_n6 \
  --req-bytes 38 \
  --resp-bytes 230

run_case c4_noncc_direct_req64_resp64 \
  --client-count 4 \
  --num-cpus 6 \
  --checkpoint-id atomic_n6 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct

run_case c32_reqsize_req256 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 256 \
  --resp-bytes 64

run_case c32_respsize_resp4096 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 4096

run_case c32_ringsize_s32 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slot-count 32

run_case c32_sparse_sc8_sq8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 8 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc16_sq8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 16 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc28_sq8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 28 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000

print_footer

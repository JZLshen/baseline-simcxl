#!/usr/bin/env bash
set -euo pipefail

MACHINE_TAG="machine6"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_noncc_dedicated_shard_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_case c1_overall_req64_resp64 \
  --client-count 1 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 64 \
  --resp-bytes 64

run_case c1_overall_req1530_resp315 \
  --client-count 1 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 1530 \
  --resp-bytes 315

run_case c1_overall_req38_resp230 \
  --client-count 1 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 38 \
  --resp-bytes 230

run_case c1_noncc_direct_req64_resp64 \
  --client-count 1 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct

run_case c2_overall_req64_resp64 \
  --client-count 2 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 64 \
  --resp-bytes 64

run_case c2_overall_req1530_resp315 \
  --client-count 2 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 1530 \
  --resp-bytes 315

run_case c2_overall_req38_resp230 \
  --client-count 2 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 38 \
  --resp-bytes 230

run_case c2_noncc_direct_req64_resp64 \
  --client-count 2 \
  --num-cpus 4 \
  --checkpoint-id atomic_n4 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct

run_case c32_reqsize_req1024 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 1024 \
  --resp-bytes 64

run_case c32_respsize_resp256 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 256

run_case c32_ringsize_s256 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slot-count 256

run_case c32_sparse_sc28_sq15 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 28 \
  --slow-count-per-client 15 \
  --slow-send-gap-ns 20000

print_footer

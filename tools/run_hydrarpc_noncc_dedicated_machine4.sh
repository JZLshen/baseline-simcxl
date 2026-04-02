#!/usr/bin/env bash
set -euo pipefail

MACHINE_TAG="machine4"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_noncc_dedicated_shard_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_case c8_overall_req38_resp230 \
  --client-count 8 \
  --num-cpus 10 \
  --checkpoint-id atomic_n10 \
  --req-bytes 38 \
  --resp-bytes 230

run_case c8_noncc_direct_req64_resp64 \
  --client-count 8 \
  --num-cpus 10 \
  --checkpoint-id atomic_n10 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct

run_case c32_noncc_direct_req64_resp64 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct

run_case c32_reqsize_req4096 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 4096 \
  --resp-bytes 64

run_case c32_respsize_resp8192 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 8192

run_case c32_ringsize_s512 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slot-count 512

run_case c32_sparse_sc16_sq15 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 16 \
  --slow-count-per-client 15 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc20_sq15 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 20 \
  --slow-count-per-client 15 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc24_sq8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 24 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000

print_footer

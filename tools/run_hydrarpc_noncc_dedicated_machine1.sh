#!/usr/bin/env bash
set -euo pipefail

MACHINE_TAG="machine1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_noncc_dedicated_shard_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_case c16_overall_req64_resp64 \
  --client-count 16 \
  --num-cpus 18 \
  --checkpoint-id atomic_n18 \
  --req-bytes 64 \
  --resp-bytes 64

run_case c16_overall_req1530_resp315 \
  --client-count 16 \
  --num-cpus 18 \
  --checkpoint-id atomic_n18 \
  --req-bytes 1530 \
  --resp-bytes 315

run_case c32_overall_req64_resp64 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64

run_case c32_respsize_resp1024 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 1024

run_case c32_ringsize_s16 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slot-count 16

run_case c32_sparse_sc20_sq30 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 20 \
  --slow-count-per-client 30 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc8_sq15 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 8 \
  --slow-count-per-client 15 \
  --slow-send-gap-ns 20000

run_case c32_sparse_sc24_sq15 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 24 \
  --slow-count-per-client 15 \
  --slow-send-gap-ns 20000

print_footer

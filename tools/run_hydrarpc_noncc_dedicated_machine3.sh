#!/usr/bin/env bash
set -euo pipefail

MACHINE_TAG="machine3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/run_hydrarpc_noncc_dedicated_shard_common.sh"

cd "$REPO_ROOT"

ensure_build
ensure_dedicated_image

run_case c8_overall_req64_resp64 \
  --client-count 8 \
  --num-cpus 10 \
  --checkpoint-id atomic_n10 \
  --req-bytes 64 \
  --resp-bytes 64

run_case c8_overall_req1530_resp315 \
  --client-count 8 \
  --num-cpus 10 \
  --checkpoint-id atomic_n10 \
  --req-bytes 1530 \
  --resp-bytes 315

run_case c32_overall_req1530_resp315 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 1530 \
  --resp-bytes 315

run_case c32_reqsize_req8192 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 8192 \
  --resp-bytes 64

run_case c32_respsize_resp8 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 8

run_case c32_ringsize_s64 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slot-count 64

run_case c32_sparse_sc4_sq15 \
  --client-count 32 \
  --num-cpus 34 \
  --checkpoint-id atomic_n34 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 4 \
  --slow-count-per-client 15 \
  --slow-send-gap-ns 20000

print_footer

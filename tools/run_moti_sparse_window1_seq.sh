#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_moti_sparse_window1_seq.sh [options]

Run the dedicated sparse motivation sweep only, with window size 1:
  - 16 clients total
  - slow client counts: 1/2/4/8
  - slow request pct: 0/25/50/100
  - req64B / resp64B
  - count-per-client: 30
  - slot-count: 1024

The script writes per-case result.json files and builds the final drop1 table
automatically under the batch root.

Options:
  --checkpoint <dir>            Existing classic checkpoint for dedicated runs.
  --root <dir>                  Output root for this batch.
  --binary <path>               gem5 binary. Default: build/X86/gem5.opt
  --disk-image <path>           Guest disk image. Default: files/parsec.img
  --count-per-client <N>        Requests per client. Default: 30
  --window-size <N>             Window size. Default: 1
  --slot-count <N>              Ring size. Default: 1024
  --num-cpus <N>                Guest CPU count. Default: 34
  --slow-send-gap-ns <N>        Base sparse gap for 25/50 runs. Default: 20000
  --skip-gem5-build-check       Skip gem5 freshness check.
  --skip-image-setup            Reuse already injected guest binary.
  --continue-on-failure         Record failures and keep advancing the queue.
  --help                        Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATE_TAG="$(date +%Y%m%d)"
STAMP="$(date +%Y%m%d_%H%M%S)"

BINARY="build/X86/gem5.opt"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
COUNT_PER_CLIENT=30
WINDOW_SIZE=1
SLOT_COUNT=1024
NUM_CPUS=34
SLOW_SEND_GAP_NS=20000
SKIP_GEM5_BUILD_CHECK=0
SKIP_IMAGE_SETUP=0
CONTINUE_ON_FAILURE=0

CHECKPOINT_DEFAULT="${REPO_ROOT}/output/motivation_m2/checkpoints/classic_n34/latest"
CHECKPOINT="$CHECKPOINT_DEFAULT"
ROOT="${REPO_ROOT}/output/DEDICATED/_batches/${DATE_TAG}/noncc_dedicated_moti_sparse_window1_${STAMP}"
QUEUE_LOG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --checkpoint)
      CHECKPOINT="$2"
      shift 2
      ;;
    --root)
      ROOT="$2"
      shift 2
      ;;
    --binary)
      BINARY="$2"
      shift 2
      ;;
    --disk-image)
      DISK_IMAGE="$2"
      shift 2
      ;;
    --count-per-client)
      COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --window-size)
      WINDOW_SIZE="$2"
      shift 2
      ;;
    --slot-count)
      SLOT_COUNT="$2"
      shift 2
      ;;
    --num-cpus)
      NUM_CPUS="$2"
      shift 2
      ;;
    --slow-send-gap-ns)
      SLOW_SEND_GAP_NS="$2"
      shift 2
      ;;
    --skip-gem5-build-check)
      SKIP_GEM5_BUILD_CHECK=1
      shift 1
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
      shift 1
      ;;
    --continue-on-failure)
      CONTINUE_ON_FAILURE=1
      shift 1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

resolve_dir() {
  local path="$1"
  (cd "$path" && pwd)
}

log_msg() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" | tee -a "$QUEUE_LOG"
}

handle_case_failure() {
  local fail_csv="$1"
  local outdir="$2"
  local reason="$3"

  log_msg "FAIL outdir=${outdir} reason=${reason}"
  if [[ ! -f "$fail_csv" ]]; then
    printf 'client_count,count_per_client,outdir,error,suite\n' >"$fail_csv"
  fi
  printf '16,%s,%s,%s,%s\n' \
    "$COUNT_PER_CLIENT" \
    "$outdir" \
    "$reason" \
    "${outdir##*/}" >>"$fail_csv"
  if [[ "$CONTINUE_ON_FAILURE" -eq 0 ]]; then
    exit 1
  fi
}

ensure_gem5_binary() {
  if [[ "$SKIP_GEM5_BUILD_CHECK" -eq 1 ]]; then
    return 0
  fi

  bash tools/check_gem5_binary_freshness.sh \
    --binary "$BINARY" \
    --label "gem5 binary for moti sparse window1" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
}

ensure_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 ]]; then
    return 0
  fi
  bash tools/setup_hydrarpc_dedicated_disk_image.sh "$DISK_IMAGE"
}

slow_count_for_pct() {
  local pct="$1"
  case "$pct" in
    0) printf '0\n' ;;
    25) printf '8\n' ;;
    50) printf '15\n' ;;
    100) printf '%s\n' "$COUNT_PER_CLIENT" ;;
    *)
      echo "unsupported sparse pct: $pct" >&2
      return 1
      ;;
  esac
}

slow_gap_for_pct() {
  local pct="$1"
  case "$pct" in
    100) printf '0\n' ;;
    *) printf '%s\n' "$SLOW_SEND_GAP_NS" ;;
  esac
}

expected_total_requests() {
  local slow_client_count="$1"
  local slow_count_per_client="$2"
  printf '%s\n' \
    $(((16 - slow_client_count) * COUNT_PER_CLIENT + slow_client_count * slow_count_per_client))
}

run_summary() {
  local suite="$1"
  local outdir="$2"
  local log_path="$3"
  local summary_csv="$4"
  local steady_csv="$5"
  local fail_csv="$6"
  local expected_total="$7"
  local slow_client_count="$8"
  local slow_count_per_client="$9"
  local slow_request_pct="${10}"
  local slow_send_gap_ns="${11}"

  python3 tools/summarize_hydrarpc_multiclient.py \
    --log "$log_path" \
    --experiment "dedicated" \
    --client-count 16 \
    --count-per-client "$COUNT_PER_CLIENT" \
    --expected-total-requests "$expected_total" \
    --drop-first-per-client 2 \
    --csv "$summary_csv" \
    --steady-csv "$steady_csv" \
    --fail-csv "$fail_csv" \
    --outdir "$outdir" \
    --result-json "$outdir/result.json" \
    --extra-field "suite=$suite" \
    --extra-field "category=moti" \
    --extra-field "req_bytes=64" \
    --extra-field "resp_bytes=64" \
    --extra-field "req_min_bytes=64" \
    --extra-field "req_max_bytes=64" \
    --extra-field "resp_min_bytes=64" \
    --extra-field "resp_max_bytes=64" \
    --extra-field "slot_count=$SLOT_COUNT" \
    --extra-field "window_size=$WINDOW_SIZE" \
    --extra-field "slow_client_count=$slow_client_count" \
    --extra-field "slow_count_per_client=$slow_count_per_client" \
    --extra-field "slow_request_pct=$slow_request_pct" \
    --extra-field "slow_send_gap_ns=$slow_send_gap_ns"
}

run_case() {
  local root="$1"
  local summary_csv="$2"
  local steady_csv="$3"
  local fail_csv="$4"
  local slow_request_pct="$5"
  local slow_client_count="$6"
  local slow_count_per_client="$7"
  local slow_send_gap_ns="$8"
  local suite="sparse16_d${slow_request_pct}_c${slow_client_count}"
  local outdir="${root}/${suite}"
  local console_log="${outdir}/console.log"
  local runner_rc=0
  local expected_total=""

  mkdir -p "$outdir"
  log_msg "START suite=${suite} outdir=${outdir} slow_count=${slow_count_per_client} slow_gap_ns=${slow_send_gap_ns}"

  set +e
  bash tools/run_e2e_hydrarpc_dedicated.sh \
    --binary "$BINARY" \
    --cpu-type TIMING \
    --boot-cpu KVM \
    --client-count 16 \
    --count-per-client "$COUNT_PER_CLIENT" \
    --window-size "$WINDOW_SIZE" \
    --slot-count "$SLOT_COUNT" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --req-min-bytes 64 \
    --req-max-bytes 64 \
    --resp-min-bytes 64 \
    --resp-max-bytes 64 \
    --slow-client-count "$slow_client_count" \
    --slow-count-per-client "$slow_count_per_client" \
    --slow-send-gap-ns "$slow_send_gap_ns" \
    --request-transfer-mode staging \
    --response-transfer-mode staging \
    --num-cpus "$NUM_CPUS" \
    --restore-checkpoint "$CHECKPOINT" \
    --skip-build \
    --skip-image-setup \
    --outdir "$outdir" \
    >"$console_log" 2>&1
  runner_rc=$?
  set -e

  if [[ ! -f "$outdir/hydrarpc_dedicated.result.log" ]]; then
    handle_case_failure "$fail_csv" "$outdir" \
      "runner_rc=${runner_rc}: missing hydrarpc_dedicated.result.log"
    return 0
  fi

  expected_total="$(expected_total_requests "$slow_client_count" "$slow_count_per_client")"
  if ! run_summary \
      "$suite" "$outdir" "$outdir/hydrarpc_dedicated.result.log" \
      "$summary_csv" "$steady_csv" "$fail_csv" \
      "$expected_total" "$slow_client_count" "$slow_count_per_client" \
      "$slow_request_pct" "$slow_send_gap_ns"; then
    handle_case_failure "$fail_csv" "$outdir" "summary_failed"
    return 0
  fi

  if [[ "$runner_rc" -ne 0 ]]; then
    log_msg "SALVAGED suite=${suite} outdir=${outdir} runner_rc=${runner_rc}"
  fi
  log_msg "END suite=${suite} outdir=${outdir}"
}

main() {
  local moti_root=""
  local summary_csv=""
  local steady_csv=""
  local fail_csv=""
  local table_root=""
  local pct=""
  local sc=""
  local sq=""
  local sg=""

  if [[ ! -f "$DISK_IMAGE" ]]; then
    echo "disk image not found: $DISK_IMAGE" >&2
    exit 1
  fi

  CHECKPOINT="$(resolve_dir "$CHECKPOINT")"
  mkdir -p "$ROOT"
  ROOT="$(resolve_dir "$ROOT")"
  QUEUE_LOG="$ROOT/queue.log"
  : >"$QUEUE_LOG"

  moti_root="$ROOT/moti"
  summary_csv="$moti_root/summary.csv"
  steady_csv="$moti_root/steady_summary.csv"
  fail_csv="$moti_root/failures.csv"
  table_root="$ROOT/tables_drop1"

  mkdir -p "$moti_root" "$table_root"
  rm -f "$summary_csv" "$steady_csv" "$fail_csv"

  log_msg "checkpoint=$CHECKPOINT"
  log_msg "root=$ROOT"
  log_msg "count_per_client=$COUNT_PER_CLIENT window_size=$WINDOW_SIZE slot_count=$SLOT_COUNT num_cpus=$NUM_CPUS"

  ensure_gem5_binary
  ensure_image

  for pct in 0 25 50 100; do
    sq="$(slow_count_for_pct "$pct")"
    sg="$(slow_gap_for_pct "$pct")"
    for sc in 1 2 4 8; do
      run_case \
        "$moti_root" "$summary_csv" "$steady_csv" "$fail_csv" \
        "$pct" "$sc" "$sq" "$sg"
    done
  done

  python3 tools/build_moti_sparse_drop1_table.py \
    --moti-root "$moti_root" \
    --outdir "$table_root"

  log_msg "complete"
  printf 'root=%s\n' "$ROOT"
  printf 'drop1_table=%s\n' "$table_root/moti_sparse_polling_dedicated.csv"
  printf 'drop1_manifest=%s\n' "$table_root/source_manifest.csv"
}

main "$@"

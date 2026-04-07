#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_sparse_aligned_reruns_seq.sh [options]

Run only the sparse-point reruns that were invalidated by the aligned
slow-client semantics update, strictly one gem5 process at a time:
  - non-CC dedicated motivation sparse (16 clients, req64/resp64)
  - non-CC dedicated sensitivity sparse (16 clients, req38/resp230 uniform)
  - shared sensitivity sparse (16 clients, req38/resp230 uniform)

Defaults:
  - count-per-client: 30
  - window-size: 16
  - slot-count: 1024
  - slow-send-gap-ns: 20000
  - num-cpus: 34
  - strict sequential execution (no parallel gem5)

Options:
  --dedicated-checkpoint <dir>  Existing classic checkpoint for dedicated runs.
  --shared-checkpoint <dir>     Existing classic checkpoint for shared runs.
  --dedicated-root <dir>        Output root for dedicated reruns.
  --shared-root <dir>           Output root for shared reruns.
  --binary <path>               gem5 binary. Default: build/X86/gem5.opt
  --disk-image <path>           Guest disk image. Default: files/parsec.img
  --count-per-client <N>        Requests per client. Default: 30
  --window-size <N>             Shared window size. Default: 16
  --slot-count <N>              Ring size. Default: 1024
  --num-cpus <N>                Guest CPU count. Default: 34
  --slow-send-gap-ns <N>        Slow-client uniform send gap. Default: 20000
  --skip-gem5-build-check       Skip gem5 freshness check.
  --skip-dedicated-image-setup  Reuse already injected dedicated guest binary.
  --skip-shared-image-setup     Reuse already injected shared guest binary.
  --continue-on-failure         Record failures and keep advancing the queue.
  --help                        Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKUP_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
DATE_TAG="$(date +%Y%m%d)"
STAMP="$(date +%Y%m%d_%H%M%S)"

BINARY="build/X86/gem5.opt"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
COUNT_PER_CLIENT=30
WINDOW_SIZE=16
SLOT_COUNT=1024
NUM_CPUS=34
SLOW_SEND_GAP_NS=20000
SKIP_GEM5_BUILD_CHECK=0
SKIP_DEDICATED_IMAGE_SETUP=0
SKIP_SHARED_IMAGE_SETUP=0
CONTINUE_ON_FAILURE=0

DEDICATED_CHECKPOINT_DEFAULT="${REPO_ROOT}/output/motivation_m2/checkpoints/classic_n34/latest"
SHARED_CHECKPOINT_DEFAULT="${REPO_ROOT}/output/hydrarpc_shared_checkpoints/classic_n34_parsec1/cpt.67646739258991"

DEDICATED_CHECKPOINT="$DEDICATED_CHECKPOINT_DEFAULT"
SHARED_CHECKPOINT="$SHARED_CHECKPOINT_DEFAULT"
DEDICATED_ROOT="${REPO_ROOT}/output/DEDICATED/_batches/${DATE_TAG}/noncc_dedicated_sparse_aligned_seq_${STAMP}"
SHARED_ROOT="${BACKUP_ROOT}/SimCXL/output/SHARED/_batches/${DATE_TAG}/shared_sparse_aligned_seq_${STAMP}"
QUEUE_LOG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dedicated-checkpoint)
      DEDICATED_CHECKPOINT="$2"
      shift 2
      ;;
    --shared-checkpoint)
      SHARED_CHECKPOINT="$2"
      shift 2
      ;;
    --dedicated-root)
      DEDICATED_ROOT="$2"
      shift 2
      ;;
    --shared-root)
      SHARED_ROOT="$2"
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
    --skip-dedicated-image-setup)
      SKIP_DEDICATED_IMAGE_SETUP=1
      shift 1
      ;;
    --skip-shared-image-setup)
      SKIP_SHARED_IMAGE_SETUP=1
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

expected_total_requests() {
  local client_count="$1"
  local slow_client_count="$2"
  local slow_count_per_client="$3"
  printf '%s\n' \
    $(((client_count - slow_client_count) * COUNT_PER_CLIENT + slow_client_count * slow_count_per_client))
}

slow_count_for_pct() {
  local pct="$1"
  case "$pct" in
    0) printf '0\n' ;;
    25) printf '8\n' ;;
    50) printf '15\n' ;;
    *)
      echo "unsupported sparse pct: $pct" >&2
      return 1
      ;;
  esac
}

log_msg() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" | tee -a "$QUEUE_LOG"
}

record_manual_failure() {
  local fail_csv="$1"
  local experiment="$2"
  local suite="$3"
  local outdir="$4"
  local reason="$5"

  if [[ ! -f "$fail_csv" ]]; then
    printf 'experiment,client_count,count_per_client,outdir,log_path,error,suite\n' >"$fail_csv"
  fi
  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$experiment" \
    "16" \
    "$COUNT_PER_CLIENT" \
    "$outdir" \
    "" \
    "$reason" \
    "$suite" \
    >>"$fail_csv"
}

handle_case_failure() {
  local fail_csv="$1"
  local experiment="$2"
  local suite="$3"
  local outdir="$4"
  local reason="$5"

  log_msg "FAIL experiment=${experiment} suite=${suite} outdir=${outdir} reason=${reason}"
  record_manual_failure "$fail_csv" "$experiment" "$suite" "$outdir" "$reason"
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
    --label "gem5 binary for sparse aligned reruns" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
}

ensure_dedicated_image() {
  if [[ "$SKIP_DEDICATED_IMAGE_SETUP" -eq 1 ]]; then
    return 0
  fi
  bash tools/setup_hydrarpc_dedicated_disk_image.sh "$DISK_IMAGE"
}

ensure_shared_image() {
  if [[ "$SKIP_SHARED_IMAGE_SETUP" -eq 1 ]]; then
    return 0
  fi
  bash tools/setup_hydrarpc_shared_disk_image.sh "$DISK_IMAGE"
}

run_summary() {
  local experiment="$1"
  local suite="$2"
  local outdir="$3"
  local log_path="$4"
  local summary_csv="$5"
  local steady_csv="$6"
  local fail_csv="$7"
  local client_count="$8"
  local expected_total="$9"
  local req_bytes="${10}"
  local resp_bytes="${11}"
  local req_min_bytes="${12}"
  local req_max_bytes="${13}"
  local resp_min_bytes="${14}"
  local resp_max_bytes="${15}"
  local slow_client_count="${16}"
  local slow_count_per_client="${17}"
  local slow_request_pct="${18}"

  python3 tools/summarize_hydrarpc_multiclient.py \
    --log "$log_path" \
    --experiment "$experiment" \
    --client-count "$client_count" \
    --count-per-client "$COUNT_PER_CLIENT" \
    --expected-total-requests "$expected_total" \
    --drop-first-per-client 2 \
    --csv "$summary_csv" \
    --steady-csv "$steady_csv" \
    --fail-csv "$fail_csv" \
    --outdir "$outdir" \
    --result-json "$outdir/result.json" \
    --extra-field "suite=$suite" \
    --extra-field "category=${suite%%/*}" \
    --extra-field "req_bytes=$req_bytes" \
    --extra-field "resp_bytes=$resp_bytes" \
    --extra-field "req_min_bytes=$req_min_bytes" \
    --extra-field "req_max_bytes=$req_max_bytes" \
    --extra-field "resp_min_bytes=$resp_min_bytes" \
    --extra-field "resp_max_bytes=$resp_max_bytes" \
    --extra-field "slot_count=$SLOT_COUNT" \
    --extra-field "window_size=$WINDOW_SIZE" \
    --extra-field "slow_client_count=$slow_client_count" \
    --extra-field "slow_count_per_client=$slow_count_per_client" \
    --extra-field "slow_request_pct=$slow_request_pct" \
    --extra-field "slow_send_gap_ns=$SLOW_SEND_GAP_NS"
}

run_dedicated_case() {
  local suite="$1"
  local outdir="$2"
  local req_bytes="$3"
  local resp_bytes="$4"
  local req_min_bytes="$5"
  local req_max_bytes="$6"
  local resp_min_bytes="$7"
  local resp_max_bytes="$8"
  local slow_request_pct="$9"
  local slow_client_count="${10}"
  local slow_count_per_client="${11}"
  local summary_csv="${12}"
  local steady_csv="${13}"
  local fail_csv="${14}"
  local console_log="$outdir/console.log"
  local runner_rc=0
  local expected_total=""

  mkdir -p "$outdir"
  log_msg "START dedicated suite=${suite} outdir=${outdir}"

  set +e
  bash tools/run_e2e_hydrarpc_dedicated.sh \
    --binary "$BINARY" \
    --cpu-type TIMING \
    --boot-cpu KVM \
    --client-count 16 \
    --count-per-client "$COUNT_PER_CLIENT" \
    --window-size "$WINDOW_SIZE" \
    --slot-count "$SLOT_COUNT" \
    --req-bytes "$req_bytes" \
    --resp-bytes "$resp_bytes" \
    --req-min-bytes "$req_min_bytes" \
    --req-max-bytes "$req_max_bytes" \
    --resp-min-bytes "$resp_min_bytes" \
    --resp-max-bytes "$resp_max_bytes" \
    --slow-client-count "$slow_client_count" \
    --slow-count-per-client "$slow_count_per_client" \
    --slow-send-gap-ns "$SLOW_SEND_GAP_NS" \
    --request-transfer-mode staging \
    --response-transfer-mode staging \
    --num-cpus "$NUM_CPUS" \
    --restore-checkpoint "$DEDICATED_CHECKPOINT" \
    --skip-build \
    --skip-image-setup \
    --outdir "$outdir" \
    >"$console_log" 2>&1
  runner_rc=$?
  set -e

  if [[ ! -f "$outdir/hydrarpc_dedicated.result.log" ]]; then
    handle_case_failure "$fail_csv" "dedicated" "$suite" "$outdir" \
      "runner_rc=${runner_rc}: missing hydrarpc_dedicated.result.log"
    return 0
  fi

  expected_total="$(expected_total_requests 16 "$slow_client_count" "$slow_count_per_client")"
  if ! run_summary \
      "dedicated" "$suite" "$outdir" "$outdir/hydrarpc_dedicated.result.log" \
      "$summary_csv" "$steady_csv" "$fail_csv" \
      "16" "$expected_total" \
      "$req_bytes" "$resp_bytes" \
      "$req_min_bytes" "$req_max_bytes" \
      "$resp_min_bytes" "$resp_max_bytes" \
      "$slow_client_count" "$slow_count_per_client" "$slow_request_pct"; then
    handle_case_failure "$fail_csv" "dedicated" "$suite" "$outdir" \
      "summary_failed"
    return 0
  fi

  if [[ "$runner_rc" -ne 0 ]]; then
    log_msg "SALVAGED dedicated suite=${suite} outdir=${outdir} runner_rc=${runner_rc}"
  fi
  log_msg "END dedicated suite=${suite} outdir=${outdir}"
}

run_shared_case() {
  local suite="$1"
  local outdir="$2"
  local slow_request_pct="$3"
  local slow_client_count="$4"
  local slow_count_per_client="$5"
  local summary_csv="$6"
  local steady_csv="$7"
  local fail_csv="$8"
  local console_log="$outdir/console.log"
  local runner_rc=0
  local expected_total=""

  mkdir -p "$outdir"
  log_msg "START shared suite=${suite} outdir=${outdir}"

  set +e
  bash tools/run_e2e_hydrarpc_shared.sh \
    --binary "$BINARY" \
    --disk-image "$DISK_IMAGE" \
    --cpu-type TIMING \
    --boot-cpu KVM \
    --client-count 16 \
    --count-per-client "$COUNT_PER_CLIENT" \
    --window-size "$WINDOW_SIZE" \
    --slot-count "$SLOT_COUNT" \
    --req-bytes 38 \
    --resp-bytes 230 \
    --req-min-bytes 19 \
    --req-max-bytes 57 \
    --resp-min-bytes 115 \
    --resp-max-bytes 345 \
    --slow-client-count "$slow_client_count" \
    --slow-count-per-client "$slow_count_per_client" \
    --slow-send-gap-ns "$SLOW_SEND_GAP_NS" \
    --num-cpus "$NUM_CPUS" \
    --restore-checkpoint "$SHARED_CHECKPOINT" \
    --skip-build \
    --skip-image-setup \
    --outdir "$outdir" \
    >"$console_log" 2>&1
  runner_rc=$?
  set -e

  if [[ ! -f "$outdir/hydrarpc_shared.result.log" ]]; then
    handle_case_failure "$fail_csv" "shared" "$suite" "$outdir" \
      "runner_rc=${runner_rc}: missing hydrarpc_shared.result.log"
    return 0
  fi

  expected_total="$(expected_total_requests 16 "$slow_client_count" "$slow_count_per_client")"
  if ! run_summary \
      "shared" "$suite" "$outdir" "$outdir/hydrarpc_shared.result.log" \
      "$summary_csv" "$steady_csv" "$fail_csv" \
      "16" "$expected_total" \
      "38" "230" \
      "19" "57" \
      "115" "345" \
      "$slow_client_count" "$slow_count_per_client" "$slow_request_pct"; then
    handle_case_failure "$fail_csv" "shared" "$suite" "$outdir" \
      "summary_failed"
    return 0
  fi

  if [[ "$runner_rc" -ne 0 ]]; then
    log_msg "SALVAGED shared suite=${suite} outdir=${outdir} runner_rc=${runner_rc}"
  fi
  log_msg "END shared suite=${suite} outdir=${outdir}"
}

run_dedicated_sparse_batches() {
  local moti_root="$DEDICATED_ROOT/moti"
  local sensitivity_root="$DEDICATED_ROOT/sensitivity"
  local moti_summary_csv="$moti_root/summary.csv"
  local moti_steady_csv="$moti_root/steady_summary.csv"
  local moti_fail_csv="$moti_root/failures.csv"
  local sensitivity_summary_csv="$sensitivity_root/summary.csv"
  local sensitivity_steady_csv="$sensitivity_root/steady_summary.csv"
  local sensitivity_fail_csv="$sensitivity_root/failures.csv"
  local pct=""
  local sc=""
  local sq=""
  local suite=""
  local outdir=""

  mkdir -p "$moti_root" "$sensitivity_root"
  rm -f \
    "$moti_summary_csv" \
    "$moti_steady_csv" \
    "$moti_fail_csv" \
    "$sensitivity_summary_csv" \
    "$sensitivity_steady_csv" \
    "$sensitivity_fail_csv"

  ensure_dedicated_image

  for pct in 0 25 50; do
    sq="$(slow_count_for_pct "$pct")"
    for sc in 1 2 4 8; do
      suite="moti/sparse16_d${pct}_c${sc}"
      outdir="$moti_root/sparse16_d${pct}_c${sc}"
      run_dedicated_case \
        "$suite" "$outdir" \
        "64" "64" \
        "64" "64" \
        "64" "64" \
        "$pct" "$sc" "$sq" \
        "$moti_summary_csv" "$moti_steady_csv" "$moti_fail_csv"
    done
  done

  for pct in 0 25 50; do
    sq="$(slow_count_for_pct "$pct")"
    for sc in 1 2 4 8; do
      suite="sensitivity/sparse16_d${pct}_c${sc}"
      outdir="$sensitivity_root/sparse16_d${pct}_c${sc}"
      run_dedicated_case \
        "$suite" "$outdir" \
        "38" "230" \
        "19" "57" \
        "115" "345" \
        "$pct" "$sc" "$sq" \
        "$sensitivity_summary_csv" "$sensitivity_steady_csv" "$sensitivity_fail_csv"
    done
  done
}

run_shared_sparse_batch() {
  local sensitivity_root="$SHARED_ROOT/sensitivity"
  local summary_csv="$sensitivity_root/summary.csv"
  local steady_csv="$sensitivity_root/steady_summary.csv"
  local fail_csv="$sensitivity_root/failures.csv"
  local pct=""
  local sc=""
  local sq=""
  local suite=""
  local outdir=""

  mkdir -p "$sensitivity_root"
  rm -f "$summary_csv" "$steady_csv" "$fail_csv"

  ensure_shared_image

  for pct in 0 25 50; do
    sq="$(slow_count_for_pct "$pct")"
    for sc in 1 2 4 8; do
      suite="sensitivity/sparse16_d${pct}_c${sc}"
      outdir="$sensitivity_root/sparse16_d${pct}_c${sc}"
      run_shared_case \
        "$suite" "$outdir" "$pct" "$sc" "$sq" \
        "$summary_csv" "$steady_csv" "$fail_csv"
    done
  done
}

main() {
  if [[ ! -f "$DISK_IMAGE" ]]; then
    echo "disk image not found: $DISK_IMAGE" >&2
    exit 1
  fi

  DEDICATED_CHECKPOINT="$(resolve_dir "$DEDICATED_CHECKPOINT")"
  SHARED_CHECKPOINT="$(resolve_dir "$SHARED_CHECKPOINT")"
  mkdir -p "$DEDICATED_ROOT" "$SHARED_ROOT"
  DEDICATED_ROOT="$(resolve_dir "$DEDICATED_ROOT")"
  SHARED_ROOT="$(resolve_dir "$SHARED_ROOT")"
  QUEUE_LOG="$DEDICATED_ROOT/queue.log"
  : >"$QUEUE_LOG"

  log_msg "dedicated_checkpoint=$DEDICATED_CHECKPOINT"
  log_msg "shared_checkpoint=$SHARED_CHECKPOINT"
  log_msg "dedicated_root=$DEDICATED_ROOT"
  log_msg "shared_root=$SHARED_ROOT"
  log_msg "count_per_client=$COUNT_PER_CLIENT window_size=$WINDOW_SIZE slot_count=$SLOT_COUNT num_cpus=$NUM_CPUS"
  log_msg "strict_mode=single_gem5_at_a_time"

  ensure_gem5_binary
  run_dedicated_sparse_batches
  run_shared_sparse_batch

  log_msg "complete"
  printf 'dedicated_root=%s\n' "$DEDICATED_ROOT"
  printf 'shared_root=%s\n' "$SHARED_ROOT"
}

main "$@"

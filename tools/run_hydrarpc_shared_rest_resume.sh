#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_shared_rest_resume.sh [options]

Run the current shared experiment set as one flat single-machine queue with a
global parallelism cap. Output directories still keep motivation / overall /
sensitivity / application groupings, but scheduling happens per single case.

Options:
  --root-outdir <dir>        Root output directory.
                             Default: output/shared_rest_<timestamp>
  --binary <path>            gem5 binary. Default: build/X86/gem5.opt
  --disk-image <path>        Guest disk image. Default: files/parsec.img
  --checkpoint <dir>         Reuse an existing classic checkpoint directory.
  --checkpoint-root <dir>    Root directory for auto-created shared checkpoints.
                             Default: output/hydrarpc_shared_checkpoints
  --refresh-checkpoint       Recreate the auto-managed checkpoint bundle.
  --parallel-jobs <N>        Max concurrent gem5 jobs across the whole queue.
                             Default: 4
  --count-per-client <N>     Requests per client. Default: 30
  --num-cpus <N>             Guest CPU count baked into the checkpoint and runs.
                             Default: 34
  --window-size <N>          Outstanding requests per client. Default: 16
  --slot-count <N>           Shared ring depth. Default: 1024
  --cpu-type <type>          Benchmark CPU type. Default: TIMING
  --boot-cpu <type>          Checkpoint boot CPU type. Default: KVM
  --cxl-node <N>             Guest NUMA node used for CXL mappings. Default: 1
  --guest-cflags <flags>     Host gcc flags used when injecting guest binaries.
  --skip-build               Reuse the existing gem5 binary after freshness checks.
  --skip-image-setup         Reuse already injected shared/shared-app binaries.
  --skip-existing            Reuse completed outdirs and summaries when possible.
  --continue-on-failure      Record failures and continue remaining queue items.
  --help                     Show this message.

Included case groups:
  - motivation shared competition
  - overall
  - sensitivity req-size
  - sensitivity resp-size
  - sensitivity ring-size
  - sensitivity sparse16
  - sensitivity cxl-latency
  - application
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
BINARY="build/X86/gem5.opt"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
CHECKPOINT_DIR=""
CHECKPOINT_ROOT="${REPO_ROOT}/output/hydrarpc_shared_checkpoints"
REFRESH_CHECKPOINT=0
PARALLEL_JOBS=4
COUNT_PER_CLIENT=30
NUM_CPUS=34
WINDOW_SIZE=16
SLOT_COUNT=1024
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
CXL_NODE=1
GUEST_CFLAGS=""
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
CONTINUE_ON_FAILURE=0
ANY_FAILURES=0

APP_PROFILES="ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_d_1k udb_a udb_b udb_c udb_d"
APP_CLIENT_COUNT=32
APP_RECORD_COUNT=10000
APP_DATASET_SEED="0x9B5D3A4781C26EF1"
APP_WORKLOAD_SEED="0xC7D51A32049EF68B"
SEND_MODE="greedy"
SEND_GAP_NS=0
DROP_FIRST_PER_CLIENT=5

BUILD_DONE=0
SHARED_IMAGE_READY=0
TOTAL_CASES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-outdir)
      ROOT_OUTDIR="$2"
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
    --checkpoint)
      CHECKPOINT_DIR="$2"
      shift 2
      ;;
    --checkpoint-root)
      CHECKPOINT_ROOT="$2"
      shift 2
      ;;
    --refresh-checkpoint)
      REFRESH_CHECKPOINT=1
      shift 1
      ;;
    --parallel-jobs)
      PARALLEL_JOBS="$2"
      shift 2
      ;;
    --count-per-client)
      COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --num-cpus)
      NUM_CPUS="$2"
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
    --cpu-type)
      CPU_TYPE="$2"
      shift 2
      ;;
    --boot-cpu)
      BOOT_CPU="$2"
      shift 2
      ;;
    --cxl-node)
      CXL_NODE="$2"
      shift 2
      ;;
    --guest-cflags)
      GUEST_CFLAGS="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift 1
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
      shift 1
      ;;
    --skip-existing)
      SKIP_EXISTING=1
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

if [[ "$PARALLEL_JOBS" -le 0 ]]; then
  echo "--parallel-jobs must be positive" >&2
  exit 1
fi
if [[ "$COUNT_PER_CLIENT" -le 0 ]]; then
  echo "--count-per-client must be positive" >&2
  exit 1
fi
if [[ "$NUM_CPUS" -le 0 ]]; then
  echo "--num-cpus must be positive" >&2
  exit 1
fi
if [[ "$WINDOW_SIZE" -le 0 ]]; then
  echo "--window-size must be positive" >&2
  exit 1
fi
if [[ "$SLOT_COUNT" -le 0 ]]; then
  echo "--slot-count must be positive" >&2
  exit 1
fi
if [[ -n "$CHECKPOINT_DIR" && "$REFRESH_CHECKPOINT" -eq 1 ]]; then
  echo "--refresh-checkpoint cannot be used together with --checkpoint" >&2
  exit 1
fi
if [[ ! -f "$DISK_IMAGE" ]]; then
  echo "disk image not found: $DISK_IMAGE" >&2
  exit 1
fi

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/shared_rest_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"

mkdir -p "$CHECKPOINT_ROOT"
CHECKPOINT_ROOT="$(cd "$CHECKPOINT_ROOT" && pwd)"

if [[ -n "$CHECKPOINT_DIR" ]]; then
  if [[ ! -d "$CHECKPOINT_DIR" ]]; then
    echo "checkpoint not found: $CHECKPOINT_DIR" >&2
    exit 1
  fi
  CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"
fi

SUMMARY_CSV="$ROOT_OUTDIR/summary.csv"
STEADY_CSV="$ROOT_OUTDIR/steady_summary.csv"
FAIL_CSV="$ROOT_OUTDIR/failures.csv"
RUN_FAILURES_TSV="$ROOT_OUTDIR/run_failures.tsv"
RUN_LOG="$ROOT_OUTDIR/run.log"
RESULTS_LOCK="$ROOT_OUTDIR/.results.lock"
CASE_MANIFEST="$ROOT_OUTDIR/cases.tsv"

rm -f "$FAIL_CSV" "$RUN_FAILURES_TSV" "$CASE_MANIFEST"
: >"$RUN_LOG"

log_msg() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" | tee -a "$RUN_LOG"
}

log_cmd() {
  printf '[%s] ' "$(date '+%F %T')" | tee -a "$RUN_LOG"
  printf '%q ' "$@" | tee -a "$RUN_LOG"
  printf '\n' | tee -a "$RUN_LOG"
}

run_cmd() {
  log_cmd "$@"
  "$@"
}

ensure_build() {
  if [[ "$BUILD_DONE" -eq 1 ]]; then
    return 0
  fi

  if [[ "$SKIP_BUILD" -eq 0 ]]; then
    run_cmd scons "$BINARY" -j"$(nproc)"
  else
    run_cmd bash tools/check_gem5_binary_freshness.sh \
      --binary "$BINARY" \
      --label "gem5 binary for shared rest resume" \
      --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
      --monitor "src/python/gem5/components/boards/x86_board.py"
  fi

  BUILD_DONE=1
}

ensure_shared_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 || "$SHARED_IMAGE_READY" -eq 1 ]]; then
    return 0
  fi

  ensure_build
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_shared_all_disk_image.sh "$DISK_IMAGE"
  else
    run_cmd bash tools/setup_hydrarpc_shared_all_disk_image.sh "$DISK_IMAGE"
  fi
  SHARED_IMAGE_READY=1
}

ensure_checkpoint() {
  local checkpoint_bundle=""
  local checkpoint_path_output=""
  local cmd=()

  if [[ -n "$CHECKPOINT_DIR" ]]; then
    return 0
  fi

  ensure_build

  checkpoint_bundle="$CHECKPOINT_ROOT/classic_n${NUM_CPUS}"
  if [[ "$REFRESH_CHECKPOINT" -eq 1 ]]; then
    run_cmd rm -rf "$checkpoint_bundle"
  fi

  cmd=(
    bash tools/create_hydrarpc_boot_checkpoint.sh
    --mode classic
    --outdir "$checkpoint_bundle"
    --binary "$BINARY"
    --boot-cpu "$BOOT_CPU"
    --num-cpus "$NUM_CPUS"
    --disk-image "$DISK_IMAGE"
    --skip-build
  )

  log_cmd "${cmd[@]}"
  checkpoint_path_output="$("${cmd[@]}")"
  CHECKPOINT_DIR="$(cd "$checkpoint_path_output" && pwd)"
}

append_case_manifest_line() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}" "${12}" "${13}" "${14}" "${15}" \
    >>"$CASE_MANIFEST"
  TOTAL_CASES=$((TOTAL_CASES + 1))
}

queue_micro_case() {
  local suite_dir="$1"
  local client_count="$2"
  local req_bytes="$3"
  local resp_bytes="$4"
  local req_min_bytes="${5:--}"
  local req_max_bytes="${6:--}"
  local resp_min_bytes="${7:--}"
  local resp_max_bytes="${8:--}"
  local slot_count="${9:-$SLOT_COUNT}"
  local slow_client_count="${10:-0}"
  local slow_count_per_client="${11:-0}"
  local slow_send_gap_ns="${12:-0}"
  local cxl_latency="${13:-0ns}"

  append_case_manifest_line \
    "micro" "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency" "-"
}

queue_app_case() {
  local profile="$1"
  append_case_manifest_line \
    "app" "application" "$APP_CLIENT_COUNT" "-" "-" \
    "-" "-" "-" "-" \
    "$SLOT_COUNT" "0" "0" "0" "0ns" "$profile"
}

build_case_manifest() {
  local client_count=""
  local resp_bytes=""
  local req_bytes=""
  local ring_size=""
  local slow_count_per_client=""
  local slow_client_count=""
  local latency=""
  local profile=""

  : >"$CASE_MANIFEST"
  TOTAL_CASES=0

  for client_count in 1 2 4 8 16 32; do
    queue_micro_case "motivation/moti_shared_competition_req64_resp64_pow2" \
      "$client_count" 64 64
  done

  for client_count in 1 2 4 8 16 32; do
    queue_micro_case "overall/req1530_resp315_uniform" \
      "$client_count" 1530 315 765 2295 158 472
    queue_micro_case "overall/req38_resp230_uniform" \
      "$client_count" 38 230 19 57 115 345
  done

  for req_bytes in 8 256 1024 4096 8192; do
    queue_micro_case "sensitivity/reqsize_req${req_bytes}_resp64" \
      32 "$req_bytes" 64
  done

  for resp_bytes in 8 256 1024 4096 8192; do
    queue_micro_case "sensitivity/respsize_req64_resp${resp_bytes}" \
      32 64 "$resp_bytes"
  done

  for ring_size in 16 32 64 128 256 512; do
    queue_micro_case "sensitivity/ringsize_s${ring_size}" \
      32 64 64 - - - - "$ring_size"
  done

  for slow_count_per_client in 0 8 15; do
    local slow_request_pct=""
    case "$slow_count_per_client" in
      0) slow_request_pct=0 ;;
      8) slow_request_pct=25 ;;
      15) slow_request_pct=50 ;;
      *)
        echo "unsupported slow_count_per_client: $slow_count_per_client" >&2
        exit 1
        ;;
    esac
    for slow_client_count in 1 2 4 8; do
      queue_micro_case "sensitivity/sparse16_d${slow_request_pct}_c${slow_client_count}" \
        16 38 230 19 57 115 345 "$SLOT_COUNT" \
        "$slow_client_count" "$slow_count_per_client" 20000
    done
  done

  for latency in 100ns 200ns 300ns; do
    queue_micro_case "sensitivity/cxl_latency_${latency}" \
      32 64 64 - - - - "$SLOT_COUNT" 0 0 0 "$latency"
  done

  for profile in $APP_PROFILES; do
    queue_app_case "$profile"
  done
}

normalize_optional() {
  local value="$1"
  if [[ "$value" == "-" ]]; then
    printf '%s\n' ""
  else
    printf '%s\n' "$value"
  fi
}

shared_req_tag() {
  local req_bytes="$1"
  local req_min_bytes="$2"
  local req_max_bytes="$3"
  local effective_min="$req_bytes"
  local effective_max="$req_bytes"

  if [[ -n "$req_min_bytes" ]]; then
    effective_min="$req_min_bytes"
  fi
  if [[ -n "$req_max_bytes" ]]; then
    effective_max="$req_max_bytes"
  fi

  if [[ "$effective_min" != "$effective_max" ]]; then
    printf 'qu%s-%s\n' "$effective_min" "$effective_max"
  else
    printf 'qb%s\n' "$req_bytes"
  fi
}

shared_resp_tag() {
  local resp_bytes="$1"
  local resp_min_bytes="$2"
  local resp_max_bytes="$3"
  local effective_min="$resp_bytes"
  local effective_max="$resp_bytes"

  if [[ -n "$resp_min_bytes" ]]; then
    effective_min="$resp_min_bytes"
  fi
  if [[ -n "$resp_max_bytes" ]]; then
    effective_max="$resp_max_bytes"
  fi

  if [[ "$effective_min" != "$effective_max" ]]; then
    printf 'pu%s-%s\n' "$effective_min" "$effective_max"
  else
    printf 'pb%s\n' "$resp_bytes"
  fi
}

micro_outdir_for() {
  local suite_dir="$1"
  local client_count="$2"
  local req_bytes="$3"
  local resp_bytes="$4"
  local req_min_bytes="$5"
  local req_max_bytes="$6"
  local resp_min_bytes="$7"
  local resp_max_bytes="$8"
  local slot_count="$9"
  local slow_client_count="${10}"
  local slow_count_per_client="${11}"
  local slow_send_gap_ns="${12}"
  local cxl_latency="${13}"
  local slow_suffix=""
  local latency_suffix=""
  local latency_tag=""
  local req_tag=""
  local resp_tag=""

  req_tag="$(shared_req_tag "$req_bytes" "$req_min_bytes" "$req_max_bytes")"
  resp_tag="$(shared_resp_tag "$resp_bytes" "$resp_min_bytes" "$resp_max_bytes")"

  if [[ "$slow_client_count" -gt 0 ]]; then
    slow_suffix="_slow${slow_client_count}_n${slow_count_per_client}_sg${slow_send_gap_ns}"
  fi
  if [[ "$cxl_latency" != "0ns" ]]; then
    latency_tag="$(printf '%s' "$cxl_latency" | tr ' /' '__')"
    latency_suffix="_cxl${latency_tag}"
  fi

  printf '%s\n' \
    "$ROOT_OUTDIR/${suite_dir}/shared_s${slot_count}_${req_tag}_${resp_tag}_m${SEND_MODE}_g${SEND_GAP_NS}${latency_suffix}${slow_suffix}_c${client_count}_r${COUNT_PER_CLIENT}"
}

app_outdir_for() {
  local profile="$1"
  local client_count="$2"
  printf '%s\n' \
    "$ROOT_OUTDIR/application/shared_${profile}_c${client_count}_r${COUNT_PER_CLIENT}"
}

outdir_for_case() {
  local case_type="$1"
  local suite_dir="$2"
  local client_count="$3"
  local req_bytes="$4"
  local resp_bytes="$5"
  local req_min_bytes="$6"
  local req_max_bytes="$7"
  local resp_min_bytes="$8"
  local resp_max_bytes="$9"
  local slot_count="${10}"
  local slow_client_count="${11}"
  local slow_count_per_client="${12}"
  local slow_send_gap_ns="${13}"
  local cxl_latency="${14}"
  local profile="${15}"

  if [[ "$case_type" == "app" ]]; then
    app_outdir_for "$profile" "$client_count"
    return 0
  fi

  micro_outdir_for \
    "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency"
}

resolve_log_path() {
  local case_type="$1"
  local outdir="$2"

  case "$case_type" in
    micro)
      [[ -f "$outdir/hydrarpc_shared.result.log" ]] || return 1
      printf '%s\n' "$outdir/hydrarpc_shared.result.log"
      ;;
    app)
      [[ -f "$outdir/hydrarpc_shared_app.result.log" ]] || return 1
      printf '%s\n' "$outdir/hydrarpc_shared_app.result.log"
      ;;
    *)
      return 1
      ;;
  esac
}

has_success_result_json() {
  local result_json="$1"
  [[ -f "$result_json" ]] || return 1
  rg -q '"status"[[:space:]]*:[[:space:]]*"success"' "$result_json" 2>/dev/null
}

record_failure() {
  local case_type="$1"
  local suite_dir="$2"
  local client_count="$3"
  local profile="$4"
  local exit_code="$5"
  local outdir="$6"
  local reason="$7"

  exec {results_fd}> "$RESULTS_LOCK"
  flock "$results_fd"
  if [[ ! -f "$RUN_FAILURES_TSV" ]]; then
    printf 'case_type\tsuite\tclient_count\tprofile\texit_code\toutdir\treason\n' \
      >"$RUN_FAILURES_TSV"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$case_type" "$suite_dir" "$client_count" "$profile" \
    "$exit_code" "$outdir" "$reason" \
    >>"$RUN_FAILURES_TSV"
  flock -u "$results_fd"
  exec {results_fd}>&-
}

archive_retry_outdir() {
  local outdir="$1"
  local attempt="$2"
  local retry_outdir="${outdir}.retry${attempt}"
  local suffix=1

  while [[ -e "$retry_outdir" ]]; do
    retry_outdir="${outdir}.retry${attempt}.rerun${suffix}"
    suffix=$((suffix + 1))
  done

  mv "$outdir" "$retry_outdir"
}

is_transient_cpu_failure() {
  local outdir="$1"

  rg -q \
    "need online cpus > max\\(server-cpu, client-count-1\\)|failed to pin server to cpu|failed to pin client" \
    "$outdir/board.pc.com_1.device" \
    "$outdir/hydrarpc_shared.result.log" \
    "$outdir/hydrarpc_shared_app.result.log" \
    2>/dev/null
}

guest_cmd_for_app() {
  local profile="$1"
  local client_count="$2"

  printf '/home/test_code/run_hydrarpc_shared_app.sh --client-count %s --count-per-client %s --window-size %s --slot-count %s --cxl-node %s --profile %s --record-count %s --dataset-seed %s --workload-seed %s\n' \
    "$client_count" "$COUNT_PER_CLIENT" "$WINDOW_SIZE" "$SLOT_COUNT" \
    "$CXL_NODE" "$profile" "$APP_RECORD_COUNT" "$APP_DATASET_SEED" "$APP_WORKLOAD_SEED"
}

run_runner_only() {
  local case_type="$1"
  local suite_dir="$2"
  local client_count="$3"
  local req_bytes="$4"
  local resp_bytes="$5"
  local req_min_bytes="$6"
  local req_max_bytes="$7"
  local resp_min_bytes="$8"
  local resp_max_bytes="$9"
  local slot_count="${10}"
  local slow_client_count="${11}"
  local slow_count_per_client="${12}"
  local slow_send_gap_ns="${13}"
  local cxl_latency="${14}"
  local profile="${15}"
  local outdir=""
  local console_log=""
  local runner_rc=0
  local runner_rc_file=""
  local attempt=1
  local max_attempts=4
  local cmd=()
  local guest_cmd=""

  outdir="$(outdir_for_case \
    "$case_type" "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency" "$profile")"
  console_log="$outdir/console.log"
  runner_rc_file="$outdir/runner.exitcode"

  mkdir -p "$outdir"
  log_msg "START case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} outdir=${outdir}"

  if [[ "$SKIP_EXISTING" -eq 1 ]] && resolve_log_path "$case_type" "$outdir" >/dev/null 2>&1; then
    log_msg "REUSE-LOG case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} outdir=${outdir}"
    printf '0\n' >"$runner_rc_file"
    return 0
  fi

  while true; do
    mkdir -p "$outdir"
    cmd=(
      bash tools/run_e2e_hydrarpc_shared.sh
      --binary "$BINARY"
      --disk-image "$DISK_IMAGE"
      --skip-build
      --skip-image-setup
      --cpu-type "$CPU_TYPE"
      --boot-cpu "$BOOT_CPU"
      --client-count "$client_count"
      --count-per-client "$COUNT_PER_CLIENT"
      --window-size "$WINDOW_SIZE"
      --slot-count "$slot_count"
      --send-mode "$SEND_MODE"
      --send-gap-ns "$SEND_GAP_NS"
      --cxl-node "$CXL_NODE"
      --num-cpus "$NUM_CPUS"
      --restore-checkpoint "$CHECKPOINT_DIR"
      --outdir "$outdir"
    )

    if [[ "$case_type" == "micro" ]]; then
      cmd+=(
        --req-bytes "$req_bytes"
        --resp-bytes "$resp_bytes"
        --slow-client-count "$slow_client_count"
        --slow-count-per-client "$slow_count_per_client"
        --slow-send-gap-ns "$slow_send_gap_ns"
        --cxl-bridge-extra-latency "$cxl_latency"
      )
      if [[ -n "$req_min_bytes" ]]; then
        cmd+=(--req-min-bytes "$req_min_bytes")
      fi
      if [[ -n "$req_max_bytes" ]]; then
        cmd+=(--req-max-bytes "$req_max_bytes")
      fi
      if [[ -n "$resp_min_bytes" ]]; then
        cmd+=(--resp-min-bytes "$resp_min_bytes")
      fi
      if [[ -n "$resp_max_bytes" ]]; then
        cmd+=(--resp-max-bytes "$resp_max_bytes")
      fi
    else
      guest_cmd="$(guest_cmd_for_app "$profile" "$client_count")"
      cmd+=(
        --result-log-name hydrarpc_shared_app.result.log
        --guest-cmd "$guest_cmd"
      )
    fi

    set +e
    "${cmd[@]}" >"$console_log" 2>&1
    runner_rc=$?
    set -e

    if [[ "$runner_rc" -eq 0 ]]; then
      break
    fi

    if [[ "$attempt" -lt "$max_attempts" ]] && is_transient_cpu_failure "$outdir"; then
      log_msg "RETRY case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} attempt=${attempt} reason=transient_guest_cpu_failure"
      archive_retry_outdir "$outdir" "$attempt"
      attempt=$((attempt + 1))
      continue
    fi

    break
  done

  printf '%s\n' "$runner_rc" >"$runner_rc_file"
}

process_case() {
  local case_type="$1"
  local suite_dir="$2"
  local client_count="$3"
  local req_bytes="$4"
  local resp_bytes="$5"
  local req_min_bytes="$6"
  local req_max_bytes="$7"
  local resp_min_bytes="$8"
  local resp_max_bytes="$9"
  local slot_count="${10}"
  local slow_client_count="${11}"
  local slow_count_per_client="${12}"
  local slow_send_gap_ns="${13}"
  local cxl_latency="${14}"
  local profile="${15}"
  local outdir=""
  local runner_rc_file=""
  local runner_rc=0
  local result_json_path=""
  local log_path=""
  local summary_text=""
  local summary_rc=0
  local category="${suite_dir%%/*}"
  local experiment="shared"
  local expected_total_requests=$((client_count * COUNT_PER_CLIENT))
  local summary_req_bytes="$req_bytes"
  local summary_resp_bytes="$resp_bytes"
  local summary_profile=""
  local summary_record_count=""
  local summary_dataset_seed=""
  local summary_workload_seed=""

  outdir="$(outdir_for_case \
    "$case_type" "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency" "$profile")"
  runner_rc_file="$outdir/runner.exitcode"
  result_json_path="$outdir/result.json"

  if [[ -f "$runner_rc_file" ]]; then
    runner_rc="$(<"$runner_rc_file")"
  fi

  if [[ "$case_type" == "app" ]]; then
    summary_req_bytes=""
    summary_resp_bytes=""
    summary_profile="$profile"
    summary_record_count="$APP_RECORD_COUNT"
    summary_dataset_seed="$APP_DATASET_SEED"
    summary_workload_seed="$APP_WORKLOAD_SEED"
  elif [[ "$slow_client_count" -gt 0 ]]; then
    expected_total_requests=$((expected_total_requests - slow_client_count * (COUNT_PER_CLIENT - slow_count_per_client)))
  fi

  if [[ "$SKIP_EXISTING" -eq 1 ]] && has_success_result_json "$result_json_path"; then
    log_msg "REUSE-SUMMARY case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} outdir=${outdir}"
    return 0
  fi

  if ! log_path="$(resolve_log_path "$case_type" "$outdir")"; then
    if [[ "$runner_rc" -ne 0 ]]; then
      log_msg "RUN-FAIL case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} rc=${runner_rc} outdir=${outdir}"
    fi
    log_msg "MISSING-LOG case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} outdir=${outdir}"
    record_failure "$case_type" "$suite_dir" "$client_count" "$profile" "1" "$outdir" "missing_result_log"
    return 1
  fi

  exec {results_fd}> "$RESULTS_LOCK"
  flock "$results_fd"
  set +e
  summary_text="$(
    python3 tools/summarize_hydrarpc_multiclient.py \
      --log "$log_path" \
      --experiment "$experiment" \
      --client-count "$client_count" \
      --count-per-client "$COUNT_PER_CLIENT" \
      --expected-total-requests "$expected_total_requests" \
      --drop-first-per-client "$DROP_FIRST_PER_CLIENT" \
      --csv "$SUMMARY_CSV" \
      --steady-csv "$STEADY_CSV" \
      --fail-csv "$FAIL_CSV" \
      --outdir "$outdir" \
      --result-json "$result_json_path" \
      --extra-field "category=$category" \
      --extra-field "suite=$suite_dir" \
      --extra-field "case_type=$case_type" \
      --extra-field "req_bytes=$summary_req_bytes" \
      --extra-field "resp_bytes=$summary_resp_bytes" \
      --extra-field "req_min_bytes=$req_min_bytes" \
      --extra-field "req_max_bytes=$req_max_bytes" \
      --extra-field "resp_min_bytes=$resp_min_bytes" \
      --extra-field "resp_max_bytes=$resp_max_bytes" \
      --extra-field "slot_count=$slot_count" \
      --extra-field "slow_client_count=$slow_client_count" \
      --extra-field "slow_count_per_client=$slow_count_per_client" \
      --extra-field "slow_send_gap_ns=$slow_send_gap_ns" \
      --extra-field "cxl_bridge_extra_latency=$cxl_latency" \
      --extra-field "profile=$summary_profile" \
      --extra-field "record_count=$summary_record_count" \
      --extra-field "dataset_seed=$summary_dataset_seed" \
      --extra-field "workload_seed=$summary_workload_seed" \
      --extra-field "window_size=$WINDOW_SIZE"
  )"
  summary_rc=$?
  set -e
  flock -u "$results_fd"
  exec {results_fd}>&-

  if [[ "$summary_rc" -ne 0 ]]; then
    if [[ "$runner_rc" -ne 0 ]]; then
      log_msg "RUN-FAIL case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} rc=${runner_rc} outdir=${outdir}"
    fi
    log_msg "SUMMARY-FAIL case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} rc=${summary_rc} outdir=${outdir}"
    printf '%s\n' "$summary_text" | tee -a "$RUN_LOG"
    record_failure "$case_type" "$suite_dir" "$client_count" "$profile" "$summary_rc" "$outdir" "summary_failed"
    return 1
  fi

  if [[ "$runner_rc" -ne 0 ]]; then
    log_msg "SALVAGED-SUCCESS case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} runner_rc=${runner_rc} outdir=${outdir}"
  fi

  printf '%s\n' "$summary_text" | tee -a "$RUN_LOG"
  log_msg "END case_type=${case_type} suite=${suite_dir} profile=${profile} client_count=${client_count} outdir=${outdir}"
  return 0
}

run_and_process_case() {
  local case_type="$1"
  local suite_dir="$2"
  local client_count="$3"
  local req_bytes="$4"
  local resp_bytes="$5"
  local req_min_bytes="$6"
  local req_max_bytes="$7"
  local resp_min_bytes="$8"
  local resp_max_bytes="$9"
  local slot_count="${10}"
  local slow_client_count="${11}"
  local slow_count_per_client="${12}"
  local slow_send_gap_ns="${13}"
  local cxl_latency="${14}"
  local profile="${15}"

  run_runner_only \
    "$case_type" "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency" "$profile"
  process_case \
    "$case_type" "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency" "$profile"
}

wait_for_background_or_fail() {
  local wait_rc=0

  set +e
  wait -n
  wait_rc=$?
  set -e

  if [[ "$wait_rc" -ne 0 ]]; then
    ANY_FAILURES=1
    if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
      log_msg "CONTINUE-FAIL background job rc=${wait_rc}; continuing remaining shared queue work"
      return 0
    fi
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    log_msg "FAIL-FAST aborting shared queue after background failure"
    exit "$wait_rc"
  fi
}

ensure_build
ensure_shared_image
ensure_checkpoint
build_case_manifest

log_msg "ROOT=$ROOT_OUTDIR"
log_msg "DISK_IMAGE=$DISK_IMAGE"
log_msg "CHECKPOINT=$CHECKPOINT_DIR"
log_msg "CASES=$TOTAL_CASES"

while IFS=$'\t' read -r case_type suite_dir client_count req_bytes resp_bytes req_min_bytes req_max_bytes resp_min_bytes resp_max_bytes slot_count slow_client_count slow_count_per_client slow_send_gap_ns cxl_latency profile; do
  req_min_bytes="$(normalize_optional "$req_min_bytes")"
  req_max_bytes="$(normalize_optional "$req_max_bytes")"
  resp_min_bytes="$(normalize_optional "$resp_min_bytes")"
  resp_max_bytes="$(normalize_optional "$resp_max_bytes")"
  profile="$(normalize_optional "$profile")"

  run_and_process_case \
    "$case_type" "$suite_dir" "$client_count" "$req_bytes" "$resp_bytes" \
    "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
    "$slot_count" "$slow_client_count" "$slow_count_per_client" \
    "$slow_send_gap_ns" "$cxl_latency" "$profile" &

  while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
    wait_for_background_or_fail
  done
done <"$CASE_MANIFEST"

while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
  wait_for_background_or_fail
done

echo
if [[ "$ANY_FAILURES" -ne 0 ]]; then
  echo "Shared queue complete with failures."
else
  echo "Shared queue complete."
fi
echo "root_outdir=$ROOT_OUTDIR"
echo "checkpoint_dir=$CHECKPOINT_DIR"
echo "cases_tsv=$CASE_MANIFEST"
echo "summary_csv=$SUMMARY_CSV"
echo "steady_csv=$STEADY_CSV"
echo "fail_csv=$FAIL_CSV"
echo "run_failures_tsv=$RUN_FAILURES_TSV"

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_app_sweep.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
  --count-per-client <N>   Requests per client. Default: 30
  --client-counts <list>   Quoted list, e.g. "32"
  --profiles <list>        Quoted list from the current supported set:
                           "ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_d_1k udb_a udb_b udb_c udb_d"
  --kinds <list>           Quoted list from "dedicated shared". Default: both
  --window-size <N>        Max outstanding requests per client. Default: 16
  --slot-count <N>         Ring depth passed to both shared and dedicated. Default: 1024
  --record-count <N>       Application KV record count. Default: 10000
  --dataset-seed <N>       Dataset seed passed into app binaries. Default: 0x9B5D3A4781C26EF1
  --workload-seed <N>      Workload seed passed into app binaries. Default: 0xC7D51A32049EF68B
  --cpu-type <type>        Switch CPU type passed to runners. Default: TIMING
  --boot-cpu <type>        Boot CPU type passed to runners. Default: KVM
  --num-cpus <N>           Guest CPU count passed to runners. Default: auto
  --restore-checkpoint <dir>
                           Reuse an existing boot checkpoint for each run.
  --guest-cflags <flags>   Host gcc flags used for the injected guest binaries.
  --skip-image-setup       Reuse already injected app guest binaries in the disk image.
  --parallel-jobs <N>      Number of runs to execute concurrently. Default: 1
  --skip-build             Skip the top-level scons build.
  --continue-on-failure    Record failed runs and continue the sweep.
  --skip-existing          Reuse existing outdirs and summary rows when possible.
  --help                   Show this message.

Experiment contract:
  - same workload profiles on shared and dedicated
  - staged, non-coherent baseline path
  - per-profile result CSVs carry profile metadata
EOF
}

ROOT_OUTDIR=""
COUNT_PER_CLIENT=30
CLIENT_COUNTS="32"
APP_PROFILES_DEFAULT="ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_d_1k udb_a udb_b udb_c udb_d"
PROFILES="$APP_PROFILES_DEFAULT"
KINDS="dedicated shared"
WINDOW_SIZE=16
SLOT_COUNT=1024
RECORD_COUNT=10000
DATASET_SEED="0x9B5D3A4781C26EF1"
WORKLOAD_SEED="0xC7D51A32049EF68B"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
NUM_CPUS=0
RESTORE_CHECKPOINT=""
GUEST_CFLAGS=""
SKIP_IMAGE_SETUP=0
PARALLEL_JOBS=1
SKIP_BUILD=0
CONTINUE_ON_FAILURE=0
SKIP_EXISTING=0
DROP_FIRST_PER_CLIENT=5
CXL_NODE=1
ANY_FAILURES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-outdir)
      ROOT_OUTDIR="$2"
      shift 2
      ;;
    --count-per-client)
      COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --client-counts)
      CLIENT_COUNTS="$2"
      shift 2
      ;;
    --profiles)
      PROFILES="$2"
      shift 2
      ;;
    --kinds)
      KINDS="$2"
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
    --record-count)
      RECORD_COUNT="$2"
      shift 2
      ;;
    --dataset-seed)
      DATASET_SEED="$2"
      shift 2
      ;;
    --workload-seed)
      WORKLOAD_SEED="$2"
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
    --num-cpus)
      NUM_CPUS="$2"
      shift 2
      ;;
    --restore-checkpoint)
      RESTORE_CHECKPOINT="$2"
      shift 2
      ;;
    --guest-cflags)
      GUEST_CFLAGS="$2"
      shift 2
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
      shift 1
      ;;
    --parallel-jobs)
      PARALLEL_JOBS="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift 1
      ;;
    --continue-on-failure)
      CONTINUE_ON_FAILURE=1
      shift 1
      ;;
    --skip-existing)
      SKIP_EXISTING=1
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export DISK_IMAGE="${DISK_IMAGE:-${REPO_ROOT}/files/parsec.img}"
cd "$REPO_ROOT"

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/hydrarpc_app_sweep_$(date +%Y%m%d_%H%M%S)"
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
  touch build/X86/gem5.opt
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "build/X86/gem5.opt" \
    --label "gem5 binary for app sweep" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
fi

if [[ "$KINDS" == *"dedicated"* && "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if [[ -n "$GUEST_CFLAGS" ]]; then
    HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_app_disk_image.sh "$DISK_IMAGE"
  else
    bash tools/setup_hydrarpc_dedicated_app_disk_image.sh "$DISK_IMAGE"
  fi
fi

if [[ "$KINDS" == *"shared"* && "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if [[ -n "$GUEST_CFLAGS" ]]; then
    HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_shared_app_disk_image.sh "$DISK_IMAGE"
  else
    bash tools/setup_hydrarpc_shared_app_disk_image.sh "$DISK_IMAGE"
  fi
fi

mkdir -p "$ROOT_OUTDIR"
SUMMARY_CSV="$ROOT_OUTDIR/summary.csv"
STEADY_CSV="$ROOT_OUTDIR/steady_summary.csv"
FAIL_CSV="$ROOT_OUTDIR/failures.csv"
RUN_LOG="$ROOT_OUTDIR/run.log"
RUN_FAILURES_TSV="$ROOT_OUTDIR/run_failures.tsv"
rm -f "$FAIL_CSV" "$RUN_FAILURES_TSV"
touch "$RUN_LOG"

has_success_result_json() {
  local result_json="$1"

  [[ -f "$result_json" ]] || return 1
  rg -q '"status"[[:space:]]*:[[:space:]]*"success"' "$result_json" 2>/dev/null
}

record_failure() {
  local kind="$1"
  local profile="$2"
  local client_count="$3"
  local exit_code="$4"
  local outdir="$5"
  local reason="$6"

  if [[ ! -f "$RUN_FAILURES_TSV" ]]; then
    printf 'experiment\tprofile\tclient_count\texit_code\toutdir\treason\n' \
      >"$RUN_FAILURES_TSV"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$kind" \
    "$profile" \
    "$client_count" \
    "$exit_code" \
    "$outdir" \
    "$reason" \
    >>"$RUN_FAILURES_TSV"
}

resolve_log_path() {
  local kind="$1"
  local outdir="$2"
  local result_log=""

  case "$kind" in
    dedicated)
      result_log="$outdir/hydrarpc_dedicated_app.result.log"
      ;;
    shared)
      result_log="$outdir/hydrarpc_shared_app.result.log"
      ;;
    *)
      return 1
      ;;
  esac

  if [[ -f "$result_log" ]]; then
    printf '%s\n' "$result_log"
    return 0
  fi

  return 1
}

run_outdir() {
  local kind="$1"
  local profile="$2"
  local client_count="$3"

  printf '%s\n' \
    "$ROOT_OUTDIR/${kind}_${profile}_c${client_count}_r${COUNT_PER_CLIENT}"
}

guest_cmd_for() {
  local kind="$1"
  local profile="$2"
  local client_count="$3"

  case "$kind" in
    dedicated)
      printf '/home/test_code/run_hydrarpc_dedicated_app.sh --client-count %s --count-per-client %s --window-size %s --slot-count %s --request-transfer-mode staging --response-transfer-mode staging --cxl-node %s --profile %s --record-count %s --dataset-seed %s --workload-seed %s\n' \
        "$client_count" "$COUNT_PER_CLIENT" "$WINDOW_SIZE" "$SLOT_COUNT" \
        "$CXL_NODE" "$profile" "$RECORD_COUNT" "$DATASET_SEED" "$WORKLOAD_SEED"
      ;;
    shared)
      printf '/home/test_code/run_hydrarpc_shared_app.sh --client-count %s --count-per-client %s --window-size %s --slot-count %s --cxl-node %s --profile %s --record-count %s --dataset-seed %s --workload-seed %s\n' \
        "$client_count" "$COUNT_PER_CLIENT" "$WINDOW_SIZE" "$SLOT_COUNT" \
        "$CXL_NODE" "$profile" "$RECORD_COUNT" "$DATASET_SEED" "$WORKLOAD_SEED"
      ;;
    *)
      return 1
      ;;
  esac
}

result_log_name_for() {
  local kind="$1"

  case "$kind" in
    dedicated)
      printf '%s\n' "hydrarpc_dedicated_app.result.log"
      ;;
    shared)
      printf '%s\n' "hydrarpc_shared_app.result.log"
      ;;
    *)
      return 1
      ;;
  esac
}

run_runner_only() {
  local kind="$1"
  local profile="$2"
  local client_count="$3"
  local outdir=""
  local console_log=""
  local runner=""
  local runner_rc=0
  local runner_rc_file=""
  local result_log_name=""
  local guest_cmd=""
  local extra_args=()

  outdir="$(run_outdir "$kind" "$profile" "$client_count")"
  console_log="$outdir/console.log"
  runner_rc_file="$outdir/runner.exitcode"
  result_log_name="$(result_log_name_for "$kind")"
  guest_cmd="$(guest_cmd_for "$kind" "$profile" "$client_count")"

  case "$kind" in
    dedicated)
      runner="tools/run_e2e_hydrarpc_dedicated.sh"
      ;;
    shared)
      runner="tools/run_e2e_hydrarpc_shared.sh"
      ;;
    *)
      echo "unknown kind: $kind" >&2
      return 1
      ;;
  esac

  mkdir -p "$outdir"
  echo "[$(date '+%F %T')] START kind=${kind} profile=${profile} client_count=${client_count} outdir=${outdir}" \
    | tee -a "$RUN_LOG"

  if [[ "$SKIP_EXISTING" -eq 1 ]] && resolve_log_path "$kind" "$outdir" >/dev/null 2>&1; then
    echo "[$(date '+%F %T')] REUSE-LOG kind=${kind} profile=${profile} client_count=${client_count} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    printf '0\n' >"$runner_rc_file"
    return 0
  fi

  if [[ "$NUM_CPUS" -gt 0 ]]; then
    extra_args+=(--num-cpus "$NUM_CPUS")
  fi
  if [[ -n "$RESTORE_CHECKPOINT" ]]; then
    extra_args+=(--restore-checkpoint "$RESTORE_CHECKPOINT")
  fi
  if [[ -n "$GUEST_CFLAGS" ]]; then
    extra_args+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$kind" == "dedicated" ]]; then
    extra_args+=(--request-transfer-mode staging --response-transfer-mode staging)
  fi

  set +e
  bash "$runner" \
    --skip-build \
    --skip-image-setup \
    --cpu-type "$CPU_TYPE" \
    --boot-cpu "$BOOT_CPU" \
    --client-count "$client_count" \
    --count-per-client "$COUNT_PER_CLIENT" \
    --window-size "$WINDOW_SIZE" \
    --slot-count "$SLOT_COUNT" \
    --result-log-name "$result_log_name" \
    --guest-cmd "$guest_cmd" \
    "${extra_args[@]}" \
    --outdir "$outdir" \
    >"$console_log" 2>&1
  runner_rc=$?
  set -e

  printf '%s\n' "$runner_rc" >"$runner_rc_file"
}

process_one() {
  local kind="$1"
  local profile="$2"
  local client_count="$3"
  local outdir=""
  local log_path=""
  local summary_text=""
  local summary_rc=0
  local runner_rc=0
  local runner_rc_file=""
  local result_json_path=""

  outdir="$(run_outdir "$kind" "$profile" "$client_count")"
  runner_rc_file="$outdir/runner.exitcode"
  result_json_path="$outdir/result.json"

  if [[ -f "$runner_rc_file" ]]; then
    runner_rc="$(<"$runner_rc_file")"
  fi

  if ! log_path="$(resolve_log_path "$kind" "$outdir")"; then
    if [[ "$runner_rc" -ne 0 ]]; then
      echo "[$(date '+%F %T')] RUN-FAIL kind=${kind} profile=${profile} client_count=${client_count} rc=${runner_rc} outdir=${outdir}" \
        | tee -a "$RUN_LOG"
    fi
    echo "[$(date '+%F %T')] MISSING-LOG kind=${kind} profile=${profile} client_count=${client_count} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    record_failure "$kind" "$profile" "$client_count" "1" "$outdir" "missing_result_log"
    return 1
  fi

  if [[ "$SKIP_EXISTING" -eq 1 ]] && has_success_result_json "$result_json_path"; then
    echo "[$(date '+%F %T')] REUSE-SUMMARY kind=${kind} profile=${profile} client_count=${client_count} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    return 0
  fi

  set +e
  summary_text="$(
    python3 tools/summarize_hydrarpc_multiclient.py \
      --log "$log_path" \
      --experiment "$kind" \
      --client-count "$client_count" \
      --count-per-client "$COUNT_PER_CLIENT" \
      --expected-total-requests "$((client_count * COUNT_PER_CLIENT))" \
      --drop-first-per-client "$DROP_FIRST_PER_CLIENT" \
      --csv "$SUMMARY_CSV" \
      --steady-csv "$STEADY_CSV" \
      --fail-csv "$FAIL_CSV" \
      --outdir "$outdir" \
      --result-json "$outdir/result.json" \
      --extra-field "profile=$profile" \
      --extra-field "record_count=$RECORD_COUNT" \
      --extra-field "dataset_seed=$DATASET_SEED" \
      --extra-field "workload_seed=$WORKLOAD_SEED"
  )"
  summary_rc=$?
  set -e

  if [[ "$summary_rc" -ne 0 ]]; then
    if [[ "$runner_rc" -ne 0 ]]; then
      echo "[$(date '+%F %T')] RUN-FAIL kind=${kind} profile=${profile} client_count=${client_count} rc=${runner_rc} outdir=${outdir}" \
        | tee -a "$RUN_LOG"
    fi
    echo "[$(date '+%F %T')] SUMMARY-FAIL kind=${kind} profile=${profile} client_count=${client_count} rc=${summary_rc} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    printf '%s\n' "$summary_text" | tee -a "$RUN_LOG"
    record_failure "$kind" "$profile" "$client_count" "$summary_rc" "$outdir" "summary_failed"
    return 1
  fi

  if [[ "$runner_rc" -ne 0 ]]; then
    echo "[$(date '+%F %T')] SALVAGED-SUCCESS kind=${kind} profile=${profile} client_count=${client_count} runner_rc=${runner_rc} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
  fi
  printf '%s\n' "$summary_text" | tee -a "$RUN_LOG"
  echo "[$(date '+%F %T')] END kind=${kind} profile=${profile} client_count=${client_count} outdir=${outdir}" \
    | tee -a "$RUN_LOG"
  return 0
}

run_and_process_one() {
  local kind="$1"
  local profile="$2"
  local client_count="$3"

  run_runner_only "$kind" "$profile" "$client_count"
  process_one "$kind" "$profile" "$client_count"
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
      local continue_msg="[$(date '+%F %T')] CONTINUE-FAIL background job rc=${wait_rc}; continuing remaining app sweep work"
      printf '%s\n' "$continue_msg"
      printf '%s\n' "$continue_msg" >>"$RUN_LOG"
      return 0
    fi
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    echo "[$(date '+%F %T')] FAIL-FAST aborting app sweep after background failure" \
      | tee -a "$RUN_LOG"
    exit "$wait_rc"
  fi
}

for profile in $PROFILES; do
  for client_count in $CLIENT_COUNTS; do
    for kind in $KINDS; do
      if [[ "$PARALLEL_JOBS" -gt 1 ]]; then
        run_and_process_one "$kind" "$profile" "$client_count" &
        while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
          wait_for_background_or_fail
        done
      else
        if ! run_and_process_one "$kind" "$profile" "$client_count"; then
          ANY_FAILURES=1
          if [[ "$CONTINUE_ON_FAILURE" -eq 0 ]]; then
            exit 1
          fi
        fi
      fi
    done
  done
done

if [[ "$PARALLEL_JOBS" -gt 1 ]]; then
  while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
    wait_for_background_or_fail
  done
fi

echo
if [[ "$ANY_FAILURES" -ne 0 ]]; then
  echo "Application sweep complete with failures."
else
  echo "Application sweep complete."
fi
echo "root_outdir=$ROOT_OUTDIR"
echo "summary_csv=$SUMMARY_CSV"
echo "steady_csv=$STEADY_CSV"
echo "fail_csv=$FAIL_CSV"
echo "run_failures_tsv=$RUN_FAILURES_TSV"

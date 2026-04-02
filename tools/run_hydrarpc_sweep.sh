#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_sweep.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
  --count-per-client <N>   Requests per client. Default: 30
  --client-counts <list>   Quoted list, e.g. "1 2 4 8 16 32"
  --kinds <list>           Quoted list from "dedicated shared". Default: both
  --slot-count <N>         Ring depth passed to both shared and dedicated. Default: 1024
  --req-bytes <N>          Request payload bytes. Default: 64
  --resp-bytes <N>         Response payload bytes. Default: 64
  --cpu-type <type>        Switch CPU type passed to runners. Default: TIMING
  --boot-cpu <type>        Boot CPU type passed to runners. Default: KVM
  --slow-client-count <N>  Mark the first N client ids as slow. Default: 0
  --slow-count-per-client <N>
                           Request count used by each slow client.
  --slow-send-gap-ns <N>   Uniform inter-request gap used by slow clients. Default: 0
  --send-mode <mode>       Client pacing mode: greedy, uniform, staggered, or uneven. Default: greedy
  --send-gap-ns <N>        Inter-request gap used by all paced modes. Default: 0
  --request-transfer-mode <mode>
                           Dedicated request publish mode: staging or direct. Default: staging
  --response-transfer-mode <mode>
                           Dedicated response publish mode: staging or direct. Default: staging
  --num-cpus <N>           Guest CPU count passed to runners. Default: auto
  --restore-checkpoint <dir>
                           Reuse an existing boot checkpoint for each run.
  --guest-cflags <flags>   Host gcc flags used for the injected guest binaries.
  --skip-image-setup       Reuse the shared/dedicated guest binaries already injected into the disk image.
  --parallel-jobs <N>      Number of runs to execute concurrently. Default: 1
  --skip-build             Skip the top-level scons build.
  --parallel-pair          Run dedicated/shared of the same client_count in parallel.
  --skip-existing          Reuse existing outdirs and summary rows when possible.
  --help                   Show this message.
EOF
}

ROOT_OUTDIR=""
COUNT_PER_CLIENT=30
CLIENT_COUNTS="1 2 4 8 16 32"
KINDS="dedicated shared"
SLOT_COUNT=1024
REQ_BYTES=64
RESP_BYTES=64
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
SLOW_CLIENT_COUNT=0
SLOW_COUNT_PER_CLIENT=0
SLOW_SEND_GAP_NS=0
SEND_MODE="greedy"
SEND_GAP_NS=0
REQUEST_TRANSFER_MODE="staging"
RESPONSE_TRANSFER_MODE="staging"
NUM_CPUS=0
RESTORE_CHECKPOINT=""
GUEST_CFLAGS=""
SKIP_IMAGE_SETUP=0
PARALLEL_JOBS=1
SKIP_BUILD=0
PARALLEL_PAIR=0
SKIP_EXISTING=0
DROP_FIRST_PER_CLIENT=5

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
    --kinds)
      KINDS="$2"
      shift 2
      ;;
    --slot-count)
      SLOT_COUNT="$2"
      shift 2
      ;;
    --req-bytes)
      REQ_BYTES="$2"
      shift 2
      ;;
    --resp-bytes)
      RESP_BYTES="$2"
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
    --slow-client-count)
      SLOW_CLIENT_COUNT="$2"
      shift 2
      ;;
    --slow-count-per-client)
      SLOW_COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --slow-send-gap-ns)
      SLOW_SEND_GAP_NS="$2"
      shift 2
      ;;
    --send-mode)
      SEND_MODE="$2"
      shift 2
      ;;
    --send-gap-ns)
      SEND_GAP_NS="$2"
      shift 2
      ;;
    --request-transfer-mode)
      REQUEST_TRANSFER_MODE="$2"
      shift 2
      ;;
    --response-transfer-mode)
      RESPONSE_TRANSFER_MODE="$2"
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
    --parallel-pair)
      PARALLEL_PAIR=1
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
cd "$REPO_ROOT"

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/hydrarpc_dedicated_shared_sweep_$(date +%Y%m%d_%H%M%S)"
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
fi

if [[ "$KINDS" == *"dedicated"* && "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if [[ -n "$GUEST_CFLAGS" ]]; then
    HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
  else
    bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
  fi
fi

if [[ "$KINDS" == *"shared"* && "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if [[ -n "$GUEST_CFLAGS" ]]; then
    HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
  else
    bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
  fi
fi

mkdir -p "$ROOT_OUTDIR"
SUMMARY_CSV="$ROOT_OUTDIR/summary.csv"
STEADY_CSV="$ROOT_OUTDIR/steady_summary.csv"
FAIL_CSV="$ROOT_OUTDIR/failures.csv"
RUN_LOG="$ROOT_OUTDIR/run.log"
RUN_FAILURES_TSV="$ROOT_OUTDIR/run_failures.tsv"
rm -f "$SUMMARY_CSV" "$STEADY_CSV" "$FAIL_CSV" "$RUN_FAILURES_TSV"
: >"$RUN_LOG"

record_failure() {
  local kind="$1"
  local client_count="$2"
  local exit_code="$3"
  local outdir="$4"
  local reason="$5"

  if [[ ! -f "$RUN_FAILURES_TSV" ]]; then
    printf 'experiment\tclient_count\texit_code\toutdir\treason\n' \
      >"$RUN_FAILURES_TSV"
  fi

  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$kind" \
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
      result_log="$outdir/hydrarpc_dedicated.result.log"
      ;;
    shared)
      result_log="$outdir/hydrarpc_shared.result.log"
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
  local client_count="$2"
  local slow_suffix=""

  if [[ "$SLOW_CLIENT_COUNT" -gt 0 ]]; then
    slow_suffix="_slow${SLOW_CLIENT_COUNT}_n${SLOW_COUNT_PER_CLIENT}_sg${SLOW_SEND_GAP_NS}"
  fi

  if [[ "$kind" == "dedicated" ]]; then
    printf '%s\n' \
      "$ROOT_OUTDIR/${kind}_s${SLOT_COUNT}_qb${REQ_BYTES}_pb${RESP_BYTES}_req${REQUEST_TRANSFER_MODE}_resp${RESPONSE_TRANSFER_MODE}_m${SEND_MODE}_g${SEND_GAP_NS}${slow_suffix}_c${client_count}_r${COUNT_PER_CLIENT}"
    return 0
  fi

  printf '%s\n' \
    "$ROOT_OUTDIR/${kind}_s${SLOT_COUNT}_qb${REQ_BYTES}_pb${RESP_BYTES}_m${SEND_MODE}_g${SEND_GAP_NS}${slow_suffix}_c${client_count}_r${COUNT_PER_CLIENT}"
}

is_transient_cpu_failure() {
  local outdir="$1"

  rg -q \
    "need online cpus > max\\(server-cpu, client-count-1\\)|failed to pin server to cpu|failed to pin client" \
    "$outdir/board.pc.com_1.device" \
    "$outdir/hydrarpc_shared.result.log" \
    "$outdir/hydrarpc_dedicated.result.log" \
    2>/dev/null
}

run_runner_only() {
  local kind="$1"
  local client_count="$2"
  local outdir=""
  local console_log=""
  local runner=""
  local runner_rc=0
  local runner_rc_file=""
  local attempt=1
  local max_attempts=4
  local extra_args=()

  outdir="$(run_outdir "$kind" "$client_count")"
  console_log="$outdir/console.log"
  runner_rc_file="$outdir/runner.exitcode"
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
  echo "[$(date '+%F %T')] START kind=${kind} client_count=${client_count} outdir=${outdir}" \
    | tee -a "$RUN_LOG"

  if [[ "$SKIP_EXISTING" -eq 1 ]] && resolve_log_path "$kind" "$outdir" >/dev/null 2>&1; then
    echo "[$(date '+%F %T')] REUSE-LOG kind=${kind} client_count=${client_count} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    printf '0\n' >"$runner_rc_file"
    return 0
  fi

  while true; do
    mkdir -p "$outdir"
    extra_args=()
    if [[ "$NUM_CPUS" -gt 0 ]]; then
      extra_args+=(--num-cpus "$NUM_CPUS")
    fi
    if [[ -n "$RESTORE_CHECKPOINT" ]]; then
      extra_args+=(--restore-checkpoint "$RESTORE_CHECKPOINT")
    fi
    if [[ "$kind" == "dedicated" ]]; then
      extra_args+=(
        --skip-image-setup
        --slot-count "$SLOT_COUNT"
        --req-bytes "$REQ_BYTES"
        --resp-bytes "$RESP_BYTES"
        --slow-client-count "$SLOW_CLIENT_COUNT"
        --slow-count-per-client "$SLOW_COUNT_PER_CLIENT"
        --slow-send-gap-ns "$SLOW_SEND_GAP_NS"
        --send-mode "$SEND_MODE"
        --send-gap-ns "$SEND_GAP_NS"
        --request-transfer-mode "$REQUEST_TRANSFER_MODE"
        --response-transfer-mode "$RESPONSE_TRANSFER_MODE"
      )
      if [[ -n "$GUEST_CFLAGS" ]]; then
        extra_args+=(--guest-cflags "$GUEST_CFLAGS")
      fi
    elif [[ "$kind" == "shared" ]]; then
      extra_args+=(
        --skip-image-setup
        --slot-count "$SLOT_COUNT"
        --req-bytes "$REQ_BYTES"
        --resp-bytes "$RESP_BYTES"
        --slow-client-count "$SLOW_CLIENT_COUNT"
        --slow-count-per-client "$SLOW_COUNT_PER_CLIENT"
        --slow-send-gap-ns "$SLOW_SEND_GAP_NS"
        --send-mode "$SEND_MODE"
        --send-gap-ns "$SEND_GAP_NS"
      )
      if [[ -n "$GUEST_CFLAGS" ]]; then
        extra_args+=(--guest-cflags "$GUEST_CFLAGS")
      fi
    fi
    set +e
    bash "$runner" \
      --skip-build \
      --cpu-type "$CPU_TYPE" \
      --boot-cpu "$BOOT_CPU" \
      --client-count "$client_count" \
      --count-per-client "$COUNT_PER_CLIENT" \
      "${extra_args[@]}" \
      --outdir "$outdir" \
      >"$console_log" 2>&1
    runner_rc=$?
    set -e

    if [[ "$runner_rc" -eq 0 ]]; then
      break
    fi

    if [[ "$attempt" -lt "$max_attempts" ]] && is_transient_cpu_failure "$outdir"; then
      echo "[$(date '+%F %T')] RETRY kind=${kind} client_count=${client_count} attempt=${attempt} reason=transient_guest_cpu_failure" \
        | tee -a "$RUN_LOG"
      mv "$outdir" "${outdir}.retry${attempt}"
      attempt=$((attempt + 1))
      continue
    fi

    break
  done

  printf '%s\n' "$runner_rc" >"$runner_rc_file"
}

run_and_process_one() {
  local kind="$1"
  local client_count="$2"

  run_runner_only "$kind" "$client_count"
  process_one "$kind" "$client_count"
}

process_one() {
  local kind="$1"
  local client_count="$2"
  local outdir=""
  local experiment=""
  local expected_total_requests=0
  local log_path=""
  local summary_text=""
  local summary_rc=0
  local runner_rc=0
  local runner_rc_file=""

  outdir="$(run_outdir "$kind" "$client_count")"
  runner_rc_file="$outdir/runner.exitcode"
  case "$kind" in
    dedicated)
      experiment="dedicated"
      ;;
    shared)
      experiment="shared"
      ;;
    *)
      echo "unknown kind: $kind" >&2
      return 1
      ;;
  esac

  if [[ -f "$runner_rc_file" ]]; then
    runner_rc="$(<"$runner_rc_file")"
  fi

  if [[ "$runner_rc" -ne 0 ]]; then
    echo "[$(date '+%F %T')] RUN-FAIL kind=${kind} client_count=${client_count} rc=${runner_rc} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    record_failure "$kind" "$client_count" "$runner_rc" "$outdir" "runner_failed"
    return 1
  fi

  if ! log_path="$(resolve_log_path "$kind" "$outdir")"; then
    echo "[$(date '+%F %T')] MISSING-LOG kind=${kind} client_count=${client_count} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    record_failure "$kind" "$client_count" "1" "$outdir" "missing_result_log"
    return 1
  fi

  expected_total_requests=$((client_count * COUNT_PER_CLIENT))
  if [[ "$SLOW_CLIENT_COUNT" -gt 0 ]]; then
    expected_total_requests=$((expected_total_requests - SLOW_CLIENT_COUNT * (COUNT_PER_CLIENT - SLOW_COUNT_PER_CLIENT)))
  fi

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
      --result-json "$outdir/result.json"
  )"
  summary_rc=$?
  set -e

  if [[ "$summary_rc" -ne 0 ]]; then
    echo "[$(date '+%F %T')] SUMMARY-FAIL kind=${kind} client_count=${client_count} rc=${summary_rc} outdir=${outdir}" \
      | tee -a "$RUN_LOG"
    printf '%s\n' "$summary_text" | tee -a "$RUN_LOG"
    record_failure "$kind" "$client_count" "$summary_rc" "$outdir" "summary_failed"
    return 1
  fi

  printf '%s\n' "$summary_text" | tee -a "$RUN_LOG"
  echo "[$(date '+%F %T')] END kind=${kind} client_count=${client_count} outdir=${outdir}" \
    | tee -a "$RUN_LOG"

  return 0
}

wait_for_background_or_fail() {
  local wait_rc=0

  set +e
  wait -n
  wait_rc=$?
  set -e

  if [[ "$wait_rc" -ne 0 ]]; then
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    echo "[$(date '+%F %T')] FAIL-FAST aborting sweep after background failure" \
      | tee -a "$RUN_LOG"
    exit "$wait_rc"
  fi
}

if [[ "$PARALLEL_JOBS" -gt 1 ]]; then
  for client_count in $CLIENT_COUNTS; do
    for kind in $KINDS; do
      run_and_process_one "$kind" "$client_count" &
      while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
        wait_for_background_or_fail
      done
    done
  done
  while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
    wait_for_background_or_fail
  done
else
  for client_count in $CLIENT_COUNTS; do
    if [[ "$PARALLEL_PAIR" -eq 1 ]]; then
      unset pid_dedicated pid_shared
      for kind in $KINDS; do
        run_runner_only "$kind" "$client_count" &
        if [[ "$kind" == "dedicated" ]]; then
          pid_dedicated=$!
        else
          pid_shared=$!
        fi
      done
      if [[ -n "${pid_dedicated:-}" ]]; then
        wait "$pid_dedicated"
      fi
      if [[ -n "${pid_shared:-}" ]]; then
        wait "$pid_shared"
      fi
      for kind in $KINDS; do
        process_one "$kind" "$client_count"
      done
    else
      for kind in $KINDS; do
        run_runner_only "$kind" "$client_count"
        process_one "$kind" "$client_count"
      done
    fi
  done
fi

echo
echo "Sweep complete."
echo "root_outdir=$ROOT_OUTDIR"
echo "summary_csv=$SUMMARY_CSV"
echo "steady_csv=$STEADY_CSV"
echo "fail_csv=$FAIL_CSV"
echo "run_failures_tsv=$RUN_FAILURES_TSV"

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_moti_overall_dedicated_pool.sh [options]

Options:
  --root-outdir <dir>         Root output directory. Required.
  --noncc-checkpoint <dir>    Existing classic checkpoint for non-cc dedicated. Required.
  --cc-checkpoint <dir>       Existing ruby checkpoint for cc dedicated. Required.
  --parallel-jobs <N>         Max concurrent gem5 jobs across noncc+cc. Default: 18
  --count-per-client <N>      Requests per client. Default: 30
  --num-cpus <N>              Guest CPU count for all runs. Default: 34
  --plan-dir <dir>            Reuse an existing plan output directory.
  --skip-build                Reuse the existing gem5 binary.
  --skip-image-setup          Reuse the already injected dedicated binaries.
  --skip-existing             Reuse completed result.json files when possible.
  --continue-on-failure       Record failures and keep advancing the queue.
  --help                      Show this message.

Scope:
  - canonical moti+overall noncc_dedicated unique points
  - canonical moti noncc/cc dedicated comparison points
  - no shared points
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
NONCC_CHECKPOINT=""
CC_CHECKPOINT=""
PARALLEL_JOBS=18
COUNT_PER_CLIENT=30
NUM_CPUS=34
PLAN_DIR=""
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
CONTINUE_ON_FAILURE=0
ANY_FAILURES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-outdir)
      ROOT_OUTDIR="$2"
      shift 2
      ;;
    --noncc-checkpoint)
      NONCC_CHECKPOINT="$2"
      shift 2
      ;;
    --cc-checkpoint)
      CC_CHECKPOINT="$2"
      shift 2
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
    --plan-dir)
      PLAN_DIR="$2"
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

if [[ -z "$ROOT_OUTDIR" ]]; then
  echo "--root-outdir is required" >&2
  exit 1
fi
if [[ -z "$NONCC_CHECKPOINT" ]]; then
  echo "--noncc-checkpoint is required" >&2
  exit 1
fi
if [[ -z "$CC_CHECKPOINT" ]]; then
  echo "--cc-checkpoint is required" >&2
  exit 1
fi
if [[ "$PARALLEL_JOBS" -lt 1 ]]; then
  echo "--parallel-jobs must be >= 1" >&2
  exit 1
fi
if [[ "$COUNT_PER_CLIENT" -lt 1 ]]; then
  echo "--count-per-client must be >= 1" >&2
  exit 1
fi
if [[ "$NUM_CPUS" -lt 2 ]]; then
  echo "--num-cpus must be >= 2" >&2
  exit 1
fi

ROOT_OUTDIR="$(mkdir -p "$ROOT_OUTDIR" && cd "$ROOT_OUTDIR" && pwd)"
NONCC_CHECKPOINT="$(cd "$NONCC_CHECKPOINT" && pwd)"
CC_CHECKPOINT="$(cd "$CC_CHECKPOINT" && pwd)"
if [[ ! -d "$NONCC_CHECKPOINT" ]]; then
  echo "noncc checkpoint not found: $NONCC_CHECKPOINT" >&2
  exit 1
fi
if [[ ! -d "$CC_CHECKPOINT" ]]; then
  echo "cc checkpoint not found: $CC_CHECKPOINT" >&2
  exit 1
fi

if [[ -z "$PLAN_DIR" ]]; then
  PLAN_DIR="$ROOT_OUTDIR/plan"
fi
PLAN_DIR="$(mkdir -p "$PLAN_DIR" && cd "$PLAN_DIR" && pwd)"

RUN_LOG="$ROOT_OUTDIR/run.log"
CASE_MANIFEST="$ROOT_OUTDIR/cases.tsv"
FAIL_LOG="$ROOT_OUTDIR/failures.tsv"
: >"$RUN_LOG"
: >"$FAIL_LOG"

log_msg() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" | tee -a "$RUN_LOG"
}

has_success_result_json() {
  local result_json="$1"
  [[ -f "$result_json" ]] || return 1
  rg -q '"status"[[:space:]]*:[[:space:]]*"success"' "$result_json" 2>/dev/null
}

slow_count_per_client_from_pct() {
  local pct="$1"
  printf '%s\n' $((((COUNT_PER_CLIENT * pct) + 50) / 100))
}

expected_total_requests() {
  local client_count="$1"
  local slow_client_count="$2"
  local slow_request_pct="$3"
  local slow_count_per_client=0

  if [[ "$slow_client_count" -le 0 ]]; then
    printf '%s\n' $((client_count * COUNT_PER_CLIENT))
    return 0
  fi

  slow_count_per_client="$(slow_count_per_client_from_pct "$slow_request_pct")"
  printf '%s\n' \
    $((((client_count - slow_client_count) * COUNT_PER_CLIENT) +
       (slow_client_count * slow_count_per_client)))
}

prepare_prereqs() {
  if [[ "$SKIP_BUILD" -eq 0 ]]; then
    log_msg "BUILD build/X86/gem5.opt"
    scons build/X86/gem5.opt -j"$(nproc)"
  fi

  if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
    log_msg "SETUP-IMAGE files/parsec.img"
    bash tools/setup_hydrarpc_dedicated_all_disk_image.sh files/parsec.img
  fi
}

build_case_manifest() {
  python3 tools/plan_moti_overall_matrix.py --outdir "$PLAN_DIR" >/dev/null
  python3 - "$PLAN_DIR" "$CASE_MANIFEST" <<'PY'
import csv
import sys
from pathlib import Path

plan_dir = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
rows = []

for name in (
    "canonical_experiments__noncc_dedicated.csv",
    "canonical_experiments__cc_dedicated.csv",
):
    with (plan_dir / name).open(newline="") as fh:
        for row in csv.DictReader(fh):
            rows.append(row)

rows.sort(
    key=lambda r: (
        -int(r["client_count"]),
        r["project"],
        r["transport"],
        r["experiment_id"],
    )
)

with manifest_path.open("w", newline="") as fh:
    writer = csv.writer(fh, delimiter="\t")
    writer.writerow([
        "project",
        "experiment_id",
        "transport",
        "client_count",
        "req_bytes",
        "resp_bytes",
        "req_min_bytes",
        "req_max_bytes",
        "resp_min_bytes",
        "resp_max_bytes",
        "slow_client_count",
        "slow_request_pct",
    ])
    for row in rows:
        writer.writerow([
            row["project"],
            row["experiment_id"],
            row["transport"],
            row["client_count"],
            row["req_bytes"],
            row["resp_bytes"],
            row["req_min_bytes"],
            row["req_max_bytes"],
            row["resp_min_bytes"],
            row["resp_max_bytes"],
            row["slow_client_count"],
            row["slow_request_pct"],
        ])
print(len(rows))
PY
}

run_noncc_case() {
  local experiment_id="$1"
  local transport="$2"
  local client_count="$3"
  local req_bytes="$4"
  local resp_bytes="$5"
  local req_min_bytes="$6"
  local req_max_bytes="$7"
  local resp_min_bytes="$8"
  local resp_max_bytes="$9"
  local slow_client_count="${10}"
  local slow_request_pct="${11}"
  local outdir="$ROOT_OUTDIR/noncc_dedicated/$experiment_id"
  local console_log="$outdir/console.log"
  local result_log="$outdir/hydrarpc_dedicated.result.log"
  local request_mode="$transport"
  local response_mode="$transport"
  local expected_total=0
  local slow_count_per_client=0
  local cmd=()

  mkdir -p "$outdir"
  if [[ "$SKIP_EXISTING" -eq 1 ]] && has_success_result_json "$outdir/result.json"; then
    log_msg "REUSE project=noncc_dedicated exp_id=$experiment_id"
    return 0
  fi

  cmd=(
    bash tools/run_e2e_hydrarpc_dedicated.sh
    --outdir "$outdir"
    --restore-checkpoint "$NONCC_CHECKPOINT"
    --skip-build
    --skip-image-setup
    --boot-cpu KVM
    --cpu-type TIMING
    --num-cpus "$NUM_CPUS"
    --client-count "$client_count"
    --count-per-client "$COUNT_PER_CLIENT"
    --window-size 1
    --slot-count 1024
    --terminal-port 0
    --req-bytes "$req_bytes"
    --resp-bytes "$resp_bytes"
    --request-transfer-mode "$request_mode"
    --response-transfer-mode "$response_mode"
  )

  if [[ "$req_min_bytes" -gt 0 ]]; then
    cmd+=(--req-min-bytes "$req_min_bytes")
  fi
  if [[ "$req_max_bytes" -gt 0 ]]; then
    cmd+=(--req-max-bytes "$req_max_bytes")
  fi
  if [[ "$resp_min_bytes" -gt 0 ]]; then
    cmd+=(--resp-min-bytes "$resp_min_bytes")
  fi
  if [[ "$resp_max_bytes" -gt 0 ]]; then
    cmd+=(--resp-max-bytes "$resp_max_bytes")
  fi
  if [[ "$slow_client_count" -gt 0 ]]; then
    slow_count_per_client="$(slow_count_per_client_from_pct "$slow_request_pct")"
    cmd+=(
      --slow-client-count "$slow_client_count"
      --slow-count-per-client "$slow_count_per_client"
      --slow-send-gap-ns 20000
    )
  fi

  log_msg "START project=noncc_dedicated exp_id=$experiment_id outdir=$outdir"
  set +e
  "${cmd[@]}" >"$console_log" 2>&1
  local rc=$?
  set -e
  printf '%s\n' "$rc" >"$outdir/runner.exitcode"
  if [[ "$rc" -ne 0 ]]; then
    log_msg "RUN-FAIL project=noncc_dedicated exp_id=$experiment_id rc=$rc"
    return "$rc"
  fi
  if [[ ! -f "$result_log" ]]; then
    log_msg "MISSING-RESULT project=noncc_dedicated exp_id=$experiment_id"
    return 1
  fi

  expected_total="$(expected_total_requests "$client_count" "$slow_client_count" "$slow_request_pct")"
  python3 tools/summarize_hydrarpc_multiclient.py \
    --log "$result_log" \
    --experiment dedicated \
    --client-count "$client_count" \
    --count-per-client "$COUNT_PER_CLIENT" \
    --expected-total-requests "$expected_total" \
    --drop-first-per-client 1 \
    --outdir "$outdir" \
    --result-json "$outdir/result.json" \
    --extra-field "project=noncc_dedicated" \
    --extra-field "transport=$transport" \
    --extra-field "experiment_id=$experiment_id" \
    --extra-field "window_size=1" \
    --extra-field "slow_client_count=$slow_client_count" \
    --extra-field "slow_request_pct=$slow_request_pct" \
    >"$outdir/summary.log" 2>&1
  log_msg "END project=noncc_dedicated exp_id=$experiment_id"
}

run_cc_case() {
  local experiment_id="$1"
  local client_count="$2"
  local outdir="$ROOT_OUTDIR/cc_dedicated/$experiment_id"
  local console_log="$outdir/console.log"
  local result_log="$outdir/hydrarpc_dedicated_coherent.result.log"
  local cmd=()

  mkdir -p "$outdir"
  if [[ "$SKIP_EXISTING" -eq 1 ]] && has_success_result_json "$outdir/result.json"; then
    log_msg "REUSE project=cc_dedicated exp_id=$experiment_id"
    return 0
  fi

  cmd=(
    bash tools/run_e2e_hydrarpc_dedicated_coherent.sh
    --outdir "$outdir"
    --restore-checkpoint "$CC_CHECKPOINT"
    --skip-build
    --skip-image-setup
    --boot-cpu KVM
    --cpu-type TIMING
    --num-cpus "$NUM_CPUS"
    --client-count "$client_count"
    --count-per-client "$COUNT_PER_CLIENT"
    --window-size 1
    --slot-count 1024
    --terminal-port 0
    --req-bytes 64
    --resp-bytes 64
    --request-transfer-mode staging
    --response-transfer-mode staging
  )

  log_msg "START project=cc_dedicated exp_id=$experiment_id outdir=$outdir"
  set +e
  "${cmd[@]}" >"$console_log" 2>&1
  local rc=$?
  set -e
  printf '%s\n' "$rc" >"$outdir/runner.exitcode"
  if [[ "$rc" -ne 0 ]]; then
    log_msg "RUN-FAIL project=cc_dedicated exp_id=$experiment_id rc=$rc"
    return "$rc"
  fi
  if [[ ! -f "$result_log" ]]; then
    log_msg "MISSING-RESULT project=cc_dedicated exp_id=$experiment_id"
    return 1
  fi

  python3 tools/summarize_hydrarpc_multiclient.py \
    --log "$result_log" \
    --experiment cc_dedicated \
    --client-count "$client_count" \
    --count-per-client "$COUNT_PER_CLIENT" \
    --expected-total-requests "$((client_count * COUNT_PER_CLIENT))" \
    --drop-first-per-client 1 \
    --outdir "$outdir" \
    --result-json "$outdir/result.json" \
    --extra-field "project=cc_dedicated" \
    --extra-field "transport=staging_cc" \
    --extra-field "experiment_id=$experiment_id" \
    --extra-field "window_size=1" \
    >"$outdir/summary.log" 2>&1
  log_msg "END project=cc_dedicated exp_id=$experiment_id"
}

launch_case() {
  local project="$1"
  local experiment_id="$2"
  local transport="$3"
  local client_count="$4"
  local req_bytes="$5"
  local resp_bytes="$6"
  local req_min_bytes="$7"
  local req_max_bytes="$8"
  local resp_min_bytes="$9"
  local resp_max_bytes="${10}"
  local slow_client_count="${11}"
  local slow_request_pct="${12}"

  (
    set +e
    if [[ "$project" == "noncc_dedicated" ]]; then
      run_noncc_case \
        "$experiment_id" "$transport" "$client_count" "$req_bytes" "$resp_bytes" \
        "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
        "$slow_client_count" "$slow_request_pct"
    else
      run_cc_case "$experiment_id" "$client_count"
    fi
    local rc=$?
    set -e
    if [[ "$rc" -ne 0 ]]; then
      printf '%s\t%s\t%s\n' "$project" "$experiment_id" "$rc" >>"$FAIL_LOG"
    fi
    exit "$rc"
  ) &
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
      log_msg "CONTINUE-FAIL rc=${wait_rc}; continuing queue"
      return 0
    fi
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    exit "$wait_rc"
  fi
}

wait_for_slot() {
  while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
    wait_for_background_or_fail
  done
}

prepare_prereqs
case_count="$(build_case_manifest)"
log_msg "QUEUED total_cases=${case_count} manifest=${CASE_MANIFEST}"

{
  read -r _
  while IFS=$'\t' read -r project experiment_id transport client_count req_bytes resp_bytes req_min_bytes req_max_bytes resp_min_bytes resp_max_bytes slow_client_count slow_request_pct; do
    launch_case \
      "$project" "$experiment_id" "$transport" "$client_count" "$req_bytes" "$resp_bytes" \
      "$req_min_bytes" "$req_max_bytes" "$resp_min_bytes" "$resp_max_bytes" \
      "$slow_client_count" "$slow_request_pct"
    wait_for_slot
  done
} <"$CASE_MANIFEST"

while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
  wait_for_background_or_fail
done

if [[ "$ANY_FAILURES" -ne 0 ]]; then
  log_msg "DONE with failures fail_log=${FAIL_LOG}"
  exit 1
fi

log_msg "DONE success"

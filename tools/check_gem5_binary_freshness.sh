#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/check_gem5_binary_freshness.sh --binary <path> [options]

Options:
  --binary <path>         gem5 binary to validate.
  --monitor <path>        Source/config file that must not be newer than the binary.
                          Repeatable.
  --label <text>          Human-readable label shown in stale-build errors.
                          Default: gem5 binary
  --help                  Show this message.

This helper is intended for scripts that accept --skip-build. It fails fast when
the selected gem5 binary is older than known runtime-relevant sources.
EOF
}

BINARY=""
LABEL="gem5 binary"
MONITOR_FILES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      BINARY="$2"
      shift 2
      ;;
    --monitor)
      MONITOR_FILES+=("$2")
      shift 2
      ;;
    --label)
      LABEL="$2"
      shift 2
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

if [[ -z "$BINARY" ]]; then
  echo "--binary is required" >&2
  exit 1
fi

if [[ ! -e "$BINARY" ]]; then
  echo "$LABEL not found: $BINARY" >&2
  echo "Run the corresponding script without --skip-build to rebuild gem5." >&2
  exit 1
fi

stale_files=()
for monitor_path in "${MONITOR_FILES[@]}"; do
  if [[ -e "$monitor_path" && "$monitor_path" -nt "$BINARY" ]]; then
    stale_files+=("$monitor_path")
  fi
done

if [[ "${#stale_files[@]}" -eq 0 ]]; then
  exit 0
fi

echo "$LABEL is stale: $BINARY is older than the following monitored files:" >&2
printf '  %s\n' "${stale_files[@]}" >&2
echo "Rerun without --skip-build, or rebuild $BINARY before resuming this workflow." >&2
exit 1

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

HYDRARPC_BINARY_NAME="hydrarpc_dedicated_app" \
HYDRARPC_GUEST_SOURCE="${REPO_ROOT}/tools/hydrarpc_dedicated_app.c" \
HYDRARPC_GUEST_LDLIBS="-lm" \
  bash "${REPO_ROOT}/tools/setup_hydrarpc_dedicated_disk_image.sh" "$@"

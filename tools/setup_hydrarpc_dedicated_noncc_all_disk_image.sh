#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/setup_hydrarpc_dedicated_noncc_all_disk_image.sh [disk-image]

Inject all non-coherent dedicated-side guest binaries needed by the current
dedicated SimCXL experiment set into one disk image:
  - hydrarpc_dedicated
  - hydrarpc_dedicated_app

Default disk image: files/parsec.img
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DISK_IMAGE="${1:-${REPO_ROOT}/files/parsec.img}"

if [[ "$DISK_IMAGE" == "--help" || "$DISK_IMAGE" == "-h" ]]; then
  usage
  exit 0
fi

bash "${REPO_ROOT}/tools/setup_hydrarpc_dedicated_disk_image.sh" "$DISK_IMAGE"
bash "${REPO_ROOT}/tools/setup_hydrarpc_dedicated_app_disk_image.sh" "$DISK_IMAGE"

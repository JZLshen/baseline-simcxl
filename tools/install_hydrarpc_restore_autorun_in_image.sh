#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/install_hydrarpc_restore_autorun_in_image.sh <disk-image> <host-wrapper-script>

Overwrite the existing coherent guest wrapper inside a disk image copy with a
per-run wrapper script. The wrapper path already exists before checkpointing,
which avoids restore-time issues with newly introduced guest file paths.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DISK_IMAGE="${1:-}"
HOST_WRAPPER_SCRIPT="${2:-}"
GUEST_DEST_DIR="/home/test_code"
GUEST_WRAPPER="${GUEST_DEST_DIR}/run_hydrarpc_dedicated_coherent.sh"
MOUNT_POINT="/tmp/hydrarpc_restore_autorun_disk_$$"
LOOP_DEVICE=""
LOOP_PARTITION=""

cleanup() {
  sudo umount "${MOUNT_POINT}" >/dev/null 2>&1 || true
  if [[ -n "${LOOP_DEVICE}" ]]; then
    sudo losetup -d "${LOOP_DEVICE}" >/dev/null 2>&1 || true
  fi
  rmdir "${MOUNT_POINT}" >/dev/null 2>&1 || true
}

if [[ -z "${DISK_IMAGE}" || -z "${HOST_WRAPPER_SCRIPT}" || "${DISK_IMAGE}" == "--help" || "${DISK_IMAGE}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ ! -f "${DISK_IMAGE}" ]]; then
  echo "disk image not found: ${DISK_IMAGE}" >&2
  exit 1
fi

if [[ ! -f "${HOST_WRAPPER_SCRIPT}" ]]; then
  echo "host wrapper script not found: ${HOST_WRAPPER_SCRIPT}" >&2
  exit 1
fi

if ! sudo -n true >/dev/null 2>&1; then
  echo "restore autorun image install requires passwordless sudo or an active sudo ticket" >&2
  exit 1
fi

mkdir -p "${MOUNT_POINT}"
trap cleanup EXIT

if ! sudo mount -o loop "${DISK_IMAGE}" "${MOUNT_POINT}" 2>/dev/null; then
  LOOP_DEVICE="$(sudo losetup --find --show -Pf "${DISK_IMAGE}" 2>/dev/null || true)"
  if [[ -z "${LOOP_DEVICE}" ]]; then
    echo "failed to attach loop device for ${DISK_IMAGE}" >&2
    exit 1
  fi

  LOOP_PARTITION="$(sudo lsblk -lnpo NAME,TYPE "${LOOP_DEVICE}" 2>/dev/null | awk '$2 == "part" {print $1; exit}')"
  if [[ -z "${LOOP_PARTITION}" ]]; then
    echo "failed to discover mountable partition for ${DISK_IMAGE}" >&2
    exit 1
  fi

  sudo mount "${LOOP_PARTITION}" "${MOUNT_POINT}"
fi

sudo mkdir -p "${MOUNT_POINT}${GUEST_DEST_DIR}"
sudo install -m 0755 "${HOST_WRAPPER_SCRIPT}" "${MOUNT_POINT}${GUEST_WRAPPER}"

sync
echo "Installed ${GUEST_WRAPPER} into ${DISK_IMAGE}"

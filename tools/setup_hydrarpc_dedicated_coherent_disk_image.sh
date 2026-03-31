#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/setup_hydrarpc_dedicated_coherent_disk_image.sh <disk-image>

Build the coherent dedicated multi-client hydrarpc guest binary on the host,
then inject the binary and a small guest-side wrapper into the disk image.

Environment:
  `HYDRARPC_GUEST_CFLAGS`  Host gcc flags for the injected guest binary.
                           Default: `-O2 -Wall -static -g -pthread`
  `CC`                     Host compiler. Default: `gcc`
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DISK_IMAGE="${1:-}"
SOURCE="${REPO_ROOT}/tools/hydrarpc_dedicated_coherent.c"
BUILD_DIR="${REPO_ROOT}/output/hydrarpc_dedicated_coherent_guest"
BINARY_NAME="hydrarpc_dedicated_coherent"
HOST_BINARY="${BUILD_DIR}/${BINARY_NAME}"
GEM5_INCLUDE_DIR="${REPO_ROOT}/include"
M5OPS_SOURCE="${REPO_ROOT}/util/m5/src/abi/x86/m5op.S"
GUEST_DEST_DIR="/home/test_code"
GUEST_BINARY="${GUEST_DEST_DIR}/${BINARY_NAME}"
GUEST_WRAPPER="${GUEST_DEST_DIR}/run_${BINARY_NAME}.sh"
LEGACY_GUEST_BINARY="${GUEST_DEST_DIR}/hydrarpc_multiclient_dedicated_coherent_send1_poll1"
LEGACY_GUEST_WRAPPER="${GUEST_DEST_DIR}/run_hydrarpc_multiclient_dedicated_coherent_send1_poll1.sh"
LEGACY_GUEST_WORKDIR="/root/hydrarpc"
ROOT_BASHRC="/root/.bashrc"
CC_BIN="${CC:-gcc}"
GUEST_CFLAGS="${HYDRARPC_GUEST_CFLAGS:--O2 -Wall -static -g -pthread}"
MOUNT_POINT="/tmp/hydrarpc_dedicated_coherent_disk_$$"
LOOP_DEVICE=""

cleanup() {
  sudo umount "${MOUNT_POINT}" >/dev/null 2>&1 || true
  if [[ -n "${LOOP_DEVICE}" ]]; then
    sudo losetup -d "${LOOP_DEVICE}" >/dev/null 2>&1 || true
  fi
  rmdir "${MOUNT_POINT}" >/dev/null 2>&1 || true
}

strip_legacy_bootstrap_from_bashrc() {
  local mounted_bashrc="${MOUNT_POINT}${ROOT_BASHRC}"

  if [[ ! -f "${mounted_bashrc}" ]]; then
    return 0
  fi

  sudo sed -i \
    '/^# hydrarpc local bootstrap$/,/^fi$/d' \
    "${mounted_bashrc}"
}

cleanup_legacy_dedicated_guest_artifacts() {
  local mounted_workdir="${MOUNT_POINT}${LEGACY_GUEST_WORKDIR}"

  sudo rm -f \
    "${MOUNT_POINT}${LEGACY_GUEST_BINARY}" \
    "${MOUNT_POINT}${LEGACY_GUEST_WRAPPER}" \
    "${mounted_workdir}/hydrarpc_autorun.env" \
    "${mounted_workdir}/hydrarpc_multiclient_dedicated_bootstrap.sh" \
    "${mounted_workdir}/hydrarpc_multiclient_dedicated_guest_runner.sh" \
    "${mounted_workdir}/hydrarpc_multiclient_dedicated_send1_poll1.c" \
    "${mounted_workdir}/hydrarpc_multiclient_dedicated_send1_poll1"
  sudo rmdir "${mounted_workdir}" >/dev/null 2>&1 || true
  strip_legacy_bootstrap_from_bashrc
}

if [[ -z "${DISK_IMAGE}" || "${DISK_IMAGE}" == "--help" || "${DISK_IMAGE}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ ! -f "${DISK_IMAGE}" ]]; then
  echo "disk image not found: ${DISK_IMAGE}" >&2
  exit 1
fi

if [[ ! -f "${SOURCE}" ]]; then
  echo "guest source not found: ${SOURCE}" >&2
  exit 1
fi

if [[ ! -f "${GEM5_INCLUDE_DIR}/gem5/m5ops.h" ]]; then
  echo "gem5 m5ops headers not found under ${GEM5_INCLUDE_DIR}" >&2
  exit 1
fi

if [[ ! -f "${M5OPS_SOURCE}" ]]; then
  echo "gem5 x86 m5ops source not found: ${M5OPS_SOURCE}" >&2
  exit 1
fi

if ! sudo -n true >/dev/null 2>&1; then
  echo "disk image setup requires passwordless sudo or an active sudo ticket" >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"

"${CC_BIN}" -std=gnu11 ${GUEST_CFLAGS} \
  -Wl,-z,noexecstack \
  -I "${GEM5_INCLUDE_DIR}" \
  "${M5OPS_SOURCE}" \
  "${SOURCE}" \
  -o "${HOST_BINARY}"

if [[ ! -x "${HOST_BINARY}" ]]; then
  echo "failed to build guest binary: ${HOST_BINARY}" >&2
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

cleanup_legacy_dedicated_guest_artifacts

sudo mkdir -p "${MOUNT_POINT}${GUEST_DEST_DIR}"
sudo install -m 0755 "${HOST_BINARY}" "${MOUNT_POINT}${GUEST_BINARY}"

WRAPPER_TMP="$(mktemp)"
cat > "${WRAPPER_TMP}" <<'EOF'
#!/bin/sh
set -eu
exec numactl -N 0 -m 0 /home/test_code/hydrarpc_dedicated_coherent "$@"
EOF
sudo install -m 0755 "${WRAPPER_TMP}" "${MOUNT_POINT}${GUEST_WRAPPER}"
rm -f "${WRAPPER_TMP}"

sync
echo "Injected ${GUEST_BINARY} into ${DISK_IMAGE}"
echo "Injected ${GUEST_WRAPPER} into ${DISK_IMAGE}"

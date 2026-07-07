#!/usr/bin/env bash
#
# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  scripts/run-isolated.sh -- <command> [args...]

Runs a command in a fresh user, mount, network, and IPC namespace with private
tmpfs mounts for /tmp and /dev/shm. This is intended for high-churn native LoLa
test harnesses that need each test generation to get its own runtime namespace
without changing the bundled S-CORE runtime.

Environment:
  LOLA_ISOLATED_STATE_DIR  Host-visible directory for holder logs/ready marker.
                           Default: target/lola-isolated/<timestamp>-<pid>
  LOLA_ISOLATED_TMP_SIZE   tmpfs size for /tmp. Default: 1g
  LOLA_ISOLATED_SHM_SIZE   tmpfs size for /dev/shm. Default: 2g

Example:
  scripts/run-isolated.sh -- cargo test --all-targets -- --ignored native
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ "${1:-}" == "--" ]]; then
    shift
fi

if [[ $# -eq 0 ]]; then
    usage >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
state_dir="${LOLA_ISOLATED_STATE_DIR:-${repo_dir}/target/lola-isolated/${stamp}-$$}"
tmp_size="${LOLA_ISOLATED_TMP_SIZE:-1g}"
shm_size="${LOLA_ISOLATED_SHM_SIZE:-2g}"
ready_path="${state_dir}/namespace-ready"
holder_log="${state_dir}/namespace-holder.log"
holder_pid=""

cleanup() {
    if [[ -n "${holder_pid}" ]] && kill -0 "${holder_pid}" 2>/dev/null; then
        kill "${holder_pid}" 2>/dev/null || true
        wait "${holder_pid}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

mkdir -p "${state_dir}"
rm -f "${ready_path}"

unshare -U --map-root-user -m -n -i sh -c '
set -eu
ip link set lo up
mount -t tmpfs -o "size=$2" tmpfs /tmp
mount -t tmpfs -o "size=$3" tmpfs /dev/shm
mkdir -p /tmp/up-streamer-iceoryx2
touch "$1"
exec sleep infinity
' sh "${ready_path}" "${tmp_size}" "${shm_size}" >"${holder_log}" 2>&1 &
holder_pid="$!"

for _ in $(seq 1 200); do
    if [[ -e "${ready_path}" ]]; then
        break
    fi
    if ! kill -0 "${holder_pid}" 2>/dev/null; then
        printf 'namespace holder exited before ready; log=%s\n' "${holder_log}" >&2
        exit 1
    fi
    sleep 0.05
done

if [[ ! -e "${ready_path}" ]]; then
    printf 'timed out waiting for namespace holder; log=%s\n' "${holder_log}" >&2
    exit 1
fi

nsenter -t "${holder_pid}" -U --preserve-credentials -m -n -i -- "$@"

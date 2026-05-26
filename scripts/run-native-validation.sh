#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-all}"
if [[ $# -gt 0 ]]; then
    shift
fi

bazelisk_version="$(tr -d '[:space:]' < "${repo_dir}/tools/bazelisk.version")"
bazelisk_sha="$(tr -d '[:space:]' < "${repo_dir}/tools/bazelisk-linux-amd64.sha256")"
tool_dir="${BAZELISK_CACHE_DIR:-${repo_dir}/.cache/tools}"
bazelisk="${tool_dir}/bazelisk-${bazelisk_version}-linux-amd64"

ensure_bazelisk() {
    mkdir -p "${tool_dir}"
    if [[ ! -x "${bazelisk}" ]]; then
        curl -fsSL \
            -o "${bazelisk}.tmp" \
            "https://github.com/bazelbuild/bazelisk/releases/download/${bazelisk_version}/bazelisk-linux-amd64"
        actual_sha="$(sha256sum "${bazelisk}.tmp" | cut -d ' ' -f 1)"
        if [[ "${actual_sha}" != "${bazelisk_sha}" ]]; then
            rm -f "${bazelisk}.tmp"
            printf 'Bazelisk checksum mismatch: expected %s, got %s\n' "${bazelisk_sha}" "${actual_sha}" >&2
            exit 1
        fi
        chmod +x "${bazelisk}.tmp"
        mv "${bazelisk}.tmp" "${bazelisk}"
    else
        actual_sha="$(sha256sum "${bazelisk}" | cut -d ' ' -f 1)"
        if [[ "${actual_sha}" != "${bazelisk_sha}" ]]; then
            printf 'Cached Bazelisk checksum mismatch: expected %s, got %s\n' "${bazelisk_sha}" "${actual_sha}" >&2
            exit 1
        fi
    fi
    export BAZEL="${bazelisk}"
    export BAZELISK_HOME="${BAZELISK_HOME:-${repo_dir}/.cache/bazelisk-home}"
}

run_stub() {
    cargo test --no-default-features --features test-stub "$@"
}

run_check() {
    ensure_bazelisk
    cargo check --all-targets "$@"
}

run_native() {
    ensure_bazelisk
    cargo test -- --ignored native "$@"
}

run_docs() {
    ensure_bazelisk
    RUSTDOCFLAGS=-Dwarnings cargo doc --no-deps "$@"
}

case "${mode}" in
    bootstrap)
        ensure_bazelisk
        "${BAZEL}" --version
        ;;
    stub)
        run_stub "$@"
        ;;
    check)
        run_check "$@"
        ;;
    native)
        run_native "$@"
        ;;
    docs)
        run_docs "$@"
        ;;
    all)
        run_stub
        run_check
        run_native
        run_docs
        ;;
    *)
        printf 'usage: %s [bootstrap|stub|check|native|docs|all]\n' "$0" >&2
        exit 2
        ;;
esac

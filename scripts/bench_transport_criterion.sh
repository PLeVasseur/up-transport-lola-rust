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

readonly TRANSPORT_NAME="lola"
readonly DEFAULT_REPORT_DIR="target/transport-perf/$TRANSPORT_NAME"
readonly DEFAULT_CRITERION_ARGS="--output-format bencher --sample-size 10 --warm-up-time 1 --measurement-time 2 --noise-threshold 0.05"
readonly BASELINE_NAME="payload_contract_representative_v1"

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TRANSPORT_BENCH_SUITE="${TRANSPORT_BENCH_SUITE:-payload-contract}"
TRANSPORT_BENCH_PROFILE="${TRANSPORT_BENCH_PROFILE:-all}"
TRANSPORT_BENCH_REPORT_DIR="${TRANSPORT_BENCH_REPORT_DIR:-$DEFAULT_REPORT_DIR}"
CRITERION_ARGS="${CRITERION_ARGS:-$DEFAULT_CRITERION_ARGS}"
BENCH_PIN_PREFIX="${BENCH_PIN_PREFIX:-}"

usage() {
    cat <<'USAGE'
Usage:
  scripts/bench_transport_criterion.sh baseline
  scripts/bench_transport_criterion.sh candidate <phase_candidate>
  scripts/bench_transport_criterion.sh guardrail <phase_candidate> <report_path>
  scripts/bench_transport_criterion.sh export

Environment:
  TRANSPORT_BENCH_REPORT_DIR  Report output directory. Default: target/transport-perf/lola
  TRANSPORT_BENCH_SUITE       raw, payload-contract, or all. Default: payload-contract
  TRANSPORT_BENCH_PROFILE     core, camera, or all. Default: all
  CRITERION_ARGS              Criterion args. Default matches representative-v1.
  BENCH_PIN_PREFIX            Optional command prefix for CPU pinning, etc.
  BAZEL                       Bazel/Bazelisk path for bundled LoLa bridge builds.

If BAZEL is unset, the script uses the repo-local Bazelisk installed by
scripts/run-native-validation.sh bootstrap when available.
USAGE
}

validate_suite() {
    case "$TRANSPORT_BENCH_SUITE" in
        raw | payload-contract | all) ;;
        *)
            printf 'TRANSPORT_BENCH_SUITE must be one of raw, payload-contract, all\n' >&2
            exit 2
            ;;
    esac
}

validate_profile() {
    case "$TRANSPORT_BENCH_PROFILE" in
        core | camera | all) ;;
        *)
            printf 'TRANSPORT_BENCH_PROFILE must be one of core, camera, all\n' >&2
            exit 2
            ;;
    esac
}

cargo_features() {
    local path="$1"

    validate_suite
    case "$path:$TRANSPORT_BENCH_SUITE" in
        zero-copy:raw)
            printf '%s\n' "zero-copy"
            ;;
        zero-copy:payload-contract | zero-copy:all)
            printf '%s\n' "zero-copy,payload-contract-large-benchmarks"
            ;;
        owned:raw)
            printf '%s\n' "benchmark-owned"
            ;;
        owned:payload-contract | owned:all)
            printf '%s\n' "benchmark-owned,payload-contract-large-benchmarks"
            ;;
        *)
            printf 'unknown benchmark path: %s\n' "$path" >&2
            exit 2
            ;;
    esac
}

selected_profiles() {
    validate_profile
    case "$TRANSPORT_BENCH_PROFILE" in
        core)
            printf '%s\n' "core"
            ;;
        camera)
            printf '%s\n' "camera"
            ;;
        all)
            printf '%s\n' "core"
            printf '%s\n' "camera"
            ;;
    esac
}

repo_bazelisk() {
    local version_file="$repo_dir/tools/bazelisk.version"
    if [[ ! -f "$version_file" ]]; then
        return 1
    fi

    local version
    version="$(tr -d '[:space:]' < "$version_file")"
    local candidate="$repo_dir/.cache/tools/bazelisk-${version}-linux-amd64"
    [[ -x "$candidate" ]] || return 1
    printf '%s\n' "$candidate"
}

resolve_bazel() {
    if [[ -n "${BAZEL:-}" ]]; then
        printf '%s\n' "$BAZEL"
        return
    fi

    local candidate
    if candidate="$(repo_bazelisk)"; then
        printf '%s\n' "$candidate"
        return
    fi

    if command -v bazelisk >/dev/null 2>&1; then
        command -v bazelisk
        return
    fi

    if command -v bazel >/dev/null 2>&1; then
        command -v bazel
        return
    fi

    printf 'LoLa benchmark script requires BAZEL or repo-local Bazelisk. Run scripts/run-native-validation.sh bootstrap or set BAZEL=/path/to/bazelisk.\n' >&2
    exit 2
}

run_cargo_bench() {
    local path="$1"
    local profile="$2"
    shift 2

    local features
    features="$(cargo_features "$path")"
    local resolved_bazel
    resolved_bazel="$(resolve_bazel)"

    read -r -a criterion_parts <<<"$CRITERION_ARGS"
    if [[ -n "$BENCH_PIN_PREFIX" ]]; then
        read -r -a pin_parts <<<"$BENCH_PIN_PREFIX"
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" \
            LOLA_BENCH_PROFILE="$profile" \
            BAZEL="$resolved_bazel" \
            "${pin_parts[@]}" cargo bench --features "$features" --bench transport_criterion -- "${criterion_parts[@]}" "$@"
    else
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" \
            LOLA_BENCH_PROFILE="$profile" \
            BAZEL="$resolved_bazel" \
            cargo bench --features "$features" --bench transport_criterion -- "${criterion_parts[@]}" "$@"
    fi
}

run_representative_benches() {
    local extra_args=("$@")
    local profile
    while IFS= read -r profile; do
        run_cargo_bench zero-copy "$profile" "${extra_args[@]}"
        run_cargo_bench owned "$profile" "${extra_args[@]}"
    done < <(selected_profiles)
}

git_value() {
    local value
    value="$(git "$@" 2>/dev/null || true)"
    if [[ -n "$value" ]]; then
        printf '%s\n' "$value"
    else
        printf '%s\n' "unknown"
    fi
}

command_line() {
    local path="$1"
    local profile="$2"
    local features
    features="$(cargo_features "$path")"
    local resolved_bazel
    resolved_bazel="$(resolve_bazel)"

    printf 'TRANSPORT_BENCH_SUITE=%s LOLA_BENCH_PROFILE=%s BAZEL=%s cargo bench --features %s --bench transport_criterion -- %s\n' \
        "$TRANSPORT_BENCH_SUITE" \
        "$profile" \
        "$resolved_bazel" \
        "$features" \
        "$CRITERION_ARGS"
}

write_summary() {
    local report_dir="$1"
    local zero_copy_core_raw_output="$2"
    local zero_copy_camera_raw_output="$3"
    local owned_core_raw_output="$4"
    local owned_camera_raw_output="$5"
    local summary="$report_dir/README.md"
    local zero_copy_features
    local owned_features
    local resolved_bazel
    zero_copy_features="$(cargo_features zero-copy)"
    owned_features="$(cargo_features owned)"
    resolved_bazel="$(resolve_bazel)"

    cat >"$summary" <<SUMMARY
# LoLa Payload-Contract Representative Benchmarks

## Commands

Zero-copy core:

\`\`\`bash
$(command_line zero-copy core)
\`\`\`

Zero-copy camera:

\`\`\`bash
$(command_line zero-copy camera)
\`\`\`

Owned core:

\`\`\`bash
$(command_line owned core)
\`\`\`

Owned camera:

\`\`\`bash
$(command_line owned camera)
\`\`\`

## Environment

- Transport: LoLa
- Git head: \`$(git_value rev-parse HEAD)\`
- Git branch: \`$(git_value branch --show-current)\`
- Rust: \`$(rustc --version)\`
- Cargo: \`$(cargo --version)\`
- OS: \`$(uname -srmo)\`
- Suite: \`$TRANSPORT_BENCH_SUITE\`
- Profile request: \`$TRANSPORT_BENCH_PROFILE\`
- Zero-copy features: \`$zero_copy_features\`
- Owned features: \`$owned_features\`
- Criterion args: \`$CRITERION_ARGS\`
- Resolved BAZEL: \`$resolved_bazel\`
- Pinning prefix: \`${BENCH_PIN_PREFIX:-none}\`
- Zero-copy core raw output: \`$zero_copy_core_raw_output\`
- Zero-copy camera raw output: \`$zero_copy_camera_raw_output\`
- Owned core raw output: \`$owned_core_raw_output\`
- Owned camera raw output: \`$owned_camera_raw_output\`

## Required Labels

- \`stable_zc_nozero_full\`
- \`protobuf_owned_full\`
- \`stable_owned_bytes_full\`

## Notes

This script is the Phase 08C2 authority wrapper for the 08C1 representative-v1 command shapes. The owned path uses the benchmark-only copying \`BenchmarkOwnedLolaTransport\` wrapper behind \`benchmark-owned\`; it is not direct true zero-copy. Default features select the bundled LoLa bridge build-from-source path. Artifacts are written only under the caller-selected report directory.
SUMMARY
}

export_results() {
    local report_dir="$TRANSPORT_BENCH_REPORT_DIR"
    local bench_data_dir="$report_dir/bench-data"
    local zero_copy_core_raw_output="$bench_data_dir/transport-criterion-zero-copy-core-bencher.txt"
    local zero_copy_camera_raw_output="$bench_data_dir/transport-criterion-zero-copy-camera-bencher.txt"
    local owned_core_raw_output="$bench_data_dir/transport-criterion-owned-core-bencher.txt"
    local owned_camera_raw_output="$bench_data_dir/transport-criterion-owned-camera-bencher.txt"

    mkdir -p "$bench_data_dir"
    run_cargo_bench zero-copy core | tee "$zero_copy_core_raw_output"
    run_cargo_bench zero-copy camera | tee "$zero_copy_camera_raw_output"
    run_cargo_bench owned core | tee "$owned_core_raw_output"
    run_cargo_bench owned camera | tee "$owned_camera_raw_output"

    rm -rf "$report_dir/criterion-html"
    mkdir -p "$report_dir/criterion-html"
    if [[ -d target/criterion ]]; then
        cp -a target/criterion/. "$report_dir/criterion-html/"
    fi

    if [[ ! -f "$report_dir/guardrail.json" ]]; then
        cat >"$report_dir/guardrail.json" <<JSON
{"status":"unavailable","reason":"criterion-guardrail utility is not available in this standalone repository"}
JSON
    fi

    write_summary \
        "$report_dir" \
        "$zero_copy_core_raw_output" \
        "$zero_copy_camera_raw_output" \
        "$owned_core_raw_output" \
        "$owned_camera_raw_output"
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

subcommand="$1"
shift

case "$subcommand" in
    baseline)
        run_representative_benches --save-baseline "$BASELINE_NAME"
        ;;
    candidate)
        if [[ $# -ne 1 ]]; then
            usage
            exit 1
        fi
        run_representative_benches --save-baseline "$1"
        ;;
    guardrail)
        if [[ $# -ne 2 ]]; then
            usage
            exit 1
        fi
        mkdir -p "$(dirname "$2")"
        cat >"$2" <<JSON
{"status":"unavailable","candidate":"$1","reason":"criterion-guardrail utility is not available in this standalone repository"}
JSON
        ;;
    export)
        export_results
        ;;
    *)
        usage
        exit 2
        ;;
esac

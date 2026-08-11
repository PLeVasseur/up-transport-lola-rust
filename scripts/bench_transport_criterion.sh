#!/usr/bin/env bash
#
# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

readonly DEFAULT_CRITERION_ARGS="--sample-size 60 --warm-up-time 3 --measurement-time 12 --noise-threshold 0.02"
readonly DEFAULT_LARGE_SENSOR_CRITERION_ARGS="--sample-size 20 --warm-up-time 2 --measurement-time 8 --noise-threshold 0.03"
readonly BASELINE_NAME="transport_owned_zc_baseline"
readonly TRANSPORT_NAME="lola"
readonly DEFAULT_REPORT_DIR="target/transport-perf/$TRANSPORT_NAME"

CRITERION_ARGS="${CRITERION_ARGS:-$DEFAULT_CRITERION_ARGS}"
LARGE_SENSOR_CRITERION_ARGS="${LARGE_SENSOR_CRITERION_ARGS:-$DEFAULT_LARGE_SENSOR_CRITERION_ARGS}"
BENCH_PIN_PREFIX="${BENCH_PIN_PREFIX:-}"
TRANSPORT_BENCH_PROFILE="${TRANSPORT_BENCH_PROFILE:-all}"
TRANSPORT_BENCH_SUITE="${TRANSPORT_BENCH_SUITE:-raw}"
TRANSPORT_BENCH_REPORT_DIR="${TRANSPORT_BENCH_REPORT_DIR:-$DEFAULT_REPORT_DIR}"
LOLA_BENCH_BACKEND="${LOLA_BENCH_BACKEND:-native}"

usage() {
    cat <<'USAGE'
Usage:
  scripts/bench_transport_criterion.sh baseline
  scripts/bench_transport_criterion.sh candidate <phase_candidate>
  scripts/bench_transport_criterion.sh guardrail <phase_candidate> <report_path>
  scripts/bench_transport_criterion.sh export

Set TRANSPORT_BENCH_SUITE=raw, payload-contract, or all. The default is raw.

Set LOLA_BENCH_BACKEND=test-stub for correctness-only smoke with
--no-default-features --features "test-stub benchmark-owned".
Native runs use the repo-owned LoLa benchmark fixtures by default and only
accept LOLA_BENCH_* overrides.
USAGE
}

run_cargo_bench() {
    local profile="$1"
    local criterion_args="$2"
    local set_profile=false
    local cargo_features="benchmark-owned"
    shift 2

    case "$TRANSPORT_BENCH_SUITE" in
        raw) ;;
        payload-contract|all)
            cargo_features="$cargo_features payload-contract-benchmarks"
            ;;
        *)
            echo "TRANSPORT_BENCH_SUITE must be one of raw, payload-contract, all" >&2
            exit 2
            ;;
    esac

    if [[ -z "${LOLA_BENCH_PROFILE+x}" ]]; then
        export LOLA_BENCH_PROFILE="$profile"
        set_profile=true
    fi
    if [[ -n "$BENCH_PIN_PREFIX" ]]; then
        read -r -a pin_parts <<<"$BENCH_PIN_PREFIX"
        if [[ "$LOLA_BENCH_BACKEND" == "test-stub" ]]; then
            TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" "${pin_parts[@]}" cargo bench --no-default-features --features "test-stub $cargo_features" --bench transport_criterion -- $criterion_args "$@"
        else
            TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" "${pin_parts[@]}" cargo bench --features "$cargo_features" --bench transport_criterion -- $criterion_args "$@"
        fi
    else
        if [[ "$LOLA_BENCH_BACKEND" == "test-stub" ]]; then
            TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" cargo bench --no-default-features --features "test-stub $cargo_features" --bench transport_criterion -- $criterion_args "$@"
        else
            TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" cargo bench --features "$cargo_features" --bench transport_criterion -- $criterion_args "$@"
        fi
    fi
    if [[ "$set_profile" == true ]]; then
        unset LOLA_BENCH_PROFILE
    fi
}

run_selected_profiles() {
    local baseline_flag="$1"
    local baseline_value="$2"
    shift 2
    case "$TRANSPORT_BENCH_PROFILE" in
        core)
            run_cargo_bench core "$CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            ;;
        camera)
            run_cargo_bench camera "$LARGE_SENSOR_CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            ;;
        all)
            run_cargo_bench core "$CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            run_cargo_bench camera "$LARGE_SENSOR_CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            ;;
        *)
            echo "TRANSPORT_BENCH_PROFILE must be one of core, camera, all" >&2
            exit 2
            ;;
    esac
}

write_summary() {
    local report_dir="$1"
    local summary="$report_dir/README.md"
    local rust_version
    local cpu_model
    rust_version="$(rustc --version)"
    cpu_model="$(awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null || true)"
    cpu_model="${cpu_model:-unknown}"

    cat >"$summary" <<SUMMARY
# LoLa Owned vs Zero-Copy Transport Benchmarks

## Environment

- Transport: LoLa with feature flags \`benchmark-owned\` and backend \`$LOLA_BENCH_BACKEND\`
- Rust: \`$rust_version\`
- OS: \`$(uname -srmo)\`
- CPU: \`$cpu_model\`
- Core Criterion args: \`$CRITERION_ARGS\`
- Large sensor Criterion args: \`$LARGE_SENSOR_CRITERION_ARGS\`
- Suite: \`$TRANSPORT_BENCH_SUITE\`
- Pinning prefix: \`${BENCH_PIN_PREFIX:-none}\`

## Methodology

The \`owned\` path uses feature-gated \`LolaOwnedCore\` under the native-prefix selected-wire adapter. It copies owned payload bytes into LoLa transmit loans and receive leases back into owned frames while preserving selected-wire metadata validation.

The headline comparator is \`owned\` vs \`zero_copy_loan_copy\`. The \`zero_copy_uninit_direct\` path is supporting best-case direct true-zero-copy transmit data.

Payload-contract suite: \`protobuf_owned_full\` transports the canonical generated protobuf fixture through the Protobuf selected-wire owned path. \`stable_zc_nozero_full\` initializes canonical stable fixtures directly in LoLa loan storage through \`StablePayloadInit\`. \`stable_owned_bytes_full\` transports the matching canonical stable bytes through the owned selected-wire path.

Payload-contract transported bytes are intentionally contract-specific: protobuf reports encoded \`BenchPayload\` bytes, while stable reports \`size_of::<StableBenchPayloadN>()\` (logical payload bytes plus a 16-byte header/checksum). This is application payload-contract data, not RawBytes transport-boundary data.

Core payload cases: \`empty_present\` 0 B, \`can_classic_max\` 8 B, \`can_fd_max\` 64 B, \`someip_single_mtu\` 1456 B, \`streamer_4k\` 4096 B, \`radar_ars548_detection_list\` 35336 B, and \`streamer_64k\` 65536 B.

Large sensor payload case: \`camera_8mp_3840x2160_raw12_packed\` 12441600 B.

## Ratio Table

Use \`bench-data/criterion-compare-bencher.txt\` and \`criterion-html/\` for exported measurements and plots. Test-stub output is correctness smoke only and must not be used as LoLa native performance evidence.

## Caveats

Native LoLa performance requires S-CORE LoLa runtime support and the repo-owned benchmark fixtures under \`benches/fixtures/\`. Payload-contract v1 is Publish-only and does not replace the existing RawBytes transport-boundary matrix. The harness fails before Criterion measurement if native fixture identity, capacity, warm round trip, or selected-profile fit checks fail.
SUMMARY
}

export_results() {
    local report_dir="$TRANSPORT_BENCH_REPORT_DIR"
    local bench_data_dir="$report_dir/bench-data"
    local report_path="$bench_data_dir/criterion-compare-bencher.txt"
    mkdir -p "$bench_data_dir"
    run_selected_profiles --baseline "$BASELINE_NAME" --output-format bencher | tee "$report_path"
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
    write_summary "$report_dir"
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

subcommand="$1"
shift

case "$subcommand" in
    baseline)
        run_selected_profiles --save-baseline "$BASELINE_NAME"
        ;;
    candidate)
        if [[ $# -ne 1 ]]; then
            usage
            exit 1
        fi
        run_selected_profiles --save-baseline "$1"
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
        exit 1
        ;;
esac

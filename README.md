# up-transport-lola-rust

Rust crate for the Eclipse S-CORE LoLa transport for Eclipse uProtocol.

This crate contains the LoLa native bridge build wiring, selected-wire
zero-copy transport proof, `LolaOwnedCore` benchmark-owned adapter path, and
benchmark scripts used by the userializer replay stack. Native LoLa validation
requires Bazel/Bazelisk and the bundled bridge build by default.

## Features

| Feature | Scope |
| --- | --- |
| `default` | Uses `bundled`, building the LoLa bridge from source. |
| `bundled` | Enables `lola-build-from-source`. |
| `lola-build-from-source` | Enables native `lola-ffi` bridge build wiring. |
| `lola-ffi` | Uses LoLa FFI/native bridge integration. |
| `zero-copy` | Enables selected-wire zero-copy benchmark/proof paths. |
| `benchmark-owned` | Enables `LolaOwnedCore`, a benchmark-owned `UOwnedTransportCore` consumed through the generic selected-wire owned adapter. |
| `payload-contract-benchmarks` | Enables shared payload-contract benchmark fixtures. |
| `payload-contract-large-benchmarks` | Enables large shared payload-contract benchmark cases. |
| `test-stub` | Enables isolated Rust tests that do not satisfy native performance claims. |

Test-stub validation can support compile/proof work, but native performance
claims require the real LoLa runtime path and recorded `LOLA_BENCH_*` benchmark
environment.

## Benchmark Backends

The benchmark script defaults to the native backend and resolves Bazel in this
order: `BAZEL`, repo-local `.cache/tools/bazelisk-<version>-linux-amd64`, then
`bazelisk`/`bazel` on `PATH`.

Native payload-contract export:

```bash
CARGO_NET_GIT_FETCH_WITH_CLI=true \
CARGO='cargo +1.95.0' \
LOLA_BENCH_BACKEND=native \
TRANSPORT_BENCH_SUITE=payload-contract \
TRANSPORT_BENCH_PROFILE=all \
TRANSPORT_BENCH_REPORT_DIR=target/transport-perf/lola-native \
scripts/bench_transport_criterion.sh export
```

Test-stub export for environments without Bazel/native LoLa:

```bash
CARGO_NET_GIT_FETCH_WITH_CLI=true \
CARGO='cargo +1.95.0' \
LOLA_BENCH_BACKEND=test-stub \
TRANSPORT_BENCH_SUITE=payload-contract \
TRANSPORT_BENCH_PROFILE=all \
TRANSPORT_BENCH_REPORT_DIR=target/transport-perf/lola-test-stub \
scripts/bench_transport_criterion.sh export
```

`LOLA_BENCH_BACKEND=test-stub` adds `--no-default-features` and the `test-stub`
feature automatically. Test-stub output is useful for adapter-shape proof and
CI smoke coverage, but native performance claims require `LOLA_BENCH_BACKEND=native`.

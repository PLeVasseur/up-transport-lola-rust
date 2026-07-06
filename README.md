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

## Native LoLa Deployment Manifests

Native LoLa is deployment-manifest driven. `LolaTransportConfig` points at an
S-CORE `mw_com_config.json` manifest with `mw_com_config_path`; that manifest
defines the LoLa service types, service IDs, instance IDs, event IDs, sample
slots, and subscriber limits used by the native bridge.

The S-CORE runtime is initialized once per process. All native LoLa transports
and subscribers in that process must use the same MW COM manifest path, or omit
the path and rely on S-CORE's default `./etc/mw_com_config.json`. The Rust and
native bridge layers reject a second different manifest path in the same
process because S-CORE would otherwise keep using the first initialized runtime
configuration.

On Linux, S-CORE LoLa stores runtime service-discovery and partial-restart state
under `/tmp/mw_com_lola`. Those files are runtime state, not uProtocol route
configuration. Stale files can affect repeated local runs after crashes. Stop
all LoLa-backed processes before cleaning that directory; do not remove it while
a native LoLa process is still running.

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

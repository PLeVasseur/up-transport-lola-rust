# up-transport-lola-rust

Rust crate for the Eclipse S-CORE LoLa transport for Eclipse uProtocol.

This crate contains the LoLa native bridge build wiring, selected-wire
zero-copy transport proof, benchmark-owned comparison wrapper, and benchmark
scripts used by the userializer replay stack. Native LoLa validation requires
Bazel/Bazelisk and the bundled bridge build by default.

## Features

| Feature | Scope |
| --- | --- |
| `default` | Uses `bundled`, building the LoLa bridge from source. |
| `bundled` | Enables `lola-build-from-source`. |
| `lola-build-from-source` | Enables native `lola-ffi` bridge build wiring. |
| `lola-ffi` | Uses LoLa FFI/native bridge integration. |
| `zero-copy` | Enables selected-wire zero-copy benchmark/proof paths. |
| `benchmark-owned` | Enables benchmark-only owned comparison support. |
| `payload-contract-benchmarks` | Enables shared payload-contract benchmark fixtures. |
| `payload-contract-large-benchmarks` | Enables large shared payload-contract benchmark cases. |
| `test-stub` | Enables isolated Rust tests that do not satisfy native performance claims. |

Test-stub validation can support compile/proof work, but native performance
claims require the real LoLa runtime path and recorded `LOLA_BENCH_*` benchmark
environment.

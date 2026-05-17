# up-transport-lola-rust

Zero-copy uProtocol transport for Eclipse S-CORE LoLa.

The default build follows the same bundled-native model as the vSomeIP transport: it uses the pinned S-CORE communication submodule, builds the C++ LoLa bridge with Bazel/Bzlmod, and links Cargo against the generated `libup_lola_bridge.so`.

## First-Time Build

Install Bazelisk or Bazel first. The build script does not download Bazelisk automatically.

```sh
cargo build
```

If Bazelisk is not on `PATH`, point `BAZEL` at it:

```sh
BAZEL=/path/to/bazelisk cargo build
```

The bundled build initializes `third_party/eclipse-score-communication` automatically with:

```sh
git submodule update --init --recursive third_party/eclipse-score-communication
```

The submodule is pinned to Eclipse S-CORE communication commit `56c36d4059d276e804c143d14012576ddf1b9e25`.

## Features

- `default = ["bundled"]`: builds the native LoLa bridge from the bundled S-CORE communication submodule.
- `bundled`: enables `lola-build-from-source`.
- `lola-build-from-source`: generates an isolated Bazel workspace under `OUT_DIR`, builds `libup_lola_bridge.so`, and links Cargo against it.
- `lola-ffi`: enables the Rust FFI backend and expects a prebuilt bridge via `LOLA_BRIDGE_LIB_DIR` unless `lola-build-from-source` is also enabled.
- `native-smoke`: enables native runtime smoke tests. These use `tests/fixtures/mw_com_config.json` by default.
- `test-stub`: enables the in-process fake backend for fast Rust unit tests. It is not a LoLa runtime.

## Build Options

Use the bundled native bridge:

```sh
cargo build
```

Use an external S-CORE communication checkout instead of the submodule:

```sh
BAZEL=/path/to/bazelisk \
LOLA_COMMUNICATION_ROOT=/path/to/eclipse-score-communication \
cargo build --features lola-build-from-source
```

Use a prebuilt bridge:

```sh
LOLA_BRIDGE_LIB_DIR=/path/to/lib \
cargo build --no-default-features --features lola-ffi
```

Run only the fake unit-test backend:

```sh
cargo test --no-default-features --features test-stub
```

## Native Bridge

The production bridge uses LoLa generic APIs:

- `score::mw::com::GenericSkeleton`
- `score::mw::com::GenericSkeletonEvent::Allocate`
- `score::mw::com::GenericSkeletonEvent::Send`
- `score::mw::com::GenericProxy`
- `score::mw::com::GenericProxyEvent::GetNewSamples`
- `SampleAllocateePtr<void>` and `SamplePtr<void>`

This avoids generated type bindings and maps uProtocol frame bytes into fixed-size LoLa event samples.

## Tests

Build and link the native bridge:

```sh
BAZEL=/path/to/bazelisk cargo test
```

Run the fake unit-test backend:

```sh
cargo test --no-default-features --features test-stub
```

The native end-to-end smoke test is compiled only with `native-smoke`. It uses `tests/fixtures/mw_com_config.json` by default. The test is skipped unless `LOLA_NATIVE_SMOKE_RUN=1` is set because LoLa shared-memory runtime setup and loopback behavior are host-dependent:

```sh
LOLA_NATIVE_SMOKE_RUN=1 \
BAZEL=/path/to/bazelisk \
cargo test --features native-smoke
```

Override the checked-in fixture when needed:

```sh
LOLA_NATIVE_SMOKE_CONFIG=/path/to/mw_com_config.json \
LOLA_NATIVE_SMOKE_RUN=1 \
BAZEL=/path/to/bazelisk \
cargo test --features native-smoke
```

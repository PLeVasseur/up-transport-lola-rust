# up-transport-lola-rust

Zero-copy uProtocol transport for Eclipse S-CORE LoLa.

`UTransportLola` implements `up_rust::zero_copy::UZeroCopyTransport`. It exposes LoLa event samples as native uProtocol frames: callers write only application payload bytes, while the binding stores routing attributes and payload encoding metadata in a hidden `ULOL` frame header and metadata block.

| uProtocol frame part | LoLa representation |
| --- | --- |
| Frame magic/version and lengths | Hidden `ULOL` header |
| `UAttributes` | Hidden native-frame metadata block |
| `UEncoding.format_id` / `content_type` / `schema_ref` | Hidden native-frame metadata block |
| Alignment padding | Hidden between metadata and payload |
| Application payload bytes | Exposed `LolaTxLoan` / `LolaRxLease` payload slice |

Metadata is final at `reserve`, so LoLa can compute the metadata length and aligned payload offset before returning the transmit loan. After reserve, `payload_mut()` is the only mutable zero-copy surface.

## Typed Payloads

Use `PayloadFormat` serializers to write directly into a LoLa loan:

```rust
use up_rust::{payload::RawBytes, zero_copy::UZeroCopyTransportExt, UFrameMetadata};

async fn send<T>(transport: &T, metadata: UFrameMetadata) -> Result<(), up_rust::UStatus>
where
    T: up_rust::zero_copy::UZeroCopyTransport,
{
let payload: &[u8] = b"payload";
transport
    .send_serialized_zero_copy::<RawBytes, _>(metadata, &payload)
    .await
}
```

Receive code should use `UZeroCopyRxFrame::deserialize_from_reader::<Codec, T>()` for owned decodes, or `UContiguousZeroCopyRxFrame::deserialize_borrowed::<Codec, T>()` when the decoded type borrows directly from the LoLa sample payload.

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
- `test-stub`: enables the in-process fake backend for fast Rust unit tests. It is not a LoLa runtime.

Native runtime tests are compiled when `lola-ffi` is enabled and are marked ignored by default. Run them explicitly with `cargo test -- --ignored`.

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

The bridge exposes the LoLa event slot's raw sample storage to Rust. S-CORE's current generic skeleton allocation path returns an owning loan whose pointer can reference the generic `EventDataStorage` object base, while the generic proxy receives bytes from the raw event slot array. The bridge keeps the original loan for `Send` ownership but writes frame bytes through `EventDataStorage::data()` so TX and RX use the same sample storage.

The bridge separates provider and subscriber ownership. `NativeTransport` owns the `GenericSkeleton`/`GenericSkeletonEvent` path used for `Allocate` and `Send`; each direct receive path or registered listener owns a separate `GenericProxy`/`GenericProxyEvent` subscription. This mirrors the S-CORE Rust binding model and lets a local listener and a streamer listener receive the same LoLa event through independent subscription queues.

## Logging

S-CORE logging is configured with `MW_LOG_CONFIG_FILE` or `./etc/logging.json`. If `MW_LOG_CONFIG_FILE` is unset, the LoLa bridge automatically uses a `logging.json` file beside the configured `mw_com_config.json` when one exists. If there is no sibling logging config, the bridge writes a quiet temporary config and points S-CORE at it. The checked-in native test fixture includes `tests/fixtures/logging.json` for explicit, reproducible test logging.

Set `MW_LOG_CONFIG_FILE` explicitly to override this behavior.

## Tests

Build and link the native bridge. Native runtime tests are ignored by default:

```sh
BAZEL=/path/to/bazelisk cargo test
```

Run the fake unit-test backend:

```sh
cargo test --no-default-features --features test-stub
```

Run the ignored native runtime tests. They use `tests/fixtures/mw_com_config.json` and its sibling `logging.json` by default:

```sh
BAZEL=/path/to/bazelisk \
cargo test -- --ignored
```

With the checked-in fixture, the native runtime tests cover direct reserve/send/receive loopback and two independent listener subscriptions receiving the same LoLa event. Invalid frame magic diagnostics still include the first four sample bytes to make stale, foreign, or incorrectly mapped shared-memory samples actionable.

Override the checked-in fixture when needed:

```sh
LOLA_NATIVE_TEST_CONFIG=/path/to/mw_com_config.json \
BAZEL=/path/to/bazelisk \
cargo test -- --ignored
```

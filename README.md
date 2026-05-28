# up-transport-lola-rust

Zero-copy uProtocol transport for Eclipse S-CORE LoLa.

`UTransportLola` implements `up_rust::zero_copy::UZeroCopyTransport`. It exposes LoLa event samples as native uProtocol frames: callers write only application payload bytes, while the binding stores routing attributes and payload encoding metadata in a hidden `ULOL` frame header and metadata block.

| uProtocol frame part | LoLa representation |
| --- | --- |
| Frame magic/version and lengths | Hidden `ULOL` header |
| `UAttributes` | Hidden native-frame metadata block |
| `PayloadEncoding` | Hidden native-frame metadata block |
| Alignment padding | Hidden between metadata and payload |
| Application payload bytes | Exposed `LolaTxLoan` / `LolaRxLease` payload slice |

Metadata is final at `loan_tx`, so LoLa can compute the metadata length and aligned payload offset before returning the transmit loan. After `loan_tx`, `payload_mut()` is the only mutable zero-copy surface.

LoLa also implements `up_rust::UZeroCopyUninitTransport`. The initialized
`loan_tx` path keeps `UTxBuffer::payload()` and `payload_mut()` sound by exposing
initialized bytes. The uninitialized path is separate and lets stable typed
payloads be constructed directly in the LoLa event sample without pre-zeroing the
application payload region; the binding still initializes the `ULOL` header,
metadata, alignment padding, and fixed-sample tail bytes.

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
Stable-container typed receive should use `borrow_stable_payload<T>()` on the
loan-backed `LolaRxLease`; the diagnostic provenance value is not a safety gate.

Stable typed payloads can avoid both a source payload copy and codec-level
default initialization:

```rust
use up_rust::{payload::StableContainerPayload, UFrameMetadata, UZeroCopyUninitTransportExt};

#[repr(C)]
#[derive(Clone, Copy, up_rust::StablePayload, up_rust::ByteBackedStablePayload)]
#[stable_payload(type_name = "example.vehicle.VehiclePose")]
struct VehiclePose {
    x: u64,
    y: u64,
}

async fn send<T>(transport: &T, metadata: UFrameMetadata) -> Result<(), up_rust::UStatus>
where
    T: up_rust::UZeroCopyUninitTransport,
{
    transport
        .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
            metadata,
            |slot| Ok(slot.write(VehiclePose { x: 1, y: 2 })),
        )
        .await
}
```

The default build follows the same bundled-native model as the vSomeIP transport: it uses the pinned S-CORE communication submodule, builds the C++ LoLa bridge with Bazel/Bzlmod, and links Cargo against the generated `libup_lola_bridge.so`.

The transport preserves the distinction between no payload and a present empty
payload: no payload has no `PayloadEncoding`, while a present empty payload keeps
its encoding and reports payload presence with length zero. Payload bytes with no
encoding are rejected before send.

Filtered pull receive preserves nonmatching samples in an internal queue so a
later matching receive call can still observe them. That queue is intentionally
not exposed as a public API and is not currently bounded by a configurable
resource policy; deployments with many mismatched pull filters should account for
that residual resource risk.

## First-Time Build

Use the checked-in helper to download Bazelisk into `.cache/tools`, verify its
SHA-256 checksum from `tools/bazelisk-linux-amd64.sha256`, and run the bundled
build with the same tool path used by CI:

```sh
scripts/run-native-validation.sh check
```

If you manage Bazelisk or Bazel externally, point `BAZEL` at it:

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

Native validation is a completion gate for zero-copy changes that touch the C++
bridge or wrapper-pool ownership model. The preferred bundled path is:

```sh
scripts/run-native-validation.sh native
```

If you do not use the bundled Bazel build, provide the prebuilt bridge explicitly:

```sh
LOLA_BRIDGE_LIB_DIR=/path/to/lib \
cargo test --no-default-features --features lola-ffi -- --ignored native
```

With an externally managed Bazel/Bazelisk, the bundled path is:

```sh
BAZEL=/path/to/bazelisk cargo test -- --ignored native
```

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

Run the Miri-feasible fake backend checks for uninitialized frame conversion and
header handling:

```sh
scripts/run-miri-test-stub.sh
```

The runner disables Miri isolation because the tests construct
`UFrameMetadata::publish`, which builds a UUID from `SystemTime::elapsed`; Miri
rejects the underlying `clock_gettime` call when isolation is enabled.

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

Rust reports LoLa payload provenance as `OpaqueTransportLoan`. The path is still a
native loan-backed LoLa sample path, but the Rust binding does not currently have
a transport-independent proof that the S-CORE allocation is a shareable memory
region with stronger `PayloadLoanProvenance::SharedMemory` semantics.

The bridge separates provider and subscriber ownership. `NativeTransport` owns the `GenericSkeleton`/`GenericSkeletonEvent` path used for `Allocate` and `Send`; each direct receive path or registered listener owns a separate `GenericProxy`/`GenericProxyEvent` subscription. This mirrors the S-CORE Rust binding model and lets a local listener and a streamer listener receive the same LoLa event through independent subscription queues.

TX loan wrappers and RX sample wrappers are drawn from bounded C++ owner pools
sized by `max_samples`. The native bridge does not allocate or delete per-sample
wrapper objects on the loan/receive happy path; pool exhaustion maps to
`RESOURCE_EXHAUSTED`, and slots are returned on send or when Rust drops unsent TX
loans and RX leases.

## Logging

S-CORE logging is configured with `MW_LOG_CONFIG_FILE` or `./etc/logging.json`. If `MW_LOG_CONFIG_FILE` is unset, the LoLa bridge automatically uses a `logging.json` file beside the configured `mw_com_config.json` when one exists. If there is no sibling logging config, the bridge writes a quiet temporary config and points S-CORE at it. The checked-in native test fixture includes `tests/fixtures/logging.json` for explicit, reproducible test logging.

Set `MW_LOG_CONFIG_FILE` explicitly to override this behavior.

## Tests

Build and link the native bridge. Native runtime tests are ignored by default:

```sh
scripts/run-native-validation.sh check
```

Run the fake unit-test backend:

```sh
cargo test --no-default-features --features test-stub
```

Run the ignored native runtime tests. They use `tests/fixtures/mw_com_config.json` and its sibling `logging.json` by default:

```sh
scripts/run-native-validation.sh native
```

With the checked-in fixture, the native runtime tests cover direct loan/send/receive loopback and two independent listener subscriptions receiving the same LoLa event. Invalid frame magic diagnostics still include the first four sample bytes to make stale, foreign, or incorrectly mapped shared-memory samples actionable.

Override the checked-in fixture when needed:

```sh
LOLA_NATIVE_TEST_CONFIG=/path/to/mw_com_config.json \
scripts/run-native-validation.sh native
```

Run the complete local validation path:

```sh
scripts/run-native-validation.sh all
```

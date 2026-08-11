# up-transport-lola-rust

Zero-copy uProtocol transport for Eclipse S-CORE LoLa.

`UTransportLola` owns LoLa sample mechanics. Applications construct a public
transport by calling `zero_copy_core().with_selected_wire(wire)`. The generic
up-rust adapter encodes and validates metadata for that selected wire while LoLa
carries the encoded metadata and application payload as opaque bytes.
The physical envelope also carries an untrusted source/sink routing hint so the
LoLa mechanics layer can retain nonmatching pull samples. The generic adapter
independently decodes the opaque metadata and rechecks both filters before any
frame reaches an application; the hint is never public identity authority.

Native-frame conformance coverage includes `ULOL` header validation, standard and
custom payload encoding preservation, stable-container metadata preservation,
rejection of payload bytes without encoding metadata, exact application payload
views that exclude the header, metadata and padding, and loan-backed
stable-container borrowing from the LoLa receive lease.

| uProtocol frame part | LoLa representation |
| --- | --- |
| Frame magic/version and lengths | Hidden `ULOL` header |
| Source/sink routing hint | Hidden, untrusted physical routing bytes |
| Selected-wire metadata | Hidden opaque metadata block |
| Alignment padding | Hidden between metadata and payload |
| Application payload bytes | Exposed `LolaTxLoan` / `LolaRxLease` payload slice |

Metadata is final when the selected-wire adapter prepares a TX loan, so LoLa can
compute the metadata length and aligned payload offset before returning it.
After loan creation, `payload_mut()` is the only mutable initialized surface.

`LolaZeroCopyCore` also implements `UZeroCopyUninitTransportCore`. Its separate
uninitialized path lets stable typed payloads be constructed directly in the
LoLa event sample without pre-zeroing the application payload region; the
binding still initializes the `ULOL` header, metadata, alignment padding, and
fixed-sample tail bytes.

## Typed Payloads

Select a wire explicitly before writing a LoLa loan:

```rust
use up_rust::{
    PayloadEncoding, UFrameMetadata, UProtocolNativeWire, UTxBuffer, UTxLoanSpec,
    UZeroCopyTransportImpl,
};
use up_transport_lola_rust::{LolaTransportConfig, UTransportLola};

async fn send(config: LolaTransportConfig) -> Result<(), up_rust::UStatus> {
    let topic = up_rust::UUri::try_from("//vehicle/4210/1/9000")?;
    let physical = UTransportLola::build(config)?;
    let transport = physical
        .zero_copy_core()
        .with_selected_wire(UProtocolNativeWire);
    let metadata = UFrameMetadata::publish(topic)
        .with_payload_encoding(PayloadEncoding::RAW)
        .build()?;
    let mut loan = transport
        .loan_validated_tx(UTxLoanSpec::payload(metadata, 7, 1)?)
        .await?;
    loan.payload_mut().copy_from_slice(b"payload");
    transport.send_validated_zero_copy(loan).await
}
```

Receive values expose validated metadata through `UFrameView`. Use
`decode_payload(PayloadDecodeLimit)` for selected-wire owned decoding or
`borrow_payload<T>()` for validated stable-container borrowing. The diagnostic
loan provenance value is not a safety gate.

Stable typed payloads can avoid both a source payload copy and codec-level
default initialization:

```rust
use up_rust::{
    PayloadCodecIdentity, StableContainerPayload, StableContainerWireFormat,
    UFrameMetadata,
};

#[repr(C)]
#[derive(Clone, Copy, up_rust::StablePayload, up_rust::StablePayloadInit)]
#[stable_payload(type_name = "example.vehicle.VehiclePose")]
struct VehiclePose {
    x: u64,
    y: u64,
}

async fn send_stable(
    physical: &std::sync::Arc<up_transport_lola_rust::UTransportLola>,
    topic: up_rust::UUri,
) -> Result<(), up_rust::UStatus> {
    let transport = physical
        .zero_copy_core()
        .with_selected_wire(StableContainerWireFormat);
    let metadata = UFrameMetadata::publish(topic)
        .with_payload_encoding(
            <StableContainerPayload<VehiclePose> as PayloadCodecIdentity>::encoding(),
        )
        .build()?;
    transport
        .send_stable_payload::<VehiclePose, _>(
            metadata,
            |init| init.into_initializer().x(1).y(2).finish(),
        )
        .await
}
```

The default build follows the same bundled-native model as the vSomeIP transport: it uses the pinned S-CORE communication submodule, builds the C++ LoLa bridge with Bazel/Bzlmod, and links Cargo against the generated `libup_lola_bridge.so`.

The transport preserves the distinction between no payload and a present empty
payload: no payload has no `PayloadEncoding`, while a present empty payload keeps
its encoding and reports payload presence with length zero. Payload bytes with no
encoding are rejected before send.

Filtered pull receive preserves nonmatching samples in a bounded internal queue
so a later matching receive call can still observe them, including through the
native LoLa backend. Public delivery always revalidates selected-wire metadata;
the physical routing hint cannot bypass source or sink filters.
`LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY` is 64 retained
mismatches. When the queue is full, the default
`LolaPullMismatchQueueFullPolicy::DropOldestAndReport` policy keeps receive calls
non-erroring and drops the oldest retained mismatch; applications that prefer an
explicit receive error can select `RejectNewestAndReport`. Use
`UTransportLola::pull_mismatch_queue_diagnostics()` to inspect current depth,
drop/rejection counters, and the last mismatch reason.

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
- `test-stub`: enables the in-process fake backend for fast Rust unit tests when `lola-ffi` is disabled. It is not a LoLa runtime.
- `benchmark-owned`: enables the feature-gated `LolaOwnedCore` selected-wire adapter used by the benchmark and owned-wire proof.

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

The bridge exposes the LoLa event slot's raw sample storage to Rust. This raw
slot mapping is pinned to Eclipse S-CORE communication commit
`56c36d4059d276e804c143d14012576ddf1b9e25`. At that commit,
`score/mw/com/impl/bindings/lola/generic_skeleton_event.h` routes
`GenericSkeletonEvent::Allocate()` through the generic `SampleAllocateePtr<void>`
abstraction, `score/mw/com/impl/bindings/lola/event_data_storage.h` stores event
slots in `EventDataStorage::data()`, and
`score/mw/com/impl/bindings/lola/generic_proxy_event.h` receives raw slot samples
from that data array. The bridge keeps the original loan for `Send` ownership but
writes frame bytes through `EventDataStorage::data()` so TX and RX use the same
sample storage.

Rust reports LoLa payload provenance as `OpaqueTransportLoan`. The path is still a
native loan-backed LoLa sample path, but this binding intentionally does not claim
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

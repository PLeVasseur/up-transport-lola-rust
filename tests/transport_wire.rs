//! Public selected-wire coverage for the LoLa test-stub backend.

use std::{future::Future, sync::Arc, task::Wake};

use async_trait::async_trait;
use std::sync::Mutex;
use up_rust::selected_wire_user_api::UNativePrefixWireTransport;
use up_rust::wire_implementer_api::{
    ProtobufWire, UProtocolNativeWire, UWire, WireIdentity, NATIVE_PREFIX_METADATA_LAYOUT_ID,
};
use up_rust::{
    PayloadEncoding, PayloadFormat, UCode, UFrameMetadata, UFrameView, UTxBuffer, UTxLoanSpec,
    UUninitTxBuffer, UUri, UZeroCopyListener, UZeroCopyTransport, UZeroCopyUninitTransport,
};
#[cfg(feature = "benchmark-owned")]
use up_rust::{UOwnedFrame, UOwnedTransport};
#[cfg(feature = "benchmark-owned")]
use up_transport_lola_rust::LolaOwnedCore;
use up_transport_lola_rust::{LolaTransportConfig, LolaZeroCopyCore, UTransportLola};
use up_wire_xcdrv2::{VehicleSignalV1, XcdrV2Wire, VEHICLE_SIGNAL_V1_GOLDEN_VALUE};

type NativeLolaTransport<W> = UNativePrefixWireTransport<LolaZeroCopyCore, W>;
type NativeLolaRx<W> = <NativeLolaTransport<W> as UZeroCopyTransport>::Rx;
type NativeLolaNativeRx = NativeLolaRx<UProtocolNativeWire>;

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on_ready<T>(future: impl Future<Output = T>) -> T {
    let waker = std::task::Waker::from(Arc::new(NoopWake));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("LoLa test future unexpectedly yielded pending"),
    }
}

fn test_config(instance_specifier: &str) -> LolaTransportConfig {
    LolaTransportConfig {
        local_authority: "vehicle".to_string(),
        instance_specifier: instance_specifier.to_string(),
        service_type: "uprotocol.LoLa".to_string(),
        event_name: "UProtocolFrame".to_string(),
        sample_size: 256,
        sample_alignment: 8,
        max_samples: 8,
        pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
        pull_mismatch_queue_full_policy:
            LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
        mw_com_config_path: native_mw_com_config_path(),
    }
}

#[cfg(feature = "lola-ffi")]
fn native_mw_com_config_path() -> Option<String> {
    Some(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/mw_com_config_transport_wire.json"
        )
        .to_string(),
    )
}

#[cfg(not(feature = "lola-ffi"))]
fn native_mw_com_config_path() -> Option<String> {
    None
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("LoLa listener test runtime")
}

fn metadata_with_encoding(topic: UUri, encoding: PayloadEncoding) -> UFrameMetadata {
    UFrameMetadata::publish(topic)
        .with_payload_encoding(encoding)
        .build()
        .expect("valid frame metadata")
}

fn payload_tx_spec_for<W>(topic: UUri, payload_len: usize, payload_alignment: usize) -> UTxLoanSpec
where
    W: PayloadFormat,
{
    UTxLoanSpec::payload(
        metadata_with_encoding(topic, W::encoding()),
        payload_len,
        payload_alignment,
    )
    .expect("valid loan spec")
}

fn native_tx_spec(topic: UUri, payload_len: usize, payload_alignment: usize) -> UTxLoanSpec {
    let metadata = UFrameMetadata::publish(topic)
        .with_payload_encoding(PayloadEncoding::RAW)
        .build()
        .expect("valid metadata");
    UTxLoanSpec::payload(metadata, payload_len, payload_alignment).expect("valid loan spec")
}

fn send_native_payload(transport: &Arc<UTransportLola>, topic: UUri, payload: &[u8]) {
    block_on_ready(async {
        let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);
        let mut loan = selected
            .loan_tx(native_tx_spec(topic, payload.len(), 1))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(payload);
        selected.send_zero_copy(loan).await.unwrap();
    });
}

fn send_wire_payload<W>(transport: &Arc<UTransportLola>, topic: UUri, payload: &[u8])
where
    W: UWire + PayloadFormat + Default + Send + Sync + 'static,
{
    block_on_ready(async {
        let selected = selected_wire(transport.zero_copy_core(), W::default());
        let mut loan = selected
            .loan_tx(payload_tx_spec_for::<W>(topic, payload.len(), 1))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(payload);
        selected.send_zero_copy(loan).await.unwrap();
    });
}

#[cfg(feature = "benchmark-owned")]
fn send_owned_payload<W>(transport: &Arc<UTransportLola>, topic: UUri, payload: &[u8])
where
    W: UWire + PayloadFormat + Default + Send + Sync + 'static,
{
    block_on_ready(async {
        let owned = LolaOwnedCore::new(transport.zero_copy_core()).with_selected_wire(W::default());
        let frame = UOwnedFrame::with_payload(
            metadata_with_encoding(topic, W::encoding()),
            payload.to_vec(),
        )
        .unwrap();
        owned.send_owned(frame).await.unwrap();
    });
}

fn selected_wire<W>(core: LolaZeroCopyCore, wire: W) -> NativeLolaTransport<W>
where
    W: UWire,
{
    core.with_selected_wire(wire)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TestCustomWire;

impl UWire for TestCustomWire {
    const WIRE_ID: WireIdentity = WireIdentity::new(
        "org.eclipse.uprotocol.test.lola-wire-proof.custom",
        up_rust::wire::LOCAL_EXPERIMENTAL_COMPACT_ID_START,
    );
    const PAYLOAD_FAMILY_ID: WireIdentity = WireIdentity::new(
        "test-lola-custom-payload",
        up_rust::wire::LOCAL_EXPERIMENTAL_COMPACT_ID_START + 1,
    );
    const METADATA_LAYOUT_ID: WireIdentity = NATIVE_PREFIX_METADATA_LAYOUT_ID;
    const FORMAT_VERSION: u16 = up_rust::wire::FORMAT_VERSION;
}

impl PayloadFormat for TestCustomWire {
    fn name() -> &'static str {
        "test-lola-custom"
    }

    fn encoding() -> PayloadEncoding {
        PayloadEncoding::custom(
            "test.lola.custom",
            "application/vnd.uprotocol.test.lola.custom",
        )
        .expect("valid custom encoding")
    }
}

#[derive(Default)]
struct RecordingListener {
    payloads: Mutex<Vec<Vec<u8>>>,
}

impl RecordingListener {
    fn payloads(&self) -> Vec<Vec<u8>> {
        self.payloads.lock().unwrap().clone()
    }
}

#[async_trait]
impl UZeroCopyListener<NativeLolaNativeRx> for RecordingListener {
    async fn on_receive_zero_copy(&self, frame: NativeLolaNativeRx) {
        self.payloads
            .lock()
            .unwrap()
            .push(frame.try_contiguous_payload().unwrap().to_vec());
    }
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn external_xcdrv2_wire_round_trips_payload() {
    let transport =
        UTransportLola::build(test_config("lola/transport_wire/external_xcdrv2")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9021").expect("valid URI");
    send_wire_payload::<XcdrV2Wire>(
        &transport,
        topic.clone(),
        &up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_BYTES,
    );

    let selected = selected_wire(transport.zero_copy_core(), XcdrV2Wire);
    let frame = block_on_ready(selected.receive_zero_copy(&topic, None)).unwrap();
    let decoded: VehicleSignalV1 = frame.decode_payload().unwrap();

    assert_eq!(decoded, VEHICLE_SIGNAL_V1_GOLDEN_VALUE);
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn custom_wire_encoding_round_trips_metadata_and_payload_bytes() {
    let transport = UTransportLola::build(test_config("lola/transport_wire/custom_wire")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9022").expect("valid URI");
    send_wire_payload::<TestCustomWire>(&transport, topic.clone(), b"custom");

    let selected = selected_wire(transport.zero_copy_core(), TestCustomWire);
    let frame = block_on_ready(selected.receive_zero_copy(&topic, None)).unwrap();

    assert_eq!(
        frame.metadata().payload_encoding(),
        Some(&TestCustomWire::encoding())
    );
    assert_eq!(frame.try_contiguous_payload(), Some(b"custom".as_slice()));
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn wrong_wire_is_rejected_before_public_receive_exposes_frame() {
    let transport = UTransportLola::build(test_config("lola/transport_wire/wrong_wire")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9023").expect("valid URI");
    send_wire_payload::<ProtobufWire>(&transport, topic.clone(), b"protobuf bytes");
    let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);

    let error = match block_on_ready(selected.receive_zero_copy(&topic, None)) {
        Ok(_) => panic!("wrong wire should be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn no_payload_and_present_empty_payload_are_distinct() {
    let transport =
        UTransportLola::build(test_config("lola/transport_wire/empty_payload")).unwrap();
    let no_payload_topic = UUri::try_from("//vehicle/4210/1/9025").expect("valid URI");
    let empty_topic = UUri::try_from("//vehicle/4210/1/9026").expect("valid URI");
    let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);
    let no_payload_metadata = UFrameMetadata::publish(no_payload_topic.clone())
        .build()
        .unwrap();
    let no_payload_loan =
        block_on_ready(selected.loan_tx(UTxLoanSpec::no_payload(no_payload_metadata).unwrap()))
            .unwrap();
    block_on_ready(selected.send_zero_copy(no_payload_loan)).unwrap();
    let no_payload = receive_with_retry(&selected, &no_payload_topic);

    let empty_metadata = metadata_with_encoding(empty_topic.clone(), PayloadEncoding::RAW);
    let empty_loan = block_on_ready(
        selected.loan_tx(UTxLoanSpec::present_empty_payload(empty_metadata).unwrap()),
    )
    .unwrap();
    block_on_ready(selected.send_zero_copy(empty_loan)).unwrap();
    let empty = receive_with_retry(&selected, &empty_topic);

    assert!(!no_payload.has_payload());
    assert!(empty.has_payload());
    assert_eq!(empty.payload_len(), 0);
}

fn receive_with_retry<W>(selected: &NativeLolaTransport<W>, topic: &UUri) -> NativeLolaRx<W>
where
    W: UWire + Send + Sync + 'static,
{
    let mut last_error = None;
    for _ in 0..50 {
        match block_on_ready(selected.receive_zero_copy(topic, None)) {
            Ok(frame) => return frame,
            Err(error) if error.get_code() == UCode::NotFound => {
                last_error = Some(error);
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => panic!("unexpected LoLa receive error: {error:?}"),
        }
    }
    panic!("LoLa receive did not return a frame: {last_error:?}");
}

#[test]
#[cfg(feature = "lola-ffi")]
fn native_pull_transport_teardown_is_repeatable() {
    let topic = UUri::try_from("//vehicle/4210/1/9030").expect("valid URI");
    for iteration in 0..20 {
        let transport =
            UTransportLola::build(test_config("lola/transport_wire/repeated_teardown")).unwrap();
        let payload = format!("teardown-{iteration}");
        send_wire_payload::<TestCustomWire>(&transport, topic.clone(), payload.as_bytes());
        let selected = selected_wire(transport.zero_copy_core(), TestCustomWire);
        let frame = receive_with_retry(&selected, &topic);
        assert_eq!(frame.try_contiguous_payload(), Some(payload.as_bytes()));
        drop(frame);
        drop(selected);
        drop(transport);
    }
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn uninit_tx_round_trips_payload() {
    let transport = UTransportLola::build(test_config("lola/transport_wire/uninit_tx")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9028").expect("valid URI");
    let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);
    let mut loan =
        block_on_ready(selected.loan_uninit_tx(native_tx_spec(topic.clone(), 3, 1))).unwrap();
    for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(*b"xyz") {
        slot.write(byte);
    }
    // SAFETY: every byte in the requested payload range was initialized above.
    let loan = unsafe { loan.assume_payload_init() };
    block_on_ready(selected.send_zero_copy(loan)).unwrap();

    let frame = block_on_ready(selected.receive_zero_copy(&topic, None)).unwrap();
    assert_eq!(frame.try_contiguous_payload(), Some(b"xyz".as_slice()));
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn listener_receives_and_drops_after_unregister() {
    runtime().block_on(async {
        let transport =
            UTransportLola::build(test_config("lola/transport_wire/listener_delivery")).unwrap();
        let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);
        let topic = UUri::try_from("//vehicle/4210/1/9015").expect("valid URI");
        let listener = Arc::new(RecordingListener::default());
        let registration: Arc<dyn UZeroCopyListener<NativeLolaNativeRx>> = listener.clone();

        selected
            .register_zero_copy_listener(&topic, None, Arc::clone(&registration))
            .await
            .unwrap();
        send_native_payload(&transport, topic.clone(), b"listen");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        selected
            .unregister_zero_copy_listener(&topic, None, registration)
            .await
            .unwrap();
        send_native_payload(&transport, topic, b"ignored");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        assert_eq!(listener.payloads(), vec![b"listen".to_vec()]);
    });
}

#[test]
#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
fn listener_drops_wrong_wire_payload() {
    runtime().block_on(async {
        let transport =
            UTransportLola::build(test_config("lola/transport_wire/listener_wrong_wire")).unwrap();
        let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);
        let topic = UUri::try_from("//vehicle/4210/1/9027").expect("valid URI");
        let listener = Arc::new(RecordingListener::default());
        let registration: Arc<dyn UZeroCopyListener<NativeLolaNativeRx>> = listener.clone();

        selected
            .register_zero_copy_listener(&topic, None, registration)
            .await
            .unwrap();
        send_wire_payload::<ProtobufWire>(&transport, topic, b"drop");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        assert!(listener.payloads().is_empty());
    });
}

#[cfg(feature = "benchmark-owned")]
#[test]
fn owned_core_round_trips_external_xcdrv2_payload() {
    let transport = UTransportLola::build(test_config("lola/transport_wire/owned_xcdrv2")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9031").expect("valid URI");
    send_owned_payload::<XcdrV2Wire>(
        &transport,
        topic.clone(),
        &up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_BYTES,
    );

    let owned = LolaOwnedCore::new(transport.zero_copy_core()).with_selected_wire(XcdrV2Wire);
    let frame = block_on_ready(owned.receive_owned(&topic, None)).unwrap();

    assert_eq!(
        frame.payload_bytes(),
        &up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_BYTES
    );
}

#[cfg(feature = "benchmark-owned")]
#[test]
fn owned_core_wrong_wire_is_rejected_before_public_receive_exposes_frame() {
    let transport =
        UTransportLola::build(test_config("lola/transport_wire/owned_wrong_wire")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9032").expect("valid URI");
    send_owned_payload::<ProtobufWire>(&transport, topic.clone(), b"protobuf bytes");

    let owned =
        LolaOwnedCore::new(transport.zero_copy_core()).with_selected_wire(UProtocolNativeWire);
    let error = match block_on_ready(owned.receive_owned(&topic, None)) {
        Ok(_) => panic!("wrong wire should be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

#[test]
fn direct_raw_tx_requires_selected_wire() {
    let transport =
        UTransportLola::build(test_config("lola/transport_wire/direct_raw_tx")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9008").expect("valid URI");
    let error = match block_on_ready(transport.loan_tx(native_tx_spec(topic, 1, 1))) {
        Ok(_) => panic!("direct raw TX should require selected wire"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::FailedPrecondition);
}

#[test]
fn tx_loan_rejects_alignment_larger_than_sample_alignment() {
    let transport =
        UTransportLola::build(test_config("lola/transport_wire/alignment_rejection")).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9008").expect("valid URI");
    let selected = selected_wire(transport.zero_copy_core(), UProtocolNativeWire);
    let error = match block_on_ready(selected.loan_tx(native_tx_spec(topic, 1, 16))) {
        Ok(_) => panic!("LoLa TX loan should reject excessive alignment"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

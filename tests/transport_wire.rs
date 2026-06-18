//! Public selected-wire coverage for the LoLa test-stub backend.

use std::{future::Future, sync::Arc, task::Wake};

use async_trait::async_trait;
use std::sync::Mutex;
use up_rust::{
    try_project_umessage_to_frame_metadata, PayloadEncoding, PayloadFormat, ProtobufWire, UCode,
    UFrameMetadata, UFrameView, UMessageBuilder, UOwnedFrame, UOwnedTransport, UPayloadFormat,
    UProtocolNativeWire, UTxBuffer, UTxLoanSpec, UUninitTxBuffer, UUri, UWire, UWireMetadata,
    UWireRx, UWithWire, UZeroCopyListener, UZeroCopyTransport, UZeroCopyUninitTransport,
    WireIdentity, NATIVE_PREFIX_METADATA_LAYOUT_ID,
};
#[cfg(feature = "benchmark-owned")]
use up_transport_lola_rust::LolaOwnedCore;
use up_transport_lola_rust::{LolaRxLease, LolaTransportConfig, UTransportLola};
use up_wire_xcdrv2::{VehicleSignalV1, XcdrV2Wire, VEHICLE_SIGNAL_V1_GOLDEN_VALUE};

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

fn test_config() -> LolaTransportConfig {
    LolaTransportConfig {
        local_authority: "vehicle".to_string(),
        instance_specifier: "lola/service/instance".to_string(),
        service_type: "uprotocol.LoLa".to_string(),
        event_name: "UProtocolFrame".to_string(),
        sample_size: 256,
        sample_alignment: 8,
        max_samples: 8,
        pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
        pull_mismatch_queue_full_policy:
            LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
        mw_com_config_path: None,
    }
}

fn metadata_with_encoding(topic: UUri, encoding: PayloadEncoding) -> UFrameMetadata {
    let message = UMessageBuilder::publish(topic)
        .build()
        .expect("valid publish message");
    UFrameMetadata::new(message.attributes().clone(), Some(encoding)).expect("valid frame metadata")
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
    let message = UMessageBuilder::publish(topic)
        .build_with_payload(Vec::new(), UPayloadFormat::Raw)
        .expect("valid publish message");
    let metadata = try_project_umessage_to_frame_metadata(&message).expect("valid metadata");
    UTxLoanSpec::payload(metadata, payload_len, payload_alignment).expect("valid loan spec")
}

fn send_native_payload(transport: &Arc<UTransportLola>, topic: UUri, payload: &[u8]) {
    block_on_ready(async {
        let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);
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
    W: UWireMetadata + PayloadFormat + Default + Send + Sync + 'static,
{
    block_on_ready(async {
        let selected = transport.zero_copy_core().with_wire(W::default());
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
    W: UWireMetadata + PayloadFormat + Default + Send + Sync + 'static,
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
impl UZeroCopyListener<UWireRx<LolaRxLease, UProtocolNativeWire>> for RecordingListener {
    async fn on_receive_zero_copy(&self, frame: UWireRx<LolaRxLease, UProtocolNativeWire>) {
        self.payloads
            .lock()
            .unwrap()
            .push(frame.try_contiguous_payload().unwrap().to_vec());
    }
}

#[test]
fn external_xcdrv2_wire_round_trips_payload() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9021").expect("valid URI");
    send_wire_payload::<XcdrV2Wire>(
        &transport,
        topic.clone(),
        &up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_BYTES,
    );

    let selected = transport.zero_copy_core().with_wire(XcdrV2Wire);
    let frame = block_on_ready(selected.receive_zero_copy(&topic, None)).unwrap();
    let decoded: VehicleSignalV1 = frame.decode_payload().unwrap();

    assert_eq!(decoded, VEHICLE_SIGNAL_V1_GOLDEN_VALUE);
}

#[test]
fn custom_wire_encoding_round_trips_metadata_and_payload_bytes() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9022").expect("valid URI");
    send_wire_payload::<TestCustomWire>(&transport, topic.clone(), b"custom");

    let selected = transport.zero_copy_core().with_wire(TestCustomWire);
    let frame = block_on_ready(selected.receive_zero_copy(&topic, None)).unwrap();

    assert_eq!(
        frame.metadata().payload_encoding(),
        Some(&TestCustomWire::encoding())
    );
    assert_eq!(frame.try_contiguous_payload(), Some(b"custom".as_slice()));
}

#[test]
fn wrong_wire_is_rejected_before_public_receive_exposes_frame() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9023").expect("valid URI");
    send_wire_payload::<ProtobufWire>(&transport, topic.clone(), b"protobuf bytes");
    let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);

    let error = match block_on_ready(selected.receive_zero_copy(&topic, None)) {
        Ok(_) => panic!("wrong wire should be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

#[test]
fn no_payload_and_present_empty_payload_are_distinct() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let no_payload_topic = UUri::try_from("//vehicle/4210/1/9025").expect("valid URI");
    let empty_topic = UUri::try_from("//vehicle/4210/1/9026").expect("valid URI");
    let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);
    let no_payload_message = UMessageBuilder::publish(no_payload_topic.clone())
        .build()
        .unwrap();
    let no_payload_metadata =
        UFrameMetadata::new(no_payload_message.attributes().clone(), None).unwrap();
    let no_payload_loan =
        block_on_ready(selected.loan_tx(UTxLoanSpec::no_payload(no_payload_metadata).unwrap()))
            .unwrap();
    block_on_ready(selected.send_zero_copy(no_payload_loan)).unwrap();
    let empty_metadata = metadata_with_encoding(
        empty_topic.clone(),
        PayloadEncoding::Standard(UPayloadFormat::Raw),
    );
    let empty_loan = block_on_ready(
        selected.loan_tx(UTxLoanSpec::present_empty_payload(empty_metadata).unwrap()),
    )
    .unwrap();
    block_on_ready(selected.send_zero_copy(empty_loan)).unwrap();

    let no_payload = block_on_ready(selected.receive_zero_copy(&no_payload_topic, None)).unwrap();
    let empty = block_on_ready(selected.receive_zero_copy(&empty_topic, None)).unwrap();

    assert!(!no_payload.has_payload());
    assert!(empty.has_payload());
    assert_eq!(empty.payload_len(), 0);
}

#[test]
fn uninit_tx_round_trips_payload() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9028").expect("valid URI");
    let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);
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
fn listener_receives_and_drops_after_unregister() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);
    let topic = UUri::try_from("//vehicle/4210/1/9015").expect("valid URI");
    let listener = Arc::new(RecordingListener::default());
    let registration: Arc<dyn UZeroCopyListener<UWireRx<LolaRxLease, UProtocolNativeWire>>> =
        listener.clone();

    block_on_ready(selected.register_zero_copy_listener(&topic, None, Arc::clone(&registration)))
        .unwrap();
    send_native_payload(&transport, topic.clone(), b"listen");
    block_on_ready(selected.unregister_zero_copy_listener(&topic, None, registration)).unwrap();
    send_native_payload(&transport, topic, b"ignored");

    assert_eq!(listener.payloads(), vec![b"listen".to_vec()]);
}

#[test]
fn listener_drops_wrong_wire_payload() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);
    let topic = UUri::try_from("//vehicle/4210/1/9027").expect("valid URI");
    let listener = Arc::new(RecordingListener::default());
    let registration: Arc<dyn UZeroCopyListener<UWireRx<LolaRxLease, UProtocolNativeWire>>> =
        listener.clone();

    block_on_ready(selected.register_zero_copy_listener(&topic, None, registration)).unwrap();
    send_wire_payload::<ProtobufWire>(&transport, topic, b"drop");

    assert!(listener.payloads().is_empty());
}

#[cfg(feature = "benchmark-owned")]
#[test]
fn owned_core_round_trips_external_xcdrv2_payload() {
    let transport = UTransportLola::build(test_config()).unwrap();
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
    let transport = UTransportLola::build(test_config()).unwrap();
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
    let transport = UTransportLola::build(test_config()).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9008").expect("valid URI");
    let error = match block_on_ready(transport.loan_tx(native_tx_spec(topic, 1, 1))) {
        Ok(_) => panic!("direct raw TX should require selected wire"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::FailedPrecondition);
}

#[test]
fn tx_loan_rejects_alignment_larger_than_sample_alignment() {
    let transport = UTransportLola::build(test_config()).unwrap();
    let topic = UUri::try_from("//vehicle/4210/1/9008").expect("valid URI");
    let selected = transport.zero_copy_core().with_wire(UProtocolNativeWire);
    let error = match block_on_ready(selected.loan_tx(native_tx_spec(topic, 1, 16))) {
        Ok(_) => panic!("LoLa TX loan should reject excessive alignment"),
        Err(error) => error,
    };

    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

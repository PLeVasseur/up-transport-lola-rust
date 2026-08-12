//! Public selected-wire coverage for LoLa's fake and native backends.

use std::sync::Arc;
#[cfg(feature = "lola-ffi")]
use std::sync::OnceLock;
use std::time::Duration;

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
use async_trait::async_trait;
use up_rust::selected_wire_user_api::UNativePrefixWireTransport;
use up_rust::UUID;
#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
use up_rust::{
    PayloadDecodeLimit, ProtobufWire, UUninitTxBuffer, UZeroCopyListener,
    UZeroCopyUninitTransportImpl,
};
use up_rust::{
    PayloadEncoding, PayloadLoanProvenance, UCode, UEncodedLoanedRxFrame, UFrameMetadata,
    UFrameView, UProtocolNativeWire, UStatus, UTxBuffer, UTxLoanSpec, UUri, UWire,
    UZeroCopyTransportImpl,
};
#[cfg(all(
    feature = "test-stub",
    not(feature = "lola-ffi"),
    feature = "benchmark-owned"
))]
use up_rust::{UOwnedFrame, UOwnedTransportImpl};
use up_transport_lola_rust::LolaDefaultRxChannel;
#[cfg(all(
    feature = "test-stub",
    not(feature = "lola-ffi"),
    feature = "benchmark-owned"
))]
use up_transport_lola_rust::LolaOwnedCore;
#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
use up_transport_lola_rust::LolaPullMismatchQueueFullPolicy;
use up_transport_lola_rust::{LolaTransportConfig, LolaZeroCopyCore, UTransportLola};
#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
use up_wire_xcdrv2::{
    VehicleSignalV1, XcdrV2Wire, VEHICLE_SIGNAL_V1_GOLDEN_BYTES, VEHICLE_SIGNAL_V1_GOLDEN_VALUE,
    XCDR_V2_ENCODING_ID,
};

type LolaWireTransport<W> = UNativePrefixWireTransport<LolaZeroCopyCore, W>;
#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
type NativeLolaRx = <LolaWireTransport<UProtocolNativeWire> as UZeroCopyTransportImpl>::Rx;

fn topic(resource_id: u16) -> UUri {
    UUri::try_from_parts("vehicle", 0x4210, 1, resource_id).expect("valid test URI")
}

fn metadata(source: UUri, encoding: PayloadEncoding) -> UFrameMetadata {
    UFrameMetadata::publish(source)
        .with_payload_encoding(encoding)
        .build()
        .expect("valid metadata")
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
fn metadata_without_payload(source: UUri) -> UFrameMetadata {
    UFrameMetadata::publish(source)
        .build()
        .expect("valid metadata")
}

fn config(instance_specifier: &str) -> LolaTransportConfig {
    LolaTransportConfig {
        local_authority: "vehicle".to_string(),
        instance_specifier: instance_specifier.to_string(),
        service_type: "/uprotocol/Transport".to_string(),
        event_name: "frame".to_string(),
        sample_size: 512,
        sample_alignment: 8,
        max_samples: 8,
        pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
        pull_mismatch_queue_full_policy:
            LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
        mw_com_config_path: None,
    }
}

fn selected<W>(transport: &Arc<UTransportLola>, wire: W) -> LolaWireTransport<W>
where
    W: UWire,
{
    transport.zero_copy_core().with_selected_wire(wire)
}

async fn send_payload<W>(
    transport: &LolaWireTransport<W>,
    frame_metadata: UFrameMetadata,
    payload: &[u8],
    alignment: usize,
) -> Result<(), UStatus>
where
    W: UWire + Send + Sync + 'static,
{
    let mut loan = transport
        .loan_validated_tx(UTxLoanSpec::payload(
            frame_metadata,
            payload.len(),
            alignment,
        )?)
        .await?;
    loan.payload_mut().copy_from_slice(payload);
    transport.send_validated_zero_copy(loan).await
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
fn request_metadata(method: UUri, reply_to: UUri) -> UFrameMetadata {
    UFrameMetadata::request(method, reply_to, Duration::from_secs(5))
        .with_payload_encoding(PayloadEncoding::RAW)
        .build()
        .expect("valid request metadata")
}

fn response_metadata(method: UUri, reply_to: UUri) -> UFrameMetadata {
    UFrameMetadata::response(method, reply_to, UUID::build())
        .with_payload_encoding(PayloadEncoding::RAW)
        .build()
        .expect("valid response metadata")
}

async fn receive_with_retry<W>(
    transport: &LolaWireTransport<W>,
    source: &UUri,
) -> Result<<LolaWireTransport<W> as UZeroCopyTransportImpl>::Rx, UStatus>
where
    W: UWire + Send + Sync + 'static,
{
    let mut last = None;
    for _ in 0..50 {
        match transport.receive_validated_zero_copy(source, None).await {
            Ok(frame) => return Ok(frame),
            Err(error) if error.code() == UCode::NotFound => {
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.expect("receive attempted at least once"))
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn external_xcdrv2_round_trips_as_loan_backed_payload() {
    let transport = UTransportLola::build(config("lola/r19/xcdrv2")).unwrap();
    let selected = selected(&transport, XcdrV2Wire);
    let source = topic(0x9001);
    send_payload(
        &selected,
        metadata(
            source.clone(),
            PayloadEncoding::from_registry_entry(XCDR_V2_ENCODING_ID),
        ),
        &VEHICLE_SIGNAL_V1_GOLDEN_BYTES,
        8,
    )
    .await
    .unwrap();

    let frame = receive_with_retry(&selected, &source).await.unwrap();
    let decoded: VehicleSignalV1 = frame.decode_payload(PayloadDecodeLimit::new(64)).unwrap();
    assert_eq!(decoded, VEHICLE_SIGNAL_V1_GOLDEN_VALUE);
    assert_eq!(
        frame
            .raw()
            .loaned_contiguous_payload()
            .unwrap()
            .provenance(),
        PayloadLoanProvenance::OpaqueTransportLoan
    );
    assert_eq!(
        frame.try_contiguous_payload().unwrap().as_ptr() as usize % 8,
        0
    );
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn dual_rpc_channels_route_requests_and_responses_separately() {
    let primary = config("lola/r19/rpc-primary");
    let response = config("lola/r19/rpc-response");
    let transport = UTransportLola::build_with_response_channel_and_default_rx(
        primary,
        Some(response),
        LolaDefaultRxChannel::Both,
    )
    .unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let method = UUri::try_from_parts("service", 0x4220, 1, 0x1010).unwrap();
    let reply_to = UUri::try_from_parts("client", 0x4230, 1, 0).unwrap();

    send_payload(
        &selected,
        response_metadata(method.clone(), reply_to.clone()),
        b"response",
        1,
    )
    .await
    .unwrap();
    send_payload(
        &selected,
        request_metadata(method.clone(), reply_to.clone()),
        b"request",
        1,
    )
    .await
    .unwrap();

    let request = selected
        .receive_validated_zero_copy(&reply_to, Some(&method))
        .await
        .unwrap();
    let response = selected
        .receive_validated_zero_copy(&method, Some(&reply_to))
        .await
        .unwrap();
    assert_eq!(
        request.try_contiguous_payload(),
        Some(b"request".as_slice())
    );
    assert_eq!(
        response.try_contiguous_payload(),
        Some(b"response".as_slice())
    );
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn broad_receive_honors_response_default_channel() {
    let transport = UTransportLola::build_with_response_channel_and_default_rx(
        config("lola/r19/default-primary"),
        Some(config("lola/r19/default-response")),
        LolaDefaultRxChannel::Response,
    )
    .unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let method = UUri::try_from_parts("service", 0x4220, 1, 0x1011).unwrap();
    let reply_to = UUri::try_from_parts("client", 0x4230, 1, 0).unwrap();
    send_payload(
        &selected,
        response_metadata(method, reply_to),
        b"response-default",
        1,
    )
    .await
    .unwrap();

    let frame = selected
        .receive_validated_zero_copy(&UUri::any(), None)
        .await
        .unwrap();
    assert_eq!(
        frame.try_contiguous_payload(),
        Some(b"response-default".as_slice())
    );
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn wrong_wire_is_rejected_before_public_receive() {
    let transport = UTransportLola::build(config("lola/r19/wrong-wire")).unwrap();
    let source = topic(0x9002);
    let publisher = selected(&transport, ProtobufWire);
    let subscriber = selected(&transport, XcdrV2Wire);
    send_payload(
        &publisher,
        metadata(source.clone(), PayloadEncoding::PROTOBUF),
        b"protobuf",
        1,
    )
    .await
    .unwrap();

    let error = subscriber
        .receive_validated_zero_copy(&source, None)
        .await
        .expect_err("wire mismatch must be rejected");
    assert_eq!(error.code(), UCode::InvalidArgument);
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn no_payload_and_present_empty_payload_remain_distinct() {
    let transport = UTransportLola::build(config("lola/r19/payload-presence")).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let absent_source = topic(0x9003);
    let empty_source = topic(0x9004);

    let absent = selected
        .loan_validated_tx(
            UTxLoanSpec::no_payload(metadata_without_payload(absent_source.clone())).unwrap(),
        )
        .await
        .unwrap();
    selected.send_validated_zero_copy(absent).await.unwrap();
    let absent = receive_with_retry(&selected, &absent_source).await.unwrap();

    let empty = selected
        .loan_validated_tx(
            UTxLoanSpec::payload(metadata(empty_source.clone(), PayloadEncoding::RAW), 0, 1)
                .unwrap(),
        )
        .await
        .unwrap();
    selected.send_validated_zero_copy(empty).await.unwrap();
    let empty = receive_with_retry(&selected, &empty_source).await.unwrap();

    assert!(!absent.has_payload());
    assert!(empty.has_payload());
    assert_eq!(empty.payload_len(), 0);
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn uninitialized_loan_round_trips_after_full_initialization() {
    let transport = UTransportLola::build(config("lola/r19/uninit")).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let source = topic(0x9005);
    let mut loan = selected
        .loan_validated_uninit_tx(
            UTxLoanSpec::payload(metadata(source.clone(), PayloadEncoding::RAW), 3, 1).unwrap(),
        )
        .await
        .unwrap();
    for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(*b"xyz") {
        slot.write(byte);
    }
    // SAFETY: every byte in the visible three-byte payload was initialized above.
    let loan = unsafe { loan.assume_payload_initialized() };
    selected.send_validated_zero_copy(loan).await.unwrap();

    let frame = receive_with_retry(&selected, &source).await.unwrap();
    assert_eq!(frame.try_contiguous_payload(), Some(b"xyz".as_slice()));
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn excessive_payload_alignment_is_rejected() {
    let transport = UTransportLola::build(config("lola/r19/alignment")).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let error = match selected
        .loan_validated_tx(
            UTxLoanSpec::payload(metadata(topic(0x9006), PayloadEncoding::RAW), 1, 16).unwrap(),
        )
        .await
    {
        Ok(_) => panic!("alignment larger than the LoLa sample alignment must fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), UCode::InvalidArgument);
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn mismatched_pull_sample_is_retained_for_a_later_filter() {
    let transport = UTransportLola::build(config("lola/r19/mismatch-retain")).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let first = topic(0x9007);
    let second = topic(0x9008);
    send_payload(
        &selected,
        metadata(first.clone(), PayloadEncoding::RAW),
        b"first",
        1,
    )
    .await
    .unwrap();
    let mismatch = selected.receive_validated_zero_copy(&second, None).await;
    assert!(mismatch.is_err_and(|error| error.code() == UCode::NotFound));
    let diagnostics = transport.pull_mismatch_queue_diagnostics().await;
    assert_eq!(diagnostics.current_depth, 1);

    send_payload(
        &selected,
        metadata(second.clone(), PayloadEncoding::RAW),
        b"second",
        1,
    )
    .await
    .unwrap();

    let second_frame = receive_with_retry(&selected, &second).await.unwrap();
    let first_frame = receive_with_retry(&selected, &first).await.unwrap();
    assert_eq!(
        second_frame.try_contiguous_payload(),
        Some(b"second".as_slice())
    );
    assert_eq!(
        first_frame.try_contiguous_payload(),
        Some(b"first".as_slice())
    );
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn bounded_mismatch_queue_reports_drop_oldest() {
    let mut test_config = config("lola/r19/mismatch-bounded");
    test_config.pull_mismatch_queue_capacity = 1;
    test_config.pull_mismatch_queue_full_policy =
        LolaPullMismatchQueueFullPolicy::DropOldestAndReport;
    let transport = UTransportLola::build(test_config).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let first = topic(0x9009);
    let second = topic(0x900a);
    let wanted = topic(0x900b);
    for (source, payload) in [
        (first, b"first".as_slice()),
        (second, b"second".as_slice()),
        (wanted.clone(), b"wanted".as_slice()),
    ] {
        send_payload(
            &selected,
            metadata(source, PayloadEncoding::RAW),
            payload,
            1,
        )
        .await
        .unwrap();
    }

    let frame = receive_with_retry(&selected, &wanted).await.unwrap();
    assert_eq!(frame.try_contiguous_payload(), Some(b"wanted".as_slice()));
    let diagnostics = transport.pull_mismatch_queue_diagnostics().await;
    assert_eq!(diagnostics.current_depth, 1);
    assert_eq!(diagnostics.dropped_mismatches, 1);
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[derive(Default)]
struct RecordingListener(std::sync::Mutex<Vec<Vec<u8>>>);

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[async_trait]
impl UZeroCopyListener<NativeLolaRx> for RecordingListener {
    async fn on_receive_zero_copy(&self, frame: NativeLolaRx) {
        self.0
            .lock()
            .unwrap()
            .push(frame.try_contiguous_payload().unwrap_or_default().to_vec());
    }
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn listener_receives_matching_selected_wire_frame() {
    let transport = UTransportLola::build(config("lola/r19/listener")).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let source = topic(0x900c);
    let listener = Arc::new(RecordingListener::default());
    let registration: Arc<dyn UZeroCopyListener<NativeLolaRx>> = listener.clone();
    selected
        .register_validated_zero_copy_listener(&source, None, Arc::clone(&registration))
        .await
        .unwrap();
    send_payload(
        &selected,
        metadata(source.clone(), PayloadEncoding::RAW),
        b"listener",
        1,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    selected
        .unregister_validated_zero_copy_listener(&source, None, registration)
        .await
        .unwrap();

    assert_eq!(
        listener.0.lock().unwrap().as_slice(),
        [b"listener".to_vec()]
    );
}

#[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
#[tokio::test]
async fn response_listener_uses_response_channel() {
    let transport = UTransportLola::build_with_response_channel_and_default_rx(
        config("lola/r19/listener-primary"),
        Some(config("lola/r19/listener-response")),
        LolaDefaultRxChannel::Primary,
    )
    .unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let method = UUri::try_from_parts("service", 0x4220, 1, 0x1012).unwrap();
    let reply_to = UUri::try_from_parts("client", 0x4230, 1, 0).unwrap();
    let listener = Arc::new(RecordingListener::default());
    let registration: Arc<dyn UZeroCopyListener<NativeLolaRx>> = listener.clone();
    selected
        .register_validated_zero_copy_listener(&method, Some(&reply_to), Arc::clone(&registration))
        .await
        .unwrap();
    send_payload(
        &selected,
        response_metadata(method.clone(), reply_to.clone()),
        b"response-listener",
        1,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    selected
        .unregister_validated_zero_copy_listener(&method, Some(&reply_to), registration)
        .await
        .unwrap();

    assert_eq!(
        listener.0.lock().unwrap().as_slice(),
        [b"response-listener".to_vec()]
    );
}

#[cfg(all(
    feature = "test-stub",
    not(feature = "lola-ffi"),
    feature = "benchmark-owned"
))]
#[tokio::test]
async fn owned_core_round_trips_external_xcdrv2_bytes() {
    let transport = UTransportLola::build(config("lola/r19/owned")).unwrap();
    let owned = LolaOwnedCore::new(transport.zero_copy_core()).with_selected_wire(XcdrV2Wire);
    let source = topic(0x900d);
    let frame = UOwnedFrame::with_payload(
        metadata(
            source.clone(),
            PayloadEncoding::from_registry_entry(XCDR_V2_ENCODING_ID),
        ),
        VEHICLE_SIGNAL_V1_GOLDEN_BYTES.to_vec(),
    )
    .unwrap();
    owned.send_validated_owned(frame).await.unwrap();
    let frame = owned.receive_validated_owned(&source, None).await.unwrap();
    assert_eq!(frame.payload_bytes(), &VEHICLE_SIGNAL_V1_GOLDEN_BYTES);
}

#[cfg(feature = "lola-ffi")]
fn native_config() -> LolaTransportConfig {
    let mut config = config("uprotocol/transport");
    config.mw_com_config_path = Some(std::env::var("LOLA_NATIVE_TEST_CONFIG").unwrap_or_else(
        |_| {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/mw_com_config.json"
            )
            .to_string()
        },
    ));
    config.max_samples = 4;
    config
}

#[cfg(feature = "lola-ffi")]
fn native_response_config() -> LolaTransportConfig {
    let mut config = native_config();
    config.instance_specifier = "uprotocol/transportResponse".to_string();
    config.service_type = "/uprotocol/TransportResponse".to_string();
    config.event_name = "frameResponse".to_string();
    config
}

#[cfg(feature = "lola-ffi")]
async fn native_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

#[cfg(feature = "lola-ffi")]
#[tokio::test]
#[ignore = "requires the native S-CORE LoLa runtime fixture"]
async fn native_selected_wire_round_trip_uses_real_lola_sample() {
    let _guard = native_guard().await;
    let transport = UTransportLola::build(native_config()).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let source = topic(0x900e);
    send_payload(
        &selected,
        metadata(source.clone(), PayloadEncoding::RAW),
        b"native",
        8,
    )
    .await
    .unwrap();
    let frame = receive_with_retry(&selected, &source).await.unwrap();
    assert_eq!(frame.try_contiguous_payload(), Some(b"native".as_slice()));
    assert_eq!(
        frame
            .raw()
            .loaned_contiguous_payload()
            .unwrap()
            .provenance(),
        PayloadLoanProvenance::OpaqueTransportLoan
    );
}

#[cfg(feature = "lola-ffi")]
#[tokio::test]
#[ignore = "requires the native S-CORE LoLa runtime fixture"]
async fn native_dual_rpc_channels_use_separate_lola_events() {
    let _guard = native_guard().await;
    let transport = UTransportLola::build_with_response_channel_and_default_rx(
        native_config(),
        Some(native_response_config()),
        LolaDefaultRxChannel::Response,
    )
    .unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let method = UUri::try_from_parts("service", 0x4220, 1, 0x1013).unwrap();
    let reply_to = UUri::try_from_parts("client", 0x4230, 1, 0).unwrap();
    send_payload(
        &selected,
        response_metadata(method.clone(), reply_to.clone()),
        b"native-response",
        8,
    )
    .await
    .unwrap();

    let frame = receive_with_retry(&selected, &method).await.unwrap();
    assert_eq!(
        frame.try_contiguous_payload(),
        Some(b"native-response".as_slice())
    );
}

#[cfg(feature = "lola-ffi")]
#[tokio::test]
#[ignore = "requires the native S-CORE LoLa runtime fixture"]
async fn native_mismatched_pull_sample_is_retained_for_a_later_filter() {
    let _guard = native_guard().await;
    let transport = UTransportLola::build(native_config()).unwrap();
    let selected = selected(&transport, UProtocolNativeWire);
    let first = topic(0x900f);
    let second = topic(0x9010);

    let initial = selected.receive_validated_zero_copy(&first, None).await;
    assert!(initial.is_err_and(|error| error.code() == UCode::NotFound));

    send_payload(
        &selected,
        metadata(first.clone(), PayloadEncoding::RAW),
        b"first-native",
        8,
    )
    .await
    .unwrap();
    let mismatch = selected.receive_validated_zero_copy(&second, None).await;
    assert!(mismatch.is_err_and(|error| error.code() == UCode::NotFound));
    let diagnostics = transport.pull_mismatch_queue_diagnostics().await;
    assert_eq!(diagnostics.current_depth, 1);

    send_payload(
        &selected,
        metadata(second.clone(), PayloadEncoding::RAW),
        b"second-native",
        8,
    )
    .await
    .unwrap();

    let second_frame = receive_with_retry(&selected, &second).await.unwrap();
    let first_frame = receive_with_retry(&selected, &first).await.unwrap();
    assert_eq!(
        second_frame.try_contiguous_payload(),
        Some(b"second-native".as_slice())
    );
    assert_eq!(
        first_frame.try_contiguous_payload(),
        Some(b"first-native".as_slice())
    );
}

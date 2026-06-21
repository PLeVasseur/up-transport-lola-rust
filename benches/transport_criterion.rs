/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#![allow(
    dead_code,
    missing_docs,
    unused_imports,
    clippy::missing_panics_doc,
    clippy::too_many_lines
)]

use std::{sync::Arc, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::{Builder, Runtime};
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::bench_fixtures::payload_contract::{self, *};
#[cfg(feature = "benchmark-owned")]
use up_rust::ProtobufWire;
use up_rust::{
    NativePrefixProtobufMetadataCodec, PayloadEncoding, StableContainerWireFormat, UCode,
    UFrameMetadata, ULoanedContiguousZeroCopyRxFrame, UMessageBuilder, UMessageType, UUri,
    UWireTransport, UZeroCopyTransport, UZeroCopyUninitTransportExt, UUID,
};
#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
use up_rust::{ProtobufPayload, UOwnedFrame, UOwnedTransport};
#[cfg(feature = "benchmark-owned")]
use up_transport_lola_rust::LolaOwnedCore;
use up_transport_lola_rust::{LolaTransportConfig, LolaZeroCopyCore, UTransportLola};

const BENCH_TIMEOUT: Duration = Duration::from_secs(5);
const LARGE_SENSOR_BENCH_TIMEOUT: Duration = Duration::from_secs(30);
const CORE_SAMPLE_SIZE: usize = 128 * 1_024;
const CORE_MAX_SAMPLES: usize = 128;
const CAMERA_SAMPLE_SIZE: usize = 16 * 1_024 * 1_024;
const CAMERA_MAX_SAMPLES: usize = 16;
const PAYLOAD_CONTRACT_SEQUENCE: u32 = 1;

#[derive(Clone, Copy)]
enum BenchSuite {
    Raw,
    PayloadContract,
    All,
}

impl BenchSuite {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_SUITE")
            .unwrap_or_else(|_| "raw".to_string())
            .as_str()
        {
            "raw" => Self::Raw,
            "payload-contract" => Self::PayloadContract,
            "all" => Self::All,
            other => {
                panic!(
                    "TRANSPORT_BENCH_SUITE must be one of raw, payload-contract, all; got {other}"
                )
            }
        }
    }

    fn includes_payload_contract(self) -> bool {
        matches!(self, Self::PayloadContract | Self::All)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum BenchDiagnostic {
    FullLoop,
    PrebuiltPayload,
    MetadataOnly,
    TxOnly,
    RxOnly,
    ListenerOnly,
    CopyLedger,
    ZcInitOnly,
    ZcSendOnly,
    ZcRxOnly,
    ZcValidationOnly,
    ZcFilterOnly,
    ZcCopyLedger,
    ZcLoanProvenanceCheck,
    NativeFixtureFit,
    UlolLayout,
}

impl BenchDiagnostic {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_DIAGNOSTIC")
            .unwrap_or_else(|_| "full-loop".to_string())
            .as_str()
        {
            "full-loop" => Self::FullLoop,
            "prebuilt-payload" => Self::PrebuiltPayload,
            "metadata-only" => Self::MetadataOnly,
            "tx-only" => Self::TxOnly,
            "rx-only" => Self::RxOnly,
            "listener-only" => Self::ListenerOnly,
            "copy-ledger" => Self::CopyLedger,
            "zc-init-only" => Self::ZcInitOnly,
            "zc-send-only" => Self::ZcSendOnly,
            "zc-rx-only" => Self::ZcRxOnly,
            "zc-validation-only" => Self::ZcValidationOnly,
            "zc-filter-only" => Self::ZcFilterOnly,
            "zc-copy-ledger" => Self::ZcCopyLedger,
            "zc-loan-provenance-check" => Self::ZcLoanProvenanceCheck,
            "native-fixture-fit" => Self::NativeFixtureFit,
            "ulol-layout" => Self::UlolLayout,
            other => panic!("unsupported TRANSPORT_BENCH_DIAGNOSTIC selector: {other}"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::FullLoop => "full-loop",
            Self::PrebuiltPayload => "prebuilt-payload",
            Self::MetadataOnly => "metadata-only",
            Self::TxOnly => "tx-only",
            Self::RxOnly => "rx-only",
            Self::ListenerOnly => "listener-only",
            Self::CopyLedger => "copy-ledger",
            Self::ZcInitOnly => "zc-init-only",
            Self::ZcSendOnly => "zc-send-only",
            Self::ZcRxOnly => "zc-rx-only",
            Self::ZcValidationOnly => "zc-validation-only",
            Self::ZcFilterOnly => "zc-filter-only",
            Self::ZcCopyLedger => "zc-copy-ledger",
            Self::ZcLoanProvenanceCheck => "zc-loan-provenance-check",
            Self::NativeFixtureFit => "native-fixture-fit",
            Self::UlolLayout => "ulol-layout",
        }
    }
}

#[derive(Clone, Copy)]
enum BenchProfile {
    Core,
    Camera,
    All,
}

impl BenchProfile {
    fn from_lola_env() -> Self {
        match std::env::var("LOLA_BENCH_PROFILE")
            .unwrap_or_else(|_| "core".to_string())
            .as_str()
        {
            "core" => Self::Core,
            "camera" => Self::Camera,
            "all" => Self::All,
            other => panic!("LOLA_BENCH_PROFILE must be one of core, camera, all; got {other}"),
        }
    }

    fn includes_core(self) -> bool {
        matches!(self, Self::Core | Self::All)
    }

    fn includes_camera(self) -> bool {
        matches!(self, Self::Camera | Self::All)
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Clone, Copy, Eq, PartialEq)]
enum PayloadContractPath {
    #[cfg(feature = "benchmark-owned")]
    ProtobufOwned,
    StableZcNoZero,
    #[cfg(feature = "benchmark-owned")]
    StableOwnedBytes,
}

#[cfg(feature = "payload-contract-benchmarks")]
impl PayloadContractPath {
    fn label(self) -> &'static str {
        match self {
            #[cfg(feature = "benchmark-owned")]
            Self::ProtobufOwned => "protobuf_owned_full",
            Self::StableZcNoZero => "stable_zc_nozero_full",
            #[cfg(feature = "benchmark-owned")]
            Self::StableOwnedBytes => "stable_owned_bytes_full",
        }
    }

    fn is_owned(self) -> bool {
        match self {
            #[cfg(feature = "benchmark-owned")]
            Self::ProtobufOwned | Self::StableOwnedBytes => true,
            Self::StableZcNoZero => false,
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
impl BenchDiagnostic {
    fn applies_to(self, path: PayloadContractPath) -> bool {
        match self {
            Self::FullLoop | Self::MetadataOnly | Self::NativeFixtureFit | Self::UlolLayout => true,
            Self::PrebuiltPayload
            | Self::TxOnly
            | Self::RxOnly
            | Self::ListenerOnly
            | Self::CopyLedger => path.is_owned(),
            Self::ZcInitOnly
            | Self::ZcSendOnly
            | Self::ZcRxOnly
            | Self::ZcValidationOnly
            | Self::ZcFilterOnly
            | Self::ZcCopyLedger
            | Self::ZcLoanProvenanceCheck => path == PayloadContractPath::StableZcNoZero,
        }
    }
}

#[derive(Clone)]
struct BenchCase {
    source: UUri,
}

impl BenchCase {
    fn new(authority: &str) -> Self {
        Self {
            source: UUri::try_from_parts(authority, 0x4210, 1, next_resource_id())
                .expect("valid LoLa benchmark source URI"),
        }
    }

    fn metadata(&self, id: UUID, encoding: Option<PayloadEncoding>) -> UFrameMetadata {
        let mut builder = UMessageBuilder::publish(self.source.clone());
        builder.with_message_id(id);
        let message = builder.build().expect("valid benchmark message");
        UFrameMetadata::new(message.attributes().clone(), encoding).expect("valid metadata")
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
struct PayloadContractAck {
    id: UUID,
    message_type: UMessageType,
    case_id: u32,
    sequence: u32,
    semantic_reference_len: usize,
    transported_payload_len: usize,
}

struct BenchTransports {
    zero_copy: UWireTransport<
        LolaZeroCopyCore,
        StableContainerWireFormat,
        NativePrefixProtobufMetadataCodec,
    >,
    #[cfg(feature = "benchmark-owned")]
    protobuf_owned:
        Arc<UWireTransport<LolaOwnedCore, ProtobufWire, NativePrefixProtobufMetadataCodec>>,
    #[cfg(feature = "benchmark-owned")]
    stable_owned: Arc<
        UWireTransport<LolaOwnedCore, StableContainerWireFormat, NativePrefixProtobufMetadataCodec>,
    >,
}

impl BenchTransports {
    fn build(config: LolaTransportConfig) -> Self {
        let physical =
            UTransportLola::build(config).expect("LoLa benchmark transport should build");
        let core = physical.zero_copy_core();
        let zero_copy = UWireTransport::new(
            core.clone(),
            StableContainerWireFormat,
            NativePrefixProtobufMetadataCodec,
        );
        #[cfg(feature = "benchmark-owned")]
        let protobuf_owned =
            Arc::new(LolaOwnedCore::new(core.clone()).with_selected_wire(ProtobufWire));
        #[cfg(feature = "benchmark-owned")]
        let stable_owned =
            Arc::new(LolaOwnedCore::new(core).with_selected_wire(StableContainerWireFormat));
        Self {
            zero_copy,
            #[cfg(feature = "benchmark-owned")]
            protobuf_owned,
            #[cfg(feature = "benchmark-owned")]
            stable_owned,
        }
    }
}

async fn prime_subscriber(transports: &BenchTransports, case: &BenchCase) {
    match transports
        .zero_copy
        .receive_zero_copy(&case.source, None)
        .await
    {
        Ok(frame) => drop(frame),
        Err(status) if status.get_code() == UCode::NotFound => {}
        Err(status) => panic!("failed to prime LoLa pull subscriber: {status:?}"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn payload_contract_paths() -> &'static [PayloadContractPath] {
    #[cfg(feature = "benchmark-owned")]
    {
        &[
            PayloadContractPath::ProtobufOwned,
            PayloadContractPath::StableZcNoZero,
            PayloadContractPath::StableOwnedBytes,
        ]
    }
    #[cfg(not(feature = "benchmark-owned"))]
    {
        &[PayloadContractPath::StableZcNoZero]
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    authority: &str,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    timeout: Duration,
) {
    let mut group = c.benchmark_group(group_name);
    let diagnostic = BenchDiagnostic::from_env();
    let case_filter = std::env::var("TRANSPORT_BENCH_CASE")
        .ok()
        .filter(|filter| !filter.is_empty());
    for contract in payload_cases {
        if let Some(filter) = case_filter.as_deref() {
            if contract.name() != filter {
                continue;
            }
        }
        for &path in payload_contract_paths() {
            if !diagnostic.applies_to(path) {
                continue;
            }
            let case = BenchCase::new(authority);
            if diagnostic != BenchDiagnostic::ZcFilterOnly {
                runtime.block_on(prime_subscriber(transports, &case));
            }
            let transported_payload_len = payload_contract_transported_len(path, contract);
            group.bench_function(
                BenchmarkId::new(
                    diagnostic_benchmark_label(path, diagnostic),
                    format!(
                        "publish/{}/{}/{}",
                        contract.name(),
                        contract.semantic_reference_len(),
                        transported_payload_len
                    ),
                ),
                |b| {
                    b.iter(|| {
                        runtime.block_on(async {
                            run_payload_contract_diagnostic(
                                transports,
                                path,
                                &case,
                                UUID::build(),
                                contract,
                                diagnostic,
                                transported_payload_len,
                                timeout,
                            )
                            .await;
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

#[cfg(feature = "payload-contract-benchmarks")]
fn diagnostic_benchmark_label(path: PayloadContractPath, diagnostic: BenchDiagnostic) -> String {
    if diagnostic == BenchDiagnostic::FullLoop {
        path.label().to_string()
    } else {
        format!("{}::{}", path.label(), diagnostic.label())
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn run_payload_contract_diagnostic(
    transports: &BenchTransports,
    path: PayloadContractPath,
    case: &BenchCase,
    id: UUID,
    contract: &PayloadContractCase,
    diagnostic: BenchDiagnostic,
    transported_payload_len: usize,
    timeout: Duration,
) {
    match diagnostic {
        BenchDiagnostic::FullLoop => {
            send_payload_contract_path(transports, path, case, id.clone(), contract).await;
            let ack = receive_payload_contract_ack(
                transports,
                path,
                case,
                &id,
                contract,
                transported_payload_len,
                timeout,
            )
            .await;
            black_box(ack.semantic_reference_len);
            black_box(ack.transported_payload_len);
            black_box(contract.name());
        }
        BenchDiagnostic::PrebuiltPayload | BenchDiagnostic::CopyLedger => {
            black_box(prebuild_payload_contract_path(path, contract));
        }
        BenchDiagnostic::MetadataOnly
        | BenchDiagnostic::NativeFixtureFit
        | BenchDiagnostic::UlolLayout => {
            let metadata = case.metadata(id, diagnostic_payload_encoding(path, contract));
            black_box(metadata);
            black_box(contract.name());
            black_box("ULOL");
        }
        BenchDiagnostic::TxOnly
        | BenchDiagnostic::RxOnly
        | BenchDiagnostic::ListenerOnly
        | BenchDiagnostic::ZcSendOnly
        | BenchDiagnostic::ZcRxOnly => {
            send_payload_contract_path(transports, path, case, id.clone(), contract).await;
            let ack = receive_payload_contract_ack(
                transports,
                path,
                case,
                &id,
                contract,
                transported_payload_len,
                timeout,
            )
            .await;
            black_box(ack.transported_payload_len);
        }
        BenchDiagnostic::ZcInitOnly | BenchDiagnostic::ZcCopyLedger => {
            black_box(payload_contract::stable_payload_len(contract));
            black_box(contract.name());
        }
        BenchDiagnostic::ZcValidationOnly | BenchDiagnostic::ZcLoanProvenanceCheck => {
            send_payload_contract_path(transports, path, case, id.clone(), contract).await;
            let frame = tokio::time::timeout(
                timeout,
                transports.zero_copy.receive_zero_copy(&case.source, None),
            )
            .await
            .expect("timed out waiting for LoLa diagnostic zero-copy receive")
            .expect("LoLa diagnostic zero-copy receive should succeed");
            if diagnostic == BenchDiagnostic::ZcLoanProvenanceCheck {
                black_box(
                    frame
                        .payload_loan_provenance()
                        .expect("stable payload should be loan-backed"),
                );
            } else {
                validate_stable_payload_for_case(&frame, contract);
            }
        }
        BenchDiagnostic::ZcFilterOnly => {
            let absent = UUri::try_from_parts("*", u32::MAX, u8::MAX, u16::MAX)
                .expect("valid LoLa wildcard source filter");
            let result = tokio::time::timeout(
                Duration::from_millis(1),
                transports.zero_copy.receive_zero_copy(&absent, None),
            )
            .await;
            black_box(result.is_err());
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn prebuild_payload_contract_path(
    path: PayloadContractPath,
    contract: &PayloadContractCase,
) -> usize {
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => {
            payload_contract::protobuf_encoded_bytes_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                .expect("protobuf benchmark payload should serialize")
                .len()
        }
        PayloadContractPath::StableZcNoZero => payload_contract::stable_payload_len(contract),
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => {
            payload_contract::stable_owned_fixture_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                .expect("stable owned fixture should initialize")
                .bytes
                .len()
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn diagnostic_payload_encoding(
    path: PayloadContractPath,
    contract: &PayloadContractCase,
) -> Option<PayloadEncoding> {
    let _ = contract;
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => Some(ProtobufPayload::encoding()),
        PayloadContractPath::StableZcNoZero => None,
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => Some(
            payload_contract::stable_owned_fixture_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                .expect("stable owned fixture should initialize")
                .encoding,
        ),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn send_payload_contract_path(
    transports: &BenchTransports,
    path: PayloadContractPath,
    case: &BenchCase,
    id: UUID,
    contract: &PayloadContractCase,
) {
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => {
            let payload =
                payload_contract::protobuf_encoded_bytes_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                    .expect("protobuf benchmark payload should serialize");
            let metadata = case.metadata(id, Some(ProtobufPayload::encoding()));
            let frame = UOwnedFrame::with_payload(metadata, payload)
                .expect("valid protobuf owned benchmark frame");
            transports
                .protobuf_owned
                .send_owned(frame)
                .await
                .expect("LoLa payload-contract protobuf send should succeed");
        }
        PayloadContractPath::StableZcNoZero => {
            let metadata = case.metadata(id, None);
            send_stable_payload_contract(transports, metadata, contract).await;
        }
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => {
            let fixture =
                payload_contract::stable_owned_fixture_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                    .expect("stable owned fixture should initialize");
            let metadata = case.metadata(id, Some(fixture.encoding));
            let frame = UOwnedFrame::with_payload(metadata, fixture.bytes)
                .expect("valid stable owned benchmark frame");
            transports
                .stable_owned
                .send_owned(frame)
                .await
                .expect("LoLa payload-contract stable owned bytes send should succeed");
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn send_stable_payload_contract(
    transports: &BenchTransports,
    metadata: UFrameMetadata,
    contract: &PayloadContractCase,
) {
    match contract.kind() {
        PayloadContractCaseKind::CanClassicMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<CanClassicFrameV1>(metadata, |payload| {
                    payload_contract::init_can_classic_max(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::CanFdMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<CanFdFrameV1>(metadata, |payload| {
                    payload_contract::init_can_fd_max(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::SomeIpSingleMtu => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<SomeIpSignalBatchMtuV1>(metadata, |payload| {
                    payload_contract::init_someip_single_mtu(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::Streamer4k => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<StreamChunk4kV1>(metadata, |payload| {
                    payload_contract::init_streamer_4k(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::RadarArs548DetectionList => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<RadarDetectionListArs548V1>(metadata, |payload| {
                    payload_contract::init_radar_ars548_detection_list(
                        payload,
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::Streamer64k => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<StreamChunk64kV1>(metadata, |payload| {
                    payload_contract::init_streamer_64k(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<LidarPointCloudHesaiAt128V1>(metadata, |payload| {
                    payload_contract::init_lidar_hesai_at128_point_cloud(
                        payload,
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<CameraBayerRggb12pFrame8mpV1>(
                    metadata,
                    |payload| {
                        payload_contract::init_camera_8mp_bayer_rggb12p(
                            payload,
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                    },
                )
                .await
        }
    }
    .expect("LoLa payload-contract stable no-zero send should succeed");
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn receive_payload_contract_ack(
    transports: &BenchTransports,
    path: PayloadContractPath,
    case: &BenchCase,
    expected_id: &UUID,
    contract: &PayloadContractCase,
    expected_transported_payload_len: usize,
    timeout: Duration,
) -> PayloadContractAck {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for matching LoLa payload-contract frame"
        );
        let result = match path {
            #[cfg(feature = "benchmark-owned")]
            PayloadContractPath::ProtobufOwned => tokio::time::timeout(
                remaining,
                transports.protobuf_owned.receive_owned(&case.source, None),
            )
            .await
            .expect("timed out waiting for LoLa payload-contract owned receive")
            .map(|frame| protobuf_payload_contract_ack(frame, contract)),
            PayloadContractPath::StableZcNoZero => tokio::time::timeout(
                remaining,
                transports.zero_copy.receive_zero_copy(&case.source, None),
            )
            .await
            .expect("timed out waiting for LoLa payload-contract zero-copy receive")
            .map(|frame| stable_payload_contract_ack(&frame, contract)),
            #[cfg(feature = "benchmark-owned")]
            PayloadContractPath::StableOwnedBytes => tokio::time::timeout(
                remaining,
                transports.stable_owned.receive_owned(&case.source, None),
            )
            .await
            .expect("timed out waiting for LoLa payload-contract stable owned receive")
            .map(|frame| stable_owned_payload_contract_ack(frame, contract)),
        };
        match result {
            Ok(ack) if &ack.id == expected_id => {
                assert_eq!(ack.message_type, UMessageType::Publish);
                assert_eq!(ack.case_id, contract.case_id());
                assert_eq!(ack.sequence, PAYLOAD_CONTRACT_SEQUENCE);
                assert_eq!(
                    ack.semantic_reference_len,
                    contract.semantic_reference_len()
                );
                assert_eq!(
                    ack.transported_payload_len,
                    expected_transported_payload_len
                );
                return ack;
            }
            Ok(_) => continue,
            Err(status) if status.get_code() == UCode::NotFound => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(status) if status.get_code() == UCode::InvalidArgument => {
                panic!("invalid LoLa payload-contract sample before measurement: {status:?}");
            }
            Err(status) => panic!("unexpected LoLa payload-contract receive error: {status:?}"),
        }
    }
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn protobuf_payload_contract_ack(
    frame: UOwnedFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    let transported_payload_len = frame.payload_bytes().len();
    let id = frame.metadata().attributes().id().clone();
    let message_type = frame.metadata().attributes().type_();
    payload_contract::validate_protobuf_bytes(
        contract,
        PAYLOAD_CONTRACT_SEQUENCE,
        frame.payload_bytes(),
    )
    .expect("protobuf payload-contract frame should validate");
    PayloadContractAck {
        id,
        message_type,
        case_id: contract.case_id(),
        sequence: PAYLOAD_CONTRACT_SEQUENCE,
        semantic_reference_len: contract.semantic_reference_len(),
        transported_payload_len,
    }
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn stable_owned_payload_contract_ack(
    frame: UOwnedFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    payload_contract::validate_stable_owned_bytes(
        contract,
        PAYLOAD_CONTRACT_SEQUENCE,
        frame.metadata().payload_encoding(),
        frame.payload_bytes(),
    )
    .expect("stable owned payload-contract frame should validate");
    PayloadContractAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().type_(),
        case_id: contract.case_id(),
        sequence: PAYLOAD_CONTRACT_SEQUENCE,
        semantic_reference_len: contract.semantic_reference_len(),
        transported_payload_len: frame.payload_bytes().len(),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn stable_payload_contract_ack(
    frame: &impl ULoanedContiguousZeroCopyRxFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    black_box(
        frame
            .payload_loan_provenance()
            .expect("stable payload should be loan-backed"),
    );
    validate_stable_payload_for_case(frame, contract);
    PayloadContractAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().type_(),
        case_id: contract.case_id(),
        sequence: PAYLOAD_CONTRACT_SEQUENCE,
        semantic_reference_len: contract.semantic_reference_len(),
        transported_payload_len: frame.payload_len(),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn validate_stable_payload_for_case(
    frame: &impl ULoanedContiguousZeroCopyRxFrame,
    contract: &PayloadContractCase,
) {
    match contract.kind() {
        PayloadContractCaseKind::CanClassicMax => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<CanClassicFrameV1>()
                .expect("CAN Classic stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::CanFdMax => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<CanFdFrameV1>()
                .expect("CAN FD stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::SomeIpSingleMtu => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<SomeIpSignalBatchMtuV1>()
                .expect("SOME/IP stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::Streamer4k => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<StreamChunk4kV1>()
                .expect("stream 4K stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::RadarArs548DetectionList => {
            payload_contract::validate_stable_payload(
                contract,
                PAYLOAD_CONTRACT_SEQUENCE,
                frame
                    .borrow_stable_payload::<RadarDetectionListArs548V1>()
                    .expect("radar stable payload-contract frame should borrow"),
            )
        }
        PayloadContractCaseKind::Streamer64k => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<StreamChunk64kV1>()
                .expect("stream 64K stable payload-contract frame should borrow"),
        ),
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            payload_contract::validate_stable_payload(
                contract,
                PAYLOAD_CONTRACT_SEQUENCE,
                frame
                    .borrow_stable_payload::<LidarPointCloudHesaiAt128V1>()
                    .expect("LiDAR stable payload-contract frame should borrow"),
            )
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            payload_contract::validate_stable_payload(
                contract,
                PAYLOAD_CONTRACT_SEQUENCE,
                frame
                    .borrow_stable_payload::<CameraBayerRggb12pFrame8mpV1>()
                    .expect("camera stable payload-contract frame should borrow"),
            )
        }
    }
    .expect("stable payload-contract frame should validate");
}

#[cfg(feature = "payload-contract-benchmarks")]
fn payload_contract_transported_len(
    path: PayloadContractPath,
    contract: &PayloadContractCase,
) -> usize {
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => {
            payload_contract::protobuf_encoded_len(contract, PAYLOAD_CONTRACT_SEQUENCE)
        }
        PayloadContractPath::StableZcNoZero => payload_contract::stable_payload_len(contract),
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => payload_contract::stable_payload_len(contract),
    }
}

#[derive(Clone)]
struct BenchConfig {
    profile: BenchProfile,
    transport: LolaTransportConfig,
}

impl BenchConfig {
    fn for_profile(profile: BenchProfile) -> Self {
        let defaults = profile_defaults(profile);
        let config_path = std::env::var("LOLA_BENCH_MW_COM_CONFIG")
            .unwrap_or_else(|_| defaults.config_path.to_string());
        let transport = LolaTransportConfig {
            local_authority: std::env::var("LOLA_BENCH_AUTHORITY")
                .unwrap_or_else(|_| "vehicle".to_string()),
            instance_specifier: std::env::var("LOLA_BENCH_INSTANCE_SPECIFIER")
                .unwrap_or_else(|_| "uprotocol/transport/benchmark".to_string()),
            service_type: std::env::var("LOLA_BENCH_SERVICE_TYPE")
                .unwrap_or_else(|_| "/uprotocol/TransportBenchmark".to_string()),
            event_name: std::env::var("LOLA_BENCH_EVENT_NAME")
                .unwrap_or_else(|_| "benchmark_frame".to_string()),
            sample_size: env_usize("LOLA_BENCH_SAMPLE_SIZE", defaults.sample_size),
            sample_alignment: env_usize("LOLA_BENCH_SAMPLE_ALIGNMENT", 8),
            max_samples: env_usize("LOLA_BENCH_MAX_SAMPLES", defaults.max_samples),
            pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
            pull_mismatch_queue_full_policy:
                LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
            mw_com_config_path: Some(config_path),
        };
        Self { profile, transport }
    }
}

struct ProfileDefaults {
    config_path: &'static str,
    sample_size: usize,
    max_samples: usize,
}

fn profile_defaults(profile: BenchProfile) -> ProfileDefaults {
    match profile {
        BenchProfile::Core => ProfileDefaults {
            config_path: concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/benches/fixtures/mw_com_config_benchmark.json"
            ),
            sample_size: CORE_SAMPLE_SIZE,
            max_samples: CORE_MAX_SAMPLES,
        },
        BenchProfile::Camera => ProfileDefaults {
            config_path: concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/benches/fixtures/mw_com_config_benchmark_large.json"
            ),
            sample_size: CAMERA_SAMPLE_SIZE,
            max_samples: CAMERA_MAX_SAMPLES,
        },
        BenchProfile::All => panic!("profile defaults require a concrete LoLa benchmark profile"),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn preflight(config: &BenchConfig) -> BenchTransports {
    reject_ambiguous_lola_env();
    validate_benchmark_config(config);
    BenchTransports::build(config.transport.clone())
}

fn reject_ambiguous_lola_env() {
    for name in [
        "LOLA_NATIVE_TEST_CONFIG",
        "LOLA_NATIVE_TEST_AUTHORITY",
        "LOLA_NATIVE_TEST_INSTANCE_SPECIFIER",
        "LOLA_NATIVE_TEST_SERVICE_TYPE",
        "LOLA_NATIVE_TEST_EVENT_NAME",
        "LOLA_NATIVE_TEST_SAMPLE_SIZE",
        "LOLA_NATIVE_TEST_SAMPLE_ALIGNMENT",
        "LOLA_NATIVE_TEST_MAX_SAMPLES",
    ] {
        assert!(
            std::env::var_os(name).is_none(),
            "LoLa transport benchmarks use LOLA_BENCH_* only; unset {name}"
        );
    }
}

fn validate_benchmark_config(config: &BenchConfig) {
    let defaults = profile_defaults(config.profile);
    let path = config
        .transport
        .mw_com_config_path
        .as_ref()
        .expect("LoLa benchmark fixture path should be configured");
    assert!(
        std::path::Path::new(path).exists(),
        "LoLa benchmark fixture does not exist: {path}"
    );
    assert!(config.transport.sample_size >= defaults.sample_size);
    assert_eq!(config.transport.sample_alignment, 8);
    assert!(config.transport.max_samples >= defaults.max_samples);
}

fn bench_transport(c: &mut Criterion) {
    let suite = BenchSuite::from_env();
    let profile = BenchProfile::from_lola_env();
    let runtime = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("tokio runtime");
    if suite.includes_payload_contract() {
        bench_payload_contract(c, &runtime, profile);
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract(c: &mut Criterion, runtime: &Runtime, profile: BenchProfile) {
    if profile.includes_core() {
        let config = BenchConfig::for_profile(BenchProfile::Core);
        let transports = preflight(&config);
        bench_payload_contract_matrix(
            c,
            runtime,
            &transports,
            &config.transport.local_authority,
            "transport_payload_contract_core",
            payload_contract::core_cases(),
            BENCH_TIMEOUT,
        );
    }
    if profile.includes_camera() {
        let config = BenchConfig::for_profile(BenchProfile::Camera);
        let transports = preflight(&config);
        bench_payload_contract_matrix(
            c,
            runtime,
            &transports,
            &config.transport.local_authority,
            "transport_payload_contract_large_sensor",
            payload_contract::large_sensor_cases(),
            LARGE_SENSOR_BENCH_TIMEOUT,
        );
    }
}

#[cfg(not(feature = "payload-contract-benchmarks"))]
fn bench_payload_contract(_c: &mut Criterion, _runtime: &Runtime, _profile: BenchProfile) {
    panic!("TRANSPORT_BENCH_SUITE=payload-contract requires feature payload-contract-benchmarks");
}

fn next_resource_id() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};

    static RESOURCE_ID: AtomicU16 = AtomicU16::new(0x9000);
    RESOURCE_ID.fetch_add(1, Ordering::Relaxed)
}

criterion_group!(transport_criterion, bench_transport);
criterion_main!(transport_criterion);

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

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::{Builder, Runtime};
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::bench_fixtures::payload_contract::{self, *};
#[cfg(feature = "benchmark-owned")]
use up_rust::ProtobufWire;
use up_rust::{
    NativePrefixProtobufMetadataCodec, PayloadEncoding, StableContainerWireFormat, UCode,
    UFrameMetadata, ULoanedContiguousZeroCopyRxFrame, UMessageBuilder, UMessageType, UUri, UWire,
    UWireMetadataCodec, UWireTransport, UZeroCopyTransport, UZeroCopyUninitTransportExt, UUID,
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
    ZcSourceExactMatchOnly,
    ZcSourceNonmatchOnly,
    ZcWildcardSourceMatchOnly,
    ZcSinkNonmatchOnly,
    ZcMismatchQueueEnqueueOnly,
    ZcMismatchQueueDropOnly,
    ZcMismatchQueueRejectOnly,
    ZcRxLolaSampleDeliveryOnly,
    ZcRxUlolHeaderParseOnly,
    ZcRxMetadataCopyOutOnly,
    ZcRxSelectedWireDecodeOnly,
    ZcRxAdapterFilterDropOnly,
    ZcRxMismatchQueueDropOnly,
    ZcRxListenerDispatchOnly,
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
            "zc-source-exact-match-only" => Self::ZcSourceExactMatchOnly,
            "zc-source-nonmatch-only" => Self::ZcSourceNonmatchOnly,
            "zc-wildcard-source-match-only" => Self::ZcWildcardSourceMatchOnly,
            "zc-sink-nonmatch-only" => Self::ZcSinkNonmatchOnly,
            "zc-mismatch-queue-enqueue-only" => Self::ZcMismatchQueueEnqueueOnly,
            "zc-mismatch-queue-drop-only" => Self::ZcMismatchQueueDropOnly,
            "zc-mismatch-queue-reject-only" => Self::ZcMismatchQueueRejectOnly,
            "zc-rx-lola-sample-delivery-only" => Self::ZcRxLolaSampleDeliveryOnly,
            "zc-rx-ulol-header-parse-only" => Self::ZcRxUlolHeaderParseOnly,
            "zc-rx-metadata-copy-out-only" => Self::ZcRxMetadataCopyOutOnly,
            "zc-rx-selected-wire-decode-only" => Self::ZcRxSelectedWireDecodeOnly,
            "zc-rx-adapter-filter-drop-only" => Self::ZcRxAdapterFilterDropOnly,
            "zc-rx-mismatch-queue-drop-only" => Self::ZcRxMismatchQueueDropOnly,
            "zc-rx-listener-dispatch-only" => Self::ZcRxListenerDispatchOnly,
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
            Self::ZcSourceExactMatchOnly => "zc-source-exact-match-only",
            Self::ZcSourceNonmatchOnly => "zc-source-nonmatch-only",
            Self::ZcWildcardSourceMatchOnly => "zc-wildcard-source-match-only",
            Self::ZcSinkNonmatchOnly => "zc-sink-nonmatch-only",
            Self::ZcMismatchQueueEnqueueOnly => "zc-mismatch-queue-enqueue-only",
            Self::ZcMismatchQueueDropOnly => "zc-mismatch-queue-drop-only",
            Self::ZcMismatchQueueRejectOnly => "zc-mismatch-queue-reject-only",
            Self::ZcRxLolaSampleDeliveryOnly => "zc-rx-lola-sample-delivery-only",
            Self::ZcRxUlolHeaderParseOnly => "zc-rx-ulol-header-parse-only",
            Self::ZcRxMetadataCopyOutOnly => "zc-rx-metadata-copy-out-only",
            Self::ZcRxSelectedWireDecodeOnly => "zc-rx-selected-wire-decode-only",
            Self::ZcRxAdapterFilterDropOnly => "zc-rx-adapter-filter-drop-only",
            Self::ZcRxMismatchQueueDropOnly => "zc-rx-mismatch-queue-drop-only",
            Self::ZcRxListenerDispatchOnly => "zc-rx-listener-dispatch-only",
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
            | Self::ZcSourceExactMatchOnly
            | Self::ZcSourceNonmatchOnly
            | Self::ZcWildcardSourceMatchOnly
            | Self::ZcSinkNonmatchOnly
            | Self::ZcMismatchQueueEnqueueOnly
            | Self::ZcMismatchQueueDropOnly
            | Self::ZcMismatchQueueRejectOnly
            | Self::ZcRxLolaSampleDeliveryOnly
            | Self::ZcRxUlolHeaderParseOnly
            | Self::ZcRxMetadataCopyOutOnly
            | Self::ZcRxSelectedWireDecodeOnly
            | Self::ZcRxAdapterFilterDropOnly
            | Self::ZcRxMismatchQueueDropOnly
            | Self::ZcRxListenerDispatchOnly
            | Self::ZcCopyLedger
            | Self::ZcLoanProvenanceCheck => path == PayloadContractPath::StableZcNoZero,
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Default)]
struct P51LolaSample<'a> {
    selector: &'a str,
    fixture: &'a str,
    scenario: &'a str,
    backend: &'a str,
    wire: &'a str,
    path: &'a str,
    publish_attempts: usize,
    delivered_samples: usize,
    exact_source_matches: usize,
    wildcard_source_matches: usize,
    source_nonmatch_count: usize,
    sink_matches: usize,
    sink_nonmatch_count: usize,
    adapter_dropped_count: usize,
    listener_dispatched_count: usize,
    mismatch_queue_depth: usize,
    mismatch_queued_count: usize,
    mismatch_dropped_count: usize,
    mismatch_rejected_count: usize,
    ulol_header_bytes: usize,
    metadata_prefix_bytes: usize,
    alignment_padding_bytes: usize,
    payload_offset_bytes: usize,
    payload_len_bytes: usize,
    sample_size_bytes: usize,
    sample_alignment_bytes: usize,
    metadata_encode_bytes: usize,
    metadata_encode_allocations: usize,
    metadata_encode_allocation_bytes: usize,
    ulol_prefix_write_bytes: usize,
    metadata_copy_in_bytes: usize,
    rx_header_parse_bytes: usize,
    metadata_copy_out_bytes: usize,
    metadata_copy_out_allocations: usize,
    metadata_copy_out_allocation_bytes: usize,
    selected_wire_decode_bytes: usize,
    selected_wire_decode_allocations: usize,
    selected_wire_decode_allocation_bytes: usize,
    owned_payload_copy_bytes: usize,
    zero_copy_payload_copy_bytes: usize,
    source_drop_allocations: usize,
    source_drop_bytes: usize,
    sink_drop_allocations: usize,
    sink_drop_bytes: usize,
    mismatch_queue_allocations: usize,
    mismatch_queue_bytes: usize,
    listener_dispatch_allocations: usize,
    listener_dispatch_bytes: usize,
    stable_payload_loaned: bool,
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
    config: &LolaTransportConfig,
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
            emit_p51_lola_sample(planned_p51_lola_sample(
                path,
                diagnostic,
                contract,
                &case,
                config,
                transported_payload_len,
            ));
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
fn planned_p51_lola_sample<'a>(
    path: PayloadContractPath,
    diagnostic: BenchDiagnostic,
    contract: &'a PayloadContractCase,
    case: &BenchCase,
    config: &LolaTransportConfig,
    transported_payload_len: usize,
) -> P51LolaSample<'a> {
    const ULOL_HEADER_LEN: usize = 20;

    let metadata = case.metadata(UUID::build(), diagnostic_payload_encoding(path, contract));
    let metadata_prefix_bytes = encoded_metadata_len(path, &metadata);
    let unaligned_payload_offset = ULOL_HEADER_LEN + metadata_prefix_bytes;
    let payload_offset_bytes = align_up(unaligned_payload_offset, config.sample_alignment);
    let alignment_padding_bytes = payload_offset_bytes.saturating_sub(unaligned_payload_offset);
    let mut sample = P51LolaSample {
        selector: diagnostic.label(),
        fixture: contract.name(),
        scenario: p51_lola_scenario(diagnostic),
        backend: if cfg!(feature = "test-stub") {
            "test-stub"
        } else {
            "native"
        },
        wire: p51_lola_wire(path),
        path: if path.is_owned() {
            "owned"
        } else {
            "zero-copy"
        },
        ulol_header_bytes: ULOL_HEADER_LEN,
        metadata_prefix_bytes,
        alignment_padding_bytes,
        payload_offset_bytes,
        payload_len_bytes: transported_payload_len,
        sample_size_bytes: config.sample_size,
        sample_alignment_bytes: config.sample_alignment,
        stable_payload_loaned: path == PayloadContractPath::StableZcNoZero,
        ..P51LolaSample::default()
    };

    match diagnostic {
        BenchDiagnostic::MetadataOnly => {
            sample.metadata_encode_bytes = metadata_prefix_bytes;
            sample.metadata_encode_allocations = 1;
            sample.metadata_encode_allocation_bytes = metadata_prefix_bytes;
        }
        BenchDiagnostic::UlolLayout | BenchDiagnostic::NativeFixtureFit => {}
        BenchDiagnostic::ZcCopyLedger => {
            sample.metadata_encode_bytes = metadata_prefix_bytes;
            sample.metadata_encode_allocations = 1;
            sample.metadata_encode_allocation_bytes = metadata_prefix_bytes;
            sample.ulol_prefix_write_bytes = payload_offset_bytes;
            sample.metadata_copy_in_bytes = metadata_prefix_bytes;
            sample.rx_header_parse_bytes = ULOL_HEADER_LEN;
            sample.metadata_copy_out_bytes = metadata_prefix_bytes;
            sample.metadata_copy_out_allocations = 1;
            sample.metadata_copy_out_allocation_bytes = metadata_prefix_bytes;
        }
        BenchDiagnostic::ZcSourceExactMatchOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.exact_source_matches = 1;
        }
        BenchDiagnostic::ZcSourceNonmatchOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.source_nonmatch_count = 1;
            sample.adapter_dropped_count = 1;
        }
        BenchDiagnostic::ZcWildcardSourceMatchOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.wildcard_source_matches = 1;
        }
        BenchDiagnostic::ZcSinkNonmatchOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.sink_nonmatch_count = 1;
            sample.adapter_dropped_count = 1;
        }
        BenchDiagnostic::ZcMismatchQueueEnqueueOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.source_nonmatch_count = 1;
            sample.mismatch_queue_depth = 1;
            sample.mismatch_queued_count = 1;
        }
        BenchDiagnostic::ZcMismatchQueueDropOnly | BenchDiagnostic::ZcRxMismatchQueueDropOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.source_nonmatch_count = 1;
            sample.mismatch_dropped_count = 1;
        }
        BenchDiagnostic::ZcMismatchQueueRejectOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.source_nonmatch_count = 1;
            sample.mismatch_rejected_count = 1;
        }
        BenchDiagnostic::ZcRxLolaSampleDeliveryOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
        }
        BenchDiagnostic::ZcRxUlolHeaderParseOnly => {
            sample.rx_header_parse_bytes = ULOL_HEADER_LEN;
        }
        BenchDiagnostic::ZcRxMetadataCopyOutOnly => {
            sample.rx_header_parse_bytes = ULOL_HEADER_LEN;
            sample.metadata_copy_out_bytes = metadata_prefix_bytes;
            sample.metadata_copy_out_allocations = 1;
            sample.metadata_copy_out_allocation_bytes = metadata_prefix_bytes;
        }
        BenchDiagnostic::ZcRxSelectedWireDecodeOnly => {
            sample.selected_wire_decode_bytes = metadata_prefix_bytes;
            sample.selected_wire_decode_allocations = 1;
            sample.selected_wire_decode_allocation_bytes = metadata_prefix_bytes;
        }
        BenchDiagnostic::ZcRxAdapterFilterDropOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.adapter_dropped_count = 1;
        }
        BenchDiagnostic::ZcRxListenerDispatchOnly => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.listener_dispatched_count = 1;
        }
        BenchDiagnostic::ZcRxOnly
        | BenchDiagnostic::FullLoop
        | BenchDiagnostic::TxOnly
        | BenchDiagnostic::RxOnly
        | BenchDiagnostic::ListenerOnly
        | BenchDiagnostic::ZcSendOnly
        | BenchDiagnostic::ZcValidationOnly
        | BenchDiagnostic::ZcLoanProvenanceCheck => {
            sample.publish_attempts = 1;
            sample.delivered_samples = 1;
            sample.exact_source_matches = 1;
        }
        BenchDiagnostic::PrebuiltPayload
        | BenchDiagnostic::CopyLedger
        | BenchDiagnostic::ZcInitOnly
        | BenchDiagnostic::ZcFilterOnly => {}
    }
    sample
}

#[cfg(feature = "payload-contract-benchmarks")]
fn emit_p51_lola_sample(sample: P51LolaSample<'_>) {
    static EMITTED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

    let line = format!(
        "P51_LOLA_SAMPLE selector={} fixture={} scenario={} backend={} wire={} path={} publish_attempts={} delivered_samples={} exact_source_matches={} wildcard_source_matches={} source_nonmatch_count={} sink_matches={} sink_nonmatch_count={} adapter_dropped_count={} listener_dispatched_count={} mismatch_queue_depth={} mismatch_queued_count={} mismatch_dropped_count={} mismatch_rejected_count={} ulol_header_bytes={} metadata_prefix_bytes={} alignment_padding_bytes={} payload_offset_bytes={} payload_len_bytes={} sample_size_bytes={} sample_alignment_bytes={} metadata_encode_bytes={} metadata_encode_allocations={} metadata_encode_allocation_bytes={} metadata_encode_allocated_bytes={} ulol_prefix_write_bytes={} metadata_copy_in_bytes={} rx_header_parse_bytes={} metadata_copy_out_bytes={} metadata_copy_out_allocations={} metadata_copy_out_allocation_bytes={} metadata_copy_out_allocated_bytes={} selected_wire_decode_bytes={} selected_wire_decode_allocations={} selected_wire_decode_allocation_bytes={} metadata_decode_bytes={} metadata_decode_allocations={} metadata_decode_allocated_bytes={} owned_payload_copy_bytes={} zero_copy_payload_copy_bytes={} source_drop_allocations={} source_drop_bytes={} sink_drop_allocations={} sink_drop_bytes={} mismatch_queue_allocations={} mismatch_queue_bytes={} listener_dispatch_allocations={} listener_dispatch_bytes={} drop_path_allocations={} drop_path_allocated_bytes={} stable_payload_loaned={} notes=public-api-limited-test-stub",
        sample.selector,
        sample.fixture,
        sample.scenario,
        sample.backend,
        sample.wire,
        sample.path,
        sample.publish_attempts,
        sample.delivered_samples,
        sample.exact_source_matches,
        sample.wildcard_source_matches,
        sample.source_nonmatch_count,
        sample.sink_matches,
        sample.sink_nonmatch_count,
        sample.adapter_dropped_count,
        sample.listener_dispatched_count,
        sample.mismatch_queue_depth,
        sample.mismatch_queued_count,
        sample.mismatch_dropped_count,
        sample.mismatch_rejected_count,
        sample.ulol_header_bytes,
        sample.metadata_prefix_bytes,
        sample.alignment_padding_bytes,
        sample.payload_offset_bytes,
        sample.payload_len_bytes,
        sample.sample_size_bytes,
        sample.sample_alignment_bytes,
        sample.metadata_encode_bytes,
        sample.metadata_encode_allocations,
        sample.metadata_encode_allocation_bytes,
        sample.metadata_encode_allocation_bytes,
        sample.ulol_prefix_write_bytes,
        sample.metadata_copy_in_bytes,
        sample.rx_header_parse_bytes,
        sample.metadata_copy_out_bytes,
        sample.metadata_copy_out_allocations,
        sample.metadata_copy_out_allocation_bytes,
        sample.metadata_copy_out_allocation_bytes,
        sample.selected_wire_decode_bytes,
        sample.selected_wire_decode_allocations,
        sample.selected_wire_decode_allocation_bytes,
        sample.selected_wire_decode_bytes,
        sample.selected_wire_decode_allocations,
        sample.selected_wire_decode_allocation_bytes,
        sample.owned_payload_copy_bytes,
        sample.zero_copy_payload_copy_bytes,
        sample.source_drop_allocations,
        sample.source_drop_bytes,
        sample.sink_drop_allocations,
        sample.sink_drop_bytes,
        sample.mismatch_queue_allocations,
        sample.mismatch_queue_bytes,
        sample.listener_dispatch_allocations,
        sample.listener_dispatch_bytes,
        sample.source_drop_allocations
            + sample.sink_drop_allocations
            + sample.mismatch_queue_allocations
            + sample.listener_dispatch_allocations,
        sample.source_drop_bytes
            + sample.sink_drop_bytes
            + sample.mismatch_queue_bytes
            + sample.listener_dispatch_bytes,
        sample.stable_payload_loaned
    );
    if EMITTED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .expect("P51 LoLa sample emission lock should not be poisoned")
        .insert(line.clone())
    {
        eprintln!("{line}");
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn encoded_metadata_len(path: PayloadContractPath, metadata: &UFrameMetadata) -> usize {
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => NativePrefixProtobufMetadataCodec
            .encode_frame_metadata(ProtobufWire::metadata_context(), metadata)
            .expect("protobuf selected-wire metadata should encode")
            .len(),
        PayloadContractPath::StableZcNoZero => NativePrefixProtobufMetadataCodec
            .encode_frame_metadata(StableContainerWireFormat::metadata_context(), metadata)
            .expect("stable selected-wire metadata should encode")
            .len(),
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => NativePrefixProtobufMetadataCodec
            .encode_frame_metadata(StableContainerWireFormat::metadata_context(), metadata)
            .expect("stable selected-wire metadata should encode")
            .len(),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn p51_lola_wire(path: PayloadContractPath) -> &'static str {
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => "ProtobufWire",
        PayloadContractPath::StableZcNoZero => "StableContainerWireFormat",
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => "StableContainerWireFormat",
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn p51_lola_scenario(diagnostic: BenchDiagnostic) -> &'static str {
    match diagnostic {
        BenchDiagnostic::ZcSourceExactMatchOnly => "source-exact-match",
        BenchDiagnostic::ZcSourceNonmatchOnly => "source-nonmatch-local-drop",
        BenchDiagnostic::ZcWildcardSourceMatchOnly => "wildcard-source-match",
        BenchDiagnostic::ZcSinkNonmatchOnly => "sink-nonmatch-local-drop",
        BenchDiagnostic::ZcMismatchQueueEnqueueOnly => "mismatch-queue-enqueue",
        BenchDiagnostic::ZcMismatchQueueDropOnly | BenchDiagnostic::ZcRxMismatchQueueDropOnly => {
            "mismatch-queue-drop"
        }
        BenchDiagnostic::ZcMismatchQueueRejectOnly => "mismatch-queue-reject",
        BenchDiagnostic::ZcRxLolaSampleDeliveryOnly => "lola-sample-delivery",
        BenchDiagnostic::ZcRxUlolHeaderParseOnly => "ulol-header-parse",
        BenchDiagnostic::ZcRxMetadataCopyOutOnly => "metadata-copy-out",
        BenchDiagnostic::ZcRxSelectedWireDecodeOnly => "selected-wire-decode",
        BenchDiagnostic::ZcRxAdapterFilterDropOnly => "adapter-filter-drop",
        BenchDiagnostic::ZcRxListenerDispatchOnly => "listener-dispatch",
        other => other.label(),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn align_up(value: usize, alignment: usize) -> usize {
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + (alignment - remainder)
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
        | BenchDiagnostic::ZcRxOnly
        | BenchDiagnostic::ZcSourceExactMatchOnly
        | BenchDiagnostic::ZcWildcardSourceMatchOnly
        | BenchDiagnostic::ZcRxLolaSampleDeliveryOnly
        | BenchDiagnostic::ZcRxUlolHeaderParseOnly
        | BenchDiagnostic::ZcRxMetadataCopyOutOnly
        | BenchDiagnostic::ZcRxSelectedWireDecodeOnly
        | BenchDiagnostic::ZcRxListenerDispatchOnly => {
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
        BenchDiagnostic::ZcSourceNonmatchOnly
        | BenchDiagnostic::ZcSinkNonmatchOnly
        | BenchDiagnostic::ZcMismatchQueueEnqueueOnly
        | BenchDiagnostic::ZcMismatchQueueDropOnly
        | BenchDiagnostic::ZcMismatchQueueRejectOnly
        | BenchDiagnostic::ZcRxAdapterFilterDropOnly
        | BenchDiagnostic::ZcRxMismatchQueueDropOnly => {
            let metadata = case.metadata(id, diagnostic_payload_encoding(path, contract));
            black_box(metadata);
            black_box(contract.name());
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
            &config.transport,
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
            &config.transport,
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

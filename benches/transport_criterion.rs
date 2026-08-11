/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#![allow(clippy::missing_panics_doc, clippy::too_many_lines)]

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::{Builder, Runtime};
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::bench_fixtures::payload_contract::{self, *};
use up_rust::selected_wire_user_api::UNativePrefixWireTransport;
use up_rust::{
    PayloadEncoding, UFrameMetadata, UFrameView, UOwnedFrame, UOwnedTransportImpl,
    UProtocolNativeWire, UTxBuffer, UTxLoanSpec, UUninitTxBuffer, UUri, UZeroCopyTransportImpl,
    UZeroCopyUninitTransportImpl,
};
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::{ProtobufWire, StableContainerWireFormat};
use up_transport_lola_rust::{
    LolaOwnedCore, LolaTransportConfig, LolaZeroCopyCore, UTransportLola,
};

const CORE_PAYLOAD_CASES: &[(&str, usize)] = &[
    ("empty_present", 0),
    ("can_classic_max", 8),
    ("can_fd_max", 64),
    ("someip_single_mtu", 1_456),
    ("streamer_4k", 4 * 1_024),
    ("radar_ars548_detection_list", 35_336),
    ("streamer_64k", 64 * 1_024),
];
const LARGE_SENSOR_PAYLOAD_CASES: &[(&str, usize)] =
    &[("camera_8mp_3840x2160_raw12_packed", 12_441_600)];
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_SEQUENCE: u32 = 1;

type RawZeroCopy = UNativePrefixWireTransport<LolaZeroCopyCore, UProtocolNativeWire>;
#[cfg(feature = "payload-contract-benchmarks")]
type StableZeroCopy = UNativePrefixWireTransport<LolaZeroCopyCore, StableContainerWireFormat>;
type RawOwned = UNativePrefixWireTransport<LolaOwnedCore, UProtocolNativeWire>;
#[cfg(feature = "payload-contract-benchmarks")]
type ProtobufOwned = UNativePrefixWireTransport<LolaOwnedCore, ProtobufWire>;
#[cfg(feature = "payload-contract-benchmarks")]
type StableOwned = UNativePrefixWireTransport<LolaOwnedCore, StableContainerWireFormat>;

#[derive(Clone, Copy)]
enum BenchPath {
    Owned,
    ZeroCopyLoanCopy,
    ZeroCopyUninitDirect,
}

impl BenchPath {
    const fn label(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::ZeroCopyLoanCopy => "zero_copy_loan_copy",
            Self::ZeroCopyUninitDirect => "zero_copy_uninit_direct",
        }
    }
}

struct BenchTransports {
    raw_zero_copy: RawZeroCopy,
    #[cfg(feature = "payload-contract-benchmarks")]
    stable_zero_copy: StableZeroCopy,
    raw_owned: RawOwned,
    #[cfg(feature = "payload-contract-benchmarks")]
    protobuf_owned: ProtobufOwned,
    #[cfg(feature = "payload-contract-benchmarks")]
    stable_owned: StableOwned,
}

impl BenchTransports {
    fn build(config: LolaTransportConfig) -> Self {
        let physical = UTransportLola::build(config).expect("LoLa benchmark transport");
        let core = physical.zero_copy_core();
        Self {
            raw_zero_copy: core.clone().with_selected_wire(UProtocolNativeWire),
            #[cfg(feature = "payload-contract-benchmarks")]
            stable_zero_copy: core.clone().with_selected_wire(StableContainerWireFormat),
            raw_owned: LolaOwnedCore::new(core.clone()).with_selected_wire(UProtocolNativeWire),
            #[cfg(feature = "payload-contract-benchmarks")]
            protobuf_owned: LolaOwnedCore::new(core.clone()).with_selected_wire(ProtobufWire),
            #[cfg(feature = "payload-contract-benchmarks")]
            stable_owned: LolaOwnedCore::new(core).with_selected_wire(StableContainerWireFormat),
        }
    }
}

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let profile = std::env::var("LOLA_BENCH_PROFILE").unwrap_or_else(|_| "core".to_string());
    let suite = std::env::var("TRANSPORT_BENCH_SUITE").unwrap_or_else(|_| "raw".to_string());
    let large = profile == "camera";
    assert!(
        profile == "core" || large,
        "LOLA_BENCH_PROFILE must be core or camera"
    );
    assert!(
        matches!(suite.as_str(), "raw" | "payload-contract" | "all"),
        "TRANSPORT_BENCH_SUITE must be raw, payload-contract or all"
    );
    let transports = BenchTransports::build(benchmark_config(large));
    prime(&runtime, &transports);

    if suite != "payload-contract" {
        let cases = if large {
            LARGE_SENSOR_PAYLOAD_CASES
        } else {
            CORE_PAYLOAD_CASES
        };
        bench_raw_matrix(c, &runtime, &transports, cases, large);
        bench_no_payload(c, &runtime, &transports);
    }

    #[cfg(feature = "payload-contract-benchmarks")]
    if suite != "raw" {
        let cases = if large {
            payload_contract::large_sensor_cases()
        } else {
            payload_contract::core_cases()
        };
        bench_payload_contract(c, &runtime, &transports, cases, large);
    }
}

fn benchmark_config(large: bool) -> LolaTransportConfig {
    let fixture = if large {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benches/fixtures/mw_com_config_benchmark_large.json"
        )
    } else {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benches/fixtures/mw_com_config_benchmark.json"
        )
    };
    LolaTransportConfig {
        local_authority: std::env::var("LOLA_BENCH_AUTHORITY")
            .unwrap_or_else(|_| "vehicle".to_string()),
        instance_specifier: std::env::var("LOLA_BENCH_INSTANCE_SPECIFIER")
            .unwrap_or_else(|_| "uprotocol/transport/benchmark".to_string()),
        service_type: std::env::var("LOLA_BENCH_SERVICE_TYPE")
            .unwrap_or_else(|_| "/uprotocol/TransportBenchmark".to_string()),
        event_name: std::env::var("LOLA_BENCH_EVENT_NAME")
            .unwrap_or_else(|_| "benchmark_frame".to_string()),
        sample_size: env_usize(
            "LOLA_BENCH_SAMPLE_SIZE",
            if large {
                16 * 1_024 * 1_024
            } else {
                128 * 1_024
            },
        ),
        sample_alignment: env_usize("LOLA_BENCH_SAMPLE_ALIGNMENT", 8),
        max_samples: env_usize("LOLA_BENCH_MAX_SAMPLES", if large { 16 } else { 128 }),
        pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
        pull_mismatch_queue_full_policy:
            LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
        mw_com_config_path: Some(
            std::env::var("LOLA_BENCH_MW_COM_CONFIG").unwrap_or_else(|_| fixture.to_string()),
        ),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn topic(resource_id: u16) -> UUri {
    UUri::try_from_parts("vehicle", 0x4210, 1, resource_id).expect("benchmark URI")
}

fn raw_metadata(source: UUri) -> UFrameMetadata {
    UFrameMetadata::publish(source)
        .with_payload_encoding(PayloadEncoding::RAW)
        .build()
        .expect("benchmark metadata")
}

fn prime(runtime: &Runtime, transports: &BenchTransports) {
    let source = topic(0x9000);
    runtime.block_on(async {
        match transports
            .raw_zero_copy
            .receive_validated_zero_copy(&source, None)
            .await
        {
            Ok(_) => panic!("unexpected LoLa frame before benchmark"),
            Err(error) if error.code() == up_rust::UCode::NotFound => {}
            Err(error) => panic!("LoLa benchmark preflight failed: {error:?}"),
        }
    });
}

fn bench_raw_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    cases: &[(&str, usize)],
    large: bool,
) {
    let mut group = c.benchmark_group(if large {
        "transport_large_sensor"
    } else {
        "transport_core"
    });
    for (index, (name, len)) in cases.iter().enumerate() {
        let source = topic(0x9000 + u16::try_from(index).expect("benchmark index"));
        let payload = payload_pattern(*len);
        for path in [
            BenchPath::Owned,
            BenchPath::ZeroCopyLoanCopy,
            BenchPath::ZeroCopyUninitDirect,
        ] {
            group.bench_function(
                BenchmarkId::new(path.label(), format!("{name}/{len}")),
                |b| {
                    b.iter(|| {
                        runtime.block_on(raw_round_trip(
                            transports,
                            path,
                            source.clone(),
                            &payload,
                        ));
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_no_payload(c: &mut Criterion, runtime: &Runtime, transports: &BenchTransports) {
    let mut group = c.benchmark_group("transport_no_payload_smoke");
    let source = topic(0x90f0);
    group.bench_function("zero_copy/no_payload", |b| {
        b.iter(|| runtime.block_on(no_payload_round_trip(transports, source.clone())));
    });
    group.finish();
}

async fn raw_round_trip(
    transports: &BenchTransports,
    path: BenchPath,
    source: UUri,
    payload: &[u8],
) {
    match path {
        BenchPath::Owned => {
            let frame = UOwnedFrame::with_payload(raw_metadata(source.clone()), payload.to_vec())
                .expect("owned frame");
            transports
                .raw_owned
                .send_validated_owned(frame)
                .await
                .expect("owned send");
            let frame = receive_owned(&transports.raw_owned, &source).await;
            black_box(frame.payload_bytes());
        }
        BenchPath::ZeroCopyLoanCopy => {
            let mut loan = transports
                .raw_zero_copy
                .loan_validated_tx(
                    UTxLoanSpec::payload(raw_metadata(source.clone()), payload.len(), 1)
                        .expect("loan spec"),
                )
                .await
                .expect("initialized loan");
            loan.payload_mut().copy_from_slice(payload);
            transports
                .raw_zero_copy
                .send_validated_zero_copy(loan)
                .await
                .expect("zero-copy send");
            let frame = receive_zero_copy(&transports.raw_zero_copy, &source).await;
            black_box(frame.try_contiguous_payload());
        }
        BenchPath::ZeroCopyUninitDirect => {
            let mut loan = transports
                .raw_zero_copy
                .loan_validated_uninit_tx(
                    UTxLoanSpec::payload(raw_metadata(source.clone()), payload.len(), 1)
                        .expect("loan spec"),
                )
                .await
                .expect("uninitialized loan");
            for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(payload) {
                slot.write(*byte);
            }
            // SAFETY: every byte in the visible payload was initialized above.
            let loan = unsafe { loan.assume_payload_initialized() };
            transports
                .raw_zero_copy
                .send_validated_zero_copy(loan)
                .await
                .expect("uninitialized send");
            let frame = receive_zero_copy(&transports.raw_zero_copy, &source).await;
            black_box(frame.try_contiguous_payload());
        }
    }
}

async fn no_payload_round_trip(transports: &BenchTransports, source: UUri) {
    let metadata = UFrameMetadata::publish(source.clone())
        .build()
        .expect("no-payload metadata");
    let loan = transports
        .raw_zero_copy
        .loan_validated_tx(UTxLoanSpec::no_payload(metadata).expect("loan spec"))
        .await
        .expect("no-payload loan");
    transports
        .raw_zero_copy
        .send_validated_zero_copy(loan)
        .await
        .expect("no-payload send");
    let frame = receive_zero_copy(&transports.raw_zero_copy, &source).await;
    assert!(!frame.has_payload());
}

async fn receive_zero_copy<T, W>(
    transport: &UNativePrefixWireTransport<T, W>,
    source: &UUri,
) -> <UNativePrefixWireTransport<T, W> as UZeroCopyTransportImpl>::Rx
where
    T: up_rust::UZeroCopyTransportCore,
    W: up_rust::UWire + Send + Sync + 'static,
{
    loop {
        match transport.receive_validated_zero_copy(source, None).await {
            Ok(frame) => return frame,
            Err(error) if error.code() == up_rust::UCode::NotFound => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => panic!("zero-copy receive failed: {error:?}"),
        }
    }
}

async fn receive_owned<T, W>(
    transport: &UNativePrefixWireTransport<T, W>,
    source: &UUri,
) -> UOwnedFrame
where
    T: up_rust::UOwnedTransportCore,
    W: up_rust::UWire + Send + Sync + 'static,
{
    loop {
        match transport.receive_validated_owned(source, None).await {
            Ok(frame) => return frame,
            Err(error) if error.code() == up_rust::UCode::NotFound => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => panic!("owned receive failed: {error:?}"),
        }
    }
}

fn payload_pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    cases: &[PayloadContractCase],
    large: bool,
) {
    let mut group = c.benchmark_group(if large {
        "payload_contract_large_sensor"
    } else {
        "payload_contract_core"
    });
    for case in cases {
        let source = topic(0x9100 + u16::try_from(case.case_id()).expect("case id"));
        group.bench_function(BenchmarkId::new("protobuf_owned_full", case.name()), |b| {
            b.iter(|| {
                runtime.block_on(protobuf_contract_round_trip(
                    transports,
                    source.clone(),
                    case,
                ))
            });
        });
        group.bench_function(
            BenchmarkId::new("stable_zc_nozero_full", case.name()),
            |b| {
                b.iter(|| {
                    runtime.block_on(stable_contract_round_trip(transports, source.clone(), case))
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("stable_owned_bytes_full", case.name()),
            |b| {
                b.iter(|| {
                    runtime.block_on(stable_owned_contract_round_trip(
                        transports,
                        source.clone(),
                        case,
                    ))
                });
            },
        );
    }
    group.finish();
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn protobuf_contract_round_trip(
    transports: &BenchTransports,
    source: UUri,
    case: &PayloadContractCase,
) {
    let payload = payload_contract::protobuf_encoded_bytes_for(case, PAYLOAD_CONTRACT_SEQUENCE)
        .expect("protobuf fixture");
    let metadata = UFrameMetadata::publish(source.clone())
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .expect("protobuf metadata");
    transports
        .protobuf_owned
        .send_validated_owned(UOwnedFrame::with_payload(metadata, payload).expect("owned frame"))
        .await
        .expect("protobuf owned send");
    let frame = receive_owned(&transports.protobuf_owned, &source).await;
    black_box(frame.payload_bytes());
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn stable_owned_contract_round_trip(
    transports: &BenchTransports,
    source: UUri,
    case: &PayloadContractCase,
) {
    let fixture = payload_contract::stable_owned_fixture_for(case, PAYLOAD_CONTRACT_SEQUENCE)
        .expect("stable fixture");
    let metadata = UFrameMetadata::publish(source.clone())
        .with_payload_encoding(fixture.encoding)
        .build()
        .expect("stable metadata");
    transports
        .stable_owned
        .send_validated_owned(
            UOwnedFrame::with_payload(metadata, fixture.bytes).expect("owned frame"),
        )
        .await
        .expect("stable owned send");
    let frame = receive_owned(&transports.stable_owned, &source).await;
    black_box(frame.payload_bytes());
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn stable_contract_round_trip(
    transports: &BenchTransports,
    source: UUri,
    case: &PayloadContractCase,
) {
    match case.kind() {
        PayloadContractCaseKind::CanClassicMax => {
            send_stable::<CanClassicFrameV1, _>(transports, source.clone(), |init| {
                payload_contract::init_can_classic_max(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        PayloadContractCaseKind::CanFdMax => {
            send_stable::<CanFdFrameV1, _>(transports, source.clone(), |init| {
                payload_contract::init_can_fd_max(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        PayloadContractCaseKind::SomeIpSingleMtu => {
            send_stable::<SomeIpSignalBatchMtuV1, _>(transports, source.clone(), |init| {
                payload_contract::init_someip_single_mtu(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        PayloadContractCaseKind::Streamer4k => {
            send_stable::<StreamChunk4kV1, _>(transports, source.clone(), |init| {
                payload_contract::init_streamer_4k(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        PayloadContractCaseKind::RadarArs548DetectionList => {
            send_stable::<RadarDetectionListArs548V1, _>(transports, source.clone(), |init| {
                payload_contract::init_radar_ars548_detection_list(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        PayloadContractCaseKind::Streamer64k => {
            send_stable::<StreamChunk64kV1, _>(transports, source.clone(), |init| {
                payload_contract::init_streamer_64k(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            send_stable::<LidarPointCloudHesaiAt128V1, _>(transports, source.clone(), |init| {
                payload_contract::init_lidar_hesai_at128_point_cloud(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            send_stable::<CameraBayerRggb12pFrame8mpV1, _>(transports, source.clone(), |init| {
                payload_contract::init_camera_8mp_bayer_rggb12p(
                    init.into_initializer(),
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .unwrap()
            })
            .await;
        }
    }
    let frame = receive_zero_copy(&transports.stable_zero_copy, &source).await;
    black_box(frame.try_contiguous_payload());
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn send_stable<T, F>(transports: &BenchTransports, source: UUri, initialize: F)
where
    T: up_rust::StablePayload + up_rust::StablePayloadInit,
    StableContainerWireFormat: up_rust::UWirePayload<T, Codec = up_rust::StableContainerPayload<T>>,
    F: for<'a> FnOnce(
            up_rust::USelectedWireStablePayloadInit<'a, T>,
        ) -> up_rust::InitializedStablePayload<'a, T>
        + Send,
{
    use up_rust::PayloadCodecIdentity;
    let metadata = UFrameMetadata::publish(source)
        .with_payload_encoding(
            <up_rust::StableContainerPayload<T> as PayloadCodecIdentity>::encoding(),
        )
        .build()
        .expect("stable metadata");
    transports
        .stable_zero_copy
        .send_stable_payload::<T, _>(metadata, initialize)
        .await
        .expect("stable no-zero send");
}

criterion_group!(benches, benchmark);
criterion_main!(benches);

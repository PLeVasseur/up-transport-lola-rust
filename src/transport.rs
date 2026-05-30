/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::collections::VecDeque;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinHandle};
use up_rust::{
    transport::ValidatedTxLoanSpec,
    zero_copy::{
        UFrameView, UZeroCopyListener, UZeroCopyTransportImpl, UZeroCopyUninitTransportImpl,
    },
    UCode, UStatus, UUri,
};

#[cfg(feature = "lola-ffi")]
use crate::sys::{NativeSubscriber, NativeTransport};
use crate::{
    config::{LolaPullMismatchQueueFullPolicy, LolaTransportConfig},
    frame::{LolaRxLease, LolaTxLoan, LolaUninitTxLoan},
};

/// Zero-copy uProtocol transport backed by a LoLa generic event.
///
/// The transport maps one native uProtocol frame to one fixed-size LoLa event
/// sample. Transmit loans expose only the application payload range; receive
/// leases keep the LoLa sample alive until the caller drops the lease.
///
/// With the `lola-ffi` feature, the transport uses the native S-CORE LoLa bridge
/// and each listener registration owns an independent LoLa proxy subscription.
/// With the `test-stub` feature, the transport uses an in-process queue for unit
/// tests and does not communicate with a LoLa runtime.
///
/// This transport implements only the zero-copy capability. Use
/// [`up_rust::transport::UOwnedFrameEndpoint::from_zero_copy_copying_adapter`]
/// when a router or bridge needs an owned-frame facade; that adapter copies at
/// the boundary.
pub struct UTransportLola {
    config: LolaTransportConfig,
    self_ref: Weak<UTransportLola>,
    listeners: Mutex<Vec<ListenerRegistration>>,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(feature = "test-stub")]
    pending: Mutex<VecDeque<Vec<u8>>>,
    pull_mismatch_queue: Mutex<PullMismatchQueueState>,
    #[cfg(feature = "lola-ffi")]
    native: NativeTransport,
    #[cfg(feature = "lola-ffi")]
    subscriber: Mutex<Option<NativeSubscriber>>,
}

impl UTransportLola {
    /// Builds a LoLa transport from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns validation errors from [`LolaTransportConfig::validate`] or native
    /// bridge initialization errors when the `lola-ffi` feature is active.
    pub fn build(config: LolaTransportConfig) -> Result<Arc<Self>, UStatus> {
        config.validate()?;
        #[cfg(feature = "lola-ffi")]
        let native = NativeTransport::new(&config)?;
        Ok(Arc::new_cyclic(|self_ref| Self {
            config: config.clone(),
            self_ref: self_ref.clone(),
            listeners: Mutex::new(Vec::new()),
            listener_task: Mutex::new(None),
            #[cfg(feature = "test-stub")]
            pending: Mutex::new(VecDeque::new()),
            pull_mismatch_queue: Mutex::new(PullMismatchQueueState::default()),
            #[cfg(feature = "lola-ffi")]
            native,
            #[cfg(feature = "lola-ffi")]
            subscriber: Mutex::new(None),
        }))
    }

    /// Returns the configuration used to build this transport.
    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
    }

    /// Returns diagnostics for the bounded pull mismatch queue.
    pub async fn pull_mismatch_queue_diagnostics(&self) -> LolaPullMismatchQueueDiagnostics {
        self.pull_mismatch_queue.lock().await.diagnostics()
    }

    #[cfg(feature = "lola-ffi")]
    async fn receive_next_zero_copy(&self) -> Result<LolaRxLease, UStatus> {
        let mut subscriber = self.subscriber.lock().await;
        if subscriber.is_none() {
            *subscriber = Some(NativeSubscriber::new(&self.config)?);
        }
        let sample = subscriber
            .as_ref()
            .expect("LoLa subscriber should be initialized")
            .receive()?;
        LolaRxLease::from_native(sample)
    }

    async fn ensure_listener_task(&self) -> Result<(), UStatus> {
        let mut task = self.listener_task.lock().await;
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }

        let transport = self.self_ref.clone();
        let handle = tokio::runtime::Handle::try_current().map_err(|error| {
            UStatus::fail_with_code(
                UCode::FAILED_PRECONDITION,
                format!("LoLa listener registration requires a Tokio runtime: {error}"),
            )
        })?;
        *task = Some(handle.spawn(async move { Self::listener_loop(transport).await }));
        Ok(())
    }

    async fn listener_loop(self_ref: Weak<Self>) {
        loop {
            let Some(transport) = self_ref.upgrade() else {
                break;
            };

            if transport.listeners.lock().await.is_empty() {
                break;
            }

            let poll_result = transport.poll_listener_frames().await;
            drop(transport);

            match poll_result {
                Ok(deliveries) if deliveries.is_empty() => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(deliveries) => {
                    for (listener, frame) in deliveries {
                        listener.on_receive_zero_copy(frame).await;
                    }
                }
                Err(status) => {
                    if status.get_code() == UCode::INVALID_ARGUMENT {
                        eprintln!("discarding invalid LoLa native listener sample: {status:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn poll_listener_frames(
        &self,
    ) -> Result<Vec<(Arc<dyn UZeroCopyListener<LolaRxLease>>, LolaRxLease)>, UStatus> {
        #[cfg(feature = "test-stub")]
        {
            let Some(sample) = self.pending.lock().await.pop_front() else {
                return Ok(Vec::new());
            };
            let probe = LolaRxLease::from_vec(sample.clone())?;
            let listeners = {
                let listeners = self.listeners.lock().await;
                listeners
                    .iter()
                    .filter(|registration| registration.matches_frame(&probe))
                    .map(|registration| Arc::clone(&registration.listener))
                    .collect::<Vec<_>>()
            };
            let mut deliveries = Vec::with_capacity(listeners.len());
            for listener in listeners {
                deliveries.push((listener, LolaRxLease::from_vec(sample.clone())?));
            }
            Ok(deliveries)
        }
        #[cfg(feature = "lola-ffi")]
        {
            let mut deliveries = Vec::new();
            let listeners = self.listeners.lock().await;
            for registration in listeners.iter() {
                match registration.subscriber.receive() {
                    Ok(sample) => {
                        let frame = LolaRxLease::from_native(sample)?;
                        if registration.matches_frame(&frame) {
                            deliveries.push((Arc::clone(&registration.listener), frame));
                        }
                    }
                    Err(status) if status.get_code() == UCode::NOT_FOUND => {}
                    Err(status) => return Err(status),
                }
            }
            Ok(deliveries)
        }
    }

    async fn pop_queued_pull_sample(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Option<LolaRxLease> {
        let mut state = self.pull_mismatch_queue.lock().await;
        let index = state
            .queue
            .iter()
            .position(|frame| frame_matches(frame, source_filter, sink_filter))?;
        state.queue.remove(index)
    }

    async fn queue_pull_sample(&self, frame: LolaRxLease) -> Result<(), UStatus> {
        let capacity = self.config.pull_mismatch_queue_capacity;
        let source = frame.metadata().attributes().source().to_uri(false);
        let mut state = self.pull_mismatch_queue.lock().await;
        if capacity == 0 {
            state.dropped_mismatches = state.dropped_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "dropped mismatched LoLa pull sample from {source}; capacity is 0"
            ));
            return Ok(());
        }

        let is_full = state.queue.len() >= capacity;
        if is_full
            && self.config.pull_mismatch_queue_full_policy
                == LolaPullMismatchQueueFullPolicy::RejectNewestAndReport
        {
            state.rejected_mismatches = state.rejected_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "rejected newest mismatched LoLa pull sample from {source}; capacity is {capacity}"
            ));
            return Err(UStatus::fail_with_code(
                UCode::RESOURCE_EXHAUSTED,
                format!("LoLa pull mismatch queue full; capacity is {capacity}"),
            ));
        }

        if is_full {
            state.queue.pop_front();
        }
        state.queue.push_back(frame);
        let depth = state.queue.len();

        if is_full {
            state.dropped_mismatches = state.dropped_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "dropped oldest mismatched LoLa pull sample from {source}; capacity is {capacity}"
            ));
        } else {
            state.last_mismatch_reason = Some(format!(
                "queued mismatched LoLa pull sample from {source}; depth is {depth}"
            ));
        }
        Ok(())
    }
}

#[derive(Default)]
struct PullMismatchQueueState {
    queue: VecDeque<LolaRxLease>,
    dropped_mismatches: u64,
    rejected_mismatches: u64,
    last_mismatch_reason: Option<String>,
}

impl PullMismatchQueueState {
    fn diagnostics(&self) -> LolaPullMismatchQueueDiagnostics {
        LolaPullMismatchQueueDiagnostics {
            current_depth: self.queue.len(),
            dropped_mismatches: self.dropped_mismatches,
            rejected_mismatches: self.rejected_mismatches,
            last_mismatch_reason: self.last_mismatch_reason.clone(),
        }
    }
}

/// Snapshot of bounded LoLa pull mismatch queue state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LolaPullMismatchQueueDiagnostics {
    /// Total retained mismatch samples.
    pub current_depth: usize,
    /// Number of mismatch samples dropped because the queue was full or capacity was zero.
    pub dropped_mismatches: u64,
    /// Number of mismatch samples rejected by [`LolaPullMismatchQueueFullPolicy::RejectNewestAndReport`].
    pub rejected_mismatches: u64,
    /// Human-readable reason recorded for the last mismatched pull sample.
    pub last_mismatch_reason: Option<String>,
}

struct ListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    listener: Arc<dyn UZeroCopyListener<LolaRxLease>>,
    #[cfg(feature = "lola-ffi")]
    subscriber: NativeSubscriber,
}

impl ListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<LolaRxLease>>,
        _config: &LolaTransportConfig,
    ) -> Result<Self, UStatus> {
        #[cfg(feature = "lola-ffi")]
        let subscriber = NativeSubscriber::new(_config)?;
        Ok(Self {
            source_filter: source_filter.to_owned(),
            sink_filter: sink_filter.map(ToOwned::to_owned),
            listener,
            #[cfg(feature = "lola-ffi")]
            subscriber,
        })
    }

    fn matches_frame(&self, frame: &LolaRxLease) -> bool {
        frame_matches(frame, &self.source_filter, self.sink_filter.as_ref())
    }

    fn has_same_identity(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: &Arc<dyn UZeroCopyListener<LolaRxLease>>,
    ) -> bool {
        self.source_filter == *source_filter
            && self.sink_filter.as_ref() == sink_filter
            && Arc::ptr_eq(&self.listener, listener)
    }
}

#[async_trait]
impl UZeroCopyTransportImpl for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_validated_tx(&self, spec: ValidatedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        validate_alignment(alignment)?;
        if alignment > self.config.sample_alignment {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!(
                    "requested payload alignment {alignment} exceeds LoLa sample alignment {}",
                    self.config.sample_alignment
                ),
            ));
        }
        #[cfg(feature = "test-stub")]
        {
            LolaTxLoan::new_vec(metadata, self.config.sample_size, payload_len, alignment)
        }
        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.loan_sample()?;
            LolaTxLoan::new_native(metadata, sample, payload_len, alignment)
        }
    }

    async fn send_validated_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        #[cfg(feature = "test-stub")]
        {
            let sample = buffer.into_vec()?;
            self.pending.lock().await.push_back(sample);
            Ok(())
        }
        #[cfg(feature = "lola-ffi")]
        {
            let loan = buffer.into_native()?;
            self.native.send(loan)
        }
    }

    async fn receive_validated_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        if let Some(frame) = self
            .pop_queued_pull_sample(source_filter, sink_filter)
            .await
        {
            return Ok(frame);
        }
        #[cfg(feature = "test-stub")]
        {
            let mut pending = self.pending.lock().await;
            while let Some(sample) = pending.pop_front() {
                let frame = LolaRxLease::from_vec(sample)?;
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
                }
                drop(pending);
                self.queue_pull_sample(frame).await?;
                pending = self.pending.lock().await;
            }
            Err(UStatus::fail_with_code(
                UCode::NOT_FOUND,
                "no LoLa sample available",
            ))
        }
        #[cfg(feature = "lola-ffi")]
        loop {
            let sample = self.receive_next_zero_copy().await?;
            if frame_matches(&sample, source_filter, sink_filter) {
                return Ok(sample);
            }
            self.queue_pull_sample(sample).await?;
        }
    }

    async fn register_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        {
            let mut listeners = self.listeners.lock().await;
            if listeners.iter().any(|registration| {
                registration.has_same_identity(source_filter, sink_filter, &listener)
            }) {
                return Err(UStatus::fail_with_code(
                    UCode::ALREADY_EXISTS,
                    "LoLa listener already registered for filters",
                ));
            }
            let registration =
                ListenerRegistration::new(source_filter, sink_filter, listener, &self.config)?;
            listeners.push(registration);
        }
        self.ensure_listener_task().await
    }

    async fn unregister_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let should_stop = {
            let mut listeners = self.listeners.lock().await;
            let Some(index) = listeners.iter().position(|registration| {
                registration.has_same_identity(source_filter, sink_filter, &listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NOT_FOUND,
                    "no such LoLa listener registered for filters",
                ));
            };
            listeners.remove(index);
            listeners.is_empty()
        };
        if should_stop {
            if let Some(task) = self.listener_task.lock().await.take() {
                task.abort();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl UZeroCopyUninitTransportImpl for UTransportLola {
    type UninitTx = LolaUninitTxLoan;

    async fn loan_validated_uninit_tx(
        &self,
        spec: ValidatedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        validate_alignment(alignment)?;
        if alignment > self.config.sample_alignment {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!(
                    "requested payload alignment {alignment} exceeds LoLa sample alignment {}",
                    self.config.sample_alignment
                ),
            ));
        }
        #[cfg(feature = "test-stub")]
        {
            LolaUninitTxLoan::new_vec(metadata, self.config.sample_size, payload_len, alignment)
        }
        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.loan_sample()?;
            LolaUninitTxLoan::new_native(metadata, sample, payload_len, alignment)
        }
    }
}

fn validate_alignment(alignment: usize) -> Result<(), UStatus> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "payload alignment must be a non-zero power of two",
        ));
    }
    Ok(())
}

fn frame_matches(frame: &LolaRxLease, source_filter: &UUri, sink_filter: Option<&UUri>) -> bool {
    source_filter.matches(frame.metadata().attributes().source())
        && sink_filter.is_none_or(|filter| {
            frame
                .metadata()
                .attributes()
                .sink()
                .is_some_and(|sink| filter.matches(sink))
        })
}

#[cfg(all(test, feature = "test-stub", not(feature = "lola-ffi")))]
mod tests {
    use std::{mem, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use protobuf::well_known_types::wrappers::StringValue;
    use tokio::sync::mpsc;
    use up_rust::{
        payload::{PayloadLayout, PlacementDefault, RawBytes, StableContainerPayload, UWireError},
        test_util::zero_copy_conformance,
        zero_copy::{
            UContiguousZeroCopyRxFrame, UFrameView, ULoanedContiguousZeroCopyRxFrame, UTxBuffer,
            UTxLoanSpec, UZeroCopyListener, UZeroCopyTransport, UZeroCopyTransportExt,
            UZeroCopyUninitTransport, UZeroCopyUninitTransportExt,
        },
        PayloadEncoding, ProtobufPayload, UAttributes, UCode, UFrameBuilder, UFrameMetadata,
        UMessageType, UUri, UUID,
    };

    use super::*;

    #[repr(C)]
    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        Eq,
        PartialEq,
        PlacementDefault,
        up_rust::StablePayload,
        up_rust::ByteBackedStablePayload,
    )]
    #[stable_payload(type_name = "example.vehicle.VehiclePose")]
    struct VehiclePose {
        x: u32,
        y: u32,
    }

    fn bytes_of_pose(pose: &VehiclePose) -> &[u8] {
        // SAFETY:
        // - `pose` is a non-null, aligned, initialized shared reference to one
        //   `VehiclePose`, so the byte range is contained in that object's
        //   allocation and is valid for reads for this borrow's lifetime.
        // - The returned slice covers exactly `size_of::<VehiclePose>()` bytes
        //   and does not outlive `pose`.
        // - `u8` has alignment 1 and may view the object representation without
        //   imposing a stronger alignment or validity requirement.
        unsafe {
            std::slice::from_raw_parts(
                (pose as *const VehiclePose).cast::<u8>(),
                mem::size_of::<VehiclePose>(),
            )
        }
    }

    fn deterministic_message_id() -> UUID {
        UUID::from_u64_pair(0x0000_0000_0001_7000, 0x8010_1010_1010_1a1a)
            .expect("fixed UUID should be valid")
    }

    fn deterministic_publish_metadata(topic: UUri) -> UFrameMetadata {
        let id = deterministic_message_id();
        UFrameMetadata::new_unchecked(
            UAttributes::new_unchecked(id, topic, None, UMessageType::Publish),
            None::<PayloadEncoding>,
        )
    }

    fn payload_loan_spec(
        metadata: UFrameMetadata,
        payload_len: usize,
        alignment: usize,
    ) -> UTxLoanSpec {
        UTxLoanSpec::payload(
            metadata,
            PayloadLayout::new(payload_len, alignment).expect("test layout should be valid"),
        )
        .expect("test metadata should be valid for payload")
    }

    struct ListenerSender(mpsc::UnboundedSender<(UFrameMetadata, Vec<u8>)>);

    #[async_trait]
    impl UZeroCopyListener<LolaRxLease> for ListenerSender {
        async fn on_receive_zero_copy(&self, frame: LolaRxLease) {
            self.0
                .send((
                    frame.metadata().clone(),
                    frame.contiguous_payload().to_vec(),
                ))
                .expect("listener result receiver should be open");
        }
    }

    fn config() -> LolaTransportConfig {
        LolaTransportConfig {
            local_authority: "vehicle".to_string(),
            instance_specifier: "uprotocol/transport".to_string(),
            service_type: "/uprotocol/Transport".to_string(),
            event_name: "frame".to_string(),
            sample_size: 512,
            sample_alignment: 8,
            max_samples: 4,
            pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
            pull_mismatch_queue_full_policy:
                LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
            mw_com_config_path: None,
        }
    }

    #[tokio::test]
    async fn loan_send_receive_round_trips_payload_in_stub_backend() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .with_message_id(deterministic_message_id())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        let mut loan = transport
            .loan_tx(payload_loan_spec(
                frame.metadata().clone(),
                frame.payload_bytes().len(),
                1,
            ))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(frame.payload_bytes());

        transport.send_zero_copy(loan).await.unwrap();
        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa native sample");

        assert_eq!(received.metadata(), frame.metadata());
        assert_eq!(received.contiguous_payload(), frame.payload_bytes());
    }

    #[tokio::test]
    async fn stub_backend_round_trips_protobuf_payload_codec() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9004).unwrap();
        let mut payload = StringValue::new();
        payload.value = "protobuf over lola stub".to_string();

        transport
            .send_serialized_zero_copy::<ProtobufPayload, _>(
                deterministic_publish_metadata(topic.clone()),
                &payload,
            )
            .await
            .unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa protobuf sample");
        let decoded: StringValue = received
            .deserialize_borrowed::<ProtobufPayload, _>()
            .unwrap();

        assert_eq!(
            received.metadata().encoding(),
            Some(&ProtobufPayload::encoding())
        );
        assert_eq!(decoded.value, payload.value);
    }

    #[tokio::test]
    async fn stub_backend_round_trips_stable_container_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9006).unwrap();

        transport
            .send_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                deterministic_publish_metadata(topic.clone()),
                |payload| {
                    payload.x = 21;
                    payload.y = 34;
                },
            )
            .await
            .unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa stable-container sample");
        zero_copy_conformance::verify_loaned_rx_payload_layout_for(
            &received,
            mem::size_of::<VehiclePose>(),
            mem::align_of::<VehiclePose>(),
        )
        .unwrap();
        let pose = received.borrow_stable_payload::<VehiclePose>().unwrap();

        assert_eq!(
            received.metadata().encoding(),
            Some(&StableContainerPayload::<VehiclePose>::encoding())
        );
        assert_eq!(
            received.contiguous_payload().as_ptr() as usize % mem::align_of::<VehiclePose>(),
            0
        );
        assert_eq!(pose, &VehiclePose { x: 21, y: 34 });
    }

    #[tokio::test]
    async fn stub_backend_round_trips_stable_container_uninit_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9008).unwrap();

        transport
            .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                deterministic_publish_metadata(topic.clone()),
                |slot| Ok(slot.write(VehiclePose { x: 55, y: 89 })),
            )
            .await
            .unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa stable-container sample");
        let pose = received.borrow_stable_payload::<VehiclePose>().unwrap();

        assert_eq!(pose, &VehiclePose { x: 55, y: 89 });
    }

    #[tokio::test]
    async fn stub_backend_uninit_loan_rejects_excessive_alignment() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9009).unwrap();

        let result = transport
            .loan_uninit_tx(payload_loan_spec(
                deterministic_publish_metadata(topic).with_encoding(RawBytes::encoding()),
                8,
                16,
            ))
            .await;
        let Err(error) = result else {
            panic!("LoLa uninit loan should reject excessive alignment");
        };

        assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT);
    }

    #[tokio::test]
    async fn stub_backend_loan_spec_rejects_payload_without_encoding() {
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9012).unwrap();

        let result = UTxLoanSpec::payload(
            deterministic_publish_metadata(topic),
            PayloadLayout::new(1, 1).unwrap(),
        );
        let Err(error) = result else {
            panic!("LoLa loan spec should reject payload bytes without encoding");
        };

        assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT);
    }

    #[tokio::test]
    async fn stub_backend_uninit_loan_spec_rejects_payload_without_encoding() {
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9013).unwrap();

        let result = UTxLoanSpec::payload(
            deterministic_publish_metadata(topic),
            PayloadLayout::new(1, 1).unwrap(),
        );
        let Err(error) = result else {
            panic!("LoLa uninit loan spec should reject payload bytes without encoding");
        };

        assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT);
    }

    #[tokio::test]
    async fn stub_backend_preserves_present_empty_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9014).unwrap();
        let loan = transport
            .loan_tx(
                UTxLoanSpec::present_empty_payload(
                    deterministic_publish_metadata(topic.clone())
                        .with_encoding(RawBytes::encoding()),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        transport.send_zero_copy(loan).await.unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa present-empty sample");
        assert!(received.has_payload());
        assert_eq!(received.payload_len(), 0);
        assert_eq!(received.metadata().encoding(), Some(&RawBytes::encoding()));
    }

    #[tokio::test]
    async fn stub_backend_preserves_no_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9015).unwrap();
        let loan = transport
            .loan_tx(
                UTxLoanSpec::no_payload(deterministic_publish_metadata(topic.clone())).unwrap(),
            )
            .await
            .unwrap();
        transport.send_zero_copy(loan).await.unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa no-payload sample");
        assert!(!received.has_payload());
        assert_eq!(received.payload_len(), 0);
        assert!(received.metadata().encoding().is_none());
    }

    #[tokio::test]
    async fn initialized_tx_exposes_initialized_payload_bytes() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9010).unwrap();

        let loan = transport
            .loan_tx(payload_loan_spec(
                deterministic_publish_metadata(topic).with_encoding(RawBytes::encoding()),
                4,
                1,
            ))
            .await
            .unwrap();

        assert_eq!(loan.payload(), &[0_u8; 4]);
    }

    #[tokio::test]
    async fn stub_backend_rejects_stable_container_wrong_type_name_metadata() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9007).unwrap();
        let pose = VehiclePose { x: 21, y: 34 };
        let encoding = zero_copy_conformance::stable_container_encoding_for::<VehiclePose>(
            "example.vehicle.OtherPose",
            "fixed",
            mem::size_of::<VehiclePose>(),
            mem::align_of::<VehiclePose>(),
        );
        let mut loan = transport
            .loan_tx(payload_loan_spec(
                deterministic_publish_metadata(topic.clone()).with_encoding(encoding),
                mem::size_of::<VehiclePose>(),
                mem::align_of::<VehiclePose>(),
            ))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(bytes_of_pose(&pose));
        transport.send_zero_copy(loan).await.unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for malformed stable-container sample");
        let error = received.borrow_stable_payload::<VehiclePose>().unwrap_err();

        assert!(matches!(
            error,
            UWireError::IncompatibleStablePayload { actual, .. } if actual.contains("OtherPose")
        ));
    }

    #[tokio::test]
    async fn stub_backend_rejects_wrong_inner_payload_codec_after_receive() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9005).unwrap();

        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic.clone()),
                &&[0x0a_u8][..],
            )
            .await
            .unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa raw sample");
        let result = received.deserialize_borrowed::<ProtobufPayload, StringValue>();

        assert_eq!(received.metadata().encoding(), Some(&RawBytes::encoding()));
        assert!(matches!(
            result,
            Err(UWireError::UnsupportedEncoding { .. })
        ));
    }

    #[tokio::test]
    async fn pull_receive_preserves_nonmatching_stub_samples() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic_a = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9011).unwrap();
        let topic_b = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9012).unwrap();

        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic_a.clone()),
                &&b"topic-a"[..],
            )
            .await
            .unwrap();
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic_b.clone()),
                &&b"topic-b"[..],
            )
            .await
            .unwrap();

        let received_b = transport.receive_zero_copy(&topic_b, None).await.unwrap();
        assert_eq!(received_b.contiguous_payload(), b"topic-b");

        let diagnostics = transport.pull_mismatch_queue_diagnostics().await;
        assert_eq!(diagnostics.current_depth, 1);
        assert_eq!(diagnostics.dropped_mismatches, 0);
        assert_eq!(diagnostics.rejected_mismatches, 0);
        assert!(diagnostics
            .last_mismatch_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("queued mismatched LoLa pull sample")));

        let received_a = transport.receive_zero_copy(&topic_a, None).await.unwrap();
        assert_eq!(received_a.contiguous_payload(), b"topic-a");
        assert_eq!(
            transport
                .pull_mismatch_queue_diagnostics()
                .await
                .current_depth,
            0
        );
    }

    #[tokio::test]
    async fn pull_mismatch_queue_drops_oldest_when_full() {
        let transport =
            UTransportLola::build(config().with_pull_mismatch_queue_capacity(1)).unwrap();
        let topic_a = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9013).unwrap();
        let topic_b = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9014).unwrap();

        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic_a.clone()),
                &&b"oldest"[..],
            )
            .await
            .unwrap();
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic_a.clone()),
                &&b"newest"[..],
            )
            .await
            .unwrap();

        let result = transport.receive_zero_copy(&topic_b, None).await;
        assert!(result.is_err_and(|status| status.get_code() == UCode::NOT_FOUND));

        let diagnostics = transport.pull_mismatch_queue_diagnostics().await;
        assert_eq!(diagnostics.current_depth, 1);
        assert_eq!(diagnostics.dropped_mismatches, 1);
        assert_eq!(diagnostics.rejected_mismatches, 0);
        assert!(diagnostics
            .last_mismatch_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("dropped oldest mismatched LoLa pull sample")));

        let received_a = transport.receive_zero_copy(&topic_a, None).await.unwrap();
        assert_eq!(received_a.contiguous_payload(), b"newest");
    }

    #[tokio::test]
    async fn pull_mismatch_queue_can_reject_newest_when_full() {
        let transport = UTransportLola::build(
            config()
                .with_pull_mismatch_queue_capacity(1)
                .with_pull_mismatch_queue_full_policy(
                    LolaPullMismatchQueueFullPolicy::RejectNewestAndReport,
                ),
        )
        .unwrap();
        let topic_a = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9015).unwrap();
        let topic_b = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9016).unwrap();

        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic_a.clone()),
                &&b"first"[..],
            )
            .await
            .unwrap();
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                deterministic_publish_metadata(topic_a.clone()),
                &&b"second"[..],
            )
            .await
            .unwrap();

        let result = transport.receive_zero_copy(&topic_b, None).await;
        assert!(result.is_err_and(|status| status.get_code() == UCode::RESOURCE_EXHAUSTED));

        let diagnostics = transport.pull_mismatch_queue_diagnostics().await;
        assert_eq!(diagnostics.current_depth, 1);
        assert_eq!(diagnostics.dropped_mismatches, 0);
        assert_eq!(diagnostics.rejected_mismatches, 1);
        assert!(diagnostics
            .last_mismatch_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("rejected newest mismatched LoLa pull sample")));

        let received_a = transport.receive_zero_copy(&topic_a, None).await.unwrap();
        assert_eq!(received_a.contiguous_payload(), b"first");
    }

    #[tokio::test]
    async fn registered_listener_receives_matching_stub_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .with_message_id(deterministic_message_id())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let listener: Arc<dyn UZeroCopyListener<LolaRxLease>> = Arc::new(ListenerSender(sender));

        transport
            .register_zero_copy_listener(&topic, None, Arc::clone(&listener))
            .await
            .unwrap();

        let mut loan = transport
            .loan_tx(payload_loan_spec(
                frame.metadata().clone(),
                frame.payload_bytes().len(),
                1,
            ))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(frame.payload_bytes());
        transport.send_zero_copy(loan).await.unwrap();

        let (metadata, payload) = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("listener should receive before timeout")
            .expect("listener should send a result");
        assert_eq!(&metadata, frame.metadata());
        assert_eq!(payload, frame.payload_bytes());

        transport
            .unregister_zero_copy_listener(&topic, None, listener)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn overlapping_stub_listeners_both_receive_matching_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let broad_topic = UUri::any_with_resource_id(topic.resource_id_raw());
        let frame = UFrameBuilder::publish(topic.clone())
            .with_message_id(deterministic_message_id())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        let (sender_a, mut receiver_a) = mpsc::unbounded_channel();
        let (sender_b, mut receiver_b) = mpsc::unbounded_channel();
        let listener_a: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(ListenerSender(sender_a));
        let listener_b: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(ListenerSender(sender_b));

        transport
            .register_zero_copy_listener(&topic, None, Arc::clone(&listener_a))
            .await
            .unwrap();
        transport
            .register_zero_copy_listener(&broad_topic, None, Arc::clone(&listener_b))
            .await
            .unwrap();

        let mut loan = transport
            .loan_tx(payload_loan_spec(
                frame.metadata().clone(),
                frame.payload_bytes().len(),
                1,
            ))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(frame.payload_bytes());
        transport.send_zero_copy(loan).await.unwrap();

        let (_, payload_a) = tokio::time::timeout(Duration::from_secs(1), receiver_a.recv())
            .await
            .expect("first listener should receive before timeout")
            .expect("first listener should send a result");
        let (_, payload_b) = tokio::time::timeout(Duration::from_secs(1), receiver_b.recv())
            .await
            .expect("second listener should receive before timeout")
            .expect("second listener should send a result");

        assert_eq!(payload_a, frame.payload_bytes());
        assert_eq!(payload_b, frame.payload_bytes());

        transport
            .unregister_zero_copy_listener(&topic, None, listener_a)
            .await
            .unwrap();
        transport
            .unregister_zero_copy_listener(&broad_topic, None, listener_b)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn targeted_stub_listeners_both_receive_exact_and_sink_wildcard_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let source = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9002).unwrap();
        let sink = UUri::try_from_parts("vehicle", 0x4220, 1, 0).unwrap();
        let sink_wildcard = UUri::try_from_parts("vehicle", 0xFFFF_FFFF, 0xFF, 0).unwrap();
        let (sender_a, mut receiver_a) = mpsc::unbounded_channel();
        let (sender_b, mut receiver_b) = mpsc::unbounded_channel();
        let listener_a: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(ListenerSender(sender_a));
        let listener_b: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(ListenerSender(sender_b));

        transport
            .register_zero_copy_listener(&source, Some(&sink), Arc::clone(&listener_a))
            .await
            .unwrap();
        transport
            .register_zero_copy_listener(&source, Some(&sink_wildcard), Arc::clone(&listener_b))
            .await
            .unwrap();

        let attributes = UAttributes::try_new(
            deterministic_message_id(),
            source.clone(),
            Some(sink.clone()),
            UMessageType::Notification,
        )
        .expect("valid notification attributes");
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                UFrameMetadata::try_new(attributes, RawBytes::encoding())
                    .expect("valid notification metadata"),
                &&b"targeted"[..],
            )
            .await
            .unwrap();

        let (metadata_a, payload_a) =
            tokio::time::timeout(Duration::from_secs(1), receiver_a.recv())
                .await
                .expect("first listener should receive before timeout")
                .expect("first listener should send a result");
        let (metadata_b, payload_b) =
            tokio::time::timeout(Duration::from_secs(1), receiver_b.recv())
                .await
                .expect("second listener should receive before timeout")
                .expect("second listener should send a result");

        assert_eq!(metadata_a.attributes().sink(), Some(&sink));
        assert_eq!(metadata_b.attributes().sink(), Some(&sink));
        assert_eq!(payload_a, b"targeted");
        assert_eq!(payload_b, b"targeted");

        transport
            .unregister_zero_copy_listener(&source, Some(&sink), listener_a)
            .await
            .unwrap();
        transport
            .unregister_zero_copy_listener(&source, Some(&sink_wildcard), listener_b)
            .await
            .unwrap();
    }
}

#[cfg(all(test, feature = "lola-ffi"))]
mod native_tests {
    use std::sync::OnceLock;

    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use up_rust::{
        payload::{PayloadLayout, RawBytes, StableContainerPayload},
        zero_copy::{
            UContiguousZeroCopyRxFrame, UFrameView, ULoanedContiguousZeroCopyRxFrame, UTxBuffer,
            UTxLoanSpec, UZeroCopyListener, UZeroCopyTransport, UZeroCopyTransportExt,
            UZeroCopyUninitTransportExt,
        },
        UAttributes, UFrameBuilder, UFrameMetadata, UMessageType, UUri, UUID,
    };

    use super::*;

    static NATIVE_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

    struct NativeListenerSender(mpsc::UnboundedSender<Vec<u8>>);

    #[repr(C)]
    #[derive(
        Clone, Copy, Debug, Eq, PartialEq, up_rust::StablePayload, up_rust::ByteBackedStablePayload,
    )]
    #[stable_payload(type_name = "example.vehicle.VehiclePose")]
    struct NativeVehiclePose {
        x: u64,
        y: u64,
    }

    #[async_trait]
    impl UZeroCopyListener<LolaRxLease> for NativeListenerSender {
        async fn on_receive_zero_copy(&self, frame: LolaRxLease) {
            self.0
                .send(frame.contiguous_payload().to_vec())
                .expect("listener result receiver should be open");
        }
    }

    fn native_test_config() -> LolaTransportConfig {
        let fixture_config_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/mw_com_config.json"
        );
        let mw_com_config_path = std::env::var("LOLA_NATIVE_TEST_CONFIG")
            .unwrap_or_else(|_| fixture_config_path.to_string());
        LolaTransportConfig {
            local_authority: std::env::var("LOLA_NATIVE_TEST_AUTHORITY")
                .unwrap_or_else(|_| "vehicle".to_string()),
            instance_specifier: std::env::var("LOLA_NATIVE_TEST_INSTANCE_SPECIFIER")
                .unwrap_or_else(|_| "uprotocol/transport".to_string()),
            service_type: std::env::var("LOLA_NATIVE_TEST_SERVICE_TYPE")
                .unwrap_or_else(|_| "/uprotocol/Transport".to_string()),
            event_name: std::env::var("LOLA_NATIVE_TEST_EVENT_NAME")
                .unwrap_or_else(|_| "frame".to_string()),
            sample_size: std::env::var("LOLA_NATIVE_TEST_SAMPLE_SIZE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(512),
            sample_alignment: std::env::var("LOLA_NATIVE_TEST_SAMPLE_ALIGNMENT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8),
            max_samples: std::env::var("LOLA_NATIVE_TEST_MAX_SAMPLES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(4),
            pull_mismatch_queue_capacity: std::env::var(
                "LOLA_NATIVE_TEST_PULL_MISMATCH_QUEUE_CAPACITY",
            )
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY),
            pull_mismatch_queue_full_policy:
                LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
            mw_com_config_path: Some(mw_com_config_path),
        }
    }

    async fn native_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        NATIVE_TEST_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    fn payload_loan_spec(
        metadata: UFrameMetadata,
        payload_len: usize,
        alignment: usize,
    ) -> UTxLoanSpec {
        UTxLoanSpec::payload(
            metadata,
            PayloadLayout::new(payload_len, alignment).expect("test layout should be valid"),
        )
        .expect("test metadata should be valid for payload")
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_loan_send_receive_round_trips_payload() {
        let _guard = native_test_guard().await;
        let config = native_test_config();
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        for _ in 0..100 {
            if let Err(status) = transport.receive_zero_copy(&topic, None).await {
                if status.get_code() == UCode::INVALID_ARGUMENT {
                    eprintln!("discarding invalid pre-existing LoLa native sample: {status:?}");
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut loan = transport
            .loan_tx(payload_loan_spec(
                frame.metadata().clone(),
                frame.payload_bytes().len(),
                1,
            ))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(frame.payload_bytes());

        transport.send_zero_copy(loan).await.unwrap();
        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) if status.get_code() == UCode::INVALID_ARGUMENT => {
                    eprintln!("discarding invalid LoLa native sample while waiting for test frame: {status:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa native sample");

        assert_eq!(received.metadata(), frame.metadata());
        assert_eq!(received.contiguous_payload(), frame.payload_bytes());
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_round_trips_stable_container_uninit_payload() {
        let _guard = native_test_guard().await;
        let config = native_test_config();
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9008).unwrap();

        transport
            .send_uninit_loaned_payload_as::<
                StableContainerPayload<NativeVehiclePose>,
                NativeVehiclePose,
            >(
                UFrameMetadata::try_publish(topic.clone()).expect("valid publish metadata"),
                |slot| Ok(slot.write(NativeVehiclePose { x: 144, y: 233 })),
            )
            .await
            .unwrap();

        let mut received = None;
        for _ in 0..100 {
            match transport.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    received = Some(frame);
                    break;
                }
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) if status.get_code() == UCode::INVALID_ARGUMENT => {
                    eprintln!("discarding invalid LoLa native sample while waiting for stable test frame: {status:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(status) => panic!("unexpected LoLa receive error: {status:?}"),
            }
        }
        let received = received.expect("timed out waiting for LoLa stable native sample");
        let pose = received
            .borrow_stable_payload::<NativeVehiclePose>()
            .unwrap();

        assert_eq!(pose, &NativeVehiclePose { x: 144, y: 233 });
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_tx_wrapper_pool_exhausts_and_reuses_after_drop() {
        let _guard = native_test_guard().await;
        let config = native_test_config();
        let authority = config.local_authority.clone();
        let max_samples = config.max_samples;
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9009).unwrap();
        let mut loans = Vec::new();
        for _ in 0..max_samples {
            loans.push(
                transport
                    .loan_tx(payload_loan_spec(
                        UFrameMetadata::try_publish(topic.clone())
                            .expect("valid publish metadata")
                            .with_encoding(RawBytes::encoding()),
                        1,
                        1,
                    ))
                    .await
                    .unwrap(),
            );
        }

        let result = transport
            .loan_tx(payload_loan_spec(
                UFrameMetadata::try_publish(topic.clone())
                    .expect("valid publish metadata")
                    .with_encoding(RawBytes::encoding()),
                1,
                1,
            ))
            .await;
        let Err(error) = result else {
            panic!("LoLa native TX wrapper/sample pool should be exhausted");
        };
        assert_eq!(error.get_code(), UCode::RESOURCE_EXHAUSTED);

        drop(loans.pop());
        transport
            .loan_tx(payload_loan_spec(
                UFrameMetadata::try_publish(topic)
                    .expect("valid publish metadata")
                    .with_encoding(RawBytes::encoding()),
                1,
                1,
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_two_listeners_receive_same_payload() {
        let _guard = native_test_guard().await;
        let config = native_test_config();
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9001).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .build_with_raw_payload(b"fanout".as_slice())
            .unwrap();
        let (sender_a, mut receiver_a) = mpsc::unbounded_channel();
        let (sender_b, mut receiver_b) = mpsc::unbounded_channel();
        let listener_a: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(NativeListenerSender(sender_a));
        let listener_b: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(NativeListenerSender(sender_b));

        transport
            .register_zero_copy_listener(&topic, None, Arc::clone(&listener_a))
            .await
            .unwrap();
        transport
            .register_zero_copy_listener(&topic, None, Arc::clone(&listener_b))
            .await
            .unwrap();

        let mut loan = transport
            .loan_tx(payload_loan_spec(
                frame.metadata().clone(),
                frame.payload_bytes().len(),
                1,
            ))
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(frame.payload_bytes());
        transport.send_zero_copy(loan).await.unwrap();

        let payload_a = tokio::time::timeout(Duration::from_secs(5), receiver_a.recv())
            .await
            .expect("first listener should receive before timeout")
            .expect("first listener should send a result");
        let payload_b = tokio::time::timeout(Duration::from_secs(5), receiver_b.recv())
            .await
            .expect("second listener should receive before timeout")
            .expect("second listener should send a result");

        assert_eq!(payload_a, frame.payload_bytes());
        assert_eq!(payload_b, frame.payload_bytes());

        transport
            .unregister_zero_copy_listener(&topic, None, listener_a)
            .await
            .unwrap();
        transport
            .unregister_zero_copy_listener(&topic, None, listener_b)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_exact_and_source_wildcard_listeners_receive_same_payload() {
        let _guard = native_test_guard().await;
        let config = native_test_config();
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9002).unwrap();
        let source_wildcard = UUri::any_with_resource_id(topic.resource_id_raw());
        let (sender_a, mut receiver_a) = mpsc::unbounded_channel();
        let (sender_b, mut receiver_b) = mpsc::unbounded_channel();
        let listener_a: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(NativeListenerSender(sender_a));
        let listener_b: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(NativeListenerSender(sender_b));

        transport
            .register_zero_copy_listener(&topic, None, Arc::clone(&listener_a))
            .await
            .unwrap();
        transport
            .register_zero_copy_listener(&source_wildcard, None, Arc::clone(&listener_b))
            .await
            .unwrap();

        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                UFrameMetadata::try_publish(topic.clone()).expect("valid publish metadata"),
                &&b"source-wildcard"[..],
            )
            .await
            .unwrap();

        let payload_a = tokio::time::timeout(Duration::from_secs(5), receiver_a.recv())
            .await
            .expect("first listener should receive before timeout")
            .expect("first listener should send a result");
        let payload_b = tokio::time::timeout(Duration::from_secs(5), receiver_b.recv())
            .await
            .expect("second listener should receive before timeout")
            .expect("second listener should send a result");

        assert_eq!(payload_a, b"source-wildcard");
        assert_eq!(payload_b, b"source-wildcard");

        transport
            .unregister_zero_copy_listener(&topic, None, listener_a)
            .await
            .unwrap();
        transport
            .unregister_zero_copy_listener(&source_wildcard, None, listener_b)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_exact_and_sink_wildcard_listeners_receive_same_targeted_payload() {
        let _guard = native_test_guard().await;
        let config = native_test_config();
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let source = UUri::try_from_parts(&authority, 0x4210, 1, 0x9003).unwrap();
        let sink = UUri::try_from_parts(&authority, 0x4220, 1, 0).unwrap();
        let sink_wildcard = UUri::try_from_parts(&authority, 0xFFFF_FFFF, 0xFF, 0).unwrap();
        let (sender_a, mut receiver_a) = mpsc::unbounded_channel();
        let (sender_b, mut receiver_b) = mpsc::unbounded_channel();
        let listener_a: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(NativeListenerSender(sender_a));
        let listener_b: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(NativeListenerSender(sender_b));

        transport
            .register_zero_copy_listener(&source, Some(&sink), Arc::clone(&listener_a))
            .await
            .unwrap();
        transport
            .register_zero_copy_listener(&source, Some(&sink_wildcard), Arc::clone(&listener_b))
            .await
            .unwrap();

        let attributes = UAttributes::try_new(
            UUID::build(),
            source.clone(),
            Some(sink.clone()),
            UMessageType::Notification,
        )
        .expect("valid notification attributes");
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                UFrameMetadata::try_new(attributes, RawBytes::encoding())
                    .expect("valid notification metadata"),
                &&b"sink-wildcard"[..],
            )
            .await
            .unwrap();

        let payload_a = tokio::time::timeout(Duration::from_secs(5), receiver_a.recv())
            .await
            .expect("first listener should receive before timeout")
            .expect("first listener should send a result");
        let payload_b = tokio::time::timeout(Duration::from_secs(5), receiver_b.recv())
            .await
            .expect("second listener should receive before timeout")
            .expect("second listener should send a result");

        assert_eq!(payload_a, b"sink-wildcard");
        assert_eq!(payload_b, b"sink-wildcard");

        transport
            .unregister_zero_copy_listener(&source, Some(&sink), listener_a)
            .await
            .unwrap();
        transport
            .unregister_zero_copy_listener(&source, Some(&sink_wildcard), listener_b)
            .await
            .unwrap();
    }
}

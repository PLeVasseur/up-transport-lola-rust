/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};
#[cfg(feature = "native")]
use std::{sync::Weak, time::Duration};

use async_trait::async_trait;
#[cfg(feature = "native")]
use tokio::task::JoinHandle;
use up_rust::{
    UCode, UFrameView, UStatus, UUri, UZeroCopyListener, UZeroCopyTransportImpl,
    UZeroCopyUninitTransportImpl, ValidatedTxLoanSpec,
};

#[cfg(any(feature = "test-stub", feature = "native"))]
use crate::config::LolaPullMismatchQueueFullPolicy;
#[cfg(feature = "native")]
use crate::sys::{NativeSubscriber, NativeTransport};
use crate::{
    config::LolaTransportConfig,
    frame::{LolaRxLease, LolaTxLoan, LolaUninitTxLoan},
};

/// Zero-copy uProtocol transport backed by a LoLa generic event.
///
/// The transport maps one native uProtocol frame to one fixed-size LoLa event
/// sample. Transmit loans expose only the application payload range; receive
/// leases keep the sample alive until callers drop the lease.
pub struct UTransportLola {
    config: LolaTransportConfig,
    #[cfg(feature = "native")]
    self_ref: Weak<UTransportLola>,
    #[cfg(feature = "test-stub")]
    pending_samples: Mutex<VecDeque<Vec<u8>>>,
    listeners: Mutex<Vec<ListenerRegistration>>,
    pull_mismatch_queue: Mutex<PullMismatchQueueState>,
    #[cfg(feature = "native")]
    listener_task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(feature = "native")]
    native: NativeTransport,
    #[cfg(feature = "native")]
    subscriber: Mutex<Option<NativeSubscriber>>,
}

impl UTransportLola {
    /// Builds a LoLa transport from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns validation errors from [`LolaTransportConfig::validate`] or native
    /// bridge initialization errors when the `native` feature is active.
    pub fn build(config: LolaTransportConfig) -> Result<Arc<Self>, UStatus> {
        config.validate()?;
        #[cfg(feature = "native")]
        let native = NativeTransport::new(&config)?;
        Ok(Arc::new_cyclic(|_self_ref| Self {
            config,
            #[cfg(feature = "native")]
            self_ref: _self_ref.clone(),
            #[cfg(feature = "test-stub")]
            pending_samples: Mutex::new(VecDeque::new()),
            listeners: Mutex::new(Vec::new()),
            pull_mismatch_queue: Mutex::new(PullMismatchQueueState::default()),
            #[cfg(feature = "native")]
            listener_task: Mutex::new(None),
            #[cfg(feature = "native")]
            native,
            #[cfg(feature = "native")]
            subscriber: Mutex::new(None),
        }))
    }

    /// Returns the configuration used to build this transport.
    #[must_use]
    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
    }

    /// Returns diagnostics for the bounded pull mismatch queue.
    pub fn pull_mismatch_queue_diagnostics(&self) -> LolaPullMismatchQueueDiagnostics {
        self.pull_mismatch_queue
            .lock()
            .expect("LoLa pull mismatch queue lock poisoned")
            .diagnostics()
    }

    #[cfg(feature = "native")]
    fn receive_next_zero_copy(&self) -> Result<LolaRxLease, UStatus> {
        let mut subscriber = self
            .subscriber
            .lock()
            .expect("LoLa native subscriber lock poisoned");
        if subscriber.is_none() {
            *subscriber = Some(NativeSubscriber::new(&self.config)?);
        }
        let sample = subscriber
            .as_ref()
            .expect("LoLa subscriber should be initialized")
            .receive()?;
        LolaRxLease::from_native(sample)
    }

    #[cfg(feature = "native")]
    fn ensure_listener_task(&self) -> Result<(), UStatus> {
        let mut task = self
            .listener_task
            .lock()
            .expect("LoLa listener task lock poisoned");
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }

        let transport = self.self_ref.clone();
        let handle = tokio::runtime::Handle::try_current().map_err(|error| {
            UStatus::fail_with_code(
                UCode::FailedPrecondition,
                format!("LoLa listener registration requires a Tokio runtime: {error}"),
            )
        })?;
        *task = Some(handle.spawn(async move { Self::listener_loop(transport).await }));
        Ok(())
    }

    #[cfg(feature = "native")]
    async fn listener_loop(self_ref: Weak<Self>) {
        loop {
            let Some(transport) = self_ref.upgrade() else {
                break;
            };

            if transport
                .listeners
                .lock()
                .expect("LoLa listener registry lock poisoned")
                .is_empty()
            {
                break;
            }

            let poll_result = transport.poll_native_listener_frames();
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
                    if status.get_code() == UCode::InvalidArgument {
                        eprintln!("discarding invalid LoLa native listener sample: {status:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    #[cfg(feature = "native")]
    fn poll_native_listener_frames(
        &self,
    ) -> Result<Vec<(Arc<dyn UZeroCopyListener<LolaRxLease>>, LolaRxLease)>, UStatus> {
        let listeners = self
            .listeners
            .lock()
            .expect("LoLa listener registry lock poisoned");
        let mut deliveries = Vec::new();
        for registration in listeners.iter() {
            match registration.subscriber.receive() {
                Ok(sample) => {
                    let frame = LolaRxLease::from_native(sample)?;
                    if registration.matches_frame(&frame) {
                        deliveries.push((Arc::clone(&registration.listener), frame));
                    }
                }
                Err(status) if status.get_code() == UCode::NotFound => {}
                Err(status) => return Err(status),
            }
        }
        Ok(deliveries)
    }

    fn pop_queued_pull_sample(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Option<LolaRxLease> {
        let mut state = self
            .pull_mismatch_queue
            .lock()
            .expect("LoLa pull mismatch queue lock poisoned");
        let index = state
            .queue
            .iter()
            .position(|frame| frame_matches(frame, source_filter, sink_filter))?;
        state.queue.remove(index)
    }

    #[cfg(any(feature = "test-stub", feature = "native"))]
    fn queue_pull_sample(&self, frame: LolaRxLease) -> Result<(), UStatus> {
        let capacity = self.config.pull_mismatch_queue_capacity;
        let mut state = self
            .pull_mismatch_queue
            .lock()
            .expect("LoLa pull mismatch queue lock poisoned");
        if capacity == 0 {
            state.dropped_mismatches = state.dropped_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some("dropped mismatched LoLa pull sample".to_string());
            return Ok(());
        }

        let is_full = state.queue.len() >= capacity;
        if is_full
            && self.config.pull_mismatch_queue_full_policy
                == LolaPullMismatchQueueFullPolicy::RejectNewestAndReport
        {
            state.rejected_mismatches = state.rejected_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "rejected newest mismatched LoLa pull sample; capacity is {capacity}"
            ));
            return Err(UStatus::fail_with_code(
                UCode::ResourceExhausted,
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
                "dropped oldest mismatched LoLa pull sample; capacity is {capacity}"
            ));
        } else {
            state.last_mismatch_reason = Some(format!(
                "queued mismatched LoLa pull sample; depth is {depth}"
            ));
        }
        Ok(())
    }

    #[cfg(feature = "test-stub")]
    async fn deliver_test_stub_sample(&self, sample: &[u8]) -> Result<(), UStatus> {
        let listeners = {
            let listeners = self
                .listeners
                .lock()
                .expect("LoLa listener registry lock poisoned");
            if listeners.is_empty() {
                return Ok(());
            }
            let probe = LolaRxLease::from_vec(sample.to_vec())?;
            listeners
                .iter()
                .filter(|registration| registration.matches_frame(&probe))
                .map(|registration| Arc::clone(&registration.listener))
                .collect::<Vec<_>>()
        };

        for listener in listeners {
            listener
                .on_receive_zero_copy(LolaRxLease::from_vec(sample.to_vec())?)
                .await;
        }
        Ok(())
    }

    fn validate_payload_alignment(&self, alignment: usize) -> Result<(), UStatus> {
        if alignment == 0 || !alignment.is_power_of_two() {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "payload alignment must be a non-zero power of two",
            ));
        }
        if alignment > self.config.sample_alignment {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!(
                    "requested payload alignment {alignment} exceeds LoLa sample alignment {}",
                    self.config.sample_alignment
                ),
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
    #[cfg(feature = "native")]
    subscriber: NativeSubscriber,
}

impl ListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<LolaRxLease>>,
        _config: &LolaTransportConfig,
    ) -> Result<Self, UStatus> {
        #[cfg(feature = "native")]
        let subscriber = NativeSubscriber::new(_config)?;
        Ok(Self {
            source_filter: source_filter.to_owned(),
            sink_filter: sink_filter.map(ToOwned::to_owned),
            listener,
            #[cfg(feature = "native")]
            subscriber,
        })
    }

    #[cfg(any(feature = "test-stub", feature = "native"))]
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

#[async_trait]
impl UZeroCopyTransportImpl for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_validated_tx(&self, spec: ValidatedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        self.validate_payload_alignment(alignment)?;

        #[cfg(feature = "native")]
        {
            let sample = self.native.loan_sample()?;
            return LolaTxLoan::new_native(metadata, sample, payload_len, alignment);
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            return LolaTxLoan::new_vec(metadata, self.config.sample_size, payload_len, alignment);
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            let _ = (metadata, payload_len);
            Err(backend_unavailable())
        }
    }

    async fn send_validated_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        #[cfg(feature = "native")]
        {
            let loan = buffer.into_native()?;
            return self.native.send(loan);
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            let sample = buffer.into_vec();
            self.deliver_test_stub_sample(&sample).await?;
            self.pending_samples
                .lock()
                .expect("LoLa test-stub pending sample lock poisoned")
                .push_back(sample);
            return Ok(());
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            let _ = buffer;
            Err(backend_unavailable())
        }
    }

    async fn receive_validated_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        if let Some(frame) = self.pop_queued_pull_sample(source_filter, sink_filter) {
            return Ok(frame);
        }

        #[cfg(feature = "native")]
        {
            loop {
                let frame = self.receive_next_zero_copy()?;
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
                }
                self.queue_pull_sample(frame)?;
            }
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            loop {
                let sample = self
                    .pending_samples
                    .lock()
                    .expect("LoLa test-stub pending sample lock poisoned")
                    .pop_front();
                let Some(sample) = sample else {
                    return Err(UStatus::fail_with_code(
                        UCode::NotFound,
                        "no LoLa sample available",
                    ));
                };
                let frame = LolaRxLease::from_vec(sample)?;
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
                }
                self.queue_pull_sample(frame)?;
            }
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            Err(backend_unavailable())
        }
    }

    async fn register_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let mut listeners = self
            .listeners
            .lock()
            .expect("LoLa listener registry lock poisoned");
        if listeners.iter().any(|registration| {
            registration.has_same_identity(source_filter, sink_filter, &listener)
        }) {
            return Err(UStatus::fail_with_code(
                UCode::AlreadyExists,
                "LoLa listener already registered for filters",
            ));
        }
        let registration =
            ListenerRegistration::new(source_filter, sink_filter, listener, &self.config)?;
        listeners.push(registration);
        drop(listeners);

        #[cfg(feature = "native")]
        self.ensure_listener_task()?;

        Ok(())
    }

    async fn unregister_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let should_stop = {
            let mut listeners = self
                .listeners
                .lock()
                .expect("LoLa listener registry lock poisoned");
            let Some(index) = listeners.iter().position(|registration| {
                registration.has_same_identity(source_filter, sink_filter, &listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NotFound,
                    "no such LoLa listener registered for filters",
                ));
            };
            listeners.remove(index);
            listeners.is_empty()
        };

        #[cfg(feature = "native")]
        if should_stop {
            if let Some(task) = self
                .listener_task
                .lock()
                .expect("LoLa listener task lock poisoned")
                .take()
            {
                task.abort();
            }
        }

        #[cfg(not(feature = "native"))]
        let _ = should_stop;

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
        self.validate_payload_alignment(alignment)?;

        #[cfg(feature = "native")]
        {
            let sample = self.native.loan_sample()?;
            return LolaUninitTxLoan::new_native(metadata, sample, payload_len, alignment);
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            return LolaUninitTxLoan::new_vec(
                metadata,
                self.config.sample_size,
                payload_len,
                alignment,
            );
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            let _ = (metadata, payload_len);
            Err(backend_unavailable())
        }
    }
}

#[cfg(not(any(feature = "test-stub", feature = "native")))]
fn backend_unavailable() -> UStatus {
    UStatus::fail_with_code(
        UCode::FailedPrecondition,
        "enable the LoLa test-stub or native backend feature to use zero-copy samples",
    )
}

#[cfg(all(
    test,
    any(
        all(feature = "test-stub", not(feature = "native")),
        not(any(feature = "test-stub", feature = "native"))
    )
))]
mod tests {
    use std::{future::Future, sync::Arc, task::Wake};

    #[cfg(feature = "test-stub")]
    use async_trait::async_trait;
    #[cfg(feature = "test-stub")]
    use std::sync::Mutex;
    use up_rust::{
        try_project_umessage_to_frame_metadata, UCode, UMessageBuilder, UPayloadFormat,
        UTxLoanSpec, UUri, UZeroCopyTransport,
    };
    #[cfg(feature = "test-stub")]
    use up_rust::{
        UFrameView, UTxBuffer, UUninitTxBuffer, UZeroCopyListener, UZeroCopyUninitTransport,
    };

    use super::*;

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

    fn tx_spec_for(topic: UUri, payload_len: usize, payload_alignment: usize) -> UTxLoanSpec {
        let message = UMessageBuilder::publish(topic)
            .build_with_payload(Vec::new(), UPayloadFormat::Raw)
            .expect("valid publish message");
        let metadata = try_project_umessage_to_frame_metadata(&message).expect("valid metadata");
        UTxLoanSpec::payload(metadata, payload_len, payload_alignment).expect("valid loan spec")
    }

    fn tx_spec(payload_len: usize, payload_alignment: usize) -> UTxLoanSpec {
        tx_spec_for(
            UUri::try_from("//vehicle/4210/1/9008").expect("valid URI"),
            payload_len,
            payload_alignment,
        )
    }

    #[cfg(feature = "test-stub")]
    fn send_payload(transport: &Arc<UTransportLola>, topic: UUri, payload: &[u8]) {
        block_on_ready(async {
            let mut loan = transport
                .loan_tx(tx_spec_for(topic, payload.len(), 1))
                .await
                .unwrap();
            loan.payload_mut().copy_from_slice(payload);
            transport.send_zero_copy(loan).await.unwrap();
        });
    }

    #[cfg(feature = "test-stub")]
    #[derive(Default)]
    struct RecordingListener {
        payloads: Mutex<Vec<Vec<u8>>>,
    }

    #[cfg(feature = "test-stub")]
    impl RecordingListener {
        fn payloads(&self) -> Vec<Vec<u8>> {
            self.payloads.lock().unwrap().clone()
        }
    }

    #[cfg(feature = "test-stub")]
    #[async_trait]
    impl UZeroCopyListener<LolaRxLease> for RecordingListener {
        async fn on_receive_zero_copy(&self, frame: LolaRxLease) {
            self.payloads
                .lock()
                .unwrap()
                .push(frame.try_contiguous_payload().unwrap().to_vec());
        }
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_send_zero_copy_stores_initialized_sample() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let mut loan = block_on_ready(transport.loan_tx(tx_spec(3, 1))).unwrap();
        loan.payload_mut().copy_from_slice(b"abc");

        block_on_ready(transport.send_zero_copy(loan)).unwrap();

        let samples = transport.pending_samples.lock().unwrap();
        assert_eq!(samples.len(), 1);
        let lease = LolaRxLease::from_vec(samples.front().unwrap().clone()).unwrap();
        assert_eq!(lease.try_contiguous_payload(), Some(b"abc".as_slice()));
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_send_zero_copy_commits_uninit_payload_without_zeroing_payload() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let mut loan = block_on_ready(transport.loan_uninit_tx(tx_spec(3, 1))).unwrap();
        for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(*b"xyz") {
            slot.write(byte);
        }

        // SAFETY: The test wrote exactly every visible application payload byte.
        let loan = unsafe { loan.assume_payload_init() };
        block_on_ready(transport.send_zero_copy(loan)).unwrap();

        let samples = transport.pending_samples.lock().unwrap();
        assert_eq!(samples.len(), 1);
        let lease = LolaRxLease::from_vec(samples.front().unwrap().clone()).unwrap();
        assert_eq!(lease.try_contiguous_payload(), Some(b"xyz".as_slice()));
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_receive_zero_copy_returns_matching_sample() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let topic = UUri::try_from("//vehicle/4210/1/9009").expect("valid URI");
        send_payload(&transport, topic.clone(), b"rx");

        let frame = block_on_ready(transport.receive_zero_copy(&topic, None)).unwrap();

        assert_eq!(frame.try_contiguous_payload(), Some(b"rx".as_slice()));
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_pull_receive_preserves_nonmatching_samples() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let topic_a = UUri::try_from("//vehicle/4210/1/9010").expect("valid URI");
        let topic_b = UUri::try_from("//vehicle/4210/1/9011").expect("valid URI");
        send_payload(&transport, topic_a.clone(), b"a");
        send_payload(&transport, topic_b.clone(), b"b");

        let frame_b = block_on_ready(transport.receive_zero_copy(&topic_b, None)).unwrap();
        assert_eq!(frame_b.try_contiguous_payload(), Some(b"b".as_slice()));
        assert_eq!(transport.pull_mismatch_queue_diagnostics().current_depth, 1);

        let frame_a = block_on_ready(transport.receive_zero_copy(&topic_a, None)).unwrap();
        assert_eq!(frame_a.try_contiguous_payload(), Some(b"a".as_slice()));
        assert_eq!(transport.pull_mismatch_queue_diagnostics().current_depth, 0);
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_pull_mismatch_queue_can_reject_newest_when_full() {
        let mut config = test_config();
        config.pull_mismatch_queue_capacity = 1;
        config.pull_mismatch_queue_full_policy =
            LolaPullMismatchQueueFullPolicy::RejectNewestAndReport;
        let transport = UTransportLola::build(config).unwrap();
        let topic_a = UUri::try_from("//vehicle/4210/1/9012").expect("valid URI");
        let topic_b = UUri::try_from("//vehicle/4210/1/9013").expect("valid URI");
        let target = UUri::try_from("//vehicle/4210/1/9014").expect("valid URI");
        send_payload(&transport, topic_a.clone(), b"a");
        send_payload(&transport, topic_b, b"b");

        let error = match block_on_ready(transport.receive_zero_copy(&target, None)) {
            Ok(_) => panic!("LoLa pull receive should reject the newest mismatch"),
            Err(error) => error,
        };

        assert_eq!(error.get_code(), UCode::ResourceExhausted);
        let diagnostics = transport.pull_mismatch_queue_diagnostics();
        assert_eq!(diagnostics.current_depth, 1);
        assert_eq!(diagnostics.rejected_mismatches, 1);
        let frame_a = block_on_ready(transport.receive_zero_copy(&topic_a, None)).unwrap();
        assert_eq!(frame_a.try_contiguous_payload(), Some(b"a".as_slice()));
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_registered_listener_receives_matching_payload() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let topic = UUri::try_from("//vehicle/4210/1/9015").expect("valid URI");
        let listener = Arc::new(RecordingListener::default());
        let registration: Arc<dyn UZeroCopyListener<LolaRxLease>> = listener.clone();

        block_on_ready(transport.register_zero_copy_listener(&topic, None, registration)).unwrap();
        send_payload(&transport, topic, b"listen");

        assert_eq!(listener.payloads(), vec![b"listen".to_vec()]);
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_unregistered_listener_stops_receiving_payloads() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let topic = UUri::try_from("//vehicle/4210/1/9016").expect("valid URI");
        let listener = Arc::new(RecordingListener::default());
        let registration: Arc<dyn UZeroCopyListener<LolaRxLease>> = listener.clone();

        block_on_ready(transport.register_zero_copy_listener(
            &topic,
            None,
            Arc::clone(&registration),
        ))
        .unwrap();
        block_on_ready(transport.unregister_zero_copy_listener(&topic, None, registration))
            .unwrap();
        send_payload(&transport, topic, b"ignored");

        assert!(listener.payloads().is_empty());
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn tx_loan_rejects_alignment_larger_than_sample_alignment() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let error = match block_on_ready(transport.loan_tx(tx_spec(1, 16))) {
            Ok(_) => panic!("LoLa TX loan should reject excessive alignment"),
            Err(error) => error,
        };
        assert_eq!(error.get_code(), UCode::InvalidArgument);
    }

    #[cfg(not(any(feature = "test-stub", feature = "native")))]
    #[test]
    fn tx_loan_requires_backend_feature() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let error = match block_on_ready(transport.loan_tx(tx_spec(1, 1))) {
            Ok(_) => panic!("LoLa TX loan should require a backend feature"),
            Err(error) => error,
        };
        assert_eq!(error.get_code(), UCode::FailedPrecondition);
    }
}

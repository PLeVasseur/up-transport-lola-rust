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
use up_rust::selected_wire_user_api::{UNativePrefixWireTransport, UWithNativePrefixWire};
use up_rust::wire_implementer_api::UWire;
use up_rust::{
    PreparedTxLoanSpec, UCode, UEncodedZeroCopyListener, UStatus, UUri, UZeroCopyTransportCore,
    UZeroCopyUninitTransportCore,
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
/// Public zero-copy and owned-frame endpoints are composed by selecting a wire
/// over [`LolaZeroCopyCore`] or the feature-gated `LolaOwnedCore`.
pub struct UTransportLola {
    config: LolaTransportConfig,
    self_ref: Weak<UTransportLola>,
    listeners: Mutex<Vec<ListenerRegistration>>,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    pending: Mutex<VecDeque<LolaRxLease>>,
    pull_mismatch_queue: Mutex<PullMismatchQueueState>,
    #[cfg(feature = "lola-ffi")]
    native: NativeTransport,
    #[cfg(feature = "lola-ffi")]
    subscriber: Mutex<Option<NativeSubscriber>>,
}

/// Cloneable LoLa mechanics core used to construct selected-wire transports.
#[derive(Clone)]
pub struct LolaZeroCopyCore {
    inner: Arc<UTransportLola>,
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
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
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

    /// Returns a cloneable core for explicit selected-wire construction.
    #[must_use]
    pub fn zero_copy_core(self: &Arc<Self>) -> LolaZeroCopyCore {
        LolaZeroCopyCore {
            inner: Arc::clone(self),
        }
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
                UCode::FailedPrecondition,
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
                        listener.on_receive_encoded_zero_copy(frame).await;
                    }
                }
                Err(status) => {
                    if status.code() == UCode::InvalidArgument {
                        eprintln!("discarding invalid LoLa native listener sample: {status:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn poll_listener_frames(
        &self,
    ) -> Result<Vec<(Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>, LolaRxLease)>, UStatus> {
        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            let Some(frame) = self.pending.lock().await.pop_front() else {
                return Ok(Vec::new());
            };
            let listeners = {
                let listeners = self.listeners.lock().await;
                listeners
                    .iter()
                    .filter(|registration| registration.matches_frame(&frame))
                    .map(|registration| Arc::clone(&registration.listener))
                    .collect::<Vec<_>>()
            };
            let mut deliveries = Vec::with_capacity(listeners.len());
            for listener in listeners {
                deliveries.push((listener, frame.clone_for_stub()?));
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
                    Err(status) if status.code() == UCode::NotFound => {}
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
        let source = frame.routing_hint().0.to_uri(false);
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
    listener: Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>,
    #[cfg(feature = "lola-ffi")]
    subscriber: NativeSubscriber,
}

impl ListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>,
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
        listener: &Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>,
    ) -> bool {
        self.source_filter == *source_filter
            && self.sink_filter.as_ref() == sink_filter
            && Arc::ptr_eq(&self.listener, listener)
    }
}

#[async_trait]
impl UZeroCopyTransportCore for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_prepared_tx(&self, spec: PreparedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let alignment = spec.payload_alignment_proof().as_usize();
        let (metadata, encoded_metadata, payload_len, _) = spec.into_parts();
        validate_alignment(alignment)?;
        if alignment > self.config.sample_alignment {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!(
                    "requested payload alignment {alignment} exceeds LoLa sample alignment {}",
                    self.config.sample_alignment
                ),
            ));
        }
        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            LolaTxLoan::new_vec(
                metadata,
                encoded_metadata,
                self.config.sample_size,
                payload_len,
                alignment,
            )
        }
        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.loan_sample()?;
            LolaTxLoan::new_native(metadata, encoded_metadata, sample, payload_len, alignment)
        }
    }

    async fn send_prepared_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            self.pending.lock().await.push_back(buffer.into_stub_rx()?);
            Ok(())
        }
        #[cfg(feature = "lola-ffi")]
        {
            let loan = buffer.into_native()?;
            self.native.send(loan)
        }
    }

    async fn receive_encoded_zero_copy(
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
        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            let mut pending = self.pending.lock().await;
            while let Some(frame) = pending.pop_front() {
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
                }
                drop(pending);
                self.queue_pull_sample(frame).await?;
                pending = self.pending.lock().await;
            }
            Err(UStatus::fail_with_code(
                UCode::NotFound,
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

    async fn register_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        {
            let mut listeners = self.listeners.lock().await;
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
        }
        self.ensure_listener_task().await
    }

    async fn unregister_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let should_stop = {
            let mut listeners = self.listeners.lock().await;
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
        if should_stop {
            if let Some(task) = self.listener_task.lock().await.take() {
                task.abort();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl UZeroCopyUninitTransportCore for UTransportLola {
    type UninitTx = LolaUninitTxLoan;

    async fn loan_prepared_uninit_tx(
        &self,
        spec: PreparedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let alignment = spec.payload_alignment_proof().as_usize();
        let (metadata, encoded_metadata, payload_len, _) = spec.into_parts();
        validate_alignment(alignment)?;
        if alignment > self.config.sample_alignment {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!(
                    "requested payload alignment {alignment} exceeds LoLa sample alignment {}",
                    self.config.sample_alignment
                ),
            ));
        }
        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            LolaUninitTxLoan::new_vec(
                metadata,
                encoded_metadata,
                self.config.sample_size,
                payload_len,
                alignment,
            )
        }
        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.loan_sample()?;
            LolaUninitTxLoan::new_native(metadata, encoded_metadata, sample, payload_len, alignment)
        }
    }
}

impl LolaZeroCopyCore {
    /// Wraps this core in the generic native-prefix selected-wire adapter.
    #[must_use]
    pub fn with_selected_wire<W>(self, wire: W) -> UNativePrefixWireTransport<Self, W>
    where
        W: UWire,
    {
        self.into_native_prefix_wire_transport(wire)
    }
}

#[async_trait]
impl UZeroCopyTransportCore for LolaZeroCopyCore {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_prepared_tx(&self, spec: PreparedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        self.inner.loan_prepared_tx(spec).await
    }

    async fn send_prepared_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        self.inner.send_prepared_zero_copy(buffer).await
    }

    async fn receive_encoded_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        self.inner
            .receive_encoded_zero_copy(source_filter, sink_filter)
            .await
    }

    async fn register_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        self.inner
            .register_encoded_zero_copy_listener(source_filter, sink_filter, listener)
            .await
    }

    async fn unregister_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        self.inner
            .unregister_encoded_zero_copy_listener(source_filter, sink_filter, listener)
            .await
    }
}

#[async_trait]
impl UZeroCopyUninitTransportCore for LolaZeroCopyCore {
    type UninitTx = LolaUninitTxLoan;

    async fn loan_prepared_uninit_tx(
        &self,
        spec: PreparedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        self.inner.loan_prepared_uninit_tx(spec).await
    }
}

fn validate_alignment(alignment: usize) -> Result<(), UStatus> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "payload alignment must be a non-zero power of two",
        ));
    }
    Ok(())
}

fn frame_matches(frame: &LolaRxLease, source_filter: &UUri, sink_filter: Option<&UUri>) -> bool {
    let (source, sink) = frame.routing_hint();
    source_filter.matches(source)
        && sink_filter.is_none_or(|filter| sink.is_some_and(|sink| filter.matches(sink)))
}

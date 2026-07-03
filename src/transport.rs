/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};
#[cfg(feature = "lola-ffi")]
use std::{sync::Weak, time::Duration};

use async_trait::async_trait;
#[cfg(feature = "lola-ffi")]
use tokio::task::JoinHandle;
use up_rust::selected_wire_user_api::{UNativePrefixWireTransport, UWithNativePrefixWire};
use up_rust::transport_implementer_api::{
    PreparedTxLoanSpec, UEncodedZeroCopyListener, UZeroCopyTransportCore,
    UZeroCopyUninitTransportCore,
};
use up_rust::wire_implementer_api::UWire;
use up_rust::{
    UCode, UFrameView, UStatus, UUri, UZeroCopyListener, UZeroCopyTransportImpl,
    UZeroCopyUninitTransportImpl, ValidatedTxLoanSpec,
};

#[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
use crate::config::LolaPullMismatchQueueFullPolicy;
#[cfg(feature = "lola-ffi")]
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
    #[cfg(feature = "lola-ffi")]
    self_ref: Weak<UTransportLola>,
    #[cfg(feature = "test-stub")]
    pending_samples: Mutex<VecDeque<LolaRxLease>>,
    encoded_listeners: Mutex<Vec<EncodedListenerRegistration>>,
    pull_mismatch_queue: Mutex<PullMismatchQueueState>,
    #[cfg(feature = "lola-ffi")]
    listener_task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(feature = "lola-ffi")]
    native: NativeTransport,
    #[cfg(feature = "lola-ffi")]
    subscriber: Mutex<Option<NativeSubscriber>>,
    #[cfg(feature = "lola-ffi")]
    listener_subscriber: Mutex<Option<NativeSubscriber>>,
}

/// Selected-wire zero-copy core for LoLa native-frame transport operations.
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
        Ok(Arc::new_cyclic(|_self_ref| Self {
            config,
            #[cfg(feature = "lola-ffi")]
            self_ref: _self_ref.clone(),
            #[cfg(feature = "test-stub")]
            pending_samples: Mutex::new(VecDeque::new()),
            encoded_listeners: Mutex::new(Vec::new()),
            pull_mismatch_queue: Mutex::new(PullMismatchQueueState::default()),
            #[cfg(feature = "lola-ffi")]
            listener_task: Mutex::new(None),
            #[cfg(feature = "lola-ffi")]
            native,
            #[cfg(feature = "lola-ffi")]
            subscriber: Mutex::new(None),
            #[cfg(feature = "lola-ffi")]
            listener_subscriber: Mutex::new(None),
        }))
    }

    /// Returns the configuration used to build this transport.
    #[must_use]
    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
    }

    /// Returns a cloneable core for explicit selected-wire adapter construction.
    #[must_use]
    pub fn zero_copy_core(self: &Arc<Self>) -> LolaZeroCopyCore {
        LolaZeroCopyCore {
            inner: Arc::clone(self),
        }
    }

    /// Returns diagnostics for the bounded pull mismatch queue.
    pub fn pull_mismatch_queue_diagnostics(&self) -> LolaPullMismatchQueueDiagnostics {
        self.pull_mismatch_queue
            .lock()
            .expect("LoLa pull mismatch queue lock poisoned")
            .diagnostics()
    }

    #[cfg(feature = "lola-ffi")]
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

    #[cfg(feature = "lola-ffi")]
    fn receive_next_listener_frame(&self) -> Result<LolaRxLease, UStatus> {
        let mut subscriber = self
            .listener_subscriber
            .lock()
            .expect("LoLa native listener subscriber lock poisoned");
        if subscriber.is_none() {
            *subscriber = Some(NativeSubscriber::new(&self.config)?);
        }
        let sample = subscriber
            .as_ref()
            .expect("LoLa listener subscriber should be initialized")
            .receive()?;
        LolaRxLease::from_native(sample)
    }

    #[cfg(feature = "lola-ffi")]
    fn drop_listener_subscriber(&self) {
        self.listener_subscriber
            .lock()
            .expect("LoLa native listener subscriber lock poisoned")
            .take();
    }

    #[cfg(feature = "lola-ffi")]
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

    #[cfg(feature = "lola-ffi")]
    async fn listener_loop(self_ref: Weak<Self>) {
        loop {
            let Some(transport) = self_ref.upgrade() else {
                break;
            };

            if transport
                .encoded_listeners
                .lock()
                .expect("LoLa encoded listener registry lock poisoned")
                .is_empty()
            {
                transport.drop_listener_subscriber();
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
                        listener.on_receive_encoded_zero_copy(frame).await;
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

    #[cfg(feature = "lola-ffi")]
    fn poll_native_listener_frames(
        &self,
    ) -> Result<Vec<(Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>, LolaRxLease)>, UStatus> {
        let frame = match self.receive_next_listener_frame() {
            Ok(frame) => frame,
            Err(status) if status.get_code() == UCode::NotFound => return Ok(Vec::new()),
            Err(status) => return Err(status),
        };
        let listeners = self
            .encoded_listeners
            .lock()
            .expect("LoLa encoded listener registry lock poisoned");
        Ok(listeners
            .iter()
            .filter(|registration| registration.matches_frame(&frame))
            .map(|registration| (Arc::clone(&registration.listener), frame.clone()))
            .collect())
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

    #[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
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
    async fn deliver_test_stub_encoded_frame(&self, frame: &LolaRxLease) -> Result<(), UStatus> {
        let listeners = {
            let listeners = self
                .encoded_listeners
                .lock()
                .expect("LoLa encoded listener registry lock poisoned");
            listeners
                .iter()
                .filter(|registration| registration.matches_frame(frame))
                .map(|registration| Arc::clone(&registration.listener))
                .collect::<Vec<_>>()
        };

        for listener in listeners {
            listener.on_receive_encoded_zero_copy(frame.clone()).await;
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

struct EncodedListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    listener: Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>,
}

impl EncodedListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<LolaRxLease>>,
    ) -> Self {
        Self {
            source_filter: source_filter.to_owned(),
            sink_filter: sink_filter.map(ToOwned::to_owned),
            listener,
        }
    }

    #[cfg(any(feature = "test-stub", feature = "lola-ffi"))]
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

impl LolaZeroCopyCore {
    /// Wraps this core in the generic selected-wire adapter.
    #[must_use]
    pub fn with_selected_wire<W>(self, wire: W) -> UNativePrefixWireTransport<Self, W>
    where
        W: UWire,
    {
        self.into_native_prefix_wire_transport(wire)
    }
}

#[async_trait]
impl UZeroCopyTransportImpl for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_validated_tx(&self, spec: ValidatedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let _ = spec;
        Err(selected_wire_required())
    }

    async fn send_validated_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        let _ = buffer;
        Err(selected_wire_required())
    }

    async fn receive_validated_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        if let Some(frame) = self.pop_queued_pull_sample(source_filter, sink_filter) {
            return Ok(frame);
        }

        #[cfg(feature = "lola-ffi")]
        {
            loop {
                let frame = self.receive_next_zero_copy()?;
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
                }
                self.queue_pull_sample(frame)?;
            }
        }

        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            loop {
                let frame = self
                    .pending_samples
                    .lock()
                    .expect("LoLa test-stub pending sample lock poisoned")
                    .pop_front();
                let Some(frame) = frame else {
                    return Err(UStatus::fail_with_code(
                        UCode::NotFound,
                        "no LoLa sample available",
                    ));
                };
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
                }
                self.queue_pull_sample(frame)?;
            }
        }

        #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
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
        let _ = (source_filter, sink_filter, listener);
        Err(selected_wire_required())
    }

    async fn unregister_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let _ = (source_filter, sink_filter, listener);
        Err(selected_wire_required())
    }
}

#[async_trait]
impl UZeroCopyUninitTransportImpl for UTransportLola {
    type UninitTx = LolaUninitTxLoan;

    async fn loan_validated_uninit_tx(
        &self,
        spec: ValidatedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let _ = spec;
        Err(selected_wire_required())
    }
}

#[async_trait]
impl UZeroCopyTransportCore for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_prepared_tx(&self, spec: PreparedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let metadata = spec.metadata().clone();
        let encoded_metadata = spec.encoded_metadata().to_vec();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment_proof().as_usize();
        self.validate_payload_alignment(alignment)?;

        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.loan_sample()?;
            return LolaTxLoan::new_native(
                metadata,
                encoded_metadata,
                sample,
                payload_len,
                alignment,
            );
        }

        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            return LolaTxLoan::new_vec(
                metadata,
                encoded_metadata,
                self.config.sample_size,
                payload_len,
                alignment,
            );
        }

        #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
        {
            let _ = (metadata, encoded_metadata, payload_len);
            Err(backend_unavailable())
        }
    }

    async fn send_prepared_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        #[cfg(feature = "lola-ffi")]
        {
            let loan = buffer.into_native()?;
            return self.native.send(loan);
        }

        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            let frame = buffer.clone_as_rx();
            self.deliver_test_stub_encoded_frame(&frame).await?;
            self.pending_samples
                .lock()
                .expect("LoLa test-stub pending sample lock poisoned")
                .push_back(frame);
            return Ok(());
        }

        #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
        {
            let _ = buffer;
            Err(backend_unavailable())
        }
    }

    async fn receive_encoded_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        // LoLa stores selected-wire metadata bytes in the physical ULOL frame.
        // Public source/sink filtering happens after UWireRx decodes them.
        let _ = (source_filter, sink_filter);

        #[cfg(feature = "lola-ffi")]
        {
            self.receive_next_zero_copy()
        }

        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            self.pending_samples
                .lock()
                .expect("LoLa test-stub pending sample lock poisoned")
                .pop_front()
                .ok_or_else(|| UStatus::fail_with_code(UCode::NotFound, "no LoLa sample available"))
        }

        #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
        {
            Err(backend_unavailable())
        }
    }

    async fn register_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let mut listeners = self
            .encoded_listeners
            .lock()
            .expect("LoLa encoded listener registry lock poisoned");
        if listeners.iter().any(|registration| {
            registration.has_same_identity(source_filter, sink_filter, &listener)
        }) {
            return Err(UStatus::fail_with_code(
                UCode::AlreadyExists,
                "LoLa encoded listener already registered for filters",
            ));
        }
        listeners.push(EncodedListenerRegistration::new(
            source_filter,
            sink_filter,
            listener,
        ));
        drop(listeners);

        #[cfg(feature = "lola-ffi")]
        self.ensure_listener_task()?;

        Ok(())
    }

    async fn unregister_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let should_stop = {
            let mut listeners = self
                .encoded_listeners
                .lock()
                .expect("LoLa encoded listener registry lock poisoned");
            let Some(index) = listeners.iter().position(|registration| {
                registration.has_same_identity(source_filter, sink_filter, &listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NotFound,
                    "no such LoLa encoded listener registered for filters",
                ));
            };
            listeners.remove(index);
            listeners.is_empty()
        };

        #[cfg(feature = "lola-ffi")]
        if should_stop {
            self.drop_listener_subscriber();
            if let Some(task) = self
                .listener_task
                .lock()
                .expect("LoLa listener task lock poisoned")
                .take()
            {
                task.abort();
            }
        }

        #[cfg(not(feature = "lola-ffi"))]
        let _ = should_stop;

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
        let metadata = spec.metadata().clone();
        let encoded_metadata = spec.encoded_metadata().to_vec();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment_proof().as_usize();
        self.validate_payload_alignment(alignment)?;

        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.loan_sample()?;
            return LolaUninitTxLoan::new_native(
                metadata,
                encoded_metadata,
                sample,
                payload_len,
                alignment,
            );
        }

        #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
        {
            return LolaUninitTxLoan::new_vec(
                metadata,
                encoded_metadata,
                self.config.sample_size,
                payload_len,
                alignment,
            );
        }

        #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
        {
            let _ = (metadata, encoded_metadata, payload_len);
            Err(backend_unavailable())
        }
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

fn selected_wire_required() -> UStatus {
    UStatus::fail_with_code(
        UCode::FailedPrecondition,
        "wrap UTransportLola.zero_copy_core() in a selected-wire adapter for LoLa transport operations",
    )
}

#[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
fn backend_unavailable() -> UStatus {
    UStatus::fail_with_code(
        UCode::FailedPrecondition,
        "enable the LoLa test-stub or native backend feature to use zero-copy samples",
    )
}

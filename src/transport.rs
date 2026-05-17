/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#[cfg(feature = "test-stub")]
use std::collections::VecDeque;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinHandle};
use up_rust::{
    transport::{validate_frame_metadata_for_payload, verify_filter_criteria},
    zero_copy::{UZeroCopyListener, UZeroCopyRxFrame, UZeroCopyTransport},
    UCode, UFrameMetadata, UStatus, UUri,
};

#[cfg(feature = "lola-ffi")]
use crate::sys::NativeTransport;
use crate::{
    config::LolaTransportConfig,
    frame::{LolaRxLease, LolaTxLoan},
};

pub struct UTransportLola {
    config: LolaTransportConfig,
    self_ref: Weak<UTransportLola>,
    listeners: Mutex<Vec<ListenerRegistration>>,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(feature = "test-stub")]
    pending: Mutex<VecDeque<LolaRxLease>>,
    #[cfg(feature = "lola-ffi")]
    native: NativeTransport,
}

impl UTransportLola {
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
            #[cfg(feature = "lola-ffi")]
            native,
        }))
    }

    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
    }

    async fn receive_next_zero_copy(&self) -> Result<LolaRxLease, UStatus> {
        #[cfg(feature = "test-stub")]
        {
            self.pending.lock().await.pop_front().ok_or_else(|| {
                UStatus::fail_with_code(UCode::NOT_FOUND, "no LoLa sample available")
            })
        }
        #[cfg(feature = "lola-ffi")]
        loop {
            let sample = self.native.receive()?;
            match LolaRxLease::from_native(sample) {
                Ok(frame) => return Ok(frame),
                Err(status) if status.get_code() == UCode::INVALID_ARGUMENT => continue,
                Err(status) => return Err(status),
            }
        }
    }

    async fn ensure_listener_task(&self) -> Result<(), UStatus> {
        let mut task = self.listener_task.lock().await;
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }

        let transport = self.self_ref.upgrade().ok_or_else(|| {
            UStatus::fail_with_code(
                UCode::FAILED_PRECONDITION,
                "LoLa transport is shutting down",
            )
        })?;
        let handle = tokio::runtime::Handle::try_current().map_err(|error| {
            UStatus::fail_with_code(
                UCode::FAILED_PRECONDITION,
                format!("LoLa listener registration requires a Tokio runtime: {error}"),
            )
        })?;
        *task = Some(handle.spawn(async move { transport.listener_loop().await }));
        Ok(())
    }

    async fn listener_loop(self: Arc<Self>) {
        loop {
            if self.listeners.lock().await.is_empty() {
                break;
            }

            match self.receive_next_zero_copy().await {
                Ok(frame) => self.dispatch_zero_copy(frame).await,
                Err(status) if status.get_code() == UCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn dispatch_zero_copy(&self, frame: LolaRxLease) {
        let listener = {
            let listeners = self.listeners.lock().await;
            listeners
                .iter()
                .find(|registration| registration.matches_frame(&frame))
                .map(|registration| Arc::clone(&registration.listener))
        };
        if let Some(listener) = listener {
            listener.on_receive_zero_copy(frame).await;
        }
    }
}

struct ListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    listener: Arc<dyn UZeroCopyListener<LolaRxLease>>,
}

impl ListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<LolaRxLease>>,
    ) -> Self {
        Self {
            source_filter: source_filter.to_owned(),
            sink_filter: sink_filter.map(ToOwned::to_owned),
            listener,
        }
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
impl UZeroCopyTransport for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn reserve(
        &self,
        metadata: UFrameMetadata,
        payload_len: usize,
        alignment: usize,
    ) -> Result<Self::Tx, UStatus> {
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
        if metadata.encoding().is_none() && payload_len != 0 {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "message payload is present but payload encoding is absent",
            ));
        }
        validate_frame_metadata_for_payload(&metadata, metadata.encoding().is_some())?;

        #[cfg(feature = "test-stub")]
        {
            LolaTxLoan::new_vec(metadata, self.config.sample_size, payload_len, alignment)
        }
        #[cfg(feature = "lola-ffi")]
        {
            let sample = self.native.reserve()?;
            LolaTxLoan::new_native(metadata, sample, payload_len, alignment)
        }
    }

    async fn send_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        #[cfg(feature = "test-stub")]
        {
            let sample = buffer.into_vec()?;
            let lease = LolaRxLease::from_vec(sample)?;
            self.pending.lock().await.push_back(lease);
            Ok(())
        }
        #[cfg(feature = "lola-ffi")]
        {
            let loan = buffer.into_native()?;
            self.native.send(loan)
        }
    }

    async fn receive_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
        #[cfg(feature = "test-stub")]
        {
            let mut pending = self.pending.lock().await;
            while let Some(sample) = pending.pop_front() {
                if frame_matches(&sample, source_filter, sink_filter) {
                    return Ok(sample);
                }
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
        }
    }

    async fn register_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
        let registration = ListenerRegistration::new(source_filter, sink_filter, listener);
        {
            let mut listeners = self.listeners.lock().await;
            if listeners.iter().any(|existing| {
                filters_overlap(
                    &existing.source_filter,
                    existing.sink_filter.as_ref(),
                    source_filter,
                    sink_filter,
                )
            }) {
                return Err(UStatus::fail_with_code(
                    UCode::ALREADY_EXISTS,
                    "LoLa listener filters overlap an existing registration",
                ));
            }
            listeners.push(registration);
        }
        self.ensure_listener_task().await
    }

    async fn unregister_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
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

fn filters_overlap(
    left_source: &UUri,
    left_sink: Option<&UUri>,
    right_source: &UUri,
    right_sink: Option<&UUri>,
) -> bool {
    uri_patterns_overlap(left_source, right_source) && sink_filters_overlap(left_sink, right_sink)
}

fn sink_filters_overlap(left: Option<&UUri>, right: Option<&UUri>) -> bool {
    left.is_none() || right.is_none() || uri_patterns_overlap(left.unwrap(), right.unwrap())
}

fn uri_patterns_overlap(left: &UUri, right: &UUri) -> bool {
    string_field_overlaps(
        left.has_wildcard_authority(),
        left.authority_name(),
        right.has_wildcard_authority(),
        right.authority_name(),
    ) && integer_field_overlaps(
        left.has_wildcard_entity_instance(),
        left.uentity_instance_id(),
        right.has_wildcard_entity_instance(),
        right.uentity_instance_id(),
    ) && integer_field_overlaps(
        left.has_wildcard_entity_type(),
        left.uentity_type_id(),
        right.has_wildcard_entity_type(),
        right.uentity_type_id(),
    ) && integer_field_overlaps(
        left.has_wildcard_version(),
        left.uentity_major_version(),
        right.has_wildcard_version(),
        right.uentity_major_version(),
    ) && integer_field_overlaps(
        left.has_wildcard_resource_id(),
        left.resource_id(),
        right.has_wildcard_resource_id(),
        right.resource_id(),
    )
}

fn string_field_overlaps(
    left_wildcard: bool,
    left: String,
    right_wildcard: bool,
    right: String,
) -> bool {
    left_wildcard || right_wildcard || left == right
}

fn integer_field_overlaps<T>(left_wildcard: bool, left: T, right_wildcard: bool, right: T) -> bool
where
    T: Eq,
{
    left_wildcard || right_wildcard || left == right
}

#[cfg(all(test, feature = "test-stub", not(feature = "lola-ffi")))]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use up_rust::{
        zero_copy::{UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyListener, UZeroCopyTransport},
        UCode, UFrameBuilder, UFrameMetadata, UUri,
    };

    use super::*;

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
            mw_com_config_path: None,
        }
    }

    #[tokio::test]
    async fn reserve_send_receive_round_trips_payload_in_stub_backend() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        let mut loan = transport
            .reserve(frame.metadata().clone(), frame.payload_bytes().len(), 1)
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
    async fn registered_listener_receives_matching_stub_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let listener: Arc<dyn UZeroCopyListener<LolaRxLease>> = Arc::new(ListenerSender(sender));

        transport
            .register_zero_copy_listener(&topic, None, Arc::clone(&listener))
            .await
            .unwrap();

        let mut loan = transport
            .reserve(frame.metadata().clone(), frame.payload_bytes().len(), 1)
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
    async fn listener_registration_rejects_overlapping_filters() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let broad_topic = UUri::any_with_resource_id(topic.resource_id_raw());
        let (sender_a, _receiver_a) = mpsc::unbounded_channel();
        let (sender_b, _receiver_b) = mpsc::unbounded_channel();
        let listener_a: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(ListenerSender(sender_a));
        let listener_b: Arc<dyn UZeroCopyListener<LolaRxLease>> =
            Arc::new(ListenerSender(sender_b));

        transport
            .register_zero_copy_listener(&topic, None, listener_a)
            .await
            .unwrap();
        let status = transport
            .register_zero_copy_listener(&broad_topic, None, listener_b)
            .await
            .unwrap_err();

        assert_eq!(status.get_code(), UCode::ALREADY_EXISTS);
    }
}

#[cfg(all(test, feature = "native-smoke"))]
mod native_tests {
    use up_rust::{
        zero_copy::{UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyTransport},
        UFrameBuilder, UUri,
    };

    use super::*;

    fn native_smoke_config() -> LolaTransportConfig {
        let fixture_config_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/mw_com_config.json"
        );
        let mw_com_config_path = std::env::var("LOLA_NATIVE_SMOKE_CONFIG")
            .unwrap_or_else(|_| fixture_config_path.to_string());
        LolaTransportConfig {
            local_authority: std::env::var("LOLA_NATIVE_SMOKE_AUTHORITY")
                .unwrap_or_else(|_| "vehicle".to_string()),
            instance_specifier: std::env::var("LOLA_NATIVE_SMOKE_INSTANCE_SPECIFIER")
                .unwrap_or_else(|_| "uprotocol/transport".to_string()),
            service_type: std::env::var("LOLA_NATIVE_SMOKE_SERVICE_TYPE")
                .unwrap_or_else(|_| "/uprotocol/Transport".to_string()),
            event_name: std::env::var("LOLA_NATIVE_SMOKE_EVENT_NAME")
                .unwrap_or_else(|_| "frame".to_string()),
            sample_size: std::env::var("LOLA_NATIVE_SMOKE_SAMPLE_SIZE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(512),
            sample_alignment: std::env::var("LOLA_NATIVE_SMOKE_SAMPLE_ALIGNMENT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8),
            max_samples: std::env::var("LOLA_NATIVE_SMOKE_MAX_SAMPLES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(4),
            mw_com_config_path: Some(mw_com_config_path),
        }
    }

    #[tokio::test]
    async fn native_reserve_send_receive_round_trips_payload() {
        if std::env::var_os("LOLA_NATIVE_SMOKE_RUN").is_none() {
            eprintln!("skipping LoLa native smoke; set LOLA_NATIVE_SMOKE_RUN=1 to execute it");
            return;
        }
        let config = native_smoke_config();
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        for _ in 0..100 {
            let _ = transport.receive_zero_copy(&topic, None).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut loan = transport
            .reserve(frame.metadata().clone(), frame.payload_bytes().len(), 1)
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
}

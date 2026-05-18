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
use crate::sys::{NativeSubscriber, NativeTransport};
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
    pending: Mutex<VecDeque<Vec<u8>>>,
    #[cfg(feature = "lola-ffi")]
    native: NativeTransport,
    #[cfg(feature = "lola-ffi")]
    subscriber: Mutex<Option<NativeSubscriber>>,
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
            #[cfg(feature = "lola-ffi")]
            subscriber: Mutex::new(None),
        }))
    }

    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
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
            self.pending.lock().await.push_back(sample);
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
                let frame = LolaRxLease::from_vec(sample)?;
                if frame_matches(&frame, source_filter, sink_filter) {
                    return Ok(frame);
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

#[cfg(all(test, feature = "test-stub", not(feature = "lola-ffi")))]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use up_rust::{
        wire::{RawBytes, WireFormat},
        zero_copy::{
            UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyListener, UZeroCopyTransport,
            UZeroCopyTransportExt,
        },
        UAttributes, UCode, UFrameBuilder, UFrameMetadata, UMessageType, UUri, UUID,
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
    async fn overlapping_stub_listeners_both_receive_matching_payload() {
        let transport = UTransportLola::build(config()).unwrap();
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9000).unwrap();
        let broad_topic = UUri::any_with_resource_id(topic.resource_id_raw());
        let frame = UFrameBuilder::publish(topic.clone())
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
            .reserve(frame.metadata().clone(), frame.payload_bytes().len(), 1)
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

        let attributes = UAttributes::new(
            UUID::build(),
            source.clone(),
            Some(sink.clone()),
            UMessageType::Notification,
        );
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                UFrameMetadata::new(attributes, RawBytes::encoding()),
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
        wire::{RawBytes, WireFormat},
        zero_copy::{
            UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyListener, UZeroCopyTransport,
            UZeroCopyTransportExt,
        },
        UAttributes, UFrameBuilder, UFrameMetadata, UMessageType, UUri, UUID,
    };

    use super::*;

    static NATIVE_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

    struct NativeListenerSender(mpsc::UnboundedSender<Vec<u8>>);

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
            mw_com_config_path: Some(mw_com_config_path),
        }
    }

    async fn native_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        NATIVE_TEST_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    #[tokio::test]
    #[ignore = "requires the native S-CORE LoLa runtime fixture"]
    async fn native_reserve_send_receive_round_trips_payload() {
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
            .reserve(frame.metadata().clone(), frame.payload_bytes().len(), 1)
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
                UFrameMetadata::publish(topic.clone()),
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

        let attributes = UAttributes::new(
            UUID::build(),
            source.clone(),
            Some(sink.clone()),
            UMessageType::Notification,
        );
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                UFrameMetadata::new(attributes, RawBytes::encoding()),
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

/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#[cfg(feature = "test-stub")]
use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "test-stub")]
use tokio::sync::Mutex;
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
        Ok(Arc::new(Self {
            config,
            #[cfg(feature = "test-stub")]
            pending: Mutex::new(VecDeque::new()),
            #[cfg(feature = "lola-ffi")]
            native,
        }))
    }

    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
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
            let sample = LolaRxLease::from_native(self.native.receive()?)?;
            if frame_matches(&sample, source_filter, sink_filter) {
                return Ok(sample);
            }
        }
    }

    async fn register_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        _listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "LoLa listener bridge is not wired yet",
        ))
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
    use up_rust::{
        zero_copy::{UTxBuffer, UZeroCopyRxFrame, UZeroCopyTransport},
        UFrameBuilder, UUri,
    };

    use super::*;

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
        let received = transport.receive_zero_copy(&topic, None).await.unwrap();

        assert_eq!(received.metadata(), frame.metadata());
        assert_eq!(received.payload(), frame.payload_bytes());
    }
}

#[cfg(all(test, feature = "lola-ffi"))]
mod native_tests {
    use up_rust::{
        zero_copy::{UTxBuffer, UZeroCopyTransport},
        UFrameBuilder, UUri,
    };

    use super::*;

    fn native_smoke_config() -> Option<LolaTransportConfig> {
        let mw_com_config_path = std::env::var("LOLA_NATIVE_SMOKE_CONFIG").ok()?;
        Some(LolaTransportConfig {
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
        })
    }

    #[tokio::test]
    #[ignore = "requires a matching Eclipse S-CORE mw_com_config.json and shared-memory runtime"]
    async fn native_reserve_send_receive_round_trips_payload() {
        let Some(config) = native_smoke_config() else {
            return;
        };
        let authority = config.local_authority.clone();
        let transport = UTransportLola::build(config).unwrap();
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9000).unwrap();
        let frame = UFrameBuilder::publish(topic.clone())
            .build_with_raw_payload(b"payload".as_slice())
            .unwrap();
        let mut loan = transport
            .reserve(frame.metadata().clone(), frame.payload_bytes().len(), 1)
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(frame.payload_bytes());

        transport.send_zero_copy(loan).await.unwrap();
        let received = transport.receive_zero_copy(&topic, None).await.unwrap();

        assert_eq!(received.metadata(), frame.metadata());
        assert_eq!(received.payload(), frame.payload_bytes());
    }
}

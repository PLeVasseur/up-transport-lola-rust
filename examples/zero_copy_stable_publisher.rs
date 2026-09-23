/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use up_rust::{StableContainerPayload, UFrameMetadata, UUri, UWithNativePrefixWire};
use up_transport_lola_rust::{LolaTransportConfig, UTransportLola};

#[path = "support/sensor_profile.rs"]
mod sensor_profile;
use sensor_profile::{NoZeroSensorFrame, NoZeroSensorHeader};

fn config() -> LolaTransportConfig {
    LolaTransportConfig {
        local_authority: std::env::var("LOLA_AUTHORITY").unwrap_or_else(|_| "vehicle".to_string()),
        instance_specifier: std::env::var("LOLA_INSTANCE_SPECIFIER")
            .unwrap_or_else(|_| "uprotocol/transport".to_string()),
        service_type: std::env::var("LOLA_SERVICE_TYPE")
            .unwrap_or_else(|_| "/uprotocol/Transport".to_string()),
        event_name: std::env::var("LOLA_EVENT_NAME").unwrap_or_else(|_| "frame".to_string()),
        sample_size: std::env::var("LOLA_SAMPLE_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8192),
        sample_alignment: std::env::var("LOLA_SAMPLE_ALIGNMENT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8),
        max_samples: std::env::var("LOLA_MAX_SAMPLES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(4),
        pull_mismatch_queue_capacity: std::env::var("LOLA_PULL_MISMATCH_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY),
        pull_mismatch_queue_full_policy:
            LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
        mw_com_config_path: std::env::var("LOLA_MW_COM_CONFIG").ok(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config();
    let authority = config.local_authority.clone();
    let transport = UTransportLola::build(config)?;
    let profile = sensor_profile::agreed_profile()?;
    let transport = transport
        .zero_copy_core()
        .into_stable_container_transport(profile.clone());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9000)?;

    for sequence in 1_u32..=100 {
        let checksum = 0x5eed_0000 | sequence;
        println!(
            "Publishing LoLa no-zero stable sensor frame [topic: {}, sequence: {}]",
            topic.to_uri(false),
            sequence
        );
        transport
            .send_stable_payload::<NoZeroSensorFrame, _>(
                UFrameMetadata::publish(topic.clone())
                    .with_native_payload_identity(
                        StableContainerPayload::<NoZeroSensorFrame>::identity(&profile)?,
                    )
                    .build()?,
                |frame| {
                    frame
                        .into_initializer()
                        .header(NoZeroSensorHeader {
                            case_id: 1,
                            sequence,
                            logical_payload_len: 4096,
                        })
                        .checksum(checksum)
                        .payload_fill(0x5a)
                        .finish()
                },
            )
            .await?;
        tokio::time::sleep(core::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

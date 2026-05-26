/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use up_rust::{payload::StableContainerPayload, UFrameMetadata, UUri, UZeroCopyUninitTransportExt};
use up_transport_lola_rust::{LolaTransportConfig, UTransportLola};

#[repr(C)]
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, up_rust::StablePayload, up_rust::ByteBackedStablePayload,
)]
#[stable_payload(type_name = "example.vehicle.VehiclePose")]
struct VehiclePose {
    x: u64,
    y: u64,
}

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
            .unwrap_or(512),
        sample_alignment: std::env::var("LOLA_SAMPLE_ALIGNMENT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8),
        max_samples: std::env::var("LOLA_MAX_SAMPLES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(4),
        mw_com_config_path: std::env::var("LOLA_MW_COM_CONFIG").ok(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config();
    let authority = config.local_authority.clone();
    let transport = UTransportLola::build(config)?;
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9000)?;

    for count in 1_u64..=100 {
        let pose = VehiclePose {
            x: count,
            y: count * 10,
        };
        println!(
            "Publishing LoLa stable pose [topic: {}, pose: {:?}]",
            topic.to_uri(false),
            pose
        );
        transport
            .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                UFrameMetadata::publish(topic.clone()),
                |slot| Ok(slot.write(pose)),
            )
            .await?;
        tokio::time::sleep(core::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
//! Shared same-host deployment contract for the paired sensor examples.
//! Both example binaries use these exact types and this explicit profile.

use std::sync::Arc;
use up_rust::{
    NativeProfile, NativeProfileAgreement, NativeProfileError, NativeProfileMode,
    NativeProfileTable, PayloadEncoding, StablePayload,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, up_rust::StablePayload, up_rust::StablePayloadInit)]
#[stable_payload(type_name = "org.eclipse.uprotocol.transport.example.NoZeroSensorHeader")]
pub struct NoZeroSensorHeader {
    pub case_id: u32,
    pub sequence: u32,
    pub logical_payload_len: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, up_rust::StablePayload, up_rust::StablePayloadInit)]
#[stable_payload(type_name = "org.eclipse.uprotocol.transport.example.NoZeroSensorFrame")]
pub struct NoZeroSensorFrame {
    pub header: NoZeroSensorHeader,
    pub checksum: u32,
    pub payload: [u8; 4096],
}

pub fn agreed_profile() -> Result<NativeProfileAgreement, NativeProfileError> {
    // This shared preset configures both peers in the example deployment. It is
    // not a network handshake or an allocation for arbitrary LoLa applications.
    let table = NativeProfileTable::new([(
        PayloadEncoding::private_use(0xF201),
        NoZeroSensorFrame::native_representation(),
    )])?;
    let profile = NativeProfile::new("lola.example.sensor", 1, NativeProfileMode::Table(table))?;
    NativeProfileAgreement::new(Arc::new(profile.clone()), &profile)
}

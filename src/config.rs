/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use up_rust::{UCode, UStatus};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LolaTransportConfig {
    pub local_authority: String,
    pub instance_specifier: String,
    pub service_type: String,
    pub event_name: String,
    pub sample_size: usize,
    pub sample_alignment: usize,
    pub max_samples: usize,
    pub mw_com_config_path: Option<String>,
}

impl LolaTransportConfig {
    pub fn validate(&self) -> Result<(), UStatus> {
        validate_non_empty("local_authority", &self.local_authority)?;
        validate_non_empty("instance_specifier", &self.instance_specifier)?;
        validate_non_empty("service_type", &self.service_type)?;
        validate_non_empty("event_name", &self.event_name)?;
        if self.sample_size == 0 {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "LoLa sample_size must be greater than zero",
            ));
        }
        if self.sample_alignment == 0 || !self.sample_alignment.is_power_of_two() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "LoLa sample_alignment must be a non-zero power of two",
            ));
        }
        if self.max_samples == 0 {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "LoLa max_samples must be greater than zero",
            ));
        }
        Ok(())
    }
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), UStatus> {
    if value.is_empty() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("LoLa {field} must be non-empty"),
        ));
    }
    Ok(())
}

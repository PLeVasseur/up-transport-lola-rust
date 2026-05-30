/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use up_rust::{UCode, UStatus};

/// Full-queue behavior for pull receive samples that do not match the requested filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LolaPullMismatchQueueFullPolicy {
    /// Preserve bounded pull receive behavior by dropping the oldest retained mismatch.
    DropOldestAndReport,
    /// Reject the newest mismatch and return [`UCode::RESOURCE_EXHAUSTED`] to the receive call.
    RejectNewestAndReport,
}

/// Configuration for a LoLa uProtocol transport instance.
///
/// The same configuration is used by the native bridge to create the LoLa
/// provider/skeleton path and by each listener registration to create an
/// independent proxy subscription. `sample_size`, `sample_alignment`, and
/// `max_samples` must match the corresponding S-CORE communication deployment
/// configuration for the selected service event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LolaTransportConfig {
    /// Local uProtocol authority represented by this transport.
    pub local_authority: String,
    /// S-CORE instance specifier used to locate the configured LoLa instance.
    pub instance_specifier: String,
    /// LoLa service type name configured for the generic event.
    pub service_type: String,
    /// LoLa event name that carries native uProtocol frames.
    pub event_name: String,
    /// Fixed LoLa event sample size in bytes.
    ///
    /// The `ULOL` header, encoded metadata, alignment padding, payload, and any
    /// unused tail all live within this sample size.
    pub sample_size: usize,
    /// Required LoLa event sample alignment in bytes.
    ///
    /// This must be a non-zero power of two and must be at least as strict as any
    /// serializer alignment requested through `loan_tx`.
    pub sample_alignment: usize,
    /// Maximum number of samples configured for the LoLa event.
    pub max_samples: usize,
    /// Maximum retained pull samples that did not match the requested filter.
    pub pull_mismatch_queue_capacity: usize,
    /// Policy applied when the pull mismatch queue is full.
    pub pull_mismatch_queue_full_policy: LolaPullMismatchQueueFullPolicy,
    /// Optional path to the S-CORE `mw_com_config.json` file.
    ///
    /// Native `lola-ffi` builds pass this to the bridge so S-CORE can initialize
    /// the configured service/event deployment. The `test-stub` backend accepts
    /// the field but does not read the file.
    pub mw_com_config_path: Option<String>,
}

impl LolaTransportConfig {
    /// Default retained mismatched pull samples for new deployments.
    pub const DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY: usize = 64;
    /// Default full-queue behavior for retained mismatched pull samples.
    pub const DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY: LolaPullMismatchQueueFullPolicy =
        LolaPullMismatchQueueFullPolicy::DropOldestAndReport;

    /// Validates the configuration before creating a transport.
    ///
    /// This checks local field invariants such as non-empty identifiers,
    /// non-zero sample size, power-of-two sample alignment, and non-zero sample
    /// count. It does not validate that the external S-CORE configuration file
    /// exists or contains matching deployment data.
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

    /// Sets the maximum retained mismatched pull samples.
    #[must_use]
    pub fn with_pull_mismatch_queue_capacity(mut self, value: usize) -> Self {
        self.pull_mismatch_queue_capacity = value;
        self
    }

    /// Sets the full-queue policy for retained mismatched pull samples.
    #[must_use]
    pub fn with_pull_mismatch_queue_full_policy(
        mut self,
        value: LolaPullMismatchQueueFullPolicy,
    ) -> Self {
        self.pull_mismatch_queue_full_policy = value;
        self
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

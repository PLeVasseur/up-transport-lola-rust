/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use up_rust::{UCode, UStatus};

/// Full-queue behavior for pull receive samples that do not match a requested filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LolaPullMismatchQueueFullPolicy {
    /// Drop the oldest retained mismatch and report the condition.
    DropOldestAndReport,
    /// Reject the newest mismatch and report the condition.
    RejectNewestAndReport,
}

/// LoLa channel used when selected-wire routing intentionally registers a broad
/// physical listener before metadata decode can distinguish request/response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LolaDefaultRxChannel {
    /// Primary LoLa event, used by default for request-oriented endpoints.
    Primary,
    /// RPC response LoLa event, used by response-oriented bridge endpoints.
    Response,
    /// Both primary and response events.
    Both,
}

/// Configuration for a LoLa transport instance.
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
    pub sample_size: usize,
    /// Required LoLa event sample alignment in bytes.
    pub sample_alignment: usize,
    /// Maximum number of samples configured for the LoLa event.
    pub max_samples: usize,
    /// Maximum retained pull samples that did not match the requested filter.
    pub pull_mismatch_queue_capacity: usize,
    /// Policy applied when the pull mismatch queue is full.
    pub pull_mismatch_queue_full_policy: LolaPullMismatchQueueFullPolicy,
    /// Optional path to the S-CORE `mw_com_config.json` deployment manifest.
    ///
    /// The native S-CORE runtime is initialized once per process. All native
    /// LoLa transports and subscribers in that process must therefore use the
    /// same manifest path, or omit the path and rely on S-CORE's default
    /// `./etc/mw_com_config.json`. The manifest defines the LoLa service IDs,
    /// instance IDs, events, sample slots, and subscriber limits; Linux service
    /// discovery and partial-restart state remains under S-CORE's runtime
    /// directory such as `/tmp/mw_com_lola`.
    pub mw_com_config_path: Option<String>,
}

impl LolaTransportConfig {
    /// Default retained mismatched pull samples for new deployments.
    pub const DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY: usize = 64;
    /// Default full-queue behavior for retained mismatched pull samples.
    pub const DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY: LolaPullMismatchQueueFullPolicy =
        LolaPullMismatchQueueFullPolicy::DropOldestAndReport;

    /// Validates local LoLa configuration invariants.
    ///
    /// # Errors
    ///
    /// Returns [`UStatus`] when a required string is empty or a numeric sample
    /// field is invalid.
    pub fn validate(&self) -> Result<(), UStatus> {
        validate_non_empty("local_authority", &self.local_authority)?;
        validate_non_empty("instance_specifier", &self.instance_specifier)?;
        validate_non_empty("service_type", &self.service_type)?;
        validate_non_empty("event_name", &self.event_name)?;
        if self.sample_size == 0 {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "LoLa sample_size must be greater than zero",
            ));
        }
        if self.sample_alignment == 0 || !self.sample_alignment.is_power_of_two() {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "LoLa sample_alignment must be a non-zero power of two",
            ));
        }
        if self.max_samples == 0 {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
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
            UCode::InvalidArgument,
            format!("LoLa {field} must be non-empty"),
        ));
    }
    Ok(())
}

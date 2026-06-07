/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::sync::Arc;
#[cfg(feature = "test-stub")]
use std::sync::Mutex;

use async_trait::async_trait;
use up_rust::{
    UCode, UStatus, UZeroCopyTransportImpl, UZeroCopyUninitTransportImpl, ValidatedTxLoanSpec,
};

#[cfg(feature = "native")]
use crate::sys::NativeTransport;
use crate::{
    config::LolaTransportConfig,
    frame::{LolaRxLease, LolaTxLoan, LolaUninitTxLoan},
};

/// Zero-copy uProtocol transport backed by a LoLa generic event.
///
/// This implementation provides the transmit side only: initialized TX loans,
/// uninitialized TX loans, and committing a TX loan. Pull receive and listener
/// behavior intentionally use the default `UCode::Unimplemented` zero-copy trait
/// methods until receive support is added.
pub struct UTransportLola {
    config: LolaTransportConfig,
    #[cfg(feature = "test-stub")]
    sent_samples: Mutex<Vec<Vec<u8>>>,
    #[cfg(feature = "native")]
    native: NativeTransport,
}

impl UTransportLola {
    /// Builds a LoLa transport from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns validation errors from [`LolaTransportConfig::validate`] or native
    /// bridge initialization errors when the `native` feature is active.
    pub fn build(config: LolaTransportConfig) -> Result<Arc<Self>, UStatus> {
        config.validate()?;
        #[cfg(feature = "native")]
        let native = NativeTransport::new(&config)?;
        Ok(Arc::new(Self {
            config,
            #[cfg(feature = "test-stub")]
            sent_samples: Mutex::new(Vec::new()),
            #[cfg(feature = "native")]
            native,
        }))
    }

    /// Returns the configuration used to build this transport.
    #[must_use]
    pub fn config(&self) -> &LolaTransportConfig {
        &self.config
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

#[async_trait]
impl UZeroCopyTransportImpl for UTransportLola {
    type Tx = LolaTxLoan;
    type Rx = LolaRxLease;

    async fn loan_validated_tx(&self, spec: ValidatedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        self.validate_payload_alignment(alignment)?;

        #[cfg(feature = "native")]
        {
            let sample = self.native.loan_sample()?;
            return LolaTxLoan::new_native(metadata, sample, payload_len, alignment);
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            return LolaTxLoan::new_vec(metadata, self.config.sample_size, payload_len, alignment);
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            let _ = (metadata, payload_len);
            Err(backend_unavailable())
        }
    }

    async fn send_validated_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        #[cfg(feature = "native")]
        {
            let loan = buffer.into_native()?;
            return self.native.send(loan);
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            let sample = buffer.into_vec();
            self.sent_samples
                .lock()
                .expect("LoLa test-stub sent sample lock poisoned")
                .push(sample);
            return Ok(());
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            let _ = buffer;
            Err(backend_unavailable())
        }
    }
}

#[async_trait]
impl UZeroCopyUninitTransportImpl for UTransportLola {
    type UninitTx = LolaUninitTxLoan;

    async fn loan_validated_uninit_tx(
        &self,
        spec: ValidatedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        self.validate_payload_alignment(alignment)?;

        #[cfg(feature = "native")]
        {
            let sample = self.native.loan_sample()?;
            return LolaUninitTxLoan::new_native(metadata, sample, payload_len, alignment);
        }

        #[cfg(all(feature = "test-stub", not(feature = "native")))]
        {
            return LolaUninitTxLoan::new_vec(
                metadata,
                self.config.sample_size,
                payload_len,
                alignment,
            );
        }

        #[cfg(not(any(feature = "test-stub", feature = "native")))]
        {
            let _ = (metadata, payload_len);
            Err(backend_unavailable())
        }
    }
}

#[cfg(not(any(feature = "test-stub", feature = "native")))]
fn backend_unavailable() -> UStatus {
    UStatus::fail_with_code(
        UCode::FailedPrecondition,
        "enable the LoLa test-stub or native backend feature to loan TX samples",
    )
}

#[cfg(all(
    test,
    any(
        all(feature = "test-stub", not(feature = "native")),
        not(any(feature = "test-stub", feature = "native"))
    )
))]
mod tests {
    use std::{future::Future, sync::Arc, task::Wake};

    use up_rust::{
        try_project_umessage_to_frame_metadata, UCode, UMessageBuilder, UPayloadFormat,
        UTxLoanSpec, UUri, UZeroCopyTransport,
    };
    #[cfg(feature = "test-stub")]
    use up_rust::{UFrameView, UTxBuffer, UUninitTxBuffer, UZeroCopyUninitTransport};

    use super::*;

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn block_on_ready<T>(future: impl Future<Output = T>) -> T {
        let waker = std::task::Waker::from(Arc::new(NoopWake));
        let mut context = std::task::Context::from_waker(&waker);
        let mut future = Box::pin(future);
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(value) => value,
            std::task::Poll::Pending => panic!("LoLa test future unexpectedly yielded pending"),
        }
    }

    fn test_config() -> LolaTransportConfig {
        LolaTransportConfig {
            local_authority: "vehicle".to_string(),
            instance_specifier: "lola/service/instance".to_string(),
            service_type: "uprotocol.LoLa".to_string(),
            event_name: "UProtocolFrame".to_string(),
            sample_size: 256,
            sample_alignment: 8,
            max_samples: 8,
            pull_mismatch_queue_capacity: LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_CAPACITY,
            pull_mismatch_queue_full_policy:
                LolaTransportConfig::DEFAULT_PULL_MISMATCH_QUEUE_FULL_POLICY,
            mw_com_config_path: None,
        }
    }

    fn tx_spec(payload_len: usize, payload_alignment: usize) -> UTxLoanSpec {
        let topic = UUri::try_from("//vehicle/4210/1/9008").expect("valid URI");
        let message = UMessageBuilder::publish(topic)
            .build_with_payload(Vec::new(), UPayloadFormat::Raw)
            .expect("valid publish message");
        let metadata = try_project_umessage_to_frame_metadata(&message).expect("valid metadata");
        UTxLoanSpec::payload(metadata, payload_len, payload_alignment).expect("valid loan spec")
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_send_zero_copy_stores_initialized_sample() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let mut loan = block_on_ready(transport.loan_tx(tx_spec(3, 1))).unwrap();
        loan.payload_mut().copy_from_slice(b"abc");

        block_on_ready(transport.send_zero_copy(loan)).unwrap();

        let samples = transport.sent_samples.lock().unwrap();
        assert_eq!(samples.len(), 1);
        let lease = LolaRxLease::from_vec(samples[0].clone()).unwrap();
        assert_eq!(lease.try_contiguous_payload(), Some(b"abc".as_slice()));
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_send_zero_copy_commits_uninit_payload_without_zeroing_payload() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let mut loan = block_on_ready(transport.loan_uninit_tx(tx_spec(3, 1))).unwrap();
        for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(*b"xyz") {
            slot.write(byte);
        }

        // SAFETY: The test wrote exactly every visible application payload byte.
        let loan = unsafe { loan.assume_payload_init() };
        block_on_ready(transport.send_zero_copy(loan)).unwrap();

        let samples = transport.sent_samples.lock().unwrap();
        assert_eq!(samples.len(), 1);
        let lease = LolaRxLease::from_vec(samples[0].clone()).unwrap();
        assert_eq!(lease.try_contiguous_payload(), Some(b"xyz".as_slice()));
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn tx_loan_rejects_alignment_larger_than_sample_alignment() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let error = match block_on_ready(transport.loan_tx(tx_spec(1, 16))) {
            Ok(_) => panic!("LoLa TX loan should reject excessive alignment"),
            Err(error) => error,
        };
        assert_eq!(error.get_code(), UCode::InvalidArgument);
    }

    #[cfg(not(any(feature = "test-stub", feature = "native")))]
    #[test]
    fn tx_loan_requires_backend_feature() {
        let transport = UTransportLola::build(test_config()).unwrap();
        let error = match block_on_ready(transport.loan_tx(tx_spec(1, 1))) {
            Ok(_) => panic!("LoLa TX loan should require a backend feature"),
            Err(error) => error,
        };
        assert_eq!(error.get_code(), UCode::FailedPrecondition);
    }
}

/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

//! Benchmark-only owned LoLa transport wrapper.
//!
//! This module exists only behind `benchmark-owned` so Criterion can compare a
//! native-frame owned path with LoLa's zero-copy path. It is not a product API
//! and does not use the generic owned copying adapter.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use up_rust::{
    transport::{UOwnedTransportImpl, ValidatedOwnedFrame},
    zero_copy::{UFrameView, UTxBuffer, UTxLoanSpec, UZeroCopyListener, UZeroCopyTransport},
    UCode, UOwnedFrame, UOwnedListener, UStatus, UUri,
};

use crate::{LolaRxLease, UTransportLola};

/// Benchmark-only owned LoLa transport wrapper.
///
/// The wrapper copies owned payload bytes into a LoLa transmit loan and copies
/// visible receive payload bytes out of a [`LolaRxLease`]. It intentionally lives
/// behind `benchmark-owned` and is not the normal LoLa API.
pub struct BenchmarkOwnedLolaTransport {
    inner: Arc<UTransportLola>,
    listeners: Mutex<Vec<OwnedListenerRegistration>>,
}

impl BenchmarkOwnedLolaTransport {
    /// Creates a benchmark-only owned wrapper around a LoLa zero-copy transport.
    #[must_use]
    pub fn new(inner: Arc<UTransportLola>) -> Self {
        Self {
            inner,
            listeners: Mutex::new(Vec::new()),
        }
    }

    /// Returns the wrapped LoLa zero-copy transport.
    #[must_use]
    pub fn inner(&self) -> &Arc<UTransportLola> {
        &self.inner
    }
}

#[async_trait]
impl UOwnedTransportImpl for BenchmarkOwnedLolaTransport {
    async fn send_validated_owned(&self, frame: ValidatedOwnedFrame) -> Result<(), UStatus> {
        let frame = frame.into_inner();
        let metadata = frame.metadata().clone();
        let mut loan = self
            .inner
            .loan_tx(tx_loan_spec(
                metadata,
                frame.has_payload(),
                frame.payload_bytes().len(),
            )?)
            .await?;
        if frame.has_payload() {
            loan.payload_mut().copy_from_slice(frame.payload_bytes());
        }
        self.inner.send_zero_copy(loan).await
    }

    async fn receive_validated_owned(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<UOwnedFrame, UStatus> {
        let frame = self
            .inner
            .receive_zero_copy(source_filter, sink_filter)
            .await?;
        lease_to_owned_frame(&frame)
    }

    async fn register_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        let zero_copy_listener = Arc::new(OwnedBenchmarkListener {
            listener: listener.clone(),
        });
        self.inner
            .register_zero_copy_listener(source_filter, sink_filter, zero_copy_listener.clone())
            .await?;
        self.listeners.lock().await.push(OwnedListenerRegistration {
            source_filter: source_filter.clone(),
            sink_filter: sink_filter.cloned(),
            owned_listener: listener,
            zero_copy_listener,
        });
        Ok(())
    }

    async fn unregister_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        let registration = {
            let mut listeners = self.listeners.lock().await;
            let Some(index) = listeners.iter().position(|registration| {
                registration.source_filter == *source_filter
                    && registration.sink_filter.as_ref() == sink_filter
                    && Arc::ptr_eq(&registration.owned_listener, &listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NOT_FOUND,
                    "no such benchmark-owned LoLa listener registered for filters",
                ));
            };
            listeners.remove(index)
        };
        self.inner
            .unregister_zero_copy_listener(
                source_filter,
                sink_filter,
                registration.zero_copy_listener,
            )
            .await
    }
}

struct OwnedListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    owned_listener: Arc<dyn UOwnedListener>,
    zero_copy_listener: Arc<OwnedBenchmarkListener>,
}

struct OwnedBenchmarkListener {
    listener: Arc<dyn UOwnedListener>,
}

#[async_trait]
impl UZeroCopyListener<LolaRxLease> for OwnedBenchmarkListener {
    async fn on_receive_zero_copy(&self, frame: LolaRxLease) {
        let frame = lease_to_owned_frame(&frame)
            .expect("benchmark-only LoLa owned listener should receive valid frames");
        self.listener.on_receive_owned(frame).await;
    }
}

fn tx_loan_spec(
    metadata: up_rust::UFrameMetadata,
    has_payload: bool,
    payload_len: usize,
) -> Result<UTxLoanSpec, UStatus> {
    if !has_payload {
        return UTxLoanSpec::no_payload(metadata);
    }
    if payload_len == 0 {
        return UTxLoanSpec::present_empty_payload(metadata);
    }
    let layout = up_rust::payload::PayloadLayout::new(payload_len, 1).map_err(UStatus::from)?;
    UTxLoanSpec::payload(metadata, layout)
}

fn lease_to_owned_frame(frame: &LolaRxLease) -> Result<UOwnedFrame, UStatus> {
    if !frame.has_payload() {
        return UOwnedFrame::try_without_payload(frame.metadata().clone()).map_err(|error| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("invalid benchmark-owned LoLa no-payload frame: {error}"),
            )
        });
    }
    let mut payload = Vec::with_capacity(frame.payload_len());
    for slice in frame.payload_slices() {
        payload.extend_from_slice(slice);
    }
    UOwnedFrame::try_with_payload(frame.metadata().clone(), payload).map_err(|error| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid benchmark-owned LoLa payload frame: {error}"),
        )
    })
}

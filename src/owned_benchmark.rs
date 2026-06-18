/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

//! Benchmark-only owned LoLa transport wrapper.
//!
//! This module is available only behind `benchmark-owned`. It is a
//! transport-local copying comparison path for benchmarks, not a product-facing
//! owned transport or generic adapter.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use up_rust::{
    UCode, UFrameView, UOwnedFrame, UOwnedListener, UOwnedTransportImpl, UStatus, UTxBuffer,
    UTxLoanSpec, UUri, UWireMetadata, UWireTransport, UWithWire, UZeroCopyListener,
    UZeroCopyTransport, ValidatedOwnedFrame,
};

use crate::{LolaRxLease, LolaZeroCopyCore};

/// Benchmark-only owned wrapper around [`UTransportLola`].
pub struct BenchmarkOwnedLolaTransport<W>
where
    W: UWireMetadata,
{
    inner: UWireTransport<LolaZeroCopyCore, W>,
    listeners: Mutex<Vec<OwnedListenerRegistration<W>>>,
}

impl<W> BenchmarkOwnedLolaTransport<W>
where
    W: UWireMetadata,
{
    /// Creates a benchmark-only owned wrapper.
    #[must_use]
    pub fn new(core: LolaZeroCopyCore, wire: W) -> Self {
        Self {
            inner: core.with_wire(wire),
            listeners: Mutex::new(Vec::new()),
        }
    }

    /// Returns the wrapped selected-wire zero-copy transport.
    #[must_use]
    pub fn inner(&self) -> &UWireTransport<LolaZeroCopyCore, W> {
        &self.inner
    }
}

#[async_trait]
impl<W> UOwnedTransportImpl for BenchmarkOwnedLolaTransport<W>
where
    W: UWireMetadata + Send + Sync + 'static,
{
    async fn send_validated_owned(&self, frame: ValidatedOwnedFrame) -> Result<(), UStatus> {
        let frame = frame.into_inner();
        let mut loan = self
            .inner
            .loan_tx(tx_loan_spec(
                frame.metadata().clone(),
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
        let zero_copy_listener = Arc::new(OwnedBenchmarkListener::<W> {
            listener: listener.clone(),
            _wire: std::marker::PhantomData,
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
                    UCode::NotFound,
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

struct OwnedListenerRegistration<W>
where
    W: UWireMetadata,
{
    source_filter: UUri,
    sink_filter: Option<UUri>,
    owned_listener: Arc<dyn UOwnedListener>,
    zero_copy_listener: Arc<OwnedBenchmarkListener<W>>,
}

struct OwnedBenchmarkListener<W>
where
    W: UWireMetadata,
{
    listener: Arc<dyn UOwnedListener>,
    _wire: std::marker::PhantomData<W>,
}

#[async_trait]
impl<W> UZeroCopyListener<up_rust::UWireRx<LolaRxLease, W>> for OwnedBenchmarkListener<W>
where
    W: UWireMetadata + Send + Sync + 'static,
{
    async fn on_receive_zero_copy(&self, frame: up_rust::UWireRx<LolaRxLease, W>) {
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
    UTxLoanSpec::payload(metadata, payload_len, 1)
}

fn lease_to_owned_frame<F>(frame: &F) -> Result<UOwnedFrame, UStatus>
where
    F: UFrameView,
{
    if !frame.has_payload() {
        return UOwnedFrame::without_payload(frame.metadata().clone()).map_err(|error| {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("invalid benchmark-owned LoLa no-payload frame: {error}"),
            )
        });
    }
    let mut payload = Vec::with_capacity(frame.payload_len());
    for slice in frame.payload_slices() {
        payload.extend_from_slice(slice);
    }
    UOwnedFrame::with_payload(frame.metadata().clone(), payload).map_err(|error| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("invalid benchmark-owned LoLa payload frame: {error}"),
        )
    })
}

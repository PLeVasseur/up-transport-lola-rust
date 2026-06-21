/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

//! Feature-gated owned-core support for LoLa benchmark measurements.
//!
//! This module is available only behind `benchmark-owned`. It provides a real
//! transport-specific owned core consumed by the generic selected-wire owned
//! adapter, instead of directly implementing the public owned transport boundary.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use up_rust::{
    EncodedOwnedFrame, NativePrefixProtobufMetadataCodec, PreparedOwnedFrame, PreparedTxLoanSpec,
    UCode, UEncodedOwnedListener, UEncodedRxFrame, UEncodedZeroCopyListener, UOwnedTransportCore,
    UStatus, UTxBuffer, UUri, UWire, UWireTransport, UZeroCopyTransportCore,
};

use crate::{LolaRxLease, LolaZeroCopyCore};

/// LoLa owned-frame core for feature-gated benchmark/support paths.
#[derive(Clone)]
pub struct LolaOwnedCore {
    inner: LolaZeroCopyCore,
    listeners: Arc<Mutex<Vec<OwnedListenerRegistration>>>,
}

impl LolaOwnedCore {
    /// Creates an owned core over real LoLa selected-wire mechanics.
    #[must_use]
    pub fn new(inner: LolaZeroCopyCore) -> Self {
        Self {
            inner,
            listeners: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Wraps this core in the generic selected-wire owned adapter.
    #[must_use]
    pub fn with_selected_wire<W>(
        self,
        wire: W,
    ) -> UWireTransport<Self, W, NativePrefixProtobufMetadataCodec>
    where
        W: UWire,
    {
        UWireTransport::new(self, wire, NativePrefixProtobufMetadataCodec)
    }

    /// Returns the wrapped selected-wire zero-copy core.
    #[must_use]
    pub fn inner(&self) -> &LolaZeroCopyCore {
        &self.inner
    }
}

struct OwnedListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    owned_listener: Arc<dyn UEncodedOwnedListener>,
    zero_copy_listener: Arc<OwnedCoreListener>,
}

struct OwnedCoreListener {
    listener: Arc<dyn UEncodedOwnedListener>,
}

#[async_trait]
impl UEncodedZeroCopyListener<LolaRxLease> for OwnedCoreListener {
    async fn on_receive_encoded_zero_copy(&self, frame: LolaRxLease) {
        match lease_to_encoded_owned(&frame) {
            Ok(frame) => self.listener.on_receive_encoded_owned(frame).await,
            Err(_error) => {}
        }
    }
}

#[async_trait]
impl UOwnedTransportCore for LolaOwnedCore {
    async fn send_prepared_owned(&self, frame: PreparedOwnedFrame) -> Result<(), UStatus> {
        let payload_len = frame.payload().map_or(0, Bytes::len);
        let spec = PreparedTxLoanSpec::from_encoded_parts(
            frame.metadata().clone(),
            frame.encoded_metadata().to_vec(),
            payload_len,
            1,
        )?;
        let mut loan = self.inner.loan_prepared_tx(spec).await?;
        if let Some(payload) = frame.payload() {
            loan.payload_mut().copy_from_slice(payload);
        }
        self.inner.send_prepared_zero_copy(loan).await
    }

    async fn receive_encoded_owned(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<EncodedOwnedFrame, UStatus> {
        let frame = self
            .inner
            .receive_encoded_zero_copy(source_filter, sink_filter)
            .await?;
        lease_to_encoded_owned(&frame)
    }

    async fn register_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        let zero_copy_listener = Arc::new(OwnedCoreListener {
            listener: listener.clone(),
        });
        self.inner
            .register_encoded_zero_copy_listener(
                source_filter,
                sink_filter,
                zero_copy_listener.clone(),
            )
            .await?;
        self.listeners.lock().await.push(OwnedListenerRegistration {
            source_filter: source_filter.clone(),
            sink_filter: sink_filter.cloned(),
            owned_listener: listener,
            zero_copy_listener,
        });
        Ok(())
    }

    async fn unregister_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
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
                    "no such LoLa owned-core listener registered for filters",
                ));
            };
            listeners.remove(index)
        };
        self.inner
            .unregister_encoded_zero_copy_listener(
                source_filter,
                sink_filter,
                registration.zero_copy_listener,
            )
            .await
    }
}

fn lease_to_encoded_owned(frame: &LolaRxLease) -> Result<EncodedOwnedFrame, UStatus> {
    let payload = frame.try_contiguous_payload().map(Bytes::copy_from_slice);
    Ok(EncodedOwnedFrame::new(
        frame.encoded_metadata().to_vec(),
        payload,
    ))
}

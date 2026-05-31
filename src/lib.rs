/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

//! Eclipse S-CORE LoLa transport for native uProtocol zero-copy frames.
//!
//! [`UTransportLola`] implements [`up_rust::zero_copy::UZeroCopyTransport`].
//! Transmit payloads are serialized directly into fixed-size LoLa event samples,
//! and receive payloads are exposed through [`LolaRxLease`] while the underlying
//! LoLa sample is alive.
//!
//! The default `bundled` feature builds and links the native C++ bridge from the
//! pinned S-CORE communication submodule. The `lola-ffi` feature uses a prebuilt
//! bridge or the bundled build output. The `test-stub` feature provides an
//! in-process fake backend for Rust unit tests and is not a LoLa runtime.
//!
//! LoLa samples contain a small `ULOL` frame header, hidden native-frame metadata,
//! alignment padding, and then the application payload bytes. The payload views
//! exposed by [`LolaTxLoan`] and [`LolaRxLease`] exclude the header, metadata, and
//! padding. Metadata is fixed when the transmit loan is created so the payload
//! offset remains stable while serializers write directly into the sample.

#![warn(rustdoc::bare_urls, rustdoc::broken_intra_doc_links)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(feature = "lola-ffi", feature = "test-stub"))]
compile_error!("features `lola-ffi` and `test-stub` are mutually exclusive");

#[cfg(not(any(feature = "lola-ffi", feature = "test-stub")))]
compile_error!(
    "enable the default `bundled` feature, `lola-ffi` with LOLA_BRIDGE_LIB_DIR, or `test-stub`"
);

mod config;
mod frame;
#[cfg(feature = "benchmark-owned")]
mod owned_benchmark;
mod transport;

#[cfg(feature = "lola-ffi")]
mod sys;

pub use config::{LolaPullMismatchQueueFullPolicy, LolaTransportConfig};
pub use frame::{LolaRxLease, LolaTxLoan, LolaUninitTxLoan};
#[cfg(feature = "benchmark-owned")]
#[cfg_attr(docsrs, doc(cfg(feature = "benchmark-owned")))]
pub use owned_benchmark::BenchmarkOwnedLolaTransport;
pub use transport::{LolaPullMismatchQueueDiagnostics, UTransportLola};

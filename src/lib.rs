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

#[cfg(all(feature = "lola-ffi", feature = "test-stub"))]
compile_error!("features `lola-ffi` and `test-stub` are mutually exclusive");

#[cfg(not(any(feature = "lola-ffi", feature = "test-stub")))]
compile_error!(
    "enable the default `bundled` feature, `lola-ffi` with LOLA_BRIDGE_LIB_DIR, or `test-stub`"
);

mod config;
mod frame;
mod transport;

#[cfg(feature = "lola-ffi")]
mod sys;

pub use config::LolaTransportConfig;
pub use frame::{LolaRxLease, LolaTxLoan};
pub use transport::UTransportLola;

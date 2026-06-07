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

//! Rust crate shell for the Eclipse S-CORE LoLa uProtocol transport.
//!
//! This crate currently exposes no transport implementation. Native LoLa
//! integration, fixed-sample frame layout, zero-copy TX/RX, and benchmark support
//! are not implemented in this crate shell.

#![warn(rustdoc::bare_urls, rustdoc::broken_intra_doc_links)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(test)]
mod tests {
    #[test]
    fn skeleton_metadata_is_available() {
        assert_eq!(env!("CARGO_PKG_NAME"), "up-transport-lola-rust");
        assert!(!env!("CARGO_PKG_VERSION").is_empty());
    }
}

#![allow(dead_code)]

/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::{mem::MaybeUninit, ptr::NonNull, slice};

use up_rust::{UCode, UStatus};

use crate::config::LolaTransportConfig;

#[repr(C)]
struct UpLolaStr {
    data: *const u8,
    len: usize,
}

impl UpLolaStr {
    fn new(value: &str) -> Self {
        Self {
            data: value.as_ptr(),
            len: value.len(),
        }
    }

    fn empty() -> Self {
        Self {
            data: std::ptr::null(),
            len: 0,
        }
    }
}

#[repr(C)]
struct UpLolaConfig {
    instance_specifier: UpLolaStr,
    service_type: UpLolaStr,
    event_name: UpLolaStr,
    mw_com_config_path: UpLolaStr,
    sample_size: usize,
    sample_alignment: usize,
    max_samples: usize,
}

impl UpLolaConfig {
    fn new(config: &LolaTransportConfig) -> Self {
        Self {
            instance_specifier: UpLolaStr::new(&config.instance_specifier),
            service_type: UpLolaStr::new(&config.service_type),
            event_name: UpLolaStr::new(&config.event_name),
            mw_com_config_path: config
                .mw_com_config_path
                .as_deref()
                .map_or_else(UpLolaStr::empty, UpLolaStr::new),
            sample_size: config.sample_size,
            sample_alignment: config.sample_alignment,
            max_samples: config.max_samples,
        }
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpLolaStatusCode {
    Ok = 0,
    InvalidArgument = 1,
    NotFound = 2,
    ResourceExhausted = 3,
    Internal = 4,
}

#[repr(C)]
pub struct UpLolaTransport {
    _private: [u8; 0],
}

#[repr(C)]
pub struct UpLolaSubscriber {
    _private: [u8; 0],
}

#[repr(C)]
pub struct UpLolaTxLoan {
    _private: [u8; 0],
}

#[repr(C)]
pub struct UpLolaRxSample {
    _private: [u8; 0],
}

unsafe extern "C" {
    fn up_lola_transport_create(
        config: *const UpLolaConfig,
        out_transport: *mut *mut UpLolaTransport,
    ) -> UpLolaStatusCode;
    fn up_lola_transport_destroy(transport: *mut UpLolaTransport);
    fn up_lola_transport_reserve(
        transport: *mut UpLolaTransport,
        out_loan: *mut *mut UpLolaTxLoan,
    ) -> UpLolaStatusCode;
    fn up_lola_tx_loan_data(loan: *mut UpLolaTxLoan) -> *mut u8;
    fn up_lola_tx_loan_size(loan: *const UpLolaTxLoan) -> usize;
    fn up_lola_tx_loan_destroy(loan: *mut UpLolaTxLoan);
    fn up_lola_transport_send(
        transport: *mut UpLolaTransport,
        loan: *mut UpLolaTxLoan,
    ) -> UpLolaStatusCode;
    fn up_lola_subscriber_create(
        config: *const UpLolaConfig,
        out_subscriber: *mut *mut UpLolaSubscriber,
    ) -> UpLolaStatusCode;
    fn up_lola_subscriber_destroy(subscriber: *mut UpLolaSubscriber);
    fn up_lola_subscriber_receive(
        subscriber: *mut UpLolaSubscriber,
        out_sample: *mut *mut UpLolaRxSample,
    ) -> UpLolaStatusCode;
    fn up_lola_rx_sample_data(sample: *const UpLolaRxSample) -> *const u8;
    fn up_lola_rx_sample_size(sample: *const UpLolaRxSample) -> usize;
    fn up_lola_rx_sample_destroy(sample: *mut UpLolaRxSample);
}

pub(crate) struct NativeTransport {
    ptr: NonNull<UpLolaTransport>,
    sample_size: usize,
    sample_alignment: usize,
}

// SAFETY: The native bridge contract treats transport handles as thread-safe
// opaque objects. Rust stores and forwards only the non-null handle.
unsafe impl Send for NativeTransport {}
// SAFETY: Shared references call bridge functions that must synchronize access
// internally according to the native bridge contract.
unsafe impl Sync for NativeTransport {}

impl NativeTransport {
    pub(crate) fn new(config: &LolaTransportConfig) -> Result<Self, UStatus> {
        let ffi_config = UpLolaConfig::new(config);
        let mut out = std::ptr::null_mut();
        // SAFETY: `ffi_config` and `out` are valid for the call; the bridge
        // writes either a null failure value or an owned transport handle.
        let status = unsafe { up_lola_transport_create(&raw const ffi_config, &raw mut out) };
        map_status(status, "create LoLa transport")?;
        let ptr = NonNull::new(out).ok_or_else(|| {
            UStatus::fail_with_code(UCode::Internal, "LoLa bridge returned null transport")
        })?;
        Ok(Self {
            ptr,
            sample_size: config.sample_size,
            sample_alignment: config.sample_alignment,
        })
    }

    pub(crate) fn loan_sample(&self) -> Result<NativeTxLoan, UStatus> {
        let mut out = std::ptr::null_mut();
        // SAFETY: `self.ptr` is a live bridge handle and `out` is a valid
        // out-parameter for one TX loan handle.
        let status = unsafe { up_lola_transport_reserve(self.ptr.as_ptr(), &raw mut out) };
        map_status(status, "loan LoLa sample")?;
        NativeTxLoan::new(out, self.sample_size, self.sample_alignment)
    }

    pub(crate) fn send(&self, mut loan: NativeTxLoan) -> Result<(), UStatus> {
        let raw = loan.take();
        // SAFETY: `raw` is an owned TX loan consumed exactly once by send.
        let status = unsafe { up_lola_transport_send(self.ptr.as_ptr(), raw.as_ptr()) };
        map_status(status, "send LoLa sample")
    }
}

impl Drop for NativeTransport {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is the live transport handle owned by this wrapper.
        unsafe { up_lola_transport_destroy(self.ptr.as_ptr()) }
    }
}

pub(crate) struct NativeSubscriber {
    ptr: NonNull<UpLolaSubscriber>,
    sample_size: usize,
    sample_alignment: usize,
}

// SAFETY: Subscriber handles are opaque native objects; Rust forwards the handle
// without accessing shared native state directly.
unsafe impl Send for NativeSubscriber {}
// SAFETY: Shared references call native functions that define their own
// synchronization contract.
unsafe impl Sync for NativeSubscriber {}

impl NativeSubscriber {
    pub(crate) fn new(config: &LolaTransportConfig) -> Result<Self, UStatus> {
        let ffi_config = UpLolaConfig::new(config);
        let mut out = std::ptr::null_mut();
        // SAFETY: `ffi_config` and `out` are valid for the call.
        let status = unsafe { up_lola_subscriber_create(&raw const ffi_config, &raw mut out) };
        map_status(status, "create LoLa subscriber")?;
        let ptr = NonNull::new(out).ok_or_else(|| {
            UStatus::fail_with_code(UCode::Internal, "LoLa bridge returned null subscriber")
        })?;
        Ok(Self {
            ptr,
            sample_size: config.sample_size,
            sample_alignment: config.sample_alignment,
        })
    }

    pub(crate) fn receive(&self) -> Result<NativeRxSample, UStatus> {
        let mut out = std::ptr::null_mut();
        // SAFETY: `self.ptr` is a live subscriber handle and `out` is a valid
        // out-parameter for one received sample handle.
        let status = unsafe { up_lola_subscriber_receive(self.ptr.as_ptr(), &raw mut out) };
        map_status(status, "receive LoLa sample")?;
        NativeRxSample::new(out, self.sample_size, self.sample_alignment)
    }
}

impl Drop for NativeSubscriber {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is the live subscriber handle owned by this wrapper.
        unsafe { up_lola_subscriber_destroy(self.ptr.as_ptr()) }
    }
}

pub(crate) struct NativeTxLoan {
    ptr: Option<NonNull<UpLolaTxLoan>>,
    expected_len: usize,
    expected_alignment: usize,
}

// SAFETY: TX loan ownership moves as an opaque native handle.
unsafe impl Send for NativeTxLoan {}

impl NativeTxLoan {
    fn new(
        ptr: *mut UpLolaTxLoan,
        expected_len: usize,
        expected_alignment: usize,
    ) -> Result<Self, UStatus> {
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            UStatus::fail_with_code(UCode::Internal, "LoLa bridge returned null TX loan")
        })?;
        let loan = Self {
            ptr: Some(ptr),
            expected_len,
            expected_alignment,
        };
        loan.data_parts("LoLa TX loan")?;
        Ok(loan)
    }

    fn take(&mut self) -> NonNull<UpLolaTxLoan> {
        self.ptr
            .take()
            .expect("LoLa native TX loan should not be consumed twice")
    }

    pub(crate) fn len(&self) -> usize {
        self.data_parts("LoLa TX loan")
            .expect("LoLa TX loan data should remain valid while owned")
            .1
    }

    fn data_parts(&self, label: &str) -> Result<(*mut u8, usize), UStatus> {
        let ptr = self.ptr.expect("LoLa TX loan is still owned");
        // SAFETY: `ptr` is a live TX loan handle still owned by this wrapper.
        let data = unsafe { up_lola_tx_loan_data(ptr.as_ptr()) };
        // SAFETY: `ptr` is the same live TX loan handle as above.
        let len = unsafe { up_lola_tx_loan_size(ptr.as_ptr()) };
        validate_sample_parts(
            data.cast_const(),
            len,
            self.expected_len,
            self.expected_alignment,
            label,
        )?;
        Ok((data, len))
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        let (data, len) = self
            .data_parts("LoLa TX loan")
            .expect("LoLa TX loan data should remain valid while owned");
        // SAFETY: `data_parts` verifies non-null, length, and alignment, and the
        // bridge owns one live allocation for this wrapper lifetime.
        unsafe { slice::from_raw_parts(data.cast_const(), len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        let (data, len) = self
            .data_parts("LoLa TX loan")
            .expect("LoLa TX loan data should remain valid while owned");
        // SAFETY: `data_parts` verifies the native allocation shape, and `&mut
        // self` gives unique Rust access to the loan while the slice exists.
        unsafe { slice::from_raw_parts_mut(data, len) }
    }

    pub(crate) fn as_uninit_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        let (data, len) = self
            .data_parts("LoLa TX loan")
            .expect("LoLa TX loan data should remain valid while owned");
        // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`; the native loan
        // shape was checked and `&mut self` provides unique mutable access.
        unsafe { slice::from_raw_parts_mut(data.cast::<MaybeUninit<u8>>(), len) }
    }
}

impl Drop for NativeTxLoan {
    fn drop(&mut self) {
        if let Some(ptr) = self.ptr.take() {
            // SAFETY: `ptr` is an owned TX loan handle that has not been sent.
            unsafe { up_lola_tx_loan_destroy(ptr.as_ptr()) }
        }
    }
}

pub(crate) struct NativeRxSample {
    ptr: NonNull<UpLolaRxSample>,
    expected_len: usize,
    expected_alignment: usize,
}

// SAFETY: RX sample ownership moves as an opaque native handle.
unsafe impl Send for NativeRxSample {}
// SAFETY: RX samples are immutable after the bridge returns them. Shared Rust
// references only read the sample data pointer and size and never mutate native
// state; destruction still happens exactly once when the owning wrapper drops.
unsafe impl Sync for NativeRxSample {}

impl NativeRxSample {
    fn new(
        ptr: *mut UpLolaRxSample,
        expected_len: usize,
        expected_alignment: usize,
    ) -> Result<Self, UStatus> {
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            UStatus::fail_with_code(UCode::Internal, "LoLa bridge returned null RX sample")
        })?;
        let sample = Self {
            ptr,
            expected_len,
            expected_alignment,
        };
        sample.data_parts("LoLa RX sample")?;
        Ok(sample)
    }

    fn data_parts(&self, label: &str) -> Result<(*const u8, usize), UStatus> {
        // SAFETY: `self.ptr` is a live RX sample handle owned by this wrapper.
        let data = unsafe { up_lola_rx_sample_data(self.ptr.as_ptr()) };
        // SAFETY: `self.ptr` is the same live RX sample handle as above.
        let len = unsafe { up_lola_rx_sample_size(self.ptr.as_ptr()) };
        validate_sample_parts(data, len, self.expected_len, self.expected_alignment, label)?;
        Ok((data, len))
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        let (data, len) = self
            .data_parts("LoLa RX sample")
            .expect("LoLa RX sample data should remain valid while owned");
        // SAFETY: `data_parts` verifies non-null, length, and alignment, and the
        // bridge owns initialized sample bytes for this wrapper lifetime.
        unsafe { slice::from_raw_parts(data, len) }
    }
}

impl Drop for NativeRxSample {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is the live RX sample handle owned by this wrapper.
        unsafe { up_lola_rx_sample_destroy(self.ptr.as_ptr()) }
    }
}

fn validate_sample_parts(
    data: *const u8,
    len: usize,
    expected_len: usize,
    expected_alignment: usize,
    label: &str,
) -> Result<(), UStatus> {
    if data.is_null() {
        return Err(UStatus::fail_with_code(
            UCode::Internal,
            format!("{label} data pointer is null"),
        ));
    }
    if len != expected_len {
        return Err(UStatus::fail_with_code(
            UCode::Internal,
            format!("{label} length {len} does not match configured sample size {expected_len}"),
        ));
    }
    if expected_alignment == 0 || !expected_alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::Internal,
            format!("{label} has invalid configured sample alignment {expected_alignment}"),
        ));
    }
    if !(data as usize).is_multiple_of(expected_alignment) {
        return Err(UStatus::fail_with_code(
            UCode::Internal,
            format!("{label} data pointer does not satisfy configured sample alignment {expected_alignment}"),
        ));
    }
    Ok(())
}

fn map_status(status: UpLolaStatusCode, operation: &str) -> Result<(), UStatus> {
    match status {
        UpLolaStatusCode::Ok => Ok(()),
        UpLolaStatusCode::InvalidArgument => Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("LoLa bridge failed to {operation}"),
        )),
        UpLolaStatusCode::NotFound => Err(UStatus::fail_with_code(
            UCode::NotFound,
            format!("LoLa bridge found no sample while trying to {operation}"),
        )),
        UpLolaStatusCode::ResourceExhausted => Err(UStatus::fail_with_code(
            UCode::ResourceExhausted,
            format!("LoLa bridge exhausted resources while trying to {operation}"),
        )),
        UpLolaStatusCode::Internal => Err(UStatus::fail_with_code(
            UCode::Internal,
            format!("LoLa bridge failed to {operation}"),
        )),
    }
}

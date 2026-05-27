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
#[allow(dead_code)]
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
}

// SAFETY: The native bridge treats `UpLolaTransport` handles as thread-safe
// transport objects. Rust only stores and forwards the opaque non-null handle;
// all synchronization requirements are part of the bridge contract.
unsafe impl Send for NativeTransport {}
// SAFETY: Shared references to `NativeTransport` only call bridge functions
// that accept the opaque handle and perform their own synchronization.
unsafe impl Sync for NativeTransport {}

impl NativeTransport {
    pub(crate) fn new(config: &LolaTransportConfig) -> Result<Self, UStatus> {
        let ffi_config = UpLolaConfig::new(config);
        let mut out = std::ptr::null_mut();
        // SAFETY: `ffi_config` and `out` are valid for the duration of the call;
        // the bridge initializes `out` to either null on failure or an owned
        // transport handle on success.
        let status = unsafe { up_lola_transport_create(&ffi_config, &mut out) };
        map_status(status, "create LoLa transport")?;
        let ptr = NonNull::new(out).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INTERNAL, "LoLa bridge returned null transport")
        })?;
        Ok(Self { ptr })
    }

    pub(crate) fn reserve(&self) -> Result<NativeTxLoan, UStatus> {
        let mut out = std::ptr::null_mut();
        // SAFETY: `self.ptr` is a live bridge handle owned by this wrapper, and
        // `out` is a valid out-parameter for one TX loan handle.
        let status = unsafe { up_lola_transport_reserve(self.ptr.as_ptr(), &mut out) };
        map_status(status, "reserve LoLa sample")?;
        NativeTxLoan::new(out)
    }

    pub(crate) fn send(&self, mut loan: NativeTxLoan) -> Result<(), UStatus> {
        let raw = loan.take();
        // SAFETY: `raw` is an owned TX loan handle consumed exactly once by the
        // bridge send call; `self.ptr` is a live transport handle.
        let status = unsafe { up_lola_transport_send(self.ptr.as_ptr(), raw.as_ptr()) };
        map_status(status, "send LoLa sample")
    }
}

impl Drop for NativeTransport {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is the live transport handle owned by this wrapper
        // and is destroyed exactly once from `Drop`.
        unsafe { up_lola_transport_destroy(self.ptr.as_ptr()) }
    }
}

pub(crate) struct NativeSubscriber {
    ptr: NonNull<UpLolaSubscriber>,
}

// SAFETY: The native bridge treats subscriber handles as thread-safe opaque
// objects; Rust only forwards the non-null handle to bridge calls.
unsafe impl Send for NativeSubscriber {}
// SAFETY: Shared references call bridge functions that must synchronize access
// internally according to the LoLa bridge contract.
unsafe impl Sync for NativeSubscriber {}

impl NativeSubscriber {
    pub(crate) fn new(config: &LolaTransportConfig) -> Result<Self, UStatus> {
        let ffi_config = UpLolaConfig::new(config);
        let mut out = std::ptr::null_mut();
        // SAFETY: `ffi_config` and `out` are valid for the duration of the call;
        // the bridge initializes `out` to an owned subscriber handle on success.
        let status = unsafe { up_lola_subscriber_create(&ffi_config, &mut out) };
        map_status(status, "create LoLa subscriber")?;
        let ptr = NonNull::new(out).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INTERNAL, "LoLa bridge returned null subscriber")
        })?;
        Ok(Self { ptr })
    }

    pub(crate) fn receive(&self) -> Result<NativeRxSample, UStatus> {
        let mut out = std::ptr::null_mut();
        // SAFETY: `self.ptr` is a live subscriber handle and `out` is a valid
        // out-parameter for one received sample handle.
        let status = unsafe { up_lola_subscriber_receive(self.ptr.as_ptr(), &mut out) };
        map_status(status, "receive LoLa sample")?;
        NativeRxSample::new(out)
    }
}

impl Drop for NativeSubscriber {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is the live subscriber handle owned by this wrapper
        // and is destroyed exactly once from `Drop`.
        unsafe { up_lola_subscriber_destroy(self.ptr.as_ptr()) }
    }
}

pub(crate) struct NativeTxLoan {
    ptr: Option<NonNull<UpLolaTxLoan>>,
}

// SAFETY: TX loan ownership moves between threads only as an opaque handle; the
// bridge owns the actual sample storage and defines the thread-safety contract.
unsafe impl Send for NativeTxLoan {}

impl NativeTxLoan {
    fn new(ptr: *mut UpLolaTxLoan) -> Result<Self, UStatus> {
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INTERNAL, "LoLa bridge returned null TX loan")
        })?;
        Ok(Self { ptr: Some(ptr) })
    }

    fn take(&mut self) -> NonNull<UpLolaTxLoan> {
        self.ptr
            .take()
            .expect("LoLa native TX loan should not be consumed twice")
    }

    pub(crate) fn len(&self) -> usize {
        let ptr = self.ptr.expect("LoLa TX loan is still owned");
        // SAFETY: `ptr` is a live TX loan handle still owned by this wrapper.
        unsafe { up_lola_tx_loan_size(ptr.as_ptr()) }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        let ptr = self.ptr.expect("LoLa TX loan is still owned");
        // SAFETY: `ptr` is a live TX loan handle. The bridge returns a pointer
        // to `len` initialized bytes valid for the loan lifetime.
        let data = unsafe { up_lola_tx_loan_data(ptr.as_ptr()) };
        // SAFETY: `ptr` is the same live TX loan handle as above.
        let len = unsafe { up_lola_tx_loan_size(ptr.as_ptr()) };
        // SAFETY:
        // - The native bridge supplies a non-null pointer to `len` initialized
        //   bytes in one live TX loan allocation.
        // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts.html#safety:
        //
        //   "`data` must be non-null, valid for reads for
        //   `len * size_of::<T>()` many bytes, and it must be properly aligned"
        //   and "The entire memory range of this slice must be contained within
        //   a single allocation!"
        unsafe { slice::from_raw_parts(data.cast_const(), len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.ptr.expect("LoLa TX loan is still owned");
        // SAFETY: `ptr` is a live TX loan handle exclusively borrowed through
        // `&mut self`; the bridge returns the mutable sample data pointer.
        let data = unsafe { up_lola_tx_loan_data(ptr.as_ptr()) };
        // SAFETY: `ptr` is the same live TX loan handle as above.
        let len = unsafe { up_lola_tx_loan_size(ptr.as_ptr()) };
        // SAFETY:
        // - The bridge guarantees `data..data+len` is one valid mutable loan
        //   allocation for initialized `u8` bytes while `&mut self` is held.
        // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts_mut.html#safety:
        //
        //   "`data` must be non-null, valid for both reads and writes for
        //   `len * size_of::<T>()` many bytes, and it must be properly aligned"
        //   and "The memory referenced by the returned slice must not be
        //   accessed through any other pointer" for the returned lifetime.
        unsafe { slice::from_raw_parts_mut(data, len) }
    }

    pub(crate) fn as_uninit_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        let ptr = self.ptr.expect("LoLa TX loan is still owned");
        // SAFETY: `ptr` is a live TX loan handle exclusively borrowed through
        // `&mut self`; the bridge returns the mutable sample data pointer.
        let data = unsafe { up_lola_tx_loan_data(ptr.as_ptr()) };
        // SAFETY: `ptr` is the same live TX loan handle as above.
        let len = unsafe { up_lola_tx_loan_size(ptr.as_ptr()) };
        // SAFETY:
        // - The bridge provides one mutable loan allocation for `len` bytes.
        // - Per https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#layout-1:
        //
        //   "`MaybeUninit<T>` is guaranteed to have the same size, alignment,
        //   and ABI as `T`."
        //
        // - The `from_raw_parts_mut` requirements for non-null, alignment,
        //   single allocation, and unique access are supplied by the native loan
        //   contract and the `&mut self` borrow.
        unsafe { slice::from_raw_parts_mut(data.cast::<MaybeUninit<u8>>(), len) }
    }
}

impl Drop for NativeTxLoan {
    fn drop(&mut self) {
        if let Some(ptr) = self.ptr.take() {
            // SAFETY: `ptr` is an owned TX loan handle that has not been sent;
            // taking it from `Option` ensures destruction happens at most once.
            unsafe { up_lola_tx_loan_destroy(ptr.as_ptr()) }
        }
    }
}

pub(crate) struct NativeRxSample {
    ptr: NonNull<UpLolaRxSample>,
}

// SAFETY: RX sample ownership moves as an opaque handle; the bridge owns the
// underlying sample storage and defines the thread-safety contract.
unsafe impl Send for NativeRxSample {}

impl NativeRxSample {
    fn new(ptr: *mut UpLolaRxSample) -> Result<Self, UStatus> {
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INTERNAL, "LoLa bridge returned null RX sample")
        })?;
        Ok(Self { ptr })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: `self.ptr` is a live RX sample handle owned by this wrapper;
        // the bridge returns a pointer to initialized sample bytes.
        let data = unsafe { up_lola_rx_sample_data(self.ptr.as_ptr()) };
        // SAFETY: `self.ptr` is the same live RX sample handle as above.
        let len = unsafe { up_lola_rx_sample_size(self.ptr.as_ptr()) };
        // SAFETY:
        // - The bridge guarantees the returned sample data pointer is non-null,
        //   aligned, and valid for `len` initialized `u8` elements for the
        //   lifetime of this RX sample wrapper.
        // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts.html#safety:
        //
        //   "`data` must point to `len` consecutive properly initialized values
        //   of type `T`" and the whole range must be contained in one allocation.
        unsafe { slice::from_raw_parts(data, len) }
    }
}

impl Drop for NativeRxSample {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is the live RX sample handle owned by this wrapper
        // and is destroyed exactly once from `Drop`.
        unsafe { up_lola_rx_sample_destroy(self.ptr.as_ptr()) }
    }
}

fn map_status(status: UpLolaStatusCode, operation: &str) -> Result<(), UStatus> {
    match status {
        UpLolaStatusCode::Ok => Ok(()),
        UpLolaStatusCode::InvalidArgument => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("LoLa bridge failed to {operation}"),
        )),
        UpLolaStatusCode::NotFound => Err(UStatus::fail_with_code(
            UCode::NOT_FOUND,
            format!("LoLa bridge found no sample while trying to {operation}"),
        )),
        UpLolaStatusCode::ResourceExhausted => Err(UStatus::fail_with_code(
            UCode::RESOURCE_EXHAUSTED,
            format!("LoLa bridge exhausted resources while trying to {operation}"),
        )),
        UpLolaStatusCode::Internal => Err(UStatus::fail_with_code(
            UCode::INTERNAL,
            format!("LoLa bridge failed to {operation}"),
        )),
    }
}

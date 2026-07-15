#![allow(dead_code)]

/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#[cfg(feature = "lola-ffi")]
use std::sync::Arc;
use std::{io::Cursor, mem::MaybeUninit};

use up_rust::transport_implementer_api::{UEncodedLoanedRxFrame, UEncodedRxFrame};
use up_rust::{
    LoanedPayload, PayloadLoanProvenance, UCode, UFrameMetadata, UFrameView,
    ULoanedContiguousZeroCopyRxFrame, UStatus, UTxBuffer, UUninitTxBuffer, UWireError,
    UZeroCopyRxLease,
};
#[cfg(test)]
use up_rust::{PayloadEncoding, UUri};

#[cfg(feature = "lola-ffi")]
use crate::sys::{NativeRxSample, NativeTxLoan};

const LOLA_FRAME_MAGIC: &[u8; 4] = b"ULOL";
const LOLA_FRAME_VERSION: u8 = 1;
const LOLA_FRAME_HEADER_LEN: usize = 20;

/// LoLa transmit loan for one native uProtocol frame.
///
/// The exposed payload range excludes the hidden `ULOL` header, serialized
/// metadata, and alignment padding.
pub struct LolaTxLoan {
    metadata: UFrameMetadata,
    encoded_metadata: Vec<u8>,
    sample: LolaTxStorage,
    channel: LolaTxChannel,
    payload_offset: usize,
    payload_len: usize,
}

/// LoLa transmit loan whose visible application payload is not initialized yet.
pub struct LolaUninitTxLoan {
    metadata: UFrameMetadata,
    encoded_metadata: Vec<u8>,
    sample: LolaUninitTxStorage,
    channel: LolaTxChannel,
    payload_offset: usize,
    payload_len: usize,
}

/// Internal LoLa event channel that owns a TX loan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LolaTxChannel {
    /// Primary LoLa event, used for non-RPC frames and RPC requests.
    Primary,
    /// Optional response LoLa event, used for RPC responses.
    Response,
}

enum LolaTxStorage {
    #[cfg(feature = "test-stub")]
    Vec(Vec<u8>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeTxLoan),
    #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
    Unavailable,
}

enum LolaUninitTxStorage {
    #[cfg(feature = "test-stub")]
    Vec(Vec<MaybeUninit<u8>>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeTxLoan),
    #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
    Unavailable,
}

impl LolaTxStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_slice(),
            #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
            Self::Unavailable => {
                unreachable!("LoLa storage is unavailable without a backend feature")
            }
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_mut_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_mut_slice(),
            #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
            Self::Unavailable => {
                unreachable!("LoLa storage is unavailable without a backend feature")
            }
        }
    }
}

impl LolaUninitTxStorage {
    fn as_uninit_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_mut_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_uninit_slice(),
            #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
            Self::Unavailable => {
                unreachable!("LoLa storage is unavailable without a backend feature")
            }
        }
    }
}

impl LolaTxLoan {
    #[cfg(feature = "test-stub")]
    pub(crate) fn new_vec(
        metadata: UFrameMetadata,
        encoded_metadata: Vec<u8>,
        sample_size: usize,
        payload_len: usize,
        payload_alignment: usize,
        channel: LolaTxChannel,
    ) -> Result<Self, UStatus> {
        let mut sample = vec![0_u8; sample_size];
        let payload_offset = write_frame_header(
            &encoded_metadata,
            &mut sample,
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
            encoded_metadata,
            sample: LolaTxStorage::Vec(sample),
            channel,
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn new_native(
        metadata: UFrameMetadata,
        encoded_metadata: Vec<u8>,
        mut sample: NativeTxLoan,
        payload_len: usize,
        payload_alignment: usize,
        channel: LolaTxChannel,
    ) -> Result<Self, UStatus> {
        let sample_len = sample.len();
        initialize_uninit_range(sample.as_uninit_slice(), 0, sample_len)?;
        let payload_offset = write_frame_header(
            &encoded_metadata,
            sample.as_mut_slice(),
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
            encoded_metadata,
            sample: LolaTxStorage::Native(sample),
            channel,
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "test-stub")]
    pub(crate) fn into_vec(self) -> Vec<u8> {
        match self.sample {
            LolaTxStorage::Vec(sample) => sample,
            #[cfg(feature = "lola-ffi")]
            LolaTxStorage::Native(_) => {
                panic!("native LoLa TX storage cannot be converted into a test vector")
            }
        }
    }

    #[cfg(feature = "test-stub")]
    pub(crate) fn clone_as_rx(&self) -> LolaRxLease {
        LolaRxLease {
            metadata: self.metadata.clone(),
            metadata_len: self.encoded_metadata.len(),
            sample: LolaRxStorage::Vec(self.sample.as_slice().to_vec()),
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
        }
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn into_native(self) -> Result<(LolaTxChannel, NativeTxLoan), UStatus> {
        if self.sample.as_slice().get(..4) != Some(LOLA_FRAME_MAGIC.as_slice()) {
            return Err(UStatus::fail_with_code(
                UCode::Internal,
                "LoLa TX frame header was not written before send",
            ));
        }
        let channel = self.channel;
        match self.sample {
            LolaTxStorage::Native(sample) => Ok((channel, sample)),
            #[cfg(feature = "test-stub")]
            LolaTxStorage::Vec(_) => Err(UStatus::fail_with_code(
                UCode::Internal,
                "test-vector LoLa TX storage cannot be sent through the native bridge",
            )),
        }
    }
}

impl LolaUninitTxLoan {
    #[cfg(feature = "test-stub")]
    pub(crate) fn new_vec(
        metadata: UFrameMetadata,
        encoded_metadata: Vec<u8>,
        sample_size: usize,
        payload_len: usize,
        payload_alignment: usize,
        channel: LolaTxChannel,
    ) -> Result<Self, UStatus> {
        let mut sample = vec![MaybeUninit::uninit(); sample_size];
        let payload_offset = write_frame_header_uninit(
            &encoded_metadata,
            &mut sample,
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
            encoded_metadata,
            sample: LolaUninitTxStorage::Vec(sample),
            channel,
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn new_native(
        metadata: UFrameMetadata,
        encoded_metadata: Vec<u8>,
        mut sample: NativeTxLoan,
        payload_len: usize,
        payload_alignment: usize,
        channel: LolaTxChannel,
    ) -> Result<Self, UStatus> {
        let payload_offset = write_frame_header_uninit(
            &encoded_metadata,
            sample.as_uninit_slice(),
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
            encoded_metadata,
            sample: LolaUninitTxStorage::Native(sample),
            channel,
            payload_offset,
            payload_len,
        })
    }
}

impl UTxBuffer for LolaTxLoan {
    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload(&self) -> &[u8] {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("LoLa payload layout overflow");
        self.sample
            .as_slice()
            .get(self.payload_offset..end)
            .expect("LoLa payload layout should be valid")
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("LoLa payload layout overflow");
        self.sample
            .as_mut_slice()
            .get_mut(self.payload_offset..end)
            .expect("LoLa payload layout should be valid")
    }
}

impl UUninitTxBuffer for LolaUninitTxLoan {
    type Initialized = LolaTxLoan;

    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload_len(&self) -> usize {
        self.payload_len
    }

    fn payload_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("LoLa payload layout overflow");
        self.sample
            .as_uninit_slice()
            .get_mut(self.payload_offset..end)
            .expect("LoLa payload layout should be valid")
    }

    unsafe fn assume_payload_init(self) -> Self::Initialized {
        let sample = match self.sample {
            #[cfg(feature = "test-stub")]
            LolaUninitTxStorage::Vec(mut sample) => {
                let len = sample.len();
                let capacity = sample.capacity();
                let ptr = sample.as_mut_ptr().cast::<u8>();
                std::mem::forget(sample);
                // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`; the
                // frame header/tail bytes were initialized before payload access,
                // and the caller guarantees the visible payload bytes are initialized.
                LolaTxStorage::Vec(unsafe { Vec::from_raw_parts(ptr, len, capacity) })
            }
            #[cfg(feature = "lola-ffi")]
            LolaUninitTxStorage::Native(sample) => LolaTxStorage::Native(sample),
            #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
            LolaUninitTxStorage::Unavailable => LolaTxStorage::Unavailable,
        };
        LolaTxLoan {
            metadata: self.metadata,
            encoded_metadata: self.encoded_metadata,
            sample,
            channel: self.channel,
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
        }
    }
}

/// LoLa receive lease for one native uProtocol frame.
///
/// Borrowed payload views refer to the visible application payload range inside
/// the sample and exclude the hidden LoLa frame header, metadata, and padding.
pub struct LolaRxLease {
    metadata: UFrameMetadata,
    metadata_len: usize,
    sample: LolaRxStorage,
    payload_offset: usize,
    payload_len: usize,
}

impl Clone for LolaRxLease {
    fn clone(&self) -> Self {
        Self {
            metadata: self.metadata.clone(),
            metadata_len: self.metadata_len,
            sample: self.sample.clone(),
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
        }
    }
}

enum LolaRxStorage {
    #[cfg(feature = "test-stub")]
    Vec(Vec<u8>),
    #[cfg(feature = "lola-ffi")]
    Native(Arc<NativeRxSample>),
    #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
    Unavailable,
}

impl Clone for LolaRxStorage {
    fn clone(&self) -> Self {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => Self::Vec(sample.clone()),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => Self::Native(Arc::clone(sample)),
            #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
            Self::Unavailable => Self::Unavailable,
        }
    }
}

impl LolaRxStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_slice(),
            #[cfg(not(any(feature = "test-stub", feature = "lola-ffi")))]
            Self::Unavailable => {
                unreachable!("LoLa storage is unavailable without a backend feature")
            }
        }
    }
}

impl LolaRxLease {
    #[cfg(feature = "test-stub")]
    pub(crate) fn from_vec(sample: Vec<u8>) -> Result<Self, UStatus> {
        let (metadata_len, payload_offset, payload_len) = read_frame_header(&sample)?;
        Ok(Self {
            metadata: unavailable_metadata(),
            metadata_len,
            sample: LolaRxStorage::Vec(sample),
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn from_native(sample: NativeRxSample) -> Result<Self, UStatus> {
        let (metadata_len, payload_offset, payload_len) = read_frame_header(sample.as_slice())?;
        Ok(Self {
            metadata: unavailable_metadata(),
            metadata_len,
            sample: LolaRxStorage::Native(Arc::new(sample)),
            payload_offset,
            payload_len,
        })
    }

    fn contiguous_payload(&self) -> &[u8] {
        UFrameView::try_contiguous_payload(self)
            .expect("LoLa receive payload layout should be valid")
    }
}

impl UFrameView for LolaRxLease {
    type PayloadReader<'a>
        = Cursor<&'a [u8]>
    where
        Self: 'a;
    type PayloadSlices<'a>
        = std::iter::Once<&'a [u8]>
    where
        Self: 'a;

    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload_len(&self) -> usize {
        self.payload_len
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        Cursor::new(self.contiguous_payload())
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(self.contiguous_payload())
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        let end = self.payload_offset.checked_add(self.payload_len)?;
        self.sample.as_slice().get(self.payload_offset..end)
    }
}

impl UEncodedRxFrame for LolaRxLease {
    type PayloadReader<'a>
        = Cursor<&'a [u8]>
    where
        Self: 'a;
    type PayloadSlices<'a>
        = std::iter::Once<&'a [u8]>
    where
        Self: 'a;

    fn encoded_metadata(&self) -> &[u8] {
        let metadata_end = LOLA_FRAME_HEADER_LEN
            .checked_add(self.metadata_len)
            .expect("LoLa metadata layout overflow");
        self.sample
            .as_slice()
            .get(LOLA_FRAME_HEADER_LEN..metadata_end)
            .expect("LoLa metadata layout should be valid")
    }

    fn payload_len(&self) -> usize {
        self.payload_len
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        Cursor::new(self.contiguous_payload())
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(self.contiguous_payload())
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        let end = self.payload_offset.checked_add(self.payload_len)?;
        self.sample.as_slice().get(self.payload_offset..end)
    }
}

impl UZeroCopyRxLease for LolaRxLease {}

impl UEncodedLoanedRxFrame for LolaRxLease {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, UWireError> {
        loaned_contiguous_payload(self)
    }
}

impl ULoanedContiguousZeroCopyRxFrame for LolaRxLease {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, UWireError> {
        loaned_contiguous_payload(self)
    }
}

fn loaned_contiguous_payload(frame: &LolaRxLease) -> Result<LoanedPayload<'_>, UWireError> {
    if !frame.has_payload() {
        return Err(UWireError::MissingPayload);
    }
    let payload = UEncodedRxFrame::try_contiguous_payload(frame)
        .ok_or_else(|| UWireError::invalid_payload("LoLa receive payload is not contiguous"))?;
    // SAFETY: `payload` is a subslice of this lease's LoLa sample and excludes
    // the hidden header, metadata, and alignment padding.
    Ok(
        unsafe {
            LoanedPayload::new_unchecked(payload, PayloadLoanProvenance::OpaqueTransportLoan)
        },
    )
}

fn write_frame_header(
    encoded_metadata: &[u8],
    sample: &mut [u8],
    payload_len: usize,
    payload_alignment: usize,
) -> Result<usize, UStatus> {
    let metadata_len = u32::try_from(encoded_metadata.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata is too large")
    })?;
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload is too large")
    })?;
    let payload_offset = payload_offset_for_len(
        encoded_metadata.len(),
        payload_alignment,
        sample.as_ptr() as usize,
    )?;
    let payload_offset_u32 = u32::try_from(payload_offset).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload offset is too large")
    })?;
    let total_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa frame layout overflow")
    })?;
    if total_len > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::ResourceExhausted,
            format!(
                "LoLa frame requires {total_len} bytes but sample has {} bytes",
                sample.len()
            ),
        ));
    }

    sample[..payload_offset].fill(0);
    sample[0..4].copy_from_slice(LOLA_FRAME_MAGIC);
    sample[4] = LOLA_FRAME_VERSION;
    sample[8..12].copy_from_slice(&metadata_len.to_le_bytes());
    sample[12..16].copy_from_slice(&payload_len_u32.to_le_bytes());
    sample[16..20].copy_from_slice(&payload_offset_u32.to_le_bytes());
    let metadata_end = LOLA_FRAME_HEADER_LEN + encoded_metadata.len();
    sample[LOLA_FRAME_HEADER_LEN..metadata_end].copy_from_slice(encoded_metadata);
    Ok(payload_offset)
}

fn write_frame_header_uninit(
    encoded_metadata: &[u8],
    sample: &mut [MaybeUninit<u8>],
    payload_len: usize,
    payload_alignment: usize,
) -> Result<usize, UStatus> {
    let metadata_len = u32::try_from(encoded_metadata.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata is too large")
    })?;
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload is too large")
    })?;
    let payload_offset = frame_layout_bounds(
        sample.len(),
        encoded_metadata.len(),
        payload_len,
        payload_alignment,
        sample.as_ptr() as usize,
    )?
    .0;
    let payload_offset_u32 = u32::try_from(payload_offset).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload offset is too large")
    })?;
    initialize_uninit_range(sample, 0, payload_offset)?;
    let payload_end = payload_offset.checked_add(payload_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa frame layout overflow")
    })?;
    initialize_uninit_range(sample, payload_end, sample.len())?;
    let initialized = initialized_prefix_mut(sample, payload_offset)?;
    initialized[0..4].copy_from_slice(LOLA_FRAME_MAGIC);
    initialized[4] = LOLA_FRAME_VERSION;
    initialized[8..12].copy_from_slice(&metadata_len.to_le_bytes());
    initialized[12..16].copy_from_slice(&payload_len_u32.to_le_bytes());
    initialized[16..20].copy_from_slice(&payload_offset_u32.to_le_bytes());
    let metadata_end = LOLA_FRAME_HEADER_LEN + encoded_metadata.len();
    initialized[LOLA_FRAME_HEADER_LEN..metadata_end].copy_from_slice(encoded_metadata);
    Ok(payload_offset)
}

fn read_frame_header(sample: &[u8]) -> Result<(usize, usize, usize), UStatus> {
    if sample.len() < LOLA_FRAME_HEADER_LEN {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa sample is shorter than frame header",
        ));
    }
    if &sample[0..4] != LOLA_FRAME_MAGIC {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!(
                "invalid LoLa frame magic: {:02X} {:02X} {:02X} {:02X}",
                sample[0], sample[1], sample[2], sample[3]
            ),
        ));
    }
    if sample[4] != LOLA_FRAME_VERSION {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "unsupported LoLa frame version",
        ));
    }
    let metadata_len = u32::from_le_bytes(sample[8..12].try_into().expect("slice length")) as usize;
    let payload_len = u32::from_le_bytes(sample[12..16].try_into().expect("slice length")) as usize;
    let payload_offset =
        u32::from_le_bytes(sample[16..20].try_into().expect("slice length")) as usize;
    let metadata_end = LOLA_FRAME_HEADER_LEN
        .checked_add(metadata_len)
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata layout overflow")
        })?;
    if metadata_end > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa metadata range is outside sample bounds",
        ));
    }
    if payload_offset < metadata_end {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa payload overlaps metadata",
        ));
    }
    let payload_end = payload_offset.checked_add(payload_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload layout overflow")
    })?;
    if payload_end > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa payload range is outside sample bounds",
        ));
    }
    Ok((metadata_len, payload_offset, payload_len))
}

fn frame_layout_bounds(
    sample_len: usize,
    metadata_len: usize,
    payload_len: usize,
    payload_alignment: usize,
    sample_address: usize,
) -> Result<(usize, usize), UStatus> {
    let payload_offset = payload_offset_for_len(metadata_len, payload_alignment, sample_address)?;
    let total_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa frame layout overflow")
    })?;
    if total_len > sample_len {
        return Err(UStatus::fail_with_code(
            UCode::ResourceExhausted,
            format!("LoLa frame requires {total_len} bytes but sample has {sample_len} bytes"),
        ));
    }
    Ok((payload_offset, total_len))
}

fn payload_offset_for_len(
    metadata_len: usize,
    payload_alignment: usize,
    sample_address: usize,
) -> Result<usize, UStatus> {
    let metadata_end = LOLA_FRAME_HEADER_LEN
        .checked_add(metadata_len)
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata layout overflow")
        })?;
    let absolute_metadata_end = sample_address.checked_add(metadata_end).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata layout overflow")
    })?;
    let absolute_payload = align_up(absolute_metadata_end, payload_alignment)?;
    absolute_payload.checked_sub(sample_address).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload layout overflow")
    })
}

fn align_up(value: usize, alignment: usize) -> Result<usize, UStatus> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa payload alignment must be a non-zero power of two",
        ));
    }
    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }
    value.checked_add(alignment - remainder).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa frame layout overflow")
    })
}

fn initialize_uninit_range(
    sample: &mut [MaybeUninit<u8>],
    start: usize,
    end: usize,
) -> Result<(), UStatus> {
    let range = sample.get_mut(start..end).ok_or_else(|| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa initialization range is invalid",
        )
    })?;
    for byte in range {
        byte.write(0);
    }
    Ok(())
}

fn initialized_prefix_mut(
    sample: &mut [MaybeUninit<u8>],
    end: usize,
) -> Result<&mut [u8], UStatus> {
    let prefix = sample.get_mut(..end).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa prefix range is invalid")
    })?;
    // SAFETY: The requested prefix is in bounds and every byte in it was written
    // before this conversion; `MaybeUninit<u8>` has the same layout as `u8`.
    Ok(unsafe { std::slice::from_raw_parts_mut(prefix.as_mut_ptr().cast::<u8>(), prefix.len()) })
}

fn unavailable_metadata() -> UFrameMetadata {
    let topic = up_rust::UUri::try_from("//vehicle/4210/1/9fff").expect("valid fallback URI");
    UFrameMetadata::publish(topic)
        .build()
        .expect("valid fallback metadata")
}

#[cfg(test)]
fn deterministic_raw_metadata() -> UFrameMetadata {
    let topic = UUri::try_from("//vehicle/4210/1/9008").expect("valid test URI");
    UFrameMetadata::publish(topic)
        .with_payload_encoding(PayloadEncoding::RAW)
        .build()
        .expect("valid frame metadata")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uninit_frame_header_does_not_zero_application_payload_range() {
        let encoded_metadata = b"prepared-metadata";
        let payload_len = 8;
        let payload_alignment = 4;
        let mut sample = vec![MaybeUninit::new(0xA5); 256];

        let payload_offset = write_frame_header_uninit(
            encoded_metadata,
            &mut sample,
            payload_len,
            payload_alignment,
        )
        .unwrap();
        let payload_end = payload_offset + payload_len;

        for byte in sample
            .get(payload_offset..payload_end)
            .expect("payload range should be in bounds")
        {
            // SAFETY: The test initialized every element with `MaybeUninit::new`.
            assert_eq!(unsafe { byte.assume_init() }, 0xA5);
        }
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_malformed_physical_layout_is_rejected() {
        let error = match LolaRxLease::from_vec(vec![0_u8; 8]) {
            Ok(_) => panic!("malformed LoLa frame should be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.code(), UCode::InvalidArgument);
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_frame_hides_header_and_reports_opaque_provenance() {
        let metadata = deterministic_raw_metadata();
        let encoded_metadata = b"prepared-metadata".to_vec();
        let mut loan = LolaTxLoan::new_vec(
            metadata,
            encoded_metadata.clone(),
            256,
            3,
            8,
            LolaTxChannel::Primary,
        )
        .unwrap();
        loan.payload_mut().copy_from_slice(b"abc");

        let sample = loan.into_vec();
        assert_eq!(&sample[0..4], LOLA_FRAME_MAGIC);
        assert_eq!(sample[4], LOLA_FRAME_VERSION);
        assert_ne!(&sample[0..3], b"abc");

        let lease = LolaRxLease::from_vec(sample).unwrap();
        assert_eq!(lease.encoded_metadata(), encoded_metadata.as_slice());
        assert_eq!(
            UFrameView::try_contiguous_payload(&lease),
            Some(b"abc".as_slice())
        );
        let payload = ULoanedContiguousZeroCopyRxFrame::loaned_contiguous_payload(&lease).unwrap();
        assert_eq!(payload.as_bytes(), b"abc");
        assert_eq!(
            payload.provenance(),
            PayloadLoanProvenance::OpaqueTransportLoan
        );
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn test_stub_uninit_tx_loan_commits_after_payload_initialization() {
        let metadata = deterministic_raw_metadata();
        let mut loan = LolaUninitTxLoan::new_vec(
            metadata,
            b"prepared-metadata".to_vec(),
            256,
            3,
            4,
            LolaTxChannel::Primary,
        )
        .unwrap();
        for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(*b"xyz") {
            slot.write(byte);
        }

        // SAFETY: The test wrote exactly every visible payload byte above.
        let loan = unsafe { loan.assume_payload_init() };
        assert_eq!(loan.payload(), b"xyz");
    }
}

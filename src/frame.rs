/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::{io::Cursor, mem::MaybeUninit};

use up_rust::transport_implementer_api::UEncodedRxFrame;
use up_rust::{
    LoanedPayload, PayloadLoanProvenance, UCode, UEncodedLoanedRxFrame, UFrameMetadata, UStatus,
    UTxBuffer, UUninitTxBuffer, UUri, UWireError,
};

#[cfg(feature = "lola-ffi")]
use crate::sys::{NativeRxSample, NativeTxLoan};

const LOLA_FRAME_MAGIC: &[u8; 4] = b"ULOL";
const LOLA_FRAME_VERSION: u8 = 2;
const LOLA_FRAME_HEADER_LEN: usize = 24;

/// LoLa transmit loan for one native uProtocol frame.
///
/// Values are returned through a selected-wire transport built from
/// [`LolaZeroCopyCore`](crate::LolaZeroCopyCore). The exposed
/// [`UTxBuffer::payload_mut`] range points at the application payload inside a
/// fixed LoLa event sample. The preceding `ULOL` header, physical routing hint,
/// encoded metadata, and alignment padding are hidden from callers.
///
/// Metadata is fixed when the loan is created so the payload offset remains
/// stable while serializers write directly into the exposed payload range.
pub struct LolaTxLoan {
    metadata: UFrameMetadata,
    sample: LolaTxStorage,
    channel: LolaTxChannel,
    payload_offset: usize,
    payload_len: usize,
}

/// LoLa transmit loan whose application payload bytes are intentionally uninitialized.
pub struct LolaUninitTxLoan {
    metadata: UFrameMetadata,
    sample: LolaUninitTxStorage,
    channel: LolaTxChannel,
    payload_offset: usize,
    payload_len: usize,
}

/// Internal LoLa event that owns a transmit loan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LolaTxChannel {
    /// Primary event used for non-RPC frames and RPC requests.
    Primary,
    /// Optional response event used for RPC responses.
    Response,
}

enum LolaTxStorage {
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    Vec(Vec<u8>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeTxLoan),
}

enum LolaUninitTxStorage {
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    Vec(Vec<MaybeUninit<u8>>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeTxLoan),
}

impl LolaTxStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
            Self::Vec(sample) => sample.as_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_slice(),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
            Self::Vec(sample) => sample.as_mut_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_mut_slice(),
        }
    }
}

impl LolaUninitTxStorage {
    fn as_uninit_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        match self {
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
            Self::Vec(sample) => sample.as_mut_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_uninit_slice(),
        }
    }
}

impl LolaTxLoan {
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
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
            &metadata,
            &encoded_metadata,
            &mut sample,
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
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
            &metadata,
            &encoded_metadata,
            sample.as_mut_slice(),
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
            sample: LolaTxStorage::Native(sample),
            channel,
            payload_offset,
            payload_len,
        })
    }

    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    pub(crate) fn into_stub_rx(self) -> Result<(LolaTxChannel, LolaRxLease), UStatus> {
        let channel = self.channel;
        match self.sample {
            LolaTxStorage::Vec(sample) => Ok((channel, LolaRxLease::from_vec(sample)?)),
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
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
            LolaTxStorage::Vec(_) => Err(UStatus::fail_with_code(
                UCode::Internal,
                "test-stub LoLa storage cannot be sent through the native bridge",
            )),
        }
    }
}

impl LolaUninitTxLoan {
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
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
            &metadata,
            &encoded_metadata,
            &mut sample,
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
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
            &metadata,
            &encoded_metadata,
            sample.as_uninit_slice(),
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
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

    unsafe fn assume_payload_initialized(self) -> Self::Initialized {
        // SAFETY CONTRACT:
        // - The caller of `UUninitTxBuffer::assume_payload_initialized` guarantees the
        //   visible application payload range returned by `payload_uninit_mut`
        //   was fully initialized before conversion.
        // - `write_frame_header_uninit` initialized the LoLa header, serialized
        //   metadata, alignment padding, and fixed-sample tail before exposing
        //   the uninitialized application payload range.
        // - Native mode transfers the same external LoLa sample handle into the
        //   initialized type-state; test-stub mode uses the `Vec::from_raw_parts`
        //   proof below to preserve allocation ownership.
        let sample = match self.sample {
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
            LolaUninitTxStorage::Vec(mut sample) => {
                let len = sample.len();
                let capacity = sample.capacity();
                let ptr = sample.as_mut_ptr().cast::<u8>();
                std::mem::forget(sample);
                // SAFETY:
                // - `MaybeUninit<u8>` has the same size, alignment, and ABI as
                //   `u8` per
                //   https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#layout-1:
                //
                //   "`MaybeUninit<T>` is guaranteed to have the same size,
                //   alignment, and ABI as `T`."
                //
                // - The caller of `assume_payload_init` guarantees the visible
                //   payload bytes are initialized; `write_frame_header_uninit`
                //   initialized the header, metadata, alignment padding, and
                //   fixed-sample tail bytes before the loan was exposed.
                // - `ptr`, `len`, and `capacity` come from the original Vec
                //   allocation, and `sample` was forgotten so the allocation has
                //   exactly one owner after `Vec::from_raw_parts`.
                // - Per https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.from_raw_parts,
                //   "The first `length` values must be properly initialized
                //   values of type `T`" and "The ownership of `ptr` is
                //   effectively transferred to the `Vec<T>`."
                LolaTxStorage::Vec(unsafe { Vec::from_raw_parts(ptr, len, capacity) })
            }
            #[cfg(feature = "lola-ffi")]
            LolaUninitTxStorage::Native(sample) => LolaTxStorage::Native(sample),
        };
        LolaTxLoan {
            metadata: self.metadata,
            sample,
            channel: self.channel,
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
        }
    }
}

/// LoLa receive lease for one native uProtocol frame.
///
/// Dropping the lease releases the underlying LoLa sample. The payload is a
/// contiguous byte range within the fixed event sample, so this type implements
/// [`UEncodedRxFrame`] while the selected-wire adapter owns metadata decoding,
/// identity checks and the public receive lease.
///
/// Invalid or stale samples are rejected before this type is constructed. The
/// parsed source/sink routing hint is used only by physical queue/listener
/// mechanics; selected-wire metadata is decoded and revalidated independently
/// before public delivery. Native diagnostics include the frame magic bytes when
/// they do not match `ULOL`.
pub struct LolaRxLease {
    routing_hint: LolaRoutingHint,
    metadata_offset: usize,
    metadata_len: usize,
    sample: LolaRxStorage,
    payload_offset: usize,
    payload_len: usize,
}

enum LolaRxStorage {
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    Vec(Vec<u8>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeRxSample),
}

struct LolaRoutingHint {
    source: UUri,
    sink: Option<UUri>,
}

impl LolaRxStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
            Self::Vec(sample) => sample.as_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_slice(),
        }
    }
}

impl LolaRxLease {
    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    pub(crate) fn from_vec(sample: Vec<u8>) -> Result<Self, UStatus> {
        let (routing_hint, metadata_offset, metadata_len, payload_offset, payload_len) =
            read_frame_header(&sample)?;
        Ok(Self {
            routing_hint,
            metadata_offset,
            metadata_len,
            sample: LolaRxStorage::Vec(sample),
            payload_offset,
            payload_len,
        })
    }

    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    pub(crate) fn clone_for_stub(&self) -> Result<Self, UStatus> {
        match &self.sample {
            LolaRxStorage::Vec(sample) => Self::from_vec(sample.clone()),
            #[cfg(feature = "lola-ffi")]
            LolaRxStorage::Native(_) => Err(UStatus::fail_with_code(
                UCode::Internal,
                "native LoLa samples cannot be cloned for the test stub",
            )),
        }
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn from_native(sample: NativeRxSample) -> Result<Self, UStatus> {
        let (routing_hint, metadata_offset, metadata_len, payload_offset, payload_len) =
            read_frame_header(sample.as_slice())?;
        Ok(Self {
            routing_hint,
            metadata_offset,
            metadata_len,
            sample: LolaRxStorage::Native(sample),
            payload_offset,
            payload_len,
        })
    }

    pub(crate) fn routing_hint(&self) -> (&UUri, Option<&UUri>) {
        (&self.routing_hint.source, self.routing_hint.sink.as_ref())
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
        let metadata_end = self
            .metadata_offset
            .checked_add(self.metadata_len)
            .expect("LoLa metadata layout overflow");
        self.sample
            .as_slice()
            .get(self.metadata_offset..metadata_end)
            .expect("LoLa metadata layout should be valid")
    }

    fn payload_len(&self) -> usize {
        self.payload_len
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        Cursor::new(self.try_contiguous_payload().unwrap_or_default())
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(self.try_contiguous_payload().unwrap_or_default())
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        let end = self.payload_offset.checked_add(self.payload_len)?;
        self.sample.as_slice().get(self.payload_offset..end)
    }
}

impl UEncodedLoanedRxFrame for LolaRxLease {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, UWireError> {
        let payload = self
            .try_contiguous_payload()
            .ok_or_else(|| UWireError::invalid_payload("LoLa payload is not contiguous"))?;
        if payload.is_empty() {
            return Err(UWireError::MissingPayload);
        }
        // SAFETY:
        // - `payload` is a borrowed range inside this receive lease's LoLa
        //   sample, so it remains valid for the lifetime of `&self`.
        // - The range excludes the hidden frame header, metadata, and padding;
        //   no allocation or coalescing is performed to produce it.
        // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts.html#safety,
        //   a borrowed slice's data must be "valid for reads" and "contained
        //   within a single allocation"; `try_contiguous_payload` returns a
        //   subslice from the sample allocation.
        Ok(unsafe {
            LoanedPayload::new_unchecked(payload, PayloadLoanProvenance::OpaqueTransportLoan)
        })
    }
}

fn write_frame_header(
    metadata: &UFrameMetadata,
    encoded_metadata: &[u8],
    sample: &mut [u8],
    payload_len: usize,
    payload_alignment: usize,
) -> Result<usize, UStatus> {
    let (source, sink) = encoded_routing_hint(metadata)?;
    let source_len = u16::try_from(source.len()).map_err(|_| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa source routing hint is too large",
        )
    })?;
    let sink_len = u16::try_from(sink.len()).map_err(|_| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa sink routing hint is too large",
        )
    })?;
    let routing_len = source.len().checked_add(sink.len()).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa routing hint layout overflow")
    })?;
    let metadata_len = u32::try_from(encoded_metadata.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata is too large")
    })?;
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload is too large")
    })?;
    let payload_offset = payload_offset_for_len(
        routing_len,
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
    sample[20..22].copy_from_slice(&source_len.to_le_bytes());
    sample[22..24].copy_from_slice(&sink_len.to_le_bytes());
    let source_end = LOLA_FRAME_HEADER_LEN + source.len();
    let sink_end = source_end + sink.len();
    let metadata_end = sink_end + encoded_metadata.len();
    sample[LOLA_FRAME_HEADER_LEN..source_end].copy_from_slice(source.as_bytes());
    sample[source_end..sink_end].copy_from_slice(sink.as_bytes());
    sample[sink_end..metadata_end].copy_from_slice(encoded_metadata);
    Ok(payload_offset)
}

fn write_frame_header_uninit(
    metadata: &UFrameMetadata,
    encoded_metadata: &[u8],
    sample: &mut [MaybeUninit<u8>],
    payload_len: usize,
    payload_alignment: usize,
) -> Result<usize, UStatus> {
    let (source, sink) = encoded_routing_hint(metadata)?;
    let source_len = u16::try_from(source.len()).map_err(|_| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa source routing hint is too large",
        )
    })?;
    let sink_len = u16::try_from(sink.len()).map_err(|_| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa sink routing hint is too large",
        )
    })?;
    let routing_len = source.len().checked_add(sink.len()).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa routing hint layout overflow")
    })?;
    let metadata_len = u32::try_from(encoded_metadata.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata is too large")
    })?;
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa payload is too large")
    })?;
    let payload_offset = frame_layout_bounds(
        sample.len(),
        routing_len,
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
    initialized[20..22].copy_from_slice(&source_len.to_le_bytes());
    initialized[22..24].copy_from_slice(&sink_len.to_le_bytes());
    let source_end = LOLA_FRAME_HEADER_LEN + source.len();
    let sink_end = source_end + sink.len();
    let metadata_end = sink_end + encoded_metadata.len();
    initialized[LOLA_FRAME_HEADER_LEN..source_end].copy_from_slice(source.as_bytes());
    initialized[source_end..sink_end].copy_from_slice(sink.as_bytes());
    initialized[sink_end..metadata_end].copy_from_slice(encoded_metadata);
    Ok(payload_offset)
}

fn frame_layout_bounds(
    sample_len: usize,
    routing_len: usize,
    metadata_len: usize,
    payload_len: usize,
    payload_alignment: usize,
    sample_address: usize,
) -> Result<(usize, usize), UStatus> {
    let payload_offset =
        payload_offset_for_len(routing_len, metadata_len, payload_alignment, sample_address)?;
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
    // SAFETY:
    // - `prefix` is in bounds and comes from one mutable slice allocation.
    // - Callers only request prefixes after `initialize_uninit_range` wrote
    //   every byte in `..end`.
    // - Per https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#layout-1:
    //
    //   "`MaybeUninit<T>` is guaranteed to have the same size, alignment, and
    //   ABI as `T`."
    //
    // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts_mut.html#safety:
    //
    //   "`data` must point to `len` consecutive properly initialized values of
    //   type `T`" and "The memory referenced by the returned slice must not be
    //   accessed through any other pointer" for the returned lifetime.
    Ok(unsafe { std::slice::from_raw_parts_mut(prefix.as_mut_ptr().cast::<u8>(), prefix.len()) })
}

fn encoded_routing_hint(metadata: &UFrameMetadata) -> Result<(String, String), UStatus> {
    let source = metadata.source().to_uri(false);
    let sink = metadata
        .sink()
        .map(|sink| sink.to_uri(false))
        .unwrap_or_default();
    if source.is_empty() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa source routing hint must not be empty",
        ));
    }
    Ok((source, sink))
}

fn read_frame_header(
    sample: &[u8],
) -> Result<(LolaRoutingHint, usize, usize, usize, usize), UStatus> {
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
    let source_len = u16::from_le_bytes(sample[20..22].try_into().expect("slice length")) as usize;
    let sink_len = u16::from_le_bytes(sample[22..24].try_into().expect("slice length")) as usize;
    let source_end = LOLA_FRAME_HEADER_LEN
        .checked_add(source_len)
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::InvalidArgument, "LoLa routing hint layout overflow")
        })?;
    let sink_end = source_end.checked_add(sink_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa routing hint layout overflow")
    })?;
    let metadata_end = sink_end.checked_add(metadata_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "LoLa metadata layout overflow")
    })?;
    if metadata_end > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "LoLa metadata range is outside sample bounds",
        ));
    }
    let source = parse_routing_uri(
        sample
            .get(LOLA_FRAME_HEADER_LEN..source_end)
            .ok_or_else(|| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    "LoLa source routing hint is outside sample bounds",
                )
            })?,
        "source",
    )?;
    let sink = if sink_len == 0 {
        None
    } else {
        Some(parse_routing_uri(
            sample.get(source_end..sink_end).ok_or_else(|| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    "LoLa sink routing hint is outside sample bounds",
                )
            })?,
            "sink",
        )?)
    };
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
    Ok((
        LolaRoutingHint { source, sink },
        sink_end,
        metadata_len,
        payload_offset,
        payload_len,
    ))
}

fn parse_routing_uri(bytes: &[u8], role: &str) -> Result<UUri, UStatus> {
    let uri = std::str::from_utf8(bytes).map_err(|error| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("LoLa {role} routing hint is not UTF-8: {error}"),
        )
    })?;
    UUri::try_from(uri).map_err(|error| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("invalid LoLa {role} routing hint: {error}"),
        )
    })
}

fn payload_offset_for_len(
    routing_len: usize,
    metadata_len: usize,
    payload_alignment: usize,
    sample_address: usize,
) -> Result<usize, UStatus> {
    let metadata_end = LOLA_FRAME_HEADER_LEN
        .checked_add(routing_len)
        .and_then(|len| len.checked_add(metadata_len))
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

#[cfg(test)]
mod tests {
    use super::*;
    use up_rust::{PayloadEncoding, UUri};

    fn deterministic_publish_metadata(topic: UUri) -> UFrameMetadata {
        UFrameMetadata::publish(topic)
            .with_payload_encoding(PayloadEncoding::RAW)
            .build()
            .expect("valid test metadata")
    }

    #[test]
    fn uninit_frame_header_does_not_zero_application_payload_range() {
        let payload_len = 8;
        let payload_alignment = 4;
        let mut sample = vec![MaybeUninit::new(0xA5); 256];
        let metadata = deterministic_publish_metadata(
            UUri::try_from_parts("vehicle", 0x4210, 1, 0x9008).unwrap(),
        );

        let payload_offset = write_frame_header_uninit(
            &metadata,
            b"selected-wire-metadata",
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
            // SAFETY: `sample` was constructed with `MaybeUninit::new(0xA5)`,
            // so every element in the checked payload range is initialized.
            assert_eq!(unsafe { byte.assume_init() }, 0xA5);
        }
    }

    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    #[test]
    fn test_stub_uninit_tx_loan_commits_after_exact_payload_initialization() {
        let topic = UUri::try_from_parts("vehicle", 0x4210, 1, 0x9009).unwrap();
        let metadata = deterministic_publish_metadata(topic);
        let mut loan = LolaUninitTxLoan::new_vec(
            metadata,
            b"selected-wire-metadata".to_vec(),
            256,
            3,
            4,
            LolaTxChannel::Primary,
        )
        .unwrap();

        for (slot, byte) in loan.payload_uninit_mut().iter_mut().zip(*b"xyz") {
            slot.write(byte);
        }

        // SAFETY: The byte writer successfully initialized exactly the full
        // visible payload range before the uninit loan is committed.
        let loan = unsafe { loan.assume_payload_initialized() };

        assert_eq!(loan.payload(), b"xyz");
        match &loan.sample {
            LolaTxStorage::Vec(sample) => {
                assert_eq!(&sample[0..4], LOLA_FRAME_MAGIC);
                assert_eq!(sample[4], LOLA_FRAME_VERSION);
            }
            #[cfg(feature = "lola-ffi")]
            LolaTxStorage::Native(_) => unreachable!("test-stub test uses Vec storage"),
        }
    }

    #[cfg(all(feature = "test-stub", not(feature = "lola-ffi")))]
    #[test]
    fn malformed_physical_routing_hint_is_rejected() {
        let metadata = deterministic_publish_metadata(
            UUri::try_from_parts("vehicle", 0x4210, 1, 0x9010).unwrap(),
        );
        let loan = LolaTxLoan::new_vec(
            metadata,
            b"selected-wire-metadata".to_vec(),
            256,
            3,
            4,
            LolaTxChannel::Primary,
        )
        .unwrap();
        let LolaTxStorage::Vec(mut sample) = loan.sample;
        sample[LOLA_FRAME_HEADER_LEN] = 0xff;

        let Err(error) = LolaRxLease::from_vec(sample) else {
            panic!("malformed physical routing hint must be rejected");
        };
        assert_eq!(error.code(), UCode::InvalidArgument);
    }
}

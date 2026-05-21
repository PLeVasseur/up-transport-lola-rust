/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::io::Cursor;

use up_rust::{
    zero_copy::{UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyRxFrame},
    PayloadEncoding, UAttributes, UCode, UFrameMetadata, UMessageType, UPayloadFormat, UPriority,
    UStatus, UUri, UUID,
};

#[cfg(feature = "lola-ffi")]
use crate::sys::{NativeRxSample, NativeTxLoan};

const LOLA_FRAME_MAGIC: &[u8; 4] = b"ULOL";
const LOLA_FRAME_VERSION: u8 = 1;
const LOLA_FRAME_HEADER_LEN: usize = 20;

/// LoLa transmit loan for one native uProtocol frame.
///
/// Values are returned by the [`UZeroCopyTransport`](up_rust::zero_copy::UZeroCopyTransport)
/// implementation for [`UTransportLola`](crate::UTransportLola). The exposed
/// [`UTxBuffer::payload_mut`] range points at the application payload inside a
/// fixed LoLa event sample. The preceding `ULOL` header, encoded metadata, and
/// alignment padding are hidden from callers.
///
/// Metadata is fixed when the loan is reserved so the payload offset remains
/// stable while serializers write directly into the exposed payload range.
pub struct LolaTxLoan {
    metadata: UFrameMetadata,
    sample: LolaTxStorage,
    payload_offset: usize,
    payload_len: usize,
}

enum LolaTxStorage {
    #[cfg(feature = "test-stub")]
    Vec(Vec<u8>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeTxLoan),
}

impl LolaTxStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_slice(),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_mut_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_mut_slice(),
        }
    }
}

impl LolaTxLoan {
    #[cfg(feature = "test-stub")]
    pub(crate) fn new_vec(
        metadata: UFrameMetadata,
        sample_size: usize,
        payload_len: usize,
        payload_alignment: usize,
    ) -> Result<Self, UStatus> {
        let mut sample = vec![0_u8; sample_size];
        let payload_offset =
            write_frame_header(&metadata, &mut sample, payload_len, payload_alignment)?;
        Ok(Self {
            metadata,
            sample: LolaTxStorage::Vec(sample),
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn new_native(
        metadata: UFrameMetadata,
        mut sample: NativeTxLoan,
        payload_len: usize,
        payload_alignment: usize,
    ) -> Result<Self, UStatus> {
        sample.as_mut_slice().fill(0);
        let payload_offset = write_frame_header(
            &metadata,
            sample.as_mut_slice(),
            payload_len,
            payload_alignment,
        )?;
        Ok(Self {
            metadata,
            sample: LolaTxStorage::Native(sample),
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "test-stub")]
    pub(crate) fn into_vec(self) -> Result<Vec<u8>, UStatus> {
        match self.sample {
            LolaTxStorage::Vec(sample) => Ok(sample),
        }
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn into_native(self) -> Result<NativeTxLoan, UStatus> {
        if self.sample.as_slice().get(..4) != Some(LOLA_FRAME_MAGIC.as_slice()) {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "LoLa TX frame header was not written before send",
            ));
        }
        match self.sample {
            LolaTxStorage::Native(sample) => Ok(sample),
        }
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

/// LoLa receive lease for one native uProtocol frame.
///
/// Dropping the lease releases the underlying LoLa sample. The payload is a
/// contiguous byte range within the fixed event sample, so this type implements
/// both [`UZeroCopyRxFrame`] and [`UContiguousZeroCopyRxFrame`]. Decoded values
/// that borrow from [`UContiguousZeroCopyRxFrame::contiguous_payload`] must not
/// outlive the lease.
///
/// Invalid or stale samples are rejected before this type is constructed. Native
/// diagnostics include the frame magic bytes when they do not match `ULOL`.
pub struct LolaRxLease {
    metadata: UFrameMetadata,
    sample: LolaRxStorage,
    payload_offset: usize,
    payload_len: usize,
}

enum LolaRxStorage {
    #[cfg(feature = "test-stub")]
    Vec(Vec<u8>),
    #[cfg(feature = "lola-ffi")]
    Native(NativeRxSample),
}

impl LolaRxStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Vec(sample) => sample.as_slice(),
            #[cfg(feature = "lola-ffi")]
            Self::Native(sample) => sample.as_slice(),
        }
    }
}

impl LolaRxLease {
    #[cfg(feature = "test-stub")]
    pub(crate) fn from_vec(sample: Vec<u8>) -> Result<Self, UStatus> {
        let (metadata, payload_offset, payload_len) = read_frame_header(&sample)?;
        Ok(Self {
            metadata,
            sample: LolaRxStorage::Vec(sample),
            payload_offset,
            payload_len,
        })
    }

    #[cfg(feature = "lola-ffi")]
    pub(crate) fn from_native(sample: NativeRxSample) -> Result<Self, UStatus> {
        let (metadata, payload_offset, payload_len) = read_frame_header(sample.as_slice())?;
        Ok(Self {
            metadata,
            sample: LolaRxStorage::Native(sample),
            payload_offset,
            payload_len,
        })
    }
}

impl UZeroCopyRxFrame for LolaRxLease {
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

impl UContiguousZeroCopyRxFrame for LolaRxLease {
    fn contiguous_payload(&self) -> &[u8] {
        self.try_contiguous_payload()
            .expect("LoLa receive payload layout should be valid")
    }
}

fn write_frame_header(
    metadata: &UFrameMetadata,
    sample: &mut [u8],
    payload_len: usize,
    payload_alignment: usize,
) -> Result<usize, UStatus> {
    let metadata_bytes = encode_metadata(metadata)?;
    let metadata_len = u32::try_from(metadata_bytes.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa metadata is too large")
    })?;
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa payload is too large")
    })?;
    let payload_offset = payload_offset_for_len(metadata_bytes.len(), payload_alignment)?;
    let payload_offset_u32 = u32::try_from(payload_offset).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa payload offset is too large")
    })?;
    let total_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa frame layout overflow")
    })?;
    if total_len > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::RESOURCE_EXHAUSTED,
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
    let metadata_end = LOLA_FRAME_HEADER_LEN + metadata_bytes.len();
    sample[LOLA_FRAME_HEADER_LEN..metadata_end].copy_from_slice(&metadata_bytes);
    Ok(payload_offset)
}

fn read_frame_header(sample: &[u8]) -> Result<(UFrameMetadata, usize, usize), UStatus> {
    if sample.len() < LOLA_FRAME_HEADER_LEN {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "LoLa sample is shorter than frame header",
        ));
    }
    if &sample[0..4] != LOLA_FRAME_MAGIC {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!(
                "invalid LoLa frame magic: {:02X} {:02X} {:02X} {:02X}",
                sample[0], sample[1], sample[2], sample[3]
            ),
        ));
    }
    if sample[4] != LOLA_FRAME_VERSION {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
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
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa metadata layout overflow")
        })?;
    if metadata_end > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "LoLa metadata range is outside sample bounds",
        ));
    }
    let metadata = decode_metadata(&sample[LOLA_FRAME_HEADER_LEN..metadata_end])?;
    if payload_offset < metadata_end {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "LoLa payload overlaps metadata",
        ));
    }
    let payload_end = payload_offset.checked_add(payload_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa payload layout overflow")
    })?;
    if payload_end > sample.len() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "LoLa payload range is outside sample bounds",
        ));
    }
    Ok((metadata, payload_offset, payload_len))
}

fn payload_offset_for_len(metadata_len: usize, payload_alignment: usize) -> Result<usize, UStatus> {
    let metadata_end = LOLA_FRAME_HEADER_LEN
        .checked_add(metadata_len)
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa metadata layout overflow")
        })?;
    align_up(metadata_end, payload_alignment)
}

fn align_up(value: usize, alignment: usize) -> Result<usize, UStatus> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "LoLa payload alignment must be a non-zero power of two",
        ));
    }
    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }
    value.checked_add(alignment - remainder).ok_or_else(|| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa frame layout overflow")
    })
}

fn encode_metadata(metadata: &UFrameMetadata) -> Result<Vec<u8>, UStatus> {
    let mut bytes = Vec::new();
    write_u64(&mut bytes, metadata.attributes().id().msb());
    write_u64(&mut bytes, metadata.attributes().id().lsb());
    bytes.push(message_type_to_byte(metadata.attributes().message_type()));
    bytes.push(priority_to_byte(metadata.attributes().priority()));
    write_optional_u32(&mut bytes, metadata.attributes().ttl());
    append_string(&mut bytes, &metadata.attributes().source().to_uri(false))?;
    append_string(
        &mut bytes,
        metadata
            .attributes()
            .sink()
            .map(|uri| uri.to_uri(false))
            .as_deref()
            .unwrap_or_default(),
    )?;
    if let Some(encoding) = metadata.encoding() {
        bytes.push(1);
        encode_payload_encoding(&mut bytes, encoding)?;
    } else {
        bytes.push(0);
    }
    write_optional_uuid(&mut bytes, metadata.attributes().request_id());
    write_optional_string(&mut bytes, metadata.attributes().traceparent())?;
    write_optional_string(&mut bytes, metadata.attributes().token())?;
    write_optional_u32(&mut bytes, metadata.attributes().permission_level());
    write_optional_code(&mut bytes, metadata.attributes().commstatus());
    Ok(bytes)
}

fn decode_metadata(mut bytes: &[u8]) -> Result<UFrameMetadata, UStatus> {
    let id = UUID::from_u64_pair(take_u64(&mut bytes)?, take_u64(&mut bytes)?)
        .map_err(|err| invalid_metadata(format!("invalid LoLa UUID: {err}")))?;
    let message_type = byte_to_message_type(take_u8(&mut bytes)?)?;
    let priority = byte_to_priority(take_u8(&mut bytes)?)?;
    let ttl = take_optional_u32(&mut bytes)?;
    let source_string = take_string(&mut bytes)?;
    let source = UUri::try_from(source_string.as_str())
        .map_err(|err| invalid_metadata(format!("invalid LoLa source URI: {err}")))?;
    let sink = {
        let sink = take_string(&mut bytes)?;
        if sink.is_empty() {
            None
        } else {
            Some(
                UUri::try_from(sink.as_str())
                    .map_err(|err| invalid_metadata(format!("invalid LoLa sink URI: {err}")))?,
            )
        }
    };
    let encoding = match take_u8(&mut bytes)? {
        0 => None,
        1 => Some(decode_payload_encoding(&mut bytes)?),
        _ => {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "invalid LoLa payload encoding presence flag",
            ))
        }
    };
    let request_id = take_optional_uuid(&mut bytes)?;
    let traceparent = take_optional_string(&mut bytes)?;
    let token = take_optional_string(&mut bytes)?;
    let permission_level = take_optional_u32(&mut bytes)?;
    let commstatus = take_optional_code(&mut bytes)?;

    let mut attributes = UAttributes::new(id, source, sink, message_type).with_priority(priority);
    if let Some(ttl) = ttl {
        attributes = attributes.with_ttl(ttl);
    }
    if let Some(request_id) = request_id {
        attributes = attributes.with_request_id(request_id);
    }
    if let Some(traceparent) = traceparent {
        attributes = attributes.with_traceparent(traceparent);
    }
    if let Some(token) = token {
        attributes = attributes.with_token(token);
    }
    if let Some(permission_level) = permission_level {
        attributes = attributes.with_permission_level(permission_level);
    }
    if let Some(commstatus) = commstatus {
        attributes = attributes.with_comm_status(commstatus);
    }
    if !bytes.is_empty() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "trailing LoLa metadata bytes",
        ));
    }
    Ok(UFrameMetadata::new(attributes, encoding))
}

fn encode_payload_encoding(bytes: &mut Vec<u8>, encoding: &PayloadEncoding) -> Result<(), UStatus> {
    match encoding {
        PayloadEncoding::Standard(format) => {
            bytes.push(0);
            bytes.push(format.value());
        }
        PayloadEncoding::Custom(custom) => {
            bytes.push(1);
            append_string(bytes, custom.id())?;
            append_string(bytes, custom.content_type())?;
        }
    }
    Ok(())
}

fn decode_payload_encoding(bytes: &mut &[u8]) -> Result<PayloadEncoding, UStatus> {
    match take_u8(bytes)? {
        0 => {
            let value = take_u8(bytes)?;
            let format = UPayloadFormat::from_u8(value)
                .ok_or_else(|| invalid_metadata(format!("invalid LoLa payload format {value}")))?;
            Ok(PayloadEncoding::standard(format))
        }
        1 => PayloadEncoding::try_custom(take_string(bytes)?, take_string(bytes)?).map_err(|err| {
            invalid_metadata(format!("invalid LoLa custom payload encoding: {err}"))
        }),
        _ => Err(invalid_metadata("invalid LoLa payload encoding kind")),
    }
}

fn write_u64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

fn write_optional_u32(dst: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            dst.push(1);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        None => dst.push(0),
    }
}

fn write_optional_uuid(dst: &mut Vec<u8>, value: Option<&UUID>) {
    match value {
        Some(value) => {
            dst.push(1);
            write_u64(dst, value.msb());
            write_u64(dst, value.lsb());
        }
        None => dst.push(0),
    }
}

fn write_optional_string(dst: &mut Vec<u8>, value: Option<&str>) -> Result<(), UStatus> {
    match value {
        Some(value) => {
            dst.push(1);
            append_string(dst, value)?;
        }
        None => dst.push(0),
    }
    Ok(())
}

fn write_optional_code(dst: &mut Vec<u8>, value: Option<UCode>) {
    match value {
        Some(value) => {
            dst.push(1);
            dst.push(value.as_u8());
        }
        None => dst.push(0),
    }
}

fn append_string(dst: &mut Vec<u8>, value: &str) -> Result<(), UStatus> {
    let len = u32::try_from(value.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "LoLa metadata field is too large")
    })?;
    dst.extend_from_slice(&len.to_le_bytes());
    dst.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_u8(src: &mut &[u8]) -> Result<u8, UStatus> {
    let (value, remaining) = src
        .split_first()
        .ok_or_else(|| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid LoLa metadata"))?;
    *src = remaining;
    Ok(*value)
}

fn take_u64(src: &mut &[u8]) -> Result<u64, UStatus> {
    let bytes = take_bytes(src, 8)?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("slice length")))
}

fn take_optional_u32(src: &mut &[u8]) -> Result<Option<u32>, UStatus> {
    match take_u8(src)? {
        0 => Ok(None),
        1 => {
            let bytes = take_bytes(src, 4)?;
            Ok(Some(u32::from_le_bytes(
                bytes.try_into().expect("slice length"),
            )))
        }
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid optional LoLa metadata value",
        )),
    }
}

fn take_optional_uuid(src: &mut &[u8]) -> Result<Option<UUID>, UStatus> {
    match take_u8(src)? {
        0 => Ok(None),
        1 => Ok(Some(
            UUID::from_u64_pair(take_u64(src)?, take_u64(src)?)
                .map_err(|err| invalid_metadata(format!("invalid LoLa UUID: {err}")))?,
        )),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid optional LoLa metadata value",
        )),
    }
}

fn take_optional_string(src: &mut &[u8]) -> Result<Option<String>, UStatus> {
    match take_u8(src)? {
        0 => Ok(None),
        1 => Ok(Some(take_string(src)?)),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid optional LoLa metadata value",
        )),
    }
}

fn take_optional_code(src: &mut &[u8]) -> Result<Option<UCode>, UStatus> {
    match take_u8(src)? {
        0 => Ok(None),
        1 => UCode::from_u8(take_u8(src)?).map(Some).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid LoLa status code")
        }),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid optional LoLa metadata value",
        )),
    }
}

fn take_string(src: &mut &[u8]) -> Result<String, UStatus> {
    let len_bytes = take_bytes(src, 4)?;
    let len = usize::try_from(u32::from_le_bytes(
        len_bytes.try_into().expect("slice length"),
    ))
    .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid LoLa string length"))?;
    let bytes = take_bytes(src, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid LoLa metadata string")
    })
}

fn take_bytes<'a>(src: &mut &'a [u8], len: usize) -> Result<&'a [u8], UStatus> {
    if src.len() < len {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid LoLa metadata",
        ));
    }
    let (bytes, remaining) = src.split_at(len);
    *src = remaining;
    Ok(bytes)
}

fn invalid_metadata(message: impl Into<String>) -> UStatus {
    UStatus::fail_with_code(UCode::INVALID_ARGUMENT, message.into())
}

fn message_type_to_byte(message_type: UMessageType) -> u8 {
    match message_type {
        UMessageType::Publish => 1,
        UMessageType::Notification => 2,
        UMessageType::Request => 3,
        UMessageType::Response => 4,
    }
}

fn byte_to_message_type(value: u8) -> Result<UMessageType, UStatus> {
    match value {
        1 => Ok(UMessageType::Publish),
        2 => Ok(UMessageType::Notification),
        3 => Ok(UMessageType::Request),
        4 => Ok(UMessageType::Response),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid LoLa message type",
        )),
    }
}

fn priority_to_byte(priority: UPriority) -> u8 {
    match priority {
        UPriority::CS0 => 0,
        UPriority::CS1 => 1,
        UPriority::CS2 => 2,
        UPriority::CS3 => 3,
        UPriority::CS4 => 4,
        UPriority::CS5 => 5,
        UPriority::CS6 => 6,
    }
}

fn byte_to_priority(value: u8) -> Result<UPriority, UStatus> {
    match value {
        0 => Ok(UPriority::CS0),
        1 => Ok(UPriority::CS1),
        2 => Ok(UPriority::CS2),
        3 => Ok(UPriority::CS3),
        4 => Ok(UPriority::CS4),
        5 => Ok(UPriority::CS5),
        6 => Ok(UPriority::CS6),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid LoLa priority",
        )),
    }
}

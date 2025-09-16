use bytemuck;
use byteorder::{LittleEndian, ReadBytesExt};
use num::{cast::AsPrimitive, traits::WrappingAdd};
use std::io::{Cursor, ErrorKind, Read};

pub mod decoders;
pub mod time;

const TDF_TIME_MASK: u16 = 0xC000;
const TDF_ARRAY_MASK: u16 = 0x3000;
const TDF_ID_MASK: u16 = 0x0FFF;

const TDF_TIME_NONE: u16 = 0x0000;
const TDF_TIME_GLOBAL: u16 = 0x4000;
const TDF_TIME_RELATIVE_U16: u16 = 0x8000;
const TDF_TIME_RELATIVE_S24: u16 = 0xC000;

const TDF_ARRAY_NONE: u16 = 0x0000;
const TDF_ARRAY_TIME: u16 = 0x1000;
const TDF_ARRAY_DIFF: u16 = 0x2000;
const TDF_ARRAY_IDX: u16 = 0x3000;

const TDF_DIFF_16_8: u8 = 1;
const TDF_DIFF_32_8: u8 = 2;
const TDF_DIFF_32_16: u8 = 3;

const TDF_PERIOD_SCALING_BIT: u16 = 0x8000;
const TDF_PERIOD_SCALING_VAL_MASK: u16 = 0x7FFF;
const TDF_PERIOD_SCALING_MULT: u16 = 8192;

pub trait TdfOutput {
    /// Write a TDF to an abstract output
    fn write(
        &mut self,
        remote_id: Option<u64>,
        tdf_id: u16,
        tdf_time: i64,
        tdf_idx: Option<u16>,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> std::io::Result<()>;
    /// Iterate over all TDFs written
    fn iter_written(&self) -> impl Iterator<Item = (&(Option<u64>, u16), &usize)>;
    /// Get the number of times a specific TDF was written
    fn written(&self, remote_id: Option<u64>, tdf_id: u16) -> usize;
}

fn diff_data_reconstruct<
    T1: Default + bytemuck::Pod + num::PrimInt + WrappingAdd,
    T2: Default + bytemuck::Pod + num::PrimInt + AsPrimitive<T1>,
>(
    cursor: &mut Cursor<&[u8]>,
    tdf_size: u8,
    base_size: u8,
    diff_num: usize,
    out: &mut Vec<u8>,
) -> std::io::Result<()> {
    if tdf_size % base_size != 0 {
        return Err(std::io::Error::new(
            ErrorKind::Other,
            "Invalid diff base TDF len",
        ));
    }
    let diff_num_fields = (tdf_size / base_size) as usize;

    // Read the base sample and diff data
    let mut base_data_buffer: Vec<T1> = vec![Default::default(); diff_num_fields];
    let mut diff_data_buffer: Vec<T2> = vec![Default::default(); diff_num_fields * diff_num];

    cursor.read_exact(bytemuck::cast_slice_mut(&mut base_data_buffer))?;
    cursor.read_exact(bytemuck::cast_slice_mut(&mut diff_data_buffer))?;

    // Allocate space for the reconstructed data
    let mut reconstructed_buffer: Vec<T1> =
        Vec::with_capacity(base_data_buffer.len() + diff_data_buffer.len());
    reconstructed_buffer.extend_from_slice(&base_data_buffer);

    // Reconstruct the original array
    let mut diff_idx: usize = 0;
    for _ in 0..diff_num {
        for _ in 0..diff_num_fields {
            let out_idx = diff_num_fields + diff_idx;
            let last_val: T1 = reconstructed_buffer[out_idx - diff_num_fields];
            let diff: T1 = diff_data_buffer[diff_idx].as_();
            let new_val: T1 = last_val.wrapping_add(&diff);
            reconstructed_buffer.push(new_val);
            diff_idx += 1;
        }
    }

    // Copy into the provided output buffer
    // Is this the most efficient way to get the data out? Probably not.
    out.extend_from_slice(bytemuck::cast_slice_mut(&mut reconstructed_buffer));
    Ok(())
}

fn tdfs_write<T: TdfOutput>(
    remote_id: Option<u64>,
    tdf_id: u16,
    tdf_size: u8,
    buffer_time: i64,
    array_sample_idx: Option<u16>,
    array_time_period: i64,
    time_flags: u16,
    array_num: u8,
    cursor: &mut Cursor<&[u8]>,
    output: &mut T,
) -> std::io::Result<()> {
    let mut sample_time = buffer_time;

    for tdf_idx in 0..array_num {
        // If we have an explicit timestamp, use it for the first sample only
        let sample_idx = match array_sample_idx {
            Some(base_idx) => {
                if tdf_idx == 0 && time_flags != 0x0000 {
                    None
                } else {
                    Some(base_idx.wrapping_add(tdf_idx as u16))
                }
            }
            None => None,
        };
        // Write the sample to the output
        output.write(remote_id, tdf_id, sample_time, sample_idx, tdf_size, cursor)?;
        // Increment the sample timestamp
        sample_time += array_time_period;
    }

    Ok(())
}

/// Decode a single TDF block, writing to an abstract output
pub fn block_decode<T: TdfOutput>(
    remote_id: Option<u64>,
    block: &[u8],
    output: &mut T,
) -> std::io::Result<()> {
    let mut cursor = Cursor::new(block);
    let mut buffer_time: i64 = 0;

    while block.len() - cursor.position() as usize > 4 {
        let header = cursor.read_u16::<LittleEndian>()?;
        if header == 0xFFFF || header == 0x0000 {
            break;
        }
        let tdf_id = header & TDF_ID_MASK;
        let time_flags = header & TDF_TIME_MASK;
        let array_flags = header & TDF_ARRAY_MASK;
        let size = cursor.read_u8()?;
        let mut array_num = 1;
        let mut array_time_period = 0;
        let mut array_sample_idx = None;
        let mut reconstructed: Option<Vec<u8>> = None;
        if size == 0 {
            // Invalid header, remainder of block can't be trusted
            return std::io::Result::Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "TDF of length 0",
            ));
        }
        match time_flags {
            TDF_TIME_NONE => {}
            TDF_TIME_GLOBAL => {
                buffer_time = ((cursor.read_u32::<LittleEndian>()? as i64) << 16)
                    + (cursor.read_u16::<LittleEndian>()? as i64);
            }
            TDF_TIME_RELATIVE_U16 => buffer_time += cursor.read_u16::<LittleEndian>()? as i64,
            TDF_TIME_RELATIVE_S24 => buffer_time += cursor.read_i24::<LittleEndian>()? as i64,
            _ => {
                panic!("How?");
            }
        }
        match array_flags {
            TDF_ARRAY_NONE => {}
            TDF_ARRAY_TIME => {
                array_num = cursor.read_u8()?;
                if array_num == 0 {
                    // Invalid header, remainder of block can't be trusted
                    return std::io::Result::Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Time array of 0 elements",
                    ));
                }
                let period_encoded = cursor.read_u16::<LittleEndian>()?;
                let period_masked = period_encoded & TDF_PERIOD_SCALING_VAL_MASK;
                // Handle time period scaling
                array_time_period = period_masked as i64;
                if period_encoded & TDF_PERIOD_SCALING_BIT != 0 {
                    array_time_period *= TDF_PERIOD_SCALING_MULT as i64;
                }
            }
            TDF_ARRAY_DIFF => {
                let diff_info = cursor.read_u8()?;
                let period_encoded = cursor.read_u16::<LittleEndian>()?;
                let period_masked = period_encoded & TDF_PERIOD_SCALING_VAL_MASK;
                // Handle time period scaling
                array_time_period = period_masked as i64;
                if period_encoded & TDF_PERIOD_SCALING_BIT != 0 {
                    array_time_period *= TDF_PERIOD_SCALING_MULT as i64;
                }
                // Handle diff data
                let diff_type = diff_info >> 6;
                let diff_num = (diff_info & 0x3F) as usize;
                let out_len = size as usize * (1 + diff_num);

                array_num = diff_num as u8 + 1;
                reconstructed = match diff_type {
                    TDF_DIFF_16_8 => {
                        let mut out: Vec<u8> = Vec::with_capacity(out_len);
                        diff_data_reconstruct::<i16, i8>(&mut cursor, size, 2, diff_num, &mut out)?;
                        Some(out)
                    }
                    TDF_DIFF_32_8 => {
                        let mut out: Vec<u8> = Vec::with_capacity(out_len);
                        diff_data_reconstruct::<i32, i8>(&mut cursor, size, 4, diff_num, &mut out)?;
                        Some(out)
                    }
                    TDF_DIFF_32_16 => {
                        let mut out: Vec<u8> = Vec::with_capacity(out_len);
                        diff_data_reconstruct::<i32, i16>(
                            &mut cursor,
                            size,
                            4,
                            diff_num,
                            &mut out,
                        )?;
                        Some(out)
                    }
                    _ => {
                        return Err(std::io::Error::new(ErrorKind::Other, "Unknown diff type"));
                    }
                };
            }
            TDF_ARRAY_IDX => {
                array_num = cursor.read_u8()?;
                array_sample_idx = Some(cursor.read_u16::<LittleEndian>()?);
            }
            _ => {
                panic!("How?");
            }
        }

        match reconstructed {
            // If we reconstructed a diff array, use that as the data source
            Some(ref r) => {
                let diff_cursor = &mut Cursor::new(&r[..]);
                tdfs_write(
                    remote_id,
                    tdf_id,
                    size,
                    buffer_time,
                    array_sample_idx,
                    array_time_period,
                    time_flags,
                    array_num,
                    diff_cursor,
                    output,
                )?;
            }
            // Otherwise continue pulling data directly from the block
            _ => {
                tdfs_write(
                    remote_id,
                    tdf_id,
                    size,
                    buffer_time,
                    array_sample_idx,
                    array_time_period,
                    time_flags,
                    array_num,
                    &mut cursor,
                    output,
                )?;
            }
        };
    }

    Ok(())
}

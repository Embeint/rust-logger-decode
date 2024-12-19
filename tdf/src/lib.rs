use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, ErrorKind};

pub mod decoders;
pub mod time;

pub trait TdfOutput {
    /// Write a TDF to an abstract output
    fn write(
        &mut self,
        remote_id: Option<u64>,
        tdf_id: u16,
        tdf_time: i64,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> std::io::Result<()>;
    /// Iterate over all TDFs written
    fn iter_written(&self) -> impl Iterator<Item = (&(Option<u64>, u16), &usize)>;
    /// Get the number of times a specific TDF was written
    fn written(&self, remote_id: Option<u64>, tdf_id: u16) -> usize;
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
        let tdf_id = header & 0x0FFF;
        let time_flags = header & 0xC000;
        let array_flags = header & 0x3000;
        let size = cursor.read_u8()?;
        let mut array_num = 1;
        let mut array_time_period = 0;
        match time_flags {
            0x0000 => {}
            0x4000 => {
                buffer_time = ((cursor.read_u32::<LittleEndian>()? as i64) << 16)
                    + (cursor.read_u16::<LittleEndian>()? as i64);
            }
            0x8000 => buffer_time += cursor.read_u16::<LittleEndian>()? as i64,
            0xC000 => buffer_time += cursor.read_i24::<LittleEndian>()? as i64,
            _ => {
                panic!("How?");
            }
        }
        match array_flags {
            0x0000 => {}
            0x1000 => {
                array_num = cursor.read_u8()?;
                let period_encoded = cursor.read_u16::<LittleEndian>()?;
                // Handle time period scaling
                if array_time_period >= 32768 {
                    array_time_period = ((period_encoded - 32768) * 8192) as i64;
                } else {
                    array_time_period = period_encoded as i64;
                }
            }
            _ => {
                return Err(std::io::Error::new(ErrorKind::Other, "Unknown array type"));
            }
        }

        let mut sample_time = buffer_time;
        for _ in 0..array_num {
            output.write(remote_id, tdf_id, sample_time, size, &mut cursor)?;
            sample_time += array_time_period;
        }
    }

    Ok(())
}

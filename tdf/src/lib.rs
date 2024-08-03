use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

pub mod decoders;
pub mod time;

pub trait TdfOutput {
    /// Write a TDF to an abstract output
    fn write(
        &mut self,
        tdf_id: u16,
        tdf_time: i64,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> std::io::Result<()>;
    /// Iterate over all TDFs written
    fn iter_written(&self) -> impl Iterator<Item = (&u16, &usize)>;
    /// Get the number of times a specific TDF was written
    fn written(&self, tdf_id: u16) -> usize;
}

/// Decode a single TDF block, writing to an abstract output
pub fn block_decode<T: TdfOutput>(block: &[u8], output: &mut T) -> std::io::Result<()> {
    let mut cursor = Cursor::new(block);
    let mut buffer_time: i64 = 0;

    while block.len() - cursor.position() as usize > 4 {
        let header = cursor.read_u16::<LittleEndian>()?;
        if header == 0xFFFF || header == 0x0000 {
            break;
        }
        let tdf_id = header & 0x0FFF;
        let size = cursor.read_u8()?;
        let mut array_num = 1;
        let mut array_time_period = 0;
        match header & 0xC000 {
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
        if header & 0x1000 != 0 {
            array_num = cursor.read_u8()?;
            array_time_period = cursor.read_u16::<LittleEndian>()? as i64;
        }

        let mut sample_time = buffer_time;
        for _ in 0..array_num {
            output.write(tdf_id, sample_time, size, &mut cursor)?;
            sample_time += array_time_period;
        }
    }

    Ok(())
}

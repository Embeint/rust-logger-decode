use byteorder::{LittleEndian, ReadBytesExt};
use tdf::TdfOutput;

pub const BLOCK_SIZE: usize = 512;

#[derive(Hash, Copy, Clone, PartialEq, Eq)]
pub enum BlockTypes {
    TDF,
    REMOTE,
    OTHER,
    EMPTY,
    ERROR,
}

impl std::fmt::Display for BlockTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BlockTypes::TDF => write!(f, "TDF"),
            BlockTypes::REMOTE => write!(f, "TDF_REMOTE"),
            BlockTypes::OTHER => write!(f, "Other"),
            BlockTypes::EMPTY => write!(f, "Empty"),
            BlockTypes::ERROR => write!(f, "Error"),
        }
    }
}

pub fn decode_block<T: TdfOutput>(tdf_output: &mut T, block: &[u8]) -> std::io::Result<BlockTypes> {
    let wrap_count = block[0];
    let block_type = block[1];

    if (wrap_count == 0x00 && block_type == 0x00) || (wrap_count == 0xFF && block_type == 0xFF) {
        return Ok(BlockTypes::EMPTY);
    } else if block_type == 0x02 {
        tdf::block_decode(None, &block[2..], tdf_output)?;
        return Ok(BlockTypes::TDF);
    } else if block_type == 0x0B {
        let remote_id = (&block[2..10]).read_u64::<LittleEndian>()?;
        tdf::block_decode(Some(remote_id), &block[10..], tdf_output)?;
        return Ok(BlockTypes::REMOTE);
    } else {
        return Ok(BlockTypes::OTHER);
    }
}

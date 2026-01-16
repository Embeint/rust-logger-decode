use clap::ValueEnum;
use std::fmt;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
#[repr(usize)]
pub enum BlockSizeOptions {
    #[value(name = "512")]
    B512 = 512,
    #[value(name = "4096")]
    B4096 = 4096,
}

impl fmt::Display for BlockSizeOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockSizeOptions::B512 => write!(f, "512"),
            BlockSizeOptions::B4096 => write!(f, "4096"),
        }
    }
}

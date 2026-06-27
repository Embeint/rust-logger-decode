use chrono::{Datelike, Utc};

fn main() {
    println!(
        "cargo:rustc-env=INFUSE_DECODER_BUILD_YEAR={}",
        Utc::now().year()
    );
}

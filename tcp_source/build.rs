use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["proto/bid_event.proto"], &["proto/"])?;
    Ok(())
}
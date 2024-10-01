fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .bytes(["."])
        .compile_protos(&["proto/s2/v1alpha/s2.proto"], &["proto"])?;

    Ok(())
}

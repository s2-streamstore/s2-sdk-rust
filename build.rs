fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .bytes(["."])
        .compile(&["proto/s2/v1alpha/s2.proto"], &["proto"])?;

    Ok(())
}

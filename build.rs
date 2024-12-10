fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "prost-build")]
    {
        tonic_build::configure()
            .out_dir("src")
            .bytes(["."])
            .compile_protos(&["proto/s2/v1alpha/s2.proto"], &["proto"])?;    
        std::fs::rename("src/s2.v1alpha.rs", "src/api.rs")?;        
    }
    println!("cargo:rustc-env=COMPILED_PROST_FILE=src/api.rs");
    Ok(())
}

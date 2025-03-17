fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-env=COMPILED_PROST_FILE=src/types/src/api.rs");
    Ok(())
}

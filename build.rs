fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Always compile proto files for examples (tonic is a dev-dependency)
    // The generated code will be available even if tonic-support feature is not enabled
    tonic_build::compile_protos("proto/helloworld.proto")?;

    Ok(())
}

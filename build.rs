fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only compile proto files when tonic-support feature is enabled
    #[cfg(feature = "tonic-support")]
    {
        tonic_prost_build::compile_protos("proto/helloworld.proto")?;
    }

    Ok(())
}

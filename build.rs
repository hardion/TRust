fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: build scripts are single threaded.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    tonic_prost_build::compile_protos("proto/tango.proto")?;
    Ok(())
}

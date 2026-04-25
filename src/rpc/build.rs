//proto2rust
fn main() {
    let proto_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    let out_dir = std::path::Path::new("src/proto_rust");

    // 收集所有proto文件名
    let proto_files: Vec<String> = std::fs::read_dir(&proto_dir)
        .unwrap_or_else(|e| panic!("Failed to read proto directory: {e}"))
        .filter_map(|entry| {
            let entry = entry.expect("Invalid directory entry");
            let path = entry.path();
            
            // 过滤出.proto扩展名的文件
            if path.extension().map_or(false, |ext| ext == "proto") {
                Some(
                    entry.file_name()
                        .to_str()
                        .expect("Non-UTF8 filename")
                        .to_string()
                )
            } else {
                None
            }
        })
        .collect();

    // 使用tonic生成Rust代码
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir(out_dir)
        .compile_protos(
            &proto_files,
            &[proto_dir.to_str().expect("Non-UTF8 path")]
        )
        .unwrap_or_else(|e| panic!("Protobuf compilation failed: {e}"));

    // 生成mod.rs内容
    let mod_content: String = proto_files
        .iter()
        .map(|file| {
            let module_name = std::path::Path::new(file)
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("Invalid filename pattern");
            
            format!("pub mod {module_name};\n")
        })
        .collect();

    // 一次性写入文件
    std::fs::write(out_dir.join("mod.rs"), mod_content)
        .unwrap_or_else(|e| panic!("Failed to create mod.rs: {e}"));
}


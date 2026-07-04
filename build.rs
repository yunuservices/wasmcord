use std::path::Path;
use std::process::Command;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let root = Path::new(&manifest_dir);

    println!("cargo:rerun-if-changed=wasm-plugins/ping/src/lib.rs");
    println!("cargo:rerun-if-changed=wasm-plugins/ping/wit/plugin.wit");

    let plugin_crate = root.join("wasm-plugins").join("ping");
    let plugins_dir = root.join("plugins");
    std::fs::create_dir_all(&plugins_dir).expect("failed to create plugins dir");

    let output = Command::new("cargo")
        .args([
            "build",
            "--target",
            "wasm32-wasip2",
            "--manifest-path",
            plugin_crate.join("Cargo.toml").to_str().unwrap(),
        ])
        .output()
        .expect("failed to run cargo build for ping plugin");

    if !output.status.success() {
        panic!(
            "ping plugin build failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let wasm_src = plugin_crate
        .join("target")
        .join("wasm32-wasip2")
        .join("debug")
        .join("ping_plugin.wasm");
    let wasm_dst = plugins_dir.join("ping.wasm");

    if wasm_src.exists() {
        std::fs::copy(&wasm_src, &wasm_dst).expect("failed to copy ping.wasm");
        println!("cargo:rerun-if-changed={}", wasm_dst.display());
    } else {
        panic!("expected wasm artifact at {}", wasm_src.display());
    }
}

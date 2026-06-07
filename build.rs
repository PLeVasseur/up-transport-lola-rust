#![allow(missing_docs)]

use std::{env, path::Path};

fn main() {
    println!("cargo:rerun-if-env-changed=LOLA_BRIDGE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LOLA_COMMUNICATION_ROOT");
    println!("cargo:rerun-if-changed=cpp/up_lola_bridge.cpp");
    println!("cargo:rerun-if-changed=cpp/up_lola_bridge.h");

    if let Ok(lib_dir) = env::var("LOLA_BRIDGE_LIB_DIR") {
        link_bridge(Path::new(&lib_dir));
    }

    if env::var_os("CARGO_FEATURE_LOLA_BUILD_FROM_SOURCE").is_some() {
        println!(
            "cargo:warning=lola-build-from-source is reserved for the native bridge build; use LOLA_BRIDGE_LIB_DIR for link checks in this branch"
        );
    }
}

fn link_bridge(lib_dir: &Path) {
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=up_lola_bridge");
}

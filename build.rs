use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const BUNDLED_COMMUNICATION_PATH: &str = "third_party/eclipse-score-communication";
const BUNDLED_COMMUNICATION_COMMIT: &str = "56c36d4059d276e804c143d14012576ddf1b9e25";

fn main() {
    println!("cargo:rerun-if-env-changed=BAZEL");
    println!("cargo:rerun-if-env-changed=LOLA_BRIDGE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LOLA_COMMUNICATION_ROOT");
    println!("cargo:rerun-if-changed=.gitmodules");
    println!("cargo:rerun-if-changed=cpp/up_lola_bridge.cpp");
    println!("cargo:rerun-if-changed=cpp/up_lola_bridge.h");

    if let Ok(lib_dir) = env::var("LOLA_BRIDGE_LIB_DIR") {
        link_bridge(Path::new(&lib_dir));
        return;
    }

    if env::var_os("CARGO_FEATURE_LOLA_BUILD_FROM_SOURCE").is_some() {
        build_bridge_from_source();
        return;
    }

    if env::var_os("CARGO_FEATURE_LOLA_FFI").is_some() {
        panic!(
            "feature `lola-ffi` was enabled without `lola-build-from-source`, but LOLA_BRIDGE_LIB_DIR is not set.\n\nHow to resolve:\n  1. Use the default bundled build: cargo build\n  2. Or build from source explicitly: cargo build --features lola-build-from-source\n  3. Or provide a prebuilt bridge: LOLA_BRIDGE_LIB_DIR=/path/to/lib cargo build --no-default-features --features lola-ffi\n  4. Or run only the fake unit backend: cargo test --no-default-features --features test-stub"
        );
    }
}

fn build_bridge_from_source() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set"));
    let communication_root = communication_root(&manifest_dir);
    validate_communication_root(&communication_root);
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by Cargo"));
    let workspace_dir = out_dir.join("up-lola-bridge-bazel");
    let output_base = out_dir.join("bazel-output-base");
    generate_bridge_workspace(&workspace_dir, &communication_root);

    let bazel = find_bazel();
    run_bazel(
        &bazel,
        &workspace_dir,
        &output_base,
        &["build", "//:up_lola_bridge"],
    );
    let cquery_output = run_bazel_capture(
        &bazel,
        &workspace_dir,
        &output_base,
        &["cquery", "--output=files", "//:up_lola_bridge"],
    );
    let library = find_bridge_library(&workspace_dir, &cquery_output);
    let lib_dir = library.parent().unwrap_or_else(|| {
        panic!(
            "bridge library has no parent directory: {}",
            library.display()
        )
    });
    link_bridge(lib_dir);
}

fn communication_root(manifest_dir: &Path) -> PathBuf {
    if let Ok(communication_root) = env::var("LOLA_COMMUNICATION_ROOT") {
        return PathBuf::from(communication_root);
    }

    let bundled_root = manifest_dir.join(BUNDLED_COMMUNICATION_PATH);
    ensure_bundled_communication(manifest_dir, &bundled_root);
    verify_bundled_revision(&bundled_root);
    bundled_root
}

fn ensure_bundled_communication(manifest_dir: &Path, bundled_root: &Path) {
    if bundled_root.join("MODULE.bazel").is_file() {
        return;
    }

    if !manifest_dir.join(".gitmodules").is_file() {
        panic!(
            "bundled LoLa build requires the S-CORE communication submodule, but .gitmodules is missing.\n\nHow to resolve:\n  1. Build from a git checkout of this repository with submodules available, then run: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\n  2. Or provide an external checkout: LOLA_COMMUNICATION_ROOT=/path/to/eclipse-score-communication cargo build\n  3. Or use a prebuilt bridge: LOLA_BRIDGE_LIB_DIR=/path/to/lib cargo build --no-default-features --features lola-ffi"
        );
    }

    let output = Command::new("git")
        .arg("submodule")
        .arg("update")
        .arg("--init")
        .arg("--recursive")
        .arg(BUNDLED_COMMUNICATION_PATH)
        .current_dir(manifest_dir)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to run git while initializing bundled S-CORE communication submodule: {error}\n\nHow to resolve:\n  1. Install git and re-run cargo build\n  2. Or initialize manually: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\n  3. Or provide an external checkout: LOLA_COMMUNICATION_ROOT=/path/to/eclipse-score-communication cargo build"
            )
        });

    if !output.status.success() {
        panic!(
            "failed to initialize bundled S-CORE communication submodule.\n\nCommand: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\nstdout:\n{}\nstderr:\n{}\nHow to resolve:\n  1. Run the command above manually and retry cargo build\n  2. Or provide an external checkout: LOLA_COMMUNICATION_ROOT=/path/to/eclipse-score-communication cargo build\n  3. Or use a prebuilt bridge: LOLA_BRIDGE_LIB_DIR=/path/to/lib cargo build --no-default-features --features lola-ffi",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    if !bundled_root.join("MODULE.bazel").is_file() {
        panic!(
            "bundled S-CORE communication submodule was initialized, but {} is still missing MODULE.bazel.\n\nHow to resolve:\n  1. Run: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\n  2. Or provide an external checkout: LOLA_COMMUNICATION_ROOT=/path/to/eclipse-score-communication cargo build",
            bundled_root.display()
        );
    }
}

fn verify_bundled_revision(bundled_root: &Path) {
    let output = Command::new("git")
        .arg("-C")
        .arg(bundled_root)
        .arg("rev-parse")
        .arg("HEAD")
        .output();
    let Ok(output) = output else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let actual = String::from_utf8_lossy(&output.stdout);
    let actual = actual.trim();
    if actual != BUNDLED_COMMUNICATION_COMMIT {
        panic!(
            "bundled S-CORE communication submodule is at {actual}, expected {BUNDLED_COMMUNICATION_COMMIT}.\n\nHow to resolve:\n  1. Run: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\n  2. Or intentionally use this checkout by setting: LOLA_COMMUNICATION_ROOT={} cargo build",
            bundled_root.display()
        );
    }
}

fn validate_communication_root(communication_root: &Path) {
    if !communication_root.join("MODULE.bazel").is_file() {
        panic!(
            "{} does not look like an Eclipse S-CORE communication checkout; MODULE.bazel is missing.\n\nHow to resolve:\n  1. Set LOLA_COMMUNICATION_ROOT to a valid eclipse-score/communication checkout\n  2. Or use the bundled submodule by unsetting LOLA_COMMUNICATION_ROOT and running: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\n  3. Or use a prebuilt bridge: LOLA_BRIDGE_LIB_DIR=/path/to/lib cargo build --no-default-features --features lola-ffi",
            communication_root.display()
        );
    }
}

fn generate_bridge_workspace(workspace_dir: &Path, communication_root: &Path) {
    fs::create_dir_all(workspace_dir).unwrap_or_else(|error| {
        panic!(
            "failed to create generated Bazel workspace {}: {error}",
            workspace_dir.display()
        )
    });

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set"));
    copy_file(
        &manifest_dir.join("cpp/up_lola_bridge.cpp"),
        &workspace_dir.join("up_lola_bridge.cpp"),
    );
    copy_file(
        &manifest_dir.join("cpp/up_lola_bridge.h"),
        &workspace_dir.join("up_lola_bridge.h"),
    );

    fs::write(workspace_dir.join(".bazelversion"), "8.3.0\n").expect("write .bazelversion");
    fs::write(workspace_dir.join(".bazelrc"), bazelrc()).expect("write .bazelrc");
    fs::write(
        workspace_dir.join("MODULE.bazel"),
        module_bazel(communication_root),
    )
    .expect("write MODULE.bazel");
    fs::write(workspace_dir.join("BUILD.bazel"), build_bazel()).expect("write BUILD.bazel");
}

fn copy_file(src: &Path, dst: &Path) {
    fs::copy(src, dst).unwrap_or_else(|error| {
        panic!(
            "failed to copy {} to {}: {error}",
            src.display(),
            dst.display()
        )
    });
}

fn bazelrc() -> &'static str {
    r#"common --action_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
common --platforms=@score_bazel_platforms//:x86_64-linux-gcc_12.2.0-posix
common --extra_toolchains=@score_gcc_x86_64_toolchain//:x86_64-linux
common --registry=https://raw.githubusercontent.com/eclipse-score/bazel_registry/refs/heads/main/
common --registry=https://bcr.bazel.build
common --@score_baselibs//score/json:base_library=nlohmann
common --@score_baselibs//score/memory/shared/flags:use_typedshmd=False
common --@score_communication//score/mw/com/flags:tracing_library=@score_baselibs//score/analysis/tracing/generic_trace_library/stub_implementation
build --spawn_strategy=standalone
build --strategy=CppCompile=standalone
"#
}

fn module_bazel(communication_root: &Path) -> String {
    format!(
        r#"module(name = "up_lola_bridge")

bazel_dep(name = "score_communication", version = "0.0.0")
local_path_override(
    module_name = "score_communication",
    path = "{}",
)

bazel_dep(name = "rules_cc", version = "0.2.17")
bazel_dep(name = "score_baselibs", version = "0.2.7")

git_override(
    module_name = "score_tooling",
    commit = "549c7ee315cfbc27a42247b11cb70c953eac75dc",
    remote = "https://github.com/eclipse-score/tooling.git",
)

git_override(
    module_name = "trlc",
    commit = "3a71fd56001b95dfbb5270a49449ec0a631cd56c",
    remote = "https://github.com/bmw-software-engineering/trlc.git",
)

git_override(
    module_name = "lobster",
    commit = "d528fbdec2cd72ff7967b51546fb0bd935810258",
    remote = "https://github.com/bmw-software-engineering/lobster.git",
)

bazel_dep(name = "score_bazel_platforms", version = "0.1.2", dev_dependency = True)
bazel_dep(name = "score_bazel_cpp_toolchains", version = "0.5.1", dev_dependency = True)

score_gcc = use_extension("@score_bazel_cpp_toolchains//extensions:gcc.bzl", "gcc", dev_dependency = True)
score_gcc.toolchain(
    name = "score_gcc_x86_64_toolchain",
    target_cpu = "x86_64",
    target_os = "linux",
    use_base_constraints_only = True,
    use_default_package = True,
    version = "12.2.0",
)
use_repo(
    score_gcc,
    "score_gcc_x86_64_toolchain",
)
"#,
        bazel_string(communication_root)
    )
}

fn build_bazel() -> &'static str {
    r#"load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_binary(
    name = "up_lola_bridge",
    srcs = [
        "up_lola_bridge.cpp",
        "up_lola_bridge.h",
    ],
    linkshared = True,
    deps = ["@score_communication//score/mw/com"],
)
"#
}

fn bazel_string(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn find_bazel() -> PathBuf {
    if let Some(bazel) = env::var_os("BAZEL") {
        let bazel = PathBuf::from(bazel);
        if let Some(path) = resolve_command(&bazel) {
            return path;
        }
        panic!(
            "BAZEL is set to `{}`, but that executable could not be found.\n\nHow to resolve:\n  1. Install Bazelisk, then set BAZEL=/path/to/bazelisk\n  2. Or put bazelisk or bazel on PATH\n  3. Or use a prebuilt bridge: LOLA_BRIDGE_LIB_DIR=/path/to/lib cargo build --no-default-features --features lola-ffi\n\nThis build script does not download Bazelisk automatically.",
            bazel.display()
        );
    }
    for candidate in ["bazelisk", "bazel"] {
        let candidate = PathBuf::from(candidate);
        if let Some(path) = resolve_command(&candidate) {
            return path;
        }
    }
    panic!(
        "LoLa native bridge build requires Bazelisk or Bazel, but neither was found on PATH.\n\nHow to resolve:\n  1. Install Bazelisk: https://github.com/bazelbuild/bazelisk\n  2. Re-run with BAZEL=/path/to/bazelisk cargo build\n  3. Or put bazelisk or bazel on PATH and re-run cargo build\n  4. Or use a prebuilt bridge: LOLA_BRIDGE_LIB_DIR=/path/to/lib cargo build --no-default-features --features lola-ffi\n  5. Or run only the fake unit backend: cargo test --no-default-features --features test-stub\n\nThis build script does not download Bazelisk automatically."
    );
}

fn resolve_command(command: &Path) -> Option<PathBuf> {
    if command.is_absolute() || command.components().count() > 1 {
        return is_executable_file(command).then(|| command.to_path_buf());
    }
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|path| path.join(command))
        .find(|path| is_executable_file(path))
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn run_bazel(bazel: &Path, workspace_dir: &Path, output_base: &Path, args: &[&str]) {
    println!(
        "cargo:warning=running Bazel for LoLa native bridge: {}",
        args.join(" ")
    );
    let status = bazel_command(bazel, workspace_dir, output_base, args)
        .status()
        .unwrap_or_else(|error| panic!("failed to execute {}: {error}", bazel.display()));
    if !status.success() {
        panic!(
            "Bazel command failed while building the LoLa native bridge.\n\nCommand: {}\nGenerated workspace: {}\nBazel output base: {}\nExit status: {}\nHow to resolve:\n  1. If Bazelisk/Bazel is missing, install Bazelisk and set BAZEL=/path/to/bazelisk\n  2. If dependency resolution failed, ensure network access to the S-CORE Bazel registries\n  3. If the S-CORE checkout is invalid, run: git submodule update --init --recursive {BUNDLED_COMMUNICATION_PATH}\n  4. Or provide a known-good checkout: LOLA_COMMUNICATION_ROOT=/path/to/eclipse-score-communication cargo build",
            args.join(" "),
            workspace_dir.display(),
            output_base.display(),
            status
        );
    }
}

fn run_bazel_capture(
    bazel: &Path,
    workspace_dir: &Path,
    output_base: &Path,
    args: &[&str],
) -> String {
    let output = bazel_command(bazel, workspace_dir, output_base, args)
        .output()
        .unwrap_or_else(|error| panic!("failed to execute {}: {error}", bazel.display()));
    if !output.status.success() {
        panic!(
            "Bazel command failed while querying the LoLa native bridge output.\n\nCommand: {}\nGenerated workspace: {}\nBazel output base: {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            workspace_dir.display(),
            output_base.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).expect("Bazel output should be UTF-8")
}

fn bazel_command(bazel: &Path, workspace_dir: &Path, output_base: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(bazel);
    command
        .arg(format!("--output_base={}", output_base.display()))
        .args(args)
        .current_dir(workspace_dir);
    command
}

fn find_bridge_library(workspace_dir: &Path, cquery_output: &str) -> PathBuf {
    for line in cquery_output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if line.ends_with(".so") && line.contains("up_lola_bridge") {
            let path = PathBuf::from(line);
            return if path.is_absolute() {
                path
            } else {
                workspace_dir.join(path)
            };
        }
    }
    panic!("Bazel did not report libup_lola_bridge.so in cquery output:\n{cquery_output}");
}

fn link_bridge(lib_dir: &Path) {
    let library = lib_dir.join("libup_lola_bridge.so");
    if !library.is_file() {
        panic!(
            "LoLa bridge library was not found at {}.\n\nHow to resolve:\n  1. Point LOLA_BRIDGE_LIB_DIR at the directory containing libup_lola_bridge.so\n  2. Or use the default bundled build: cargo build\n  3. Or build from source explicitly: cargo build --features lola-build-from-source",
            library.display()
        );
    }
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=up_lola_bridge");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
}

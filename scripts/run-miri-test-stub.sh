#!/usr/bin/env bash
set -euo pipefail

rustup +nightly component add miri
cargo +nightly miri setup

# The test-stub path constructs `UFrameMetadata::publish`, which builds a UUID
# from `SystemTime::elapsed`; Miri isolation rejects the underlying
# `clock_gettime` call, so disable isolation unless the caller supplies explicit
# `MIRIFLAGS`.
MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation}" \
    cargo +nightly miri test --no-default-features --features test-stub --lib "$@"

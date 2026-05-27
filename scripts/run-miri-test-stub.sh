#!/usr/bin/env bash
set -euo pipefail

rustup +nightly component add miri
cargo +nightly miri setup

# The test-stub path builds deterministic metadata fixtures, so the runner is
# isolation-clean by default. Callers may still provide explicit MIRIFLAGS.
cargo +nightly miri test --no-default-features --features test-stub --lib "$@"

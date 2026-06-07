# up-transport-lola-rust

Rust crate shell for the Eclipse S-CORE LoLa transport for Eclipse uProtocol.

This crate currently contains package metadata, licensing, lints, and a
compileable Rust module shell. The LoLa native runtime integration, fixed-sample
frame layout, zero-copy TX/RX APIs, benchmark harnesses, scripts, and submodule
configuration are not implemented in this crate shell.

## Features

| Feature | Scope in this skeleton |
| --- | --- |
| `default` | Empty. Does not build or link LoLa native code. |
| `native` | Placeholder vocabulary for later native runtime work. |
| `test-stub` | Placeholder vocabulary for later isolated Rust tests. |

The `zero-copy`, `benchmark-owned`, and payload-contract benchmark features are
not defined by this crate shell. Enabling any currently defined feature does not
claim LoLa transport behavior.

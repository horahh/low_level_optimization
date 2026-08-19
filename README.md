# low_level_optimization

Low-level patterns to optimize hardware-facing code while still keeping the APIs clean.

## Circular buffer variants

This repository now contains a small set of circular-buffer implementations that build up from simple and safe to concurrent and throughput-oriented:

- **Level 1**: safe ring buffer with **k + 1** internal slots to distinguish full vs empty
- **Level 2**: mutex-protected wrapper for straightforward MT-safe usage
- **Level 3a**: lock-free **SPSC** queue with atomics
- **Level 3b**: the same SPSC queue with configurable relaxed vs stricter memory ordering
- **Level 4**: bounded **MPMC** queue optimized for throughput

## Layout

- `./rust`
  - tested Rust implementations for levels 1-4
- `./c`
  - C reference implementation for levels 1-3
- `./cpp`
  - C++ reference implementation for levels 1-3

## Rust usage

Run tests:

```bash
cargo test --manifest-path rust/Cargo.toml
```

Build a release artifact:

```bash
cargo build --release --manifest-path rust/Cargo.toml
```

## C and C++ reference files

The C and C++ directories are intentionally small reference implementations so the paradigms line up with the Rust code without pulling in a larger build system yet.

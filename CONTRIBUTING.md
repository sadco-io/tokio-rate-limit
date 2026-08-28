# Contributing

## Build

```bash
cargo test --lib --tests --examples
cargo test --all-features --lib --tests --examples
cargo test --all-features --doc
cargo clippy --all-features --lib --tests --examples -- -D warnings
cargo fmt --all -- --check
```

MSRV: 1.85 (default / `middleware`). `tonic-support` needs 1.88. The library
itself does not require `protoc`.

The gRPC greeter demo is a separate crate:

```bash
cargo run -p grpc-hello --bin grpc-hello
```

That crate does need `protoc`.

## Release

1. Bump `version` in `Cargo.toml`.
2. Write the `[X.Y.Z]` section in `CHANGELOG.md`.
3. `cargo package --allow-dirty` and check the `.crate` does not contain
   `docs/archive/` or benches.
4. Tag `vX.Y.Z` and publish.

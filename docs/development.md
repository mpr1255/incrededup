# Development

Run the same checks used by CI:

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo run --example generate_cli_docs
git diff --exit-code README.md docs/cli.md
cargo test --all-targets
cargo test --doc
cargo run --example sqlite_demo
cargo run --example text_files_demo
cargo publish --dry-run
```

The PostgreSQL integration tests use a local Unix socket when available. In CI,
set `POSTGRES_TEST_URL`, for example:

```bash
POSTGRES_TEST_URL='postgresql://postgres:postgres@localhost:5432/postgres' \
  cargo test --test postgres_integration_tests
```

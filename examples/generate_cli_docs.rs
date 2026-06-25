use clap::CommandFactory;
use incrededup::Args;
use std::fs;
use std::path::Path;

const README_START: &str = "<!-- BEGIN GENERATED CLI REFERENCE -->";
const README_END: &str = "<!-- END GENERATED CLI REFERENCE -->";

fn cli_reference() -> String {
    let mut command = Args::command();
    let help = command.render_long_help().to_string();
    let version = format!("{} {}", command.get_name(), env!("CARGO_PKG_VERSION"));

    format!(
        r#"# CLI reference

This file is generated from the current Clap definition in `src/cli.rs`. Do
not edit it by hand. Regenerate it with:

```bash
cargo run --example generate_cli_docs
```

Version: `{}`

The option order follows the declaration order in `src/cli.rs`: input and mode
selection first, then matching parameters, state/write controls, daemon
controls, and sidecar maintenance.

## Primary contract

```text
{}
```

## Runtime notes

1. PostgreSQL modes read `DATABASE_URL`.
2. SQLite modes take the database path from `--sqlite`.
3. Standalone `--sync` writes to PostgreSQL and reads `DATABASE_URL`.
4. `--cleanup report` only reads sidecars. `--cleanup mark-parent` and
   `--cleanup delete` write to PostgreSQL and read `DATABASE_URL`.
5. `--sync` uses `SYNC_WORKERS` for parent and child marking. Default: `8`.
6. Sidecars live under `<data-dir>/<source-name>/`.
7. `--inspect`, `--build-adjacency`, and `--cleanup report` do not require a
   database connection.
"#,
        version,
        help.trim_end()
    )
}

fn readme_cli_section(cli_doc: &str) -> String {
    let body = cli_doc
        .strip_prefix("# CLI reference\n\n")
        .unwrap_or(cli_doc)
        .replace("This file is generated", "This section is generated");
    let body = body.trim_end();

    format!(
        r#"## CLI reference

The same generated reference is also available at [`docs/cli.md`](docs/cli.md).

{}
"#,
        body
    )
}

fn update_readme(path: &Path, cli_doc: &str) {
    let readme = fs::read_to_string(path).expect("read README.md");
    let start = readme.find(README_START).expect("README start marker");
    let end = readme.find(README_END).expect("README end marker");
    assert!(start < end, "README CLI markers are reversed");

    let before = &readme[..start + README_START.len()];
    let after = &readme[end..];
    let replacement = readme_cli_section(cli_doc);
    let updated = format!("{}\n{}\n{}", before, replacement.trim_end(), after);

    fs::write(path, updated).expect("write README.md");
}

fn main() {
    let cli_doc = cli_reference();
    fs::write("docs/cli.md", &cli_doc).expect("write docs/cli.md");
    update_readme(Path::new("README.md"), &cli_doc);
}

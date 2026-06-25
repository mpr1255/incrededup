#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Generate docs/cli.md from the current Clap help output."""

from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = ROOT / "docs" / "cli.md"


def run_command(args: list[str]) -> str:
    completed = subprocess.run(
        args,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def main() -> None:
    version = run_command(["cargo", "run", "--quiet", "--", "--version"])
    help_text = run_command(["cargo", "run", "--quiet", "--", "--help"])

    DOC_PATH.write_text(
        f"""# CLI reference

This file is generated from the current CLI help output. Do not edit it by
hand. Regenerate it with:

```bash
uv run --script scripts/generate_cli_docs.py
```

Version: `{version}`

## Primary contract

```text
{help_text}
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
""",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()

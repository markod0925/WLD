## Code Exploration Policy
Use `cymbal` CLI for code navigation — prefer it over Read, Grep, Glob, or Bash for code exploration.
- **To run tests**: use the already available project `.venv` (for example, `.venv/bin/pytest` or `.venv/bin/python -m pytest`).
- **To add Python packages**: install them into the already available project `.venv`; do not use the system Python environment.
- **New to a repo?**: `cymbal structure --db .\.cymbal\index.db` — entry points, hotspots, central packages. Start here.
- **To understand a symbol**: `cymbal investigate <symbol> --db .\.cymbal\index.db` — returns source, callers, impact, or members based on what the symbol is.
- **To understand multiple symbols**: `cymbal investigate Foo Bar Baz --db .\.cymbal\index.db` — batch mode, one invocation.
- **To trace an execution path**: `cymbal trace <symbol> --db .\.cymbal\index.db` — follows the call graph downward (what does X call, what do those call).
- **To assess change risk**: `cymbal impact <symbol> --db .\.cymbal\index.db` — follows the call graph upward (what breaks if X changes).
- Before reading a file: `cymbal outline <file> --db .\.cymbal\index.db` or `cymbal show <file:L1-L2> --db .\.cymbal\index.db`
- Before searching: `cymbal search <query> --db .\.cymbal\index.db` (symbols) or `cymbal search <query> --text --db .\.cymbal\index.db` (grep)
- Before exploring structure: `cymbal ls --db .\.cymbal\index.db` (tree) or `cymbal ls --stats --db .\.cymbal\index.db` (overview)
- To disambiguate: `cymbal show path/to/file.go:SymbolName --db .\.cymbal\index.db` or `cymbal investigate file.go:Symbol --db .\.cymbal\index.db`
- The index auto-builds on first use — no manual indexing step needed. Queries auto-refresh incrementally.
- All commands support `--json` for structured output.
- Always add `--db .\.cymbal\index.db` for compatibility with Codex

## Test Execution (WSL)

When running tests under WSL, do not call `pytest` directly.

WSL + Windows-mounted temp directories (e.g. `/mnt/c/.../Temp`) can break pytest fd capture teardown, causing FileNotFoundError during shutdown.

Use:

```bash
tools/run_tests.sh
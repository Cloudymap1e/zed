# Private Zed Copy

This directory is a vendored private copy of `zed-industries/codex-acp`.

- Upstream repository: `https://github.com/zed-industries/codex-acp`
- Imported tag: `v0.14.0`
- Imported commit: `156cb0da12f6c7b1c697f90b5f22d5e14be31165`

This copy is intentionally not a Zed workspace member. Keep it as an independent
Rust package so local adapter changes do not perturb Zed's main workspace
dependency graph or lockfile. Its `Cargo.toml` has an empty `[workspace]`
table for that reason.

Build the private adapter:

```sh
cargo build --manifest-path vendor/codex-acp/Cargo.toml
```

Point Zed at the private adapter with a settings override:

```json
{
  "agent_servers": {
    "codex-acp": {
      "type": "custom",
      "command": "/path/to/zed/vendor/codex-acp/target/debug/codex-acp"
    }
  }
}
```

Use `target/release/codex-acp` instead after a release build.

`/goal` uses Codex's experimental goals feature. Enable it in Codex's
`config.toml`:

```toml
[features]
goals = true
```

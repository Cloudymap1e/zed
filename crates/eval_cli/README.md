# eval-cli

Headless Rust binary for the legacy native-agent evaluation and benchmark
harness. The native runtime it used to launch has been removed, so `eval-cli`
now exits with a runtime error instead of constructing that removed path.

This directory also contains `zed_eval/`, the Python `zed-eval` package used to
build this binary, launch remote benchmark runs on Modal/Harbor/Pier, and fetch
historical results. A replacement benchmark harness should target Codex or
another External Agent instead of this legacy binary.

## Building

### Native, for local testing on the same OS

```sh
cargo build --release -p eval_cli
```

### Linux x86_64, for Harbor/Pier sandboxes

Harbor and Pier containers run Linux x86_64. From the repository root, use the
Docker-based build script:

```sh
crates/eval_cli/script/build-linux
```

This produces `target/eval-cli`, an x86_64 Linux ELF binary. You can also
specify a custom output path:

```sh
crates/eval_cli/script/build-linux --output ~/bin/eval-cli-linux
```

## Standalone usage

```sh
eval-cli \
  --workdir /testbed \
  --model anthropic/claude-sonnet-4-6 \
  --instruction "Fix the bug described in..." \
  --timeout 600 \
  --output-dir /logs/agent
```

`eval-cli` writes `result.json` with an error status. It no longer creates
`thread.md` or `thread.json`, because those artifacts depended on the removed
native runtime.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Agent finished |
| 1 | Error, such as model/auth/runtime failure |
| 2 | Timeout |
| 3 | Interrupted by SIGTERM or SIGINT |

## Running benchmarks

The Python `zed-eval` CLI is retained for historical result tooling, but launch
paths that depend on `eval-cli` need a replacement External Agent harness before
they can run new benchmark jobs. From the repository root:

```sh
crates/eval_cli/script/install-zed-eval
zed-eval doctor --create-volume
zed-eval run rf --from local --n-tasks 2
```

For one-off source runs without installing the tool globally, use
`crates/eval_cli/script/zed-eval <args>`.

See [`zed_eval/README.md`](zed_eval/README.md) for historical benchmark setup,
reporting, rejudging, and baseline tooling.

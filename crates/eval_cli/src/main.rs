//! Headless CLI binary for the legacy evaluation harness.
//!
//! Designed to work inside containerized environments (like Harbor/termbench) where:
//! - The repository is already checked out at the working directory
//! - The model API key was provided via environment variables
//! - Results are written to an output directory (default: `/logs/agent/`)
//!
//! ## Usage
//!
//! ```text
//! eval-cli --workdir /testbed --model anthropic/claude-sonnet-4-6-latest \
//!          --instruction "Fix the bug described in..." --timeout 600
//! ```
//!
//! ## Output
//!
//! Writes to `--output-dir` (default `/logs/agent/`):
//!   - `result.json`  — structured error result
//!
//! ## Exit codes
//!
//! | Code | Meaning |
//! |------|---------|
//! | 0    | Legacy runtime finished |
//! | 1    | Error (model/auth/runtime failure) |
//! | 2    | Timeout |
//! | 3    | Interrupted (SIGTERM/SIGINT) |

mod headless;

use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use feature_flags::FeatureFlagAppExt as _;

use gpui::AsyncApp;

#[derive(Parser, Debug)]
#[command(name = "eval-cli", about = "Run Zed's legacy evaluation harness")]
struct Args {
    /// Output current environment variables as JSON to stdout.
    /// Used internally by Zed's shell environment capture.
    #[arg(long, hide = true)]
    printenv: bool,

    /// Path to the repository working directory. Defaults to the current directory.
    #[arg(long, default_value = ".")]
    workdir: PathBuf,

    /// Instruction/prompt text. If omitted, read from stdin.
    #[arg(long, allow_hyphen_values = true)]
    instruction: Option<String>,

    /// File containing additional instruction text appended after the task prompt.
    #[arg(long)]
    instruction_suffix_file: Option<PathBuf>,

    /// Language model to use, in `provider/model` format.
    #[arg(long, default_value = "anthropic/claude-sonnet-4-6-latest")]
    model: String,

    /// Maximum wall-clock time in seconds for the agent run.
    #[arg(long)]
    timeout: Option<u64>,

    /// Directory for output artifacts.
    #[arg(long, default_value = ".")]
    output_dir: PathBuf,

    /// Disable staff mode (staff mode is enabled by default).
    #[arg(long)]
    no_staff: bool,

    /// Reasoning effort level for models that support thinking (low, medium, high).
    /// Defaults to "high" for thinking-capable models.
    #[arg(long)]
    reasoning_effort: Option<String>,

    /// Enable or disable extended thinking. Defaults to model auto-detection if omitted.
    #[arg(long)]
    thinking: Option<bool>,
}

#[derive(serde::Serialize)]
struct EvalResult {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    duration_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_secs: Option<u64>,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_creation_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_read_input_tokens: Option<u64>,
    /// Number of agent (assistant) turns, i.e. model round-trips in the agentic
    /// loop. Reported as "steps" by the eval harness.
    #[serde(skip_serializing_if = "Option::is_none")]
    step_count: Option<u64>,
    /// Total number of tool calls across all steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_count: Option<u64>,
    /// Tool calls broken down by tool name.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<std::collections::BTreeMap<String, u64>>,
}

/// Per-run statistics collected from the finished thread, written into
/// `result.json` so the post-hoc report can compute success-conditioned metrics.
#[derive(Default)]
struct RunStats {
    token_usage: Option<language_model::TokenUsage>,
    step_count: Option<u64>,
    tool_call_count: Option<u64>,
    tool_calls: Option<std::collections::BTreeMap<String, u64>>,
}

const EXIT_OK: i32 = 0;
const EXIT_ERROR: i32 = 1;
const REMOVED_NATIVE_AGENT_ERROR: &str =
    "eval-cli used the removed legacy evaluation runtime and is no longer available";

fn main() {
    let args = Args::parse();

    if args.printenv {
        util::shell_env::print_env();
        return;
    }

    env_logger::init();

    let instruction = read_instruction(&args).unwrap_or_else(|e| {
        eprintln!("Error reading instruction: {e}");
        process::exit(EXIT_ERROR);
    });

    let workdir = args.workdir.canonicalize().unwrap_or_else(|e| {
        eprintln!("Invalid --workdir {:?}: {e}", args.workdir);
        process::exit(EXIT_ERROR);
    });

    let output_dir = args.output_dir.clone();
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        eprintln!("Error creating output dir {}: {e}", output_dir.display());
        process::exit(EXIT_ERROR);
    }
    let output_dir = output_dir.canonicalize().unwrap_or_else(|e| {
        eprintln!("Invalid --output-dir {:?}: {e}", output_dir);
        process::exit(EXIT_ERROR);
    });

    let http_client = Arc::new(reqwest_client::ReqwestClient::new());
    let app = gpui_platform::headless().with_http_client(http_client);

    app.run(move |cx| {
        headless::init(cx);
        cx.set_staff(!args.no_staff);

        let model_name = args.model.clone();
        let timeout = args.timeout;
        let thinking_override = args.thinking;
        let reasoning_effort = args.reasoning_effort.clone();

        cx.spawn(async move |cx| {
            let start = Instant::now();

            let (outcome, stats) = run_agent(
                &workdir,
                &instruction,
                &model_name,
                timeout,
                thinking_override,
                reasoning_effort.as_deref(),
                Some(&output_dir),
                cx,
            )
            .await;

            let duration = start.elapsed();

            let (status, error, exit_code) = match &outcome {
                Ok(()) => ("completed".to_string(), None, EXIT_OK),
                Err(e) => {
                    eprintln!("Error: {e:#}");
                    ("error".to_string(), Some(format!("{e:#}")), EXIT_ERROR)
                }
            };

            let token_usage = stats.token_usage;
            let result = EvalResult {
                status,
                error,
                duration_secs: duration.as_secs_f64(),
                timeout_secs: timeout,
                model: model_name.clone(),
                input_tokens: token_usage.as_ref().map(|u| u.input_tokens),
                output_tokens: token_usage.as_ref().map(|u| u.output_tokens),
                cache_creation_input_tokens: token_usage
                    .as_ref()
                    .filter(|u| u.cache_creation_input_tokens > 0)
                    .map(|u| u.cache_creation_input_tokens),
                cache_read_input_tokens: token_usage
                    .as_ref()
                    .filter(|u| u.cache_read_input_tokens > 0)
                    .map(|u| u.cache_read_input_tokens),
                step_count: stats.step_count,
                tool_call_count: stats.tool_call_count,
                tool_calls: stats.tool_calls,
            };

            match serde_json::to_string_pretty(&result) {
                Ok(json) => {
                    if let Err(e) = std::fs::write(output_dir.join("result.json"), &json) {
                        eprintln!("Error writing result.json: {e:#}");
                    }
                    eprintln!("[eval-cli] result: {json}");
                }
                Err(e) => eprintln!("Error serializing result: {e:#}"),
            }

            cx.update(|cx| cx.quit());
            process::exit(exit_code);
        })
        .detach();
    });
}

fn read_instruction(args: &Args) -> Result<String> {
    let mut text = if let Some(text) = &args.instruction {
        text.clone()
    } else {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("reading instruction from stdin")?;
        buf
    };
    anyhow::ensure!(!text.trim().is_empty(), "instruction is empty");

    if let Some(path) = &args.instruction_suffix_file {
        let suffix = read_instruction_suffix_file(path)?;
        text.push_str("\n\n");
        text.push_str(&suffix);
    }
    Ok(text)
}

fn read_instruction_suffix_file(path: &PathBuf) -> Result<String> {
    let suffix = std::fs::read_to_string(path)
        .with_context(|| format!("reading instruction suffix file {}", path.display()))?;
    let suffix = suffix.trim().to_string();
    anyhow::ensure!(!suffix.is_empty(), "instruction suffix file is empty");
    Ok(suffix)
}

async fn run_agent(
    _workdir: &std::path::Path,
    _instruction: &str,
    _model_name: &str,
    _timeout: Option<u64>,
    _thinking_override: Option<bool>,
    _reasoning_effort: Option<&str>,
    _output_dir: Option<&std::path::Path>,
    _cx: &mut AsyncApp,
) -> (Result<()>, RunStats) {
    (
        Err(anyhow::anyhow!(REMOVED_NATIVE_AGENT_ERROR)),
        RunStats::default(),
    )
}

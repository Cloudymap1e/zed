use acp_thread::{
    AcpThread, AgentConnection, AgentSessionInfo, AgentSessionList, AgentSessionListRequest,
    AgentSessionListResponse, PermissionOptions, SessionListUpdate, UserMessageId,
};
use action_log::ActionLog;
use agent_client_protocol::schema as acp;
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{TimeZone as _, Utc};
use futures::FutureExt as _;
use gpui::{App, AppContext as _, AsyncApp, Entity, SharedString, Task, WeakEntity};
use project::{AgentId, Project};
use serde_json::{Value, json};
use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    env,
    ffi::OsString,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};
use util::ResultExt as _;
use util::path_list::PathList;

const MINIMUM_CODEX_VERSION: CodexVersion = CodexVersion {
    major: 0,
    minor: 142,
    patch: 0,
};
const CODEX_NATIVE_TELEMETRY_ID: &str = "codex-native";
const ZED_CODEX_NATIVE_ENV: &str = "ZED_CODEX_NATIVE";
const ZED_CODEX_EXECUTABLE_ENV: &str = "ZED_CODEX_EXECUTABLE";
const ZED_SPAWN_AGENT_TOOL_NAME: &str = "spawn_agent";
const NATIVE_TURN_START_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct CodexVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

impl std::fmt::Display for CodexVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Clone, Debug)]
struct CodexExecutable {
    path: PathBuf,
}

#[derive(Clone)]
struct CodexAppServerClient {
    inner: Arc<CodexAppServerTransport>,
}

struct CodexAppServerTransport {
    outbound_tx: mpsc::Sender<String>,
    pending: Arc<Mutex<HashMap<u64, async_channel::Sender<Result<Value, JsonRpcFailure>>>>>,
    next_id: AtomicU64,
}

#[derive(Clone, Debug)]
struct JsonRpcFailure {
    message: String,
    data: Option<Value>,
}

impl std::fmt::Display for JsonRpcFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(data) = &self.data {
            write!(formatter, "{}: {}", self.message, data)
        } else {
            write!(formatter, "{}", self.message)
        }
    }
}

impl std::error::Error for JsonRpcFailure {}

#[derive(Debug)]
enum CodexInboundMessage {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    TransportClosed(String),
}

#[derive(Debug)]
enum ParsedJsonRpcMessage {
    Response {
        id: u64,
        result: Result<Value, JsonRpcFailure>,
    },
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

struct CodexNativeSession {
    thread: WeakEntity<AcpThread>,
    active_turn_id: Option<String>,
}

enum NativeSlashCommand {
    Review { target: Value },
    Compact,
    Goal(GoalCommand),
}

enum GoalCommand {
    Status,
    Clear,
    Pause,
    Resume,
    Set(String),
}

#[derive(Clone)]
struct CodexNativeSessionList {
    client: CodexAppServerClient,
    updates_tx: async_channel::Sender<SessionListUpdate>,
    updates_rx: async_channel::Receiver<SessionListUpdate>,
}

impl CodexNativeSessionList {
    fn new(client: CodexAppServerClient) -> Self {
        let (updates_tx, updates_rx) = async_channel::unbounded();
        Self {
            client,
            updates_tx,
            updates_rx,
        }
    }

    fn send_info_update(&self, session_id: acp::SessionId, update: acp::SessionInfoUpdate) {
        self.updates_tx
            .try_send(SessionListUpdate::SessionInfo { session_id, update })
            .log_err();
    }
}

impl AgentSessionList for CodexNativeSessionList {
    fn list_sessions(
        &self,
        request: AgentSessionListRequest,
        cx: &mut App,
    ) -> Task<Result<AgentSessionListResponse>> {
        let client = self.client.clone();
        cx.foreground_executor().spawn(async move {
            let mut params = serde_json::Map::new();
            if let Some(cursor) = request.cursor {
                params.insert("cursor".into(), json!(cursor));
            }
            if let Some(cwd) = request.cwd {
                params.insert("cwd".into(), json!(cwd));
            }
            params.insert("sourceKinds".into(), json!(["appServer"]));
            params.insert("limit".into(), json!(50));

            let response = client
                .send_request("thread/list", Value::Object(params))
                .await?;
            let sessions = response
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(agent_session_info_from_thread_value)
                .collect();
            Ok(AgentSessionListResponse {
                sessions,
                next_cursor: response
                    .get("nextCursor")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                meta: None,
            })
        })
    }

    fn watch(&self, _cx: &mut App) -> Option<async_channel::Receiver<SessionListUpdate>> {
        Some(self.updates_rx.clone())
    }

    fn notify_refresh(&self) {
        self.updates_tx
            .try_send(SessionListUpdate::Refresh)
            .log_err();
    }

    fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

pub struct CodexNativeConnection {
    id: AgentId,
    telemetry_id: SharedString,
    client: CodexAppServerClient,
    sessions: Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    pending_turn_starts: Rc<RefCell<HashMap<acp::SessionId, async_channel::Sender<String>>>>,
    pending_turns: Rc<RefCell<HashMap<String, async_channel::Sender<acp::StopReason>>>>,
    completed_turns: Rc<RefCell<HashMap<String, acp::StopReason>>>,
    auth_methods: Vec<acp::AuthMethod>,
    default_model: Option<acp::ModelId>,
    session_list: Rc<CodexNativeSessionList>,
    _dispatch_task: Task<Result<()>>,
}

pub async fn connect(
    agent_id: AgentId,
    project: Entity<Project>,
    default_model: Option<acp::ModelId>,
    extra_env: HashMap<String, String>,
    cx: &mut AsyncApp,
) -> Result<Rc<dyn AgentConnection>> {
    let is_local = project.read_with(cx, |project, _cx| project.is_local());
    if !is_local {
        bail!("native Codex app-server is only available for local projects");
    }

    if native_codex_disabled() {
        bail!("native Codex app-server disabled by {ZED_CODEX_NATIVE_ENV}=0");
    }

    let executable = resolve_local_codex()?;
    let (inbound_tx, inbound_rx) = async_channel::unbounded();
    let client = CodexAppServerClient::spawn(&executable, extra_env, inbound_tx)?;

    let version = cx.update(|cx| release_channel::AppVersion::global(cx).to_string());
    client
        .send_request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "Zed",
                    "version": version,
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            }),
        )
        .await
        .context("failed to initialize Codex app-server")?;

    let sessions = Rc::new(RefCell::new(HashMap::new()));
    let pending_turn_starts = Rc::new(RefCell::new(HashMap::new()));
    let pending_turns = Rc::new(RefCell::new(HashMap::new()));
    let completed_turns = Rc::new(RefCell::new(HashMap::new()));
    let tool_outputs = Rc::new(RefCell::new(HashMap::new()));
    let session_list = Rc::new(CodexNativeSessionList::new(client.clone()));

    let dispatch_task = cx.spawn({
        let client = client.clone();
        let sessions = sessions.clone();
        let pending_turn_starts = pending_turn_starts.clone();
        let pending_turns = pending_turns.clone();
        let completed_turns = completed_turns.clone();
        let session_list = session_list.clone();
        async move |cx| {
            while let Ok(message) = inbound_rx.recv().await {
                handle_inbound_message(
                    message,
                    &client,
                    &sessions,
                    &pending_turn_starts,
                    &pending_turns,
                    &completed_turns,
                    &tool_outputs,
                    &session_list,
                    cx,
                )
                .await;
            }
            Ok(())
        }
    });

    Ok(Rc::new(CodexNativeConnection {
        id: agent_id,
        telemetry_id: CODEX_NATIVE_TELEMETRY_ID.into(),
        client,
        sessions,
        pending_turn_starts,
        pending_turns,
        completed_turns,
        auth_methods: Vec::new(),
        default_model,
        session_list,
        _dispatch_task: dispatch_task,
    }) as Rc<dyn AgentConnection>)
}

impl AgentConnection for CodexNativeConnection {
    fn agent_id(&self) -> AgentId {
        self.id.clone()
    }

    fn telemetry_id(&self) -> SharedString {
        self.telemetry_id.clone()
    }

    fn new_session(
        self: Rc<Self>,
        project: Entity<Project>,
        work_dirs: PathList,
        cx: &mut App,
    ) -> Task<Result<Entity<AcpThread>>> {
        let Some(cwd) = work_dirs.ordered_paths().next().cloned() else {
            return Task::ready(Err(anyhow!("Working directory cannot be empty")));
        };

        cx.spawn(async move |cx| {
            let mut params = serde_json::Map::new();
            params.insert("cwd".into(), json!(cwd));
            params.insert("threadSource".into(), json!("appServer"));
            params.insert(
                "runtimeWorkspaceRoots".into(),
                json!(paths_to_strings(&work_dirs)),
            );
            if let Some(default_model) = &self.default_model {
                params.insert("model".into(), json!(default_model.to_string()));
            }

            let response = self
                .client
                .send_request("thread/start", Value::Object(params))
                .await
                .context("failed to start native Codex thread")?;
            let thread_id = response
                .get("thread")
                .and_then(|thread| thread.get("id"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .context("Codex thread/start response did not include thread.id")?;
            let session_id = acp::SessionId::new(thread_id);
            let title = response
                .get("thread")
                .and_then(|thread| thread.get("name"))
                .and_then(Value::as_str)
                .map(SharedString::from);

            let action_log = cx.new(|_| ActionLog::new(project.clone()));
            let thread = cx.new(|cx| {
                AcpThread::new(
                    None,
                    title,
                    Some(work_dirs),
                    self.clone(),
                    project,
                    action_log,
                    session_id.clone(),
                    watch::Receiver::constant(acp::PromptCapabilities::new()),
                    cx,
                )
            });

            self.sessions.borrow_mut().insert(
                session_id,
                CodexNativeSession {
                    thread: thread.downgrade(),
                    active_turn_id: None,
                },
            );

            thread
                .update(cx, |thread, cx| {
                    thread.handle_session_update(
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(native_available_commands()),
                        ),
                        cx,
                    )
                })
                .context("failed to install native Codex command list")?;

            Ok(thread)
        })
    }

    fn supports_resume_session(&self) -> bool {
        true
    }

    fn resume_session(
        self: Rc<Self>,
        session_id: acp::SessionId,
        project: Entity<Project>,
        work_dirs: PathList,
        title: Option<SharedString>,
        cx: &mut App,
    ) -> Task<Result<Entity<AcpThread>>> {
        let Some(cwd) = work_dirs.ordered_paths().next().cloned() else {
            return Task::ready(Err(anyhow!("Working directory cannot be empty")));
        };

        cx.spawn(async move |cx| {
            let mut params = serde_json::Map::new();
            params.insert("threadId".into(), json!(session_id.to_string()));
            params.insert("cwd".into(), json!(cwd));
            params.insert("excludeTurns".into(), json!(true));
            params.insert(
                "runtimeWorkspaceRoots".into(),
                json!(paths_to_strings(&work_dirs)),
            );
            if let Some(default_model) = &self.default_model {
                params.insert("model".into(), json!(default_model.to_string()));
            }

            self.client
                .send_request("thread/resume", Value::Object(params))
                .await
                .context("failed to resume native Codex thread")?;

            let action_log = cx.new(|_| ActionLog::new(project.clone()));
            let thread = cx.new(|cx| {
                AcpThread::new(
                    None,
                    title,
                    Some(work_dirs),
                    self.clone(),
                    project,
                    action_log,
                    session_id.clone(),
                    watch::Receiver::constant(acp::PromptCapabilities::new()),
                    cx,
                )
            });

            self.sessions.borrow_mut().insert(
                session_id.clone(),
                CodexNativeSession {
                    thread: thread.downgrade(),
                    active_turn_id: None,
                },
            );

            thread
                .update(cx, |thread, cx| {
                    thread.handle_session_update(
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(native_available_commands()),
                        ),
                        cx,
                    )
                })
                .context("failed to install native Codex command list")?;

            Ok(thread)
        })
    }

    fn auth_methods(&self) -> &[acp::AuthMethod] {
        &self.auth_methods
    }

    fn authenticate(&self, _method: acp::AuthMethodId, _cx: &mut App) -> Task<Result<()>> {
        Task::ready(Ok(()))
    }

    fn prompt(
        &self,
        _user_message_id: UserMessageId,
        params: acp::PromptRequest,
        cx: &mut App,
    ) -> Task<Result<acp::PromptResponse>> {
        let client = self.client.clone();
        let sessions = self.sessions.clone();
        let pending_turn_starts = self.pending_turn_starts.clone();
        let pending_turns = self.pending_turns.clone();
        let completed_turns = self.completed_turns.clone();
        let session_id = params.session_id.clone();

        cx.spawn(async move |cx| {
            let input = prompt_blocks_to_codex_input(params.prompt)?;
            let Some(command) = native_slash_command(&input)? else {
                let response = client
                    .send_request(
                        "turn/start",
                        json!({
                            "threadId": session_id.to_string(),
                            "input": input,
                        }),
                    )
                    .await
                    .context("failed to start native Codex turn")?;
                let turn_id = response
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .context("Codex turn/start response did not include turn.id")?;

                let stop_reason = wait_for_native_turn(
                    &session_id,
                    turn_id,
                    &sessions,
                    &pending_turns,
                    &completed_turns,
                )
                .await;
                return Ok(acp::PromptResponse::new(stop_reason));
            };

            run_native_slash_command(
                &client,
                &session_id,
                &sessions,
                &pending_turn_starts,
                &pending_turns,
                &completed_turns,
                command,
                cx,
            )
            .await
        })
    }

    fn cancel(&self, session_id: &acp::SessionId, cx: &mut App) {
        let Some(turn_id) = self
            .sessions
            .borrow()
            .get(session_id)
            .and_then(|session| session.active_turn_id.clone())
        else {
            return;
        };
        let client = self.client.clone();
        let session_id = session_id.clone();
        cx.foreground_executor()
            .spawn(async move {
                client
                    .send_request(
                        "turn/interrupt",
                        json!({
                            "threadId": session_id.to_string(),
                            "turnId": turn_id,
                        }),
                    )
                    .await
                    .map(|_| ())
            })
            .detach_and_log_err(cx);
    }

    fn session_list(&self, _cx: &mut App) -> Option<Rc<dyn AgentSessionList>> {
        Some(self.session_list.clone())
    }

    fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

impl CodexAppServerClient {
    #[expect(
        clippy::disallowed_methods,
        reason = "native Codex app-server requires stdio pipes owned by reader and writer threads"
    )]
    fn spawn(
        executable: &CodexExecutable,
        extra_env: HashMap<String, String>,
        inbound_tx: async_channel::Sender<CodexInboundMessage>,
    ) -> Result<Self> {
        let mut child = Command::new(&executable.path)
            .arg("app-server")
            .arg("--stdio")
            .envs(extra_env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| {
                format!("failed to spawn `{}` app-server", executable.path.display())
            })?;
        let mut stdin = child.stdin.take().context("failed to take Codex stdin")?;
        let stdout = child.stdout.take().context("failed to take Codex stdout")?;
        let stderr = child.stderr.take().context("failed to take Codex stderr")?;
        let (outbound_tx, outbound_rx) = mpsc::channel::<String>();
        let pending: Arc<
            Mutex<HashMap<u64, async_channel::Sender<Result<Value, JsonRpcFailure>>>>,
        > = Arc::new(Mutex::new(HashMap::new()));

        thread::Builder::new()
            .name("codex-app-server-stdin".into())
            .spawn(move || {
                while let Ok(line) = outbound_rx.recv() {
                    if writeln!(stdin, "{line}").is_err() {
                        break;
                    }
                    if stdin.flush().is_err() {
                        break;
                    }
                }
            })
            .context("failed to spawn Codex stdin writer thread")?;

        thread::Builder::new()
            .name("codex-app-server-stdout".into())
            .spawn({
                let pending = pending.clone();
                move || {
                    let reader = BufReader::new(stdout);
                    for line_result in reader.lines() {
                        let line = match line_result {
                            Ok(line) => line,
                            Err(error) => {
                                drain_pending(
                                    &pending,
                                    format!("failed reading Codex stdout: {error}"),
                                );
                                inbound_tx
                                    .try_send(CodexInboundMessage::TransportClosed(format!(
                                        "failed reading Codex stdout: {error}"
                                    )))
                                    .log_err();
                                return;
                            }
                        };

                        match parse_json_rpc_line(&line) {
                            Ok(ParsedJsonRpcMessage::Response { id, result }) => {
                                let sender = pending
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .remove(&id);
                                if let Some(sender) = sender {
                                    sender.try_send(result).log_err();
                                }
                            }
                            Ok(ParsedJsonRpcMessage::Notification { method, params }) => {
                                inbound_tx
                                    .try_send(CodexInboundMessage::Notification { method, params })
                                    .log_err();
                            }
                            Ok(ParsedJsonRpcMessage::Request { id, method, params }) => {
                                inbound_tx
                                    .try_send(CodexInboundMessage::Request { id, method, params })
                                    .log_err();
                            }
                            Err(error) => {
                                log::warn!(
                                    "failed to parse Codex app-server JSON-RPC line: {error}"
                                );
                            }
                        }
                    }
                    drain_pending(&pending, "Codex app-server stdout closed".to_owned());
                    inbound_tx
                        .try_send(CodexInboundMessage::TransportClosed(
                            "Codex app-server stdout closed".to_owned(),
                        ))
                        .log_err();
                }
            })
            .context("failed to spawn Codex stdout reader thread")?;

        thread::Builder::new()
            .name("codex-app-server-stderr".into())
            .spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    log::debug!("codex app-server stderr: {line}");
                }
            })
            .context("failed to spawn Codex stderr reader thread")?;

        thread::Builder::new()
            .name("codex-app-server-wait".into())
            .spawn(move || match child.wait() {
                Ok(status) => log::debug!("Codex app-server exited with {status}"),
                Err(error) => log::warn!("failed waiting for Codex app-server: {error}"),
            })
            .context("failed to spawn Codex wait thread")?;

        Ok(Self {
            inner: Arc::new(CodexAppServerTransport {
                outbound_tx,
                pending,
                next_id: AtomicU64::new(1),
            }),
        })
    }

    async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let (response_tx, response_rx) = async_channel::bounded(1);
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, response_tx);

        let line = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        })
        .to_string();

        if let Err(error) = self.inner.outbound_tx.send(line) {
            self.inner
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&id);
            bail!("failed to send Codex request `{method}`: {error}");
        }

        response_rx
            .recv()
            .await
            .context("Codex app-server response channel closed")?
            .map_err(anyhow::Error::from)
    }

    fn send_response(&self, id: Value, result: Value) -> Result<()> {
        let line = json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        })
        .to_string();
        self.inner
            .outbound_tx
            .send(line)
            .context("failed to send Codex app-server response")
    }

    fn send_error_response(&self, id: Value, message: impl Into<String>) -> Result<()> {
        let line = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": message.into(),
            },
        })
        .to_string();
        self.inner
            .outbound_tx
            .send(line)
            .context("failed to send Codex app-server error response")
    }
}

async fn handle_inbound_message(
    message: CodexInboundMessage,
    client: &CodexAppServerClient,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    pending_turn_starts: &Rc<RefCell<HashMap<acp::SessionId, async_channel::Sender<String>>>>,
    pending_turns: &Rc<RefCell<HashMap<String, async_channel::Sender<acp::StopReason>>>>,
    completed_turns: &Rc<RefCell<HashMap<String, acp::StopReason>>>,
    tool_outputs: &Rc<RefCell<HashMap<String, String>>>,
    session_list: &Rc<CodexNativeSessionList>,
    cx: &mut AsyncApp,
) {
    match message {
        CodexInboundMessage::Notification { method, params } => {
            handle_server_notification(
                &method,
                &params,
                sessions,
                pending_turn_starts,
                pending_turns,
                completed_turns,
                tool_outputs,
                session_list,
                cx,
            )
            .await;
        }
        CodexInboundMessage::Request { id, method, params } => {
            handle_server_request(id, &method, params, client, sessions, cx).await;
        }
        CodexInboundMessage::TransportClosed(reason) => {
            log::warn!("Codex native app-server transport closed: {reason}");
            for sender in pending_turns.borrow_mut().drain().map(|(_, sender)| sender) {
                sender.try_send(acp::StopReason::Cancelled).log_err();
            }
        }
    }
}

async fn handle_server_notification(
    method: &str,
    params: &Value,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    pending_turn_starts: &Rc<RefCell<HashMap<acp::SessionId, async_channel::Sender<String>>>>,
    pending_turns: &Rc<RefCell<HashMap<String, async_channel::Sender<acp::StopReason>>>>,
    completed_turns: &Rc<RefCell<HashMap<String, acp::StopReason>>>,
    tool_outputs: &Rc<RefCell<HashMap<String, String>>>,
    session_list: &Rc<CodexNativeSessionList>,
    cx: &mut AsyncApp,
) {
    if method == "turn/started" {
        start_turn(params, pending_turn_starts);
    }

    if method == "turn/completed" {
        complete_turn(params, sessions, pending_turns, completed_turns);
    }

    let updates = {
        let mut tool_outputs = tool_outputs.borrow_mut();
        session_updates_from_notification(method, params, &mut tool_outputs)
    };

    for (session_id, update) in updates {
        if let acp::SessionUpdate::SessionInfoUpdate(info_update) = &update {
            session_list.send_info_update(session_id.clone(), info_update.clone());
        }

        let thread = sessions
            .borrow()
            .get(&session_id)
            .map(|session| session.thread.clone());
        let Some(thread) = thread else {
            log::debug!("native Codex notification for unknown thread `{session_id}`: {method}");
            continue;
        };
        thread
            .update(cx, |thread, cx| thread.handle_session_update(update, cx))
            .log_err();
    }
}

async fn handle_server_request(
    id: Value,
    method: &str,
    params: Value,
    client: &CodexAppServerClient,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    cx: &mut AsyncApp,
) {
    let result = match method {
        "item/commandExecution/requestApproval" => {
            handle_approval_request(params, "Run command", "accept", "decline", sessions, cx).await
        }
        "item/fileChange/requestApproval" => {
            handle_approval_request(
                params,
                "Apply file changes",
                "accept",
                "decline",
                sessions,
                cx,
            )
            .await
        }
        "item/permissions/requestApproval" => {
            handle_permissions_request(params, sessions, cx).await
        }
        "item/tool/requestUserInput" => Ok(json!({ "answers": {} })),
        "mcpServer/elicitation/request" => Ok(json!({ "action": "cancel" })),
        "currentTime/read" => Ok(json!({ "currentTimeAt": Utc::now().timestamp() })),
        _ => Err(anyhow!("unsupported Codex app-server request `{method}`")),
    };

    match result {
        Ok(result) => {
            client.send_response(id, result).log_err();
        }
        Err(error) => {
            client.send_error_response(id, error.to_string()).log_err();
        }
    }
}

async fn handle_approval_request(
    params: Value,
    title_fallback: &str,
    allow_decision: &str,
    deny_decision: &str,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    cx: &mut AsyncApp,
) -> Result<Value> {
    let thread_id = required_string(&params, "threadId")?;
    let item_id = required_string(&params, "itemId")?;
    let session_id = acp::SessionId::new(thread_id.clone());
    let thread = sessions
        .borrow()
        .get(&session_id)
        .map(|session| session.thread.clone())
        .context("approval request for unknown Codex thread")?;

    let title = params
        .get("command")
        .and_then(Value::as_str)
        .filter(|command| !command.trim().is_empty())
        .unwrap_or(title_fallback)
        .to_owned();
    let content = params
        .get("reason")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| approval_markdown(&params));
    let options = vec![
        acp::PermissionOption::new(
            allow_decision.to_owned(),
            "Allow",
            acp::PermissionOptionKind::AllowOnce,
        ),
        acp::PermissionOption::new(
            deny_decision.to_owned(),
            "Deny",
            acp::PermissionOptionKind::RejectOnce,
        ),
        acp::PermissionOption::new("cancel", "Cancel", acp::PermissionOptionKind::RejectOnce),
    ];
    let tool_call = acp::ToolCallUpdate::new(
        acp::ToolCallId::new(item_id),
        acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Execute)
            .status(acp::ToolCallStatus::Pending)
            .title(title)
            .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(content)),
            ))])
            .raw_input(params),
    );

    let authorization_task = thread
        .update(cx, |thread, cx| {
            thread.request_tool_call_authorization(tool_call, PermissionOptions::Flat(options), cx)
        })
        .context("failed to request native Codex approval")??;

    let outcome = authorization_task.await;
    let decision = match outcome {
        acp_thread::RequestPermissionOutcome::Selected(selected)
            if selected.option_kind == acp::PermissionOptionKind::AllowOnce
                || selected.option_kind == acp::PermissionOptionKind::AllowAlways =>
        {
            selected.option_id.to_string()
        }
        acp_thread::RequestPermissionOutcome::Selected(selected) => selected.option_id.to_string(),
        acp_thread::RequestPermissionOutcome::Cancelled => "cancel".to_owned(),
    };

    Ok(json!({ "decision": decision }))
}

async fn handle_permissions_request(
    params: Value,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    cx: &mut AsyncApp,
) -> Result<Value> {
    let thread_id = required_string(&params, "threadId")?;
    let item_id = required_string(&params, "itemId")?;
    let session_id = acp::SessionId::new(thread_id.clone());
    let thread = sessions
        .borrow()
        .get(&session_id)
        .map(|session| session.thread.clone())
        .context("permissions request for unknown Codex thread")?;

    let content = params
        .get("reason")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "Codex requested additional permissions.".to_owned());
    let options = vec![
        acp::PermissionOption::new("allow", "Allow", acp::PermissionOptionKind::AllowOnce),
        acp::PermissionOption::new("cancel", "Cancel", acp::PermissionOptionKind::RejectOnce),
    ];
    let tool_call = acp::ToolCallUpdate::new(
        acp::ToolCallId::new(item_id),
        acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Other)
            .status(acp::ToolCallStatus::Pending)
            .title("Approve permissions")
            .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(content)),
            ))])
            .raw_input(params.clone()),
    );

    let authorization_task = thread
        .update(cx, |thread, cx| {
            thread.request_tool_call_authorization(tool_call, PermissionOptions::Flat(options), cx)
        })
        .context("failed to request native Codex permissions approval")??;

    let outcome = authorization_task.await;
    let permissions = match outcome {
        acp_thread::RequestPermissionOutcome::Selected(selected)
            if selected.option_kind == acp::PermissionOptionKind::AllowOnce
                || selected.option_kind == acp::PermissionOptionKind::AllowAlways =>
        {
            params
                .get("permissions")
                .cloned()
                .unwrap_or_else(|| json!({}))
        }
        _ => json!({}),
    };

    Ok(json!({
        "permissions": permissions,
        "scope": "turn",
    }))
}

fn session_updates_from_notification(
    method: &str,
    params: &Value,
    tool_outputs: &mut HashMap<String, String>,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    match method {
        "item/agentMessage/delta" => {
            text_delta_update(params, acp::SessionUpdate::AgentMessageChunk)
        }
        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
            text_delta_update(params, acp::SessionUpdate::AgentThoughtChunk)
        }
        "turn/plan/updated" => plan_update(params),
        "thread/name/updated" => thread_name_update(params),
        "thread/goal/updated" => thread_goal_update(params),
        "thread/goal/cleared" => thread_goal_cleared_update(params),
        "thread/tokenUsage/updated" => token_usage_update(params),
        "item/commandExecution/outputDelta" | "item/fileChange/outputDelta" => {
            tool_output_delta_update(params, tool_outputs)
        }
        "item/fileChange/patchUpdated" => file_change_patch_update(params),
        "item/started" => item_lifecycle_update(params, false),
        "item/completed" => item_lifecycle_update(params, true),
        "hook/started" => hook_lifecycle_update(params, false),
        "hook/completed" => hook_lifecycle_update(params, true),
        "context/compacted" => compacted_update(params),
        _ => Vec::new(),
    }
}

fn text_delta_update(
    params: &Value,
    build: fn(acp::ContentChunk) -> acp::SessionUpdate,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(delta) = params.get("delta").and_then(Value::as_str) else {
        return Vec::new();
    };
    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        build(acp::ContentChunk::new(acp::ContentBlock::Text(
            acp::TextContent::new(delta),
        ))),
    )]
}

fn plan_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let entries = params
        .get("plan")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let step = entry.get("step")?.as_str()?;
            let status = match entry.get("status").and_then(Value::as_str) {
                Some("completed") => acp::PlanEntryStatus::Completed,
                Some("inProgress") => acp::PlanEntryStatus::InProgress,
                _ => acp::PlanEntryStatus::Pending,
            };
            Some(acp::PlanEntry::new(
                step,
                acp::PlanEntryPriority::Medium,
                status,
            ))
        })
        .collect();

    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::Plan(acp::Plan::new(entries)),
    )]
}

fn thread_name_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(title) = params.get("threadName").and_then(Value::as_str) else {
        return Vec::new();
    };
    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::SessionInfoUpdate(
            acp::SessionInfoUpdate::new().title(title.to_owned()),
        ),
    )]
}

fn thread_goal_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(goal) = params.get("goal") else {
        return Vec::new();
    };
    text_message_update(thread_id, goal_message(goal))
}

fn thread_goal_cleared_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    text_message_update(thread_id, "Goal cleared.".to_owned())
}

fn compacted_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    text_message_update(thread_id, "Context compacted.".to_owned())
}

fn text_message_update(thread_id: &str, text: String) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(acp::ContentBlock::Text(
            acp::TextContent::new(text),
        ))),
    )]
}

fn goal_message(goal: &Value) -> String {
    let objective = goal
        .get("objective")
        .and_then(Value::as_str)
        .unwrap_or("No objective");
    let status = goal
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mut lines = vec![format!("Goal updated ({status}): {objective}")];
    if let Some(tokens_used) = goal.get("tokensUsed").and_then(Value::as_u64) {
        lines.push(format!("Tokens used: {tokens_used}"));
    }
    if let Some(token_budget) = goal.get("tokenBudget").and_then(Value::as_u64) {
        lines.push(format!("Token budget: {token_budget}"));
    }
    lines.join("\n")
}

fn token_usage_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(total) = params
        .get("tokenUsage")
        .and_then(|usage| usage.get("total"))
    else {
        return Vec::new();
    };
    let input_tokens = total
        .get("inputTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = total
        .get("outputTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_read_input_tokens = total
        .get("cachedInputTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let used = total
        .get("totalTokens")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            input_tokens
                .saturating_add(output_tokens)
                .saturating_add(cache_read_input_tokens)
        });
    let size = params
        .get("tokenUsage")
        .and_then(|usage| usage.get("modelContextWindow"))
        .and_then(Value::as_u64)
        .unwrap_or(used);
    let meta = acp_thread::meta_with_session_token_usage(acp_thread::SessionTokenUsageMeta {
        input_tokens,
        output_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens: 0,
    });

    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::UsageUpdate(acp::UsageUpdate::new(used, size).meta(meta)),
    )]
}

fn tool_output_delta_update(
    params: &Value,
    tool_outputs: &mut HashMap<String, String>,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(delta) = params.get("delta").and_then(Value::as_str) else {
        return Vec::new();
    };
    let output = tool_outputs.entry(item_id.to_owned()).or_default();
    output.push_str(delta);
    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
            item_id.to_owned(),
            acp::ToolCallUpdateFields::new().content(vec![acp::ToolCallContent::Content(
                acp::Content::new(acp::ContentBlock::Text(acp::TextContent::new(
                    output.clone(),
                ))),
            )]),
        )),
    )]
}

fn file_change_patch_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let summary = summarize_file_changes(params.get("changes"));
    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
            item_id.to_owned(),
            acp::ToolCallUpdateFields::new()
                .kind(acp::ToolKind::Edit)
                .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                    acp::ContentBlock::Text(acp::TextContent::new(summary)),
                ))]),
        )),
    )]
}

fn item_lifecycle_update(
    params: &Value,
    completed: bool,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(item) = params.get("item") else {
        return Vec::new();
    };
    let Some(item_type) = item.get("type").and_then(Value::as_str) else {
        return Vec::new();
    };

    let session_id = acp::SessionId::new(thread_id.to_owned());
    match item_type {
        "userMessage" if !completed => user_message_update(&session_id, item),
        "commandExecution" => {
            tool_lifecycle_update(&session_id, item, acp::ToolKind::Execute, completed)
        }
        "fileChange" => tool_lifecycle_update(&session_id, item, acp::ToolKind::Edit, completed),
        "mcpToolCall"
        | "dynamicToolCall"
        | "collabAgentToolCall"
        | "webSearch"
        | "imageGeneration" => tool_lifecycle_update(
            &session_id,
            item,
            tool_kind_for_item_type(item_type),
            completed,
        ),
        _ => Vec::new(),
    }
}

fn user_message_update(
    session_id: &acp::SessionId,
    item: &Value,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let blocks = item
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(user_input_to_content_block)
        .map(|content| {
            (
                session_id.clone(),
                acp::SessionUpdate::UserMessageChunk(acp::ContentChunk::new(content)),
            )
        })
        .collect();
    blocks
}

fn tool_lifecycle_update(
    session_id: &acp::SessionId,
    item: &Value,
    kind: acp::ToolKind,
    completed: bool,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(item_id) = item.get("id").and_then(Value::as_str) else {
        return Vec::new();
    };
    let status = tool_status_from_item(item, completed);
    if completed {
        let mut fields = acp::ToolCallUpdateFields::new().kind(kind).status(status);
        if let Some(content) = tool_item_content(item) {
            fields = fields.content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(content)),
            ))]);
        }
        let mut update = acp::ToolCallUpdate::new(item_id.to_owned(), fields);
        update.meta = collab_tool_call_meta(item);
        vec![(
            session_id.clone(),
            acp::SessionUpdate::ToolCallUpdate(update),
        )]
    } else {
        let mut tool_call = acp::ToolCall::new(item_id.to_owned(), tool_title(item))
            .kind(kind)
            .status(status)
            .raw_input(item.clone());
        tool_call.meta = collab_tool_call_meta(item);
        vec![(session_id.clone(), acp::SessionUpdate::ToolCall(tool_call))]
    }
}

fn hook_lifecycle_update(
    params: &Value,
    completed: bool,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let hook_name = params
        .get("hookName")
        .or_else(|| params.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("hook");
    let item_id = params
        .get("itemId")
        .or_else(|| params.get("hookId"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("hook:{hook_name}"));
    let session_id = acp::SessionId::new(thread_id.to_owned());
    if completed {
        vec![(
            session_id,
            acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
                item_id,
                acp::ToolCallUpdateFields::new().status(acp::ToolCallStatus::Completed),
            )),
        )]
    } else {
        vec![(
            session_id,
            acp::SessionUpdate::ToolCall(
                acp::ToolCall::new(item_id, format!("Hook: {hook_name}"))
                    .kind(acp::ToolKind::Other)
                    .status(acp::ToolCallStatus::InProgress),
            ),
        )]
    }
}

fn complete_turn(
    params: &Value,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    pending_turns: &Rc<RefCell<HashMap<String, async_channel::Sender<acp::StopReason>>>>,
    completed_turns: &Rc<RefCell<HashMap<String, acp::StopReason>>>,
) {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return;
    };
    let Some(turn) = params.get("turn") else {
        return;
    };
    let Some(turn_id) = turn.get("id").and_then(Value::as_str) else {
        return;
    };
    let session_id = acp::SessionId::new(thread_id.to_owned());
    if let Some(session) = sessions.borrow_mut().get_mut(&session_id) {
        session.active_turn_id = None;
    }
    let stop_reason = match turn.get("status").and_then(Value::as_str) {
        Some("interrupted") => acp::StopReason::Cancelled,
        _ => acp::StopReason::EndTurn,
    };
    let turn_key = turn_key(&session_id, turn_id);
    if let Some(sender) = pending_turns.borrow_mut().remove(&turn_key) {
        sender.try_send(stop_reason).log_err();
    } else {
        completed_turns.borrow_mut().insert(turn_key, stop_reason);
    }
}

fn start_turn(
    params: &Value,
    pending_turn_starts: &Rc<RefCell<HashMap<acp::SessionId, async_channel::Sender<String>>>>,
) {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return;
    };
    let Some(turn_id) = params
        .get("turn")
        .and_then(|turn| turn.get("id"))
        .and_then(Value::as_str)
    else {
        return;
    };
    let session_id = acp::SessionId::new(thread_id.to_owned());
    if let Some(sender) = pending_turn_starts.borrow_mut().remove(&session_id) {
        sender.try_send(turn_id.to_owned()).log_err();
    }
}

fn watch_next_native_turn_start(
    session_id: &acp::SessionId,
    pending_turn_starts: &Rc<RefCell<HashMap<acp::SessionId, async_channel::Sender<String>>>>,
) -> async_channel::Receiver<String> {
    let (turn_started_tx, turn_started_rx) = async_channel::bounded(1);
    pending_turn_starts
        .borrow_mut()
        .insert(session_id.clone(), turn_started_tx);
    turn_started_rx
}

async fn wait_for_native_turn_start_with_timeout(
    turn_started: async_channel::Receiver<String>,
    cx: &mut AsyncApp,
) -> Option<String> {
    let timeout = cx
        .background_executor()
        .timer(NATIVE_TURN_START_TIMEOUT)
        .fuse();
    let turn_started = turn_started.recv().map(|result| result.ok()).fuse();
    futures::pin_mut!(timeout);
    futures::pin_mut!(turn_started);
    futures::select_biased! {
        turn_id = turn_started => turn_id,
        _ = timeout => None,
    }
}

async fn wait_for_native_turn(
    session_id: &acp::SessionId,
    turn_id: String,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    pending_turns: &Rc<RefCell<HashMap<String, async_channel::Sender<acp::StopReason>>>>,
    completed_turns: &Rc<RefCell<HashMap<String, acp::StopReason>>>,
) -> acp::StopReason {
    if let Some(session) = sessions.borrow_mut().get_mut(session_id) {
        session.active_turn_id = Some(turn_id.clone());
    }

    let turn_key = turn_key(session_id, &turn_id);
    if let Some(stop_reason) = completed_turns.borrow_mut().remove(&turn_key) {
        return stop_reason;
    }

    let (turn_completed_tx, turn_completed_rx) = async_channel::bounded(1);
    pending_turns
        .borrow_mut()
        .insert(turn_key, turn_completed_tx);
    turn_completed_rx
        .recv()
        .await
        .unwrap_or(acp::StopReason::Cancelled)
}

fn show_goal_status(
    session_id: &acp::SessionId,
    sessions: &HashMap<acp::SessionId, CodexNativeSession>,
    response: &Value,
    cx: &mut AsyncApp,
) -> Result<()> {
    let Some(thread) = sessions
        .get(session_id)
        .map(|session| session.thread.clone())
    else {
        return Ok(());
    };
    let message = if let Some(goal) = response.get("goal") {
        goal_message(goal)
    } else {
        "No active goal.".to_owned()
    };
    let update = acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
        acp::ContentBlock::Text(acp::TextContent::new(message)),
    ));
    thread
        .update(cx, |thread, cx| thread.handle_session_update(update, cx))?
        .map_err(anyhow::Error::from)
}

fn prompt_blocks_to_codex_input(blocks: Vec<acp::ContentBlock>) -> Result<Vec<Value>> {
    let mut input = Vec::new();
    for block in blocks {
        match block {
            acp::ContentBlock::Text(text) => input.push(json!({
                "type": "text",
                "text": text.text,
            })),
            acp::ContentBlock::Resource(resource) => input.push(json!({
                "type": "text",
                "text": serde_json::to_string(&resource).context("failed to serialize prompt resource")?,
            })),
            acp::ContentBlock::ResourceLink(resource) => input.push(json!({
                "type": "text",
                "text": serde_json::to_string(&resource).context("failed to serialize prompt resource link")?,
            })),
            acp::ContentBlock::Image(image) => input.push(json!({
                "type": "text",
                "text": format!("[Image content omitted by native Codex Zed adapter: {:?}]", image),
            })),
            acp::ContentBlock::Audio(audio) => input.push(json!({
                "type": "text",
                "text": format!("[Audio content omitted by native Codex Zed adapter: {:?}]", audio),
            })),
            _ => input.push(json!({
                "type": "text",
                "text": "[Unsupported content omitted by native Codex Zed adapter]",
            })),
        }
    }
    Ok(input)
}

fn native_slash_command(input: &[Value]) -> Result<Option<NativeSlashCommand>> {
    let Some((name, rest)) = extract_native_slash_command(input) else {
        return Ok(None);
    };

    match name {
        "review" => {
            let target = if rest.trim().is_empty() {
                json!({ "type": "uncommittedChanges" })
            } else {
                json!({
                    "type": "custom",
                    "instructions": rest.trim(),
                })
            };
            Ok(Some(NativeSlashCommand::Review { target }))
        }
        "compact" => Ok(Some(NativeSlashCommand::Compact)),
        "goal" => {
            let trimmed = rest.trim();
            let command = match trimmed.to_ascii_lowercase().as_str() {
                "" | "status" => GoalCommand::Status,
                "clear" => GoalCommand::Clear,
                "pause" => GoalCommand::Pause,
                "resume" => GoalCommand::Resume,
                _ => GoalCommand::Set(trimmed.to_owned()),
            };
            Ok(Some(NativeSlashCommand::Goal(command)))
        }
        _ => Ok(None),
    }
}

fn extract_native_slash_command(input: &[Value]) -> Option<(&str, &str)> {
    let text = input
        .first()
        .and_then(|block| {
            block
                .get("type")
                .and_then(Value::as_str)
                .zip(block.get("text"))
        })
        .and_then(|(block_type, text)| {
            if block_type == "text" {
                text.as_str()
            } else {
                None
            }
        })?;
    let stripped = text.strip_prefix('/')?;
    let mut name_end = stripped.len();
    for (index, character) in stripped.char_indices() {
        if character.is_whitespace() {
            name_end = index;
            break;
        }
    }
    let name = &stripped[..name_end];
    if name.is_empty() {
        return None;
    }
    Some((name, stripped[name_end..].trim_start()))
}

async fn run_native_slash_command(
    client: &CodexAppServerClient,
    session_id: &acp::SessionId,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    pending_turn_starts: &Rc<RefCell<HashMap<acp::SessionId, async_channel::Sender<String>>>>,
    pending_turns: &Rc<RefCell<HashMap<String, async_channel::Sender<acp::StopReason>>>>,
    completed_turns: &Rc<RefCell<HashMap<String, acp::StopReason>>>,
    command: NativeSlashCommand,
    cx: &mut AsyncApp,
) -> Result<acp::PromptResponse> {
    match command {
        NativeSlashCommand::Review { target } => {
            let response = client
                .send_request(
                    "review/start",
                    json!({
                        "threadId": session_id.to_string(),
                        "target": target,
                        "delivery": "inline",
                    }),
                )
                .await
                .context("failed to start native Codex review")?;
            let turn_id = response
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .context("Codex review/start response did not include turn.id")?;
            let stop_reason = wait_for_native_turn(
                session_id,
                turn_id,
                sessions,
                pending_turns,
                completed_turns,
            )
            .await;
            return Ok(acp::PromptResponse::new(stop_reason));
        }
        NativeSlashCommand::Compact => {
            let turn_started = watch_next_native_turn_start(session_id, pending_turn_starts);
            if let Err(error) = client
                .send_request(
                    "thread/compact/start",
                    json!({ "threadId": session_id.to_string() }),
                )
                .await
            {
                pending_turn_starts.borrow_mut().remove(session_id);
                return Err(error).context("failed to start native Codex compaction");
            }
            if let Some(turn_id) = wait_for_native_turn_start_with_timeout(turn_started, cx).await {
                let stop_reason = wait_for_native_turn(
                    session_id,
                    turn_id,
                    sessions,
                    pending_turns,
                    completed_turns,
                )
                .await;
                return Ok(acp::PromptResponse::new(stop_reason));
            }
            pending_turn_starts.borrow_mut().remove(session_id);
        }
        NativeSlashCommand::Goal(command) => {
            let params = match command {
                GoalCommand::Status => {
                    let response = client
                        .send_request(
                            "thread/goal/get",
                            json!({ "threadId": session_id.to_string() }),
                        )
                        .await
                        .context("failed to read native Codex goal")?;
                    show_goal_status(session_id, &sessions.borrow(), &response, cx)
                        .context("failed to show native Codex goal")?;
                    return Ok(acp::PromptResponse::new(acp::StopReason::EndTurn));
                }
                GoalCommand::Clear => {
                    client
                        .send_request(
                            "thread/goal/clear",
                            json!({ "threadId": session_id.to_string() }),
                        )
                        .await
                        .context("failed to clear native Codex goal")?;
                    return Ok(acp::PromptResponse::new(acp::StopReason::EndTurn));
                }
                GoalCommand::Pause => json!({
                    "threadId": session_id.to_string(),
                    "status": "paused",
                }),
                GoalCommand::Resume => json!({
                    "threadId": session_id.to_string(),
                    "status": "active",
                }),
                GoalCommand::Set(objective) => json!({
                    "threadId": session_id.to_string(),
                    "objective": objective,
                    "status": "active",
                }),
            };
            client
                .send_request("thread/goal/set", params)
                .await
                .context("failed to update native Codex goal")?;
        }
    }
    Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
}

fn user_input_to_content_block(input: &Value) -> Option<acp::ContentBlock> {
    match input.get("type").and_then(Value::as_str) {
        Some("text") => input
            .get("text")
            .and_then(Value::as_str)
            .map(|text| acp::ContentBlock::Text(acp::TextContent::new(text))),
        Some("localImage") => input.get("path").and_then(Value::as_str).map(|path| {
            acp::ContentBlock::Text(acp::TextContent::new(format!("[Local image: {path}]")))
        }),
        Some("image") => input
            .get("url")
            .and_then(Value::as_str)
            .map(|url| acp::ContentBlock::Text(acp::TextContent::new(format!("[Image: {url}]")))),
        _ => None,
    }
}

fn collab_tool_call_meta(item: &Value) -> Option<acp::Meta> {
    if item.get("type").and_then(Value::as_str) != Some("collabAgentToolCall") {
        return None;
    }

    let mut meta = acp_thread::meta_with_tool_name(ZED_SPAWN_AGENT_TOOL_NAME);
    if let Some(session_id) = item
        .get("receiverThreadIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .next()
    {
        meta.insert(
            acp_thread::SUBAGENT_SESSION_INFO_META_KEY.to_owned(),
            json!({
                "session_id": session_id,
                "message_start_index": 0,
                "message_end_index": null,
            }),
        );
    }
    Some(meta)
}

fn tool_status_from_item(item: &Value, completed: bool) -> acp::ToolCallStatus {
    match item.get("status").and_then(Value::as_str) {
        Some("completed") => acp::ToolCallStatus::Completed,
        Some("failed") | Some("declined") => acp::ToolCallStatus::Failed,
        _ if completed => acp::ToolCallStatus::Completed,
        _ => acp::ToolCallStatus::InProgress,
    }
}

fn tool_kind_for_item_type(item_type: &str) -> acp::ToolKind {
    match item_type {
        "webSearch" => acp::ToolKind::Fetch,
        "fileChange" => acp::ToolKind::Edit,
        "commandExecution" => acp::ToolKind::Execute,
        _ => acp::ToolKind::Other,
    }
}

fn tool_title(item: &Value) -> String {
    match item.get("type").and_then(Value::as_str) {
        Some("commandExecution") => item
            .get("command")
            .and_then(Value::as_str)
            .map(|command| format!("Run `{command}`"))
            .unwrap_or_else(|| "Run command".to_owned()),
        Some("fileChange") => "Apply file changes".to_owned(),
        Some("mcpToolCall") => {
            let server = item.get("server").and_then(Value::as_str).unwrap_or("MCP");
            let tool = item.get("tool").and_then(Value::as_str).unwrap_or("tool");
            format!("{server}: {tool}")
        }
        Some("dynamicToolCall") => item
            .get("tool")
            .and_then(Value::as_str)
            .map(|tool| format!("Tool: {tool}"))
            .unwrap_or_else(|| "Dynamic tool".to_owned()),
        Some("collabAgentToolCall") => item
            .get("tool")
            .and_then(Value::as_str)
            .map(|tool| format!("Subagent: {tool}"))
            .unwrap_or_else(|| "Subagent".to_owned()),
        Some("webSearch") => item
            .get("query")
            .and_then(Value::as_str)
            .map(|query| format!("Search the web for `{query}`"))
            .unwrap_or_else(|| "Search the web".to_owned()),
        Some("imageGeneration") => "Image generation".to_owned(),
        _ => "Codex tool".to_owned(),
    }
}

fn tool_item_content(item: &Value) -> Option<String> {
    if let Some(output) = item.get("aggregatedOutput").and_then(Value::as_str)
        && !output.is_empty()
    {
        return Some(output.to_owned());
    }
    if item.get("type").and_then(Value::as_str) == Some("fileChange") {
        return Some(summarize_file_changes(item.get("changes")));
    }
    if let Some(error) = item.get("error")
        && !error.is_null()
    {
        return Some(format!("Error: {error}"));
    }
    None
}

fn summarize_file_changes(changes: Option<&Value>) -> String {
    let Some(changes) = changes.and_then(Value::as_array) else {
        return "File changes updated.".to_owned();
    };
    if changes.is_empty() {
        return "File changes updated.".to_owned();
    }
    let paths = changes
        .iter()
        .filter_map(|change| {
            change
                .get("path")
                .or_else(|| change.get("file"))
                .or_else(|| change.get("absPath"))
                .and_then(Value::as_str)
        })
        .take(10)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        format!("{} file change(s) updated.", changes.len())
    } else {
        format!(
            "Changed files:\n{}",
            paths
                .iter()
                .map(|path| format!("- {path}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

fn approval_markdown(params: &Value) -> String {
    let mut lines = Vec::new();
    if let Some(command) = params.get("command").and_then(Value::as_str) {
        lines.push(format!("Command: `{command}`"));
    }
    if let Some(cwd) = params.get("cwd").and_then(Value::as_str) {
        lines.push(format!("Directory: `{cwd}`"));
    }
    if lines.is_empty() {
        "Codex requested approval.".to_owned()
    } else {
        lines.join("\n")
    }
}

fn required_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("missing `{key}`"))
}

fn turn_key(session_id: &acp::SessionId, turn_id: &str) -> String {
    format!("{session_id}:{turn_id}")
}

fn paths_to_strings(paths: &PathList) -> Vec<String> {
    paths
        .ordered_paths()
        .map(|path| path.display().to_string())
        .collect()
}

fn native_available_commands() -> Vec<acp::AvailableCommand> {
    vec![
        acp::AvailableCommand::new("review", "Review my current changes and find issues").input(
            acp::AvailableCommandInput::Unstructured(acp::UnstructuredCommandInput::new(
                "optional custom review instructions",
            )),
        ),
        acp::AvailableCommand::new(
            "init",
            "create an AGENTS.md file with instructions for Codex",
        ),
        acp::AvailableCommand::new(
            "compact",
            "summarize conversation to prevent hitting the context limit",
        ),
        acp::AvailableCommand::new("goal", "Set, show, pause, resume, or clear the thread goal")
            .input(acp::AvailableCommandInput::Unstructured(
                acp::UnstructuredCommandInput::new("objective | pause | resume | clear"),
            )),
        acp::AvailableCommand::new("resume", "Resume a native Codex thread"),
        acp::AvailableCommand::new("fork", "Fork a native Codex thread"),
        acp::AvailableCommand::new("history", "Show native Codex thread history"),
        acp::AvailableCommand::new("model", "Show or change the Codex model"),
        acp::AvailableCommand::new("config", "Show or change Codex configuration"),
        acp::AvailableCommand::new("skills", "List Codex skills"),
        acp::AvailableCommand::new("plugins", "List Codex plugins"),
        acp::AvailableCommand::new("hooks", "List Codex hooks"),
        acp::AvailableCommand::new("mcp", "List Codex MCP servers"),
    ]
}

fn agent_session_info_from_thread_value(value: &Value) -> Option<AgentSessionInfo> {
    let id = value.get("id")?.as_str()?;
    let mut info = AgentSessionInfo::new(acp::SessionId::new(id.to_owned()));
    info.title = value
        .get("name")
        .and_then(Value::as_str)
        .map(SharedString::from);
    info.work_dirs = value
        .get("cwd")
        .and_then(Value::as_str)
        .map(|cwd| PathList::new(&[PathBuf::from(cwd)]));
    info.updated_at = value
        .get("updatedAt")
        .and_then(Value::as_i64)
        .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single());
    info.created_at = value
        .get("createdAt")
        .and_then(Value::as_i64)
        .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single());
    Some(info)
}

fn parse_json_rpc_line(line: &str) -> Result<ParsedJsonRpcMessage> {
    let value: Value = serde_json::from_str(line).context("invalid JSON")?;
    let object = value
        .as_object()
        .context("JSON-RPC message must be an object")?;
    if let Some(method) = object.get("method").and_then(Value::as_str) {
        let params = object.get("params").cloned().unwrap_or(Value::Null);
        if let Some(id) = object.get("id") {
            Ok(ParsedJsonRpcMessage::Request {
                id: id.clone(),
                method: method.to_owned(),
                params,
            })
        } else {
            Ok(ParsedJsonRpcMessage::Notification {
                method: method.to_owned(),
                params,
            })
        }
    } else {
        let id = object
            .get("id")
            .and_then(json_rpc_id_as_u64)
            .context("JSON-RPC response missing numeric id")?;
        if let Some(error) = object.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Codex app-server returned an error")
                .to_owned();
            Ok(ParsedJsonRpcMessage::Response {
                id,
                result: Err(JsonRpcFailure {
                    message,
                    data: error.get("data").cloned(),
                }),
            })
        } else {
            Ok(ParsedJsonRpcMessage::Response {
                id,
                result: Ok(object.get("result").cloned().unwrap_or(Value::Null)),
            })
        }
    }
}

fn json_rpc_id_as_u64(id: &Value) -> Option<u64> {
    id.as_u64()
        .or_else(|| id.as_str().and_then(|id| id.parse::<u64>().ok()))
}

fn drain_pending(
    pending: &Arc<Mutex<HashMap<u64, async_channel::Sender<Result<Value, JsonRpcFailure>>>>>,
    message: String,
) {
    let senders = pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .drain()
        .map(|(_, sender)| sender)
        .collect::<Vec<_>>();
    for sender in senders {
        sender
            .try_send(Err(JsonRpcFailure {
                message: message.clone(),
                data: None,
            }))
            .log_err();
    }
}

fn resolve_local_codex() -> Result<CodexExecutable> {
    let configured_path = env::var_os(ZED_CODEX_EXECUTABLE_ENV).map(PathBuf::from);
    let path = resolve_codex_path(configured_path, env::var_os("PATH"))?;
    validate_codex_executable(path)
}

fn native_codex_disabled() -> bool {
    env::var(ZED_CODEX_NATIVE_ENV)
        .map(|value| matches!(value.as_str(), "0" | "false" | "off" | "disabled"))
        .unwrap_or(false)
}

fn resolve_codex_path(
    configured_path: Option<PathBuf>,
    path_env: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(path) = configured_path {
        return Ok(path);
    }
    find_on_path("codex", path_env.as_deref()).context("could not find `codex` on PATH")
}

fn find_on_path(binary: &str, path_env: Option<&std::ffi::OsStr>) -> Option<PathBuf> {
    let path_env = path_env?;
    for directory in env::split_paths(path_env) {
        let candidate = directory.join(binary);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let candidate = directory.join(format!("{binary}.exe"));
            if is_executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

#[expect(
    clippy::disallowed_methods,
    reason = "Codex discovery is a short startup probe before the app-server connection exists"
)]
fn validate_codex_executable(path: PathBuf) -> Result<CodexExecutable> {
    let version_output = Command::new(&path)
        .arg("--version")
        .output()
        .with_context(|| format!("failed to run `{}` --version", path.display()))?;
    if !version_output.status.success() {
        bail!(
            "`{}` --version exited with {}",
            path.display(),
            version_output.status
        );
    }
    let version_output = String::from_utf8_lossy(&version_output.stdout)
        .trim()
        .to_owned();
    let version = parse_codex_version(&version_output)
        .with_context(|| format!("failed to parse Codex version from `{version_output}`"))?;
    if version < MINIMUM_CODEX_VERSION {
        bail!(
            "Codex {} is not supported; need at least {}",
            version,
            MINIMUM_CODEX_VERSION
        );
    }

    let help_output = Command::new(&path)
        .arg("app-server")
        .arg("--help")
        .output()
        .with_context(|| format!("failed to run `{}` app-server --help", path.display()))?;
    let help_text = format!(
        "{}{}",
        String::from_utf8_lossy(&help_output.stdout),
        String::from_utf8_lossy(&help_output.stderr)
    );
    if !help_output.status.success() || !help_text.contains("--stdio") {
        bail!(
            "`{}` does not expose `codex app-server --stdio`",
            path.display()
        );
    }

    Ok(CodexExecutable { path })
}

fn parse_codex_version(output: &str) -> Result<CodexVersion> {
    let version_text = output
        .split_whitespace()
        .find(|part| {
            part.chars()
                .next()
                .is_some_and(|character| character.is_ascii_digit())
        })
        .context("missing version number")?;
    let mut parts = version_text.split('.');
    let major = parts
        .next()
        .context("missing major version")?
        .parse()
        .context("invalid major version")?;
    let minor = parts
        .next()
        .context("missing minor version")?
        .parse()
        .context("invalid minor version")?;
    let patch_text = parts.next().context("missing patch version")?;
    let patch_digits = patch_text
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    let patch = patch_digits.parse().context("invalid patch version")?;
    Ok(CodexVersion {
        major,
        minor,
        patch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_codex_cli_version() {
        let version = parse_codex_version("codex-cli 0.142.3").expect("version should parse");
        assert_eq!(
            version,
            CodexVersion {
                major: 0,
                minor: 142,
                patch: 3,
            }
        );
    }

    #[test]
    fn parses_json_rpc_message_shapes() {
        let response = parse_json_rpc_line(r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#)
            .expect("response should parse");
        assert!(matches!(
            response,
            ParsedJsonRpcMessage::Response {
                id: 7,
                result: Ok(_)
            }
        ));

        let notification = parse_json_rpc_line(
            r#"{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"threadId":"t","delta":"hi"}}"#,
        )
        .expect("notification should parse");
        assert!(matches!(
            notification,
            ParsedJsonRpcMessage::Notification { method, .. } if method == "item/agentMessage/delta"
        ));

        let request = parse_json_rpc_line(
            r#"{"jsonrpc":"2.0","id":"9","method":"currentTime/read","params":{"threadId":"t"}}"#,
        )
        .expect("request should parse");
        assert!(matches!(
            request,
            ParsedJsonRpcMessage::Request { method, .. } if method == "currentTime/read"
        ));
    }

    #[test]
    fn maps_agent_delta_to_acp_update() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/agentMessage/delta",
            &json!({"threadId":"thread-1","turnId":"turn-1","itemId":"item-1","delta":"hello"}),
            &mut tool_outputs,
        );
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0.to_string(), "thread-1");
        match &updates[0].1 {
            acp::SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
                acp::ContentBlock::Text(text) => assert_eq!(text.text, "hello"),
                other => panic!("unexpected content block: {other:?}"),
            },
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_token_usage_to_zed_meta() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "thread/tokenUsage/updated",
            &json!({
                "threadId":"thread-1",
                "turnId":"turn-1",
                "tokenUsage": {
                    "modelContextWindow": 1000,
                    "total": {
                        "inputTokens": 10,
                        "cachedInputTokens": 3,
                        "outputTokens": 5,
                        "reasoningOutputTokens": 2,
                        "totalTokens": 18
                    },
                    "last": {
                        "inputTokens": 10,
                        "cachedInputTokens": 3,
                        "outputTokens": 5,
                        "reasoningOutputTokens": 2,
                        "totalTokens": 18
                    }
                }
            }),
            &mut tool_outputs,
        );
        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::UsageUpdate(update) => {
                assert_eq!(update.used, 18);
                assert_eq!(update.size, 1000);
                let usage = acp_thread::session_token_usage_from_meta(&update.meta)
                    .expect("token usage meta should exist");
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 5);
                assert_eq!(usage.cache_read_input_tokens, 3);
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn parses_native_slash_commands() {
        let review = native_slash_command(&[json!({
            "type": "text",
            "text": "/review check edge cases"
        })])
        .expect("slash parsing should succeed")
        .expect("review should be native");
        match review {
            NativeSlashCommand::Review { target } => {
                assert_eq!(target.get("type").and_then(Value::as_str), Some("custom"));
                assert_eq!(
                    target.get("instructions").and_then(Value::as_str),
                    Some("check edge cases")
                );
            }
            _ => panic!("unexpected slash command"),
        }

        let compact = native_slash_command(&[json!({
            "type": "text",
            "text": "/compact"
        })])
        .expect("slash parsing should succeed")
        .expect("compact should be native");
        assert!(matches!(compact, NativeSlashCommand::Compact));
    }

    #[test]
    fn maps_collab_tool_call_to_subagent_meta() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/completed",
            &json!({
                "threadId": "thread-1",
                "item": {
                    "id": "collab-1",
                    "type": "collabAgentToolCall",
                    "tool": "spawnAgent",
                    "status": "completed",
                    "receiverThreadIds": ["subagent-1"]
                }
            }),
            &mut tool_outputs,
        );
        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                let meta = update.meta.as_ref().expect("subagent meta should exist");
                assert_eq!(
                    meta.get(acp_thread::TOOL_NAME_META_KEY),
                    Some(&json!(ZED_SPAWN_AGENT_TOOL_NAME))
                );
                assert_eq!(
                    meta.get(acp_thread::SUBAGENT_SESSION_INFO_META_KEY)
                        .and_then(|value| value.get("session_id"))
                        .and_then(Value::as_str),
                    Some("subagent-1")
                );
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_goal_update_to_visible_message() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "thread/goal/updated",
            &json!({
                "threadId": "thread-1",
                "goal": {
                    "objective": "finish native Codex",
                    "status": "active",
                    "tokensUsed": 12
                }
            }),
            &mut tool_outputs,
        );
        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
                acp::ContentBlock::Text(text) => {
                    assert!(text.text.contains("Goal updated (active):"));
                    assert!(text.text.contains("finish native Codex"));
                }
                other => panic!("unexpected content block: {other:?}"),
            },
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn formats_missing_goal_status() {
        assert_eq!(
            goal_message(&json!({
                "objective": "finish native Codex",
                "status": "paused",
                "tokensUsed": 10,
                "tokenBudget": 50
            })),
            "Goal updated (paused): finish native Codex\nTokens used: 10\nToken budget: 50"
        );
    }

    #[gpui::test]
    async fn waits_for_turn_started_notification(cx: &mut gpui::TestAppContext) {
        let pending_turn_starts = Rc::new(RefCell::new(HashMap::new()));
        let session_id = acp::SessionId::new("thread-1");
        let turn_start = watch_next_native_turn_start(&session_id, &pending_turn_starts);
        start_turn(
            &json!({
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1"
                }
            }),
            &pending_turn_starts,
        );
        assert_eq!(turn_start.recv().await.ok().as_deref(), Some("turn-1"));
        assert!(!pending_turn_starts.borrow().contains_key(&session_id));

        let turn_start = watch_next_native_turn_start(&session_id, &pending_turn_starts);
        let turn_id = cx
            .update(|cx| {
                let mut cx = cx.to_async();
                async move { wait_for_native_turn_start_with_timeout(turn_start, &mut cx).await }
            })
            .await;
        assert_eq!(turn_id, None);
    }

    #[test]
    fn resolves_codex_path_from_explicit_setting() {
        let path = PathBuf::from("/tmp/custom-codex");
        let resolved = resolve_codex_path(Some(path.clone()), None).expect("path should resolve");
        assert_eq!(resolved, path);
    }
}

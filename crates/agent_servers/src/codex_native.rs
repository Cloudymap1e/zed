use action_log::ActionLog;
use agent_client_protocol::schema as acp;
use agent_thread::{
    AgentConnection, AgentModelInfo, AgentModelList, AgentModelSelector, AgentSessionConfigOptions,
    AgentSessionInfo, AgentSessionList, AgentSessionListRequest, AgentSessionListResponse,
    AgentThread, PermissionOptions, SessionListUpdate, TerminalProviderEvent, UserMessageId,
};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{TimeZone as _, Utc};
use diffy::Patch;
use futures::{FutureExt as _, future::BoxFuture};
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
use terminal::TerminalBuilder;
use terminal::terminal_settings::{AlternateScroll, CursorShape};
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
const CODEX_NATIVE_COLLAB_META_KEY: &str = "codex_native_collab";
const NATIVE_TURN_START_TIMEOUT: Duration = Duration::from_secs(5);
const CODEX_NATIVE_COMMAND_TERMINAL_PREFIX: &str = "codex-native-command";

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
    inner: Arc<dyn CodexAppServerRpc>,
}

trait CodexAppServerRpc {
    fn send_request(&self, method: &str, params: Value) -> BoxFuture<'static, Result<Value>>;
    fn send_response(&self, id: Value, result: Value) -> Result<()>;
    fn send_error_response(&self, id: Value, message: String) -> Result<()>;
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
    thread: WeakEntity<AgentThread>,
    active_turn_id: Option<String>,
}

#[derive(Default)]
struct CodexNativeState {
    config_options: HashMap<acp::SessionId, Rc<CodexNativeConfigOptionsState>>,
}

#[derive(Debug, PartialEq)]
enum NativeSlashCommand {
    Review { target: Value },
    Compact,
    Goal(GoalCommand),
    Model,
    Config,
    Skills,
    Plugins,
    Hooks,
    Mcp,
    Fork,
    History,
}

#[derive(Debug, PartialEq)]
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

    fn supports_delete(&self) -> bool {
        true
    }

    fn supports_archive(&self) -> bool {
        true
    }

    fn archive_session(&self, session_id: &acp::SessionId, cx: &mut App) -> Task<Result<()>> {
        let client = self.client.clone();
        let session_id = session_id.clone();
        let updates_tx = self.updates_tx.clone();
        cx.foreground_executor().spawn(async move {
            client
                .send_request(
                    "thread/archive",
                    json!({ "threadId": session_id.to_string() }),
                )
                .await
                .context("failed to archive native Codex thread")?;
            updates_tx.try_send(SessionListUpdate::Refresh).log_err();
            Ok(())
        })
    }

    fn unarchive_session(&self, session_id: &acp::SessionId, cx: &mut App) -> Task<Result<()>> {
        let client = self.client.clone();
        let session_id = session_id.clone();
        let updates_tx = self.updates_tx.clone();
        cx.foreground_executor().spawn(async move {
            client
                .send_request(
                    "thread/unarchive",
                    json!({ "threadId": session_id.to_string() }),
                )
                .await
                .context("failed to unarchive native Codex thread")?;
            updates_tx.try_send(SessionListUpdate::Refresh).log_err();
            Ok(())
        })
    }

    fn delete_session(&self, session_id: &acp::SessionId, cx: &mut App) -> Task<Result<()>> {
        let client = self.client.clone();
        let session_id = session_id.clone();
        let updates_tx = self.updates_tx.clone();
        cx.foreground_executor().spawn(async move {
            client
                .send_request(
                    "thread/delete",
                    json!({ "threadId": session_id.to_string() }),
                )
                .await
                .context("failed to delete native Codex thread")?;
            updates_tx.try_send(SessionListUpdate::Refresh).log_err();
            Ok(())
        })
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

struct CodexNativeModelSelector {
    client: CodexAppServerClient,
    session_id: acp::SessionId,
    selected_models: Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    default_model: Option<acp::ModelId>,
    watch_tx: Rc<RefCell<watch::Sender<()>>>,
    watch_rx: watch::Receiver<()>,
}

struct CodexNativeConfigOptionsState {
    options: RefCell<Vec<acp::SessionConfigOption>>,
    watch_tx: Rc<RefCell<watch::Sender<()>>>,
    watch_rx: watch::Receiver<()>,
}

struct CodexNativeConfigOptions {
    client: CodexAppServerClient,
    state: Rc<CodexNativeConfigOptionsState>,
    refresh_task: Rc<RefCell<Option<Task<()>>>>,
}

impl AgentModelSelector for CodexNativeModelSelector {
    fn list_models(&self, cx: &mut App) -> Task<Result<AgentModelList>> {
        let client = self.client.clone();
        cx.foreground_executor().spawn(async move {
            let response = client
                .send_request("model/list", model_list_params())
                .await?;
            Ok(AgentModelList::Flat(agent_model_infos_from_response(
                &response,
            )))
        })
    }

    fn select_model(&self, model_id: acp::ModelId, _cx: &mut App) -> Task<Result<()>> {
        self.selected_models
            .borrow_mut()
            .insert(self.session_id.clone(), model_id.clone());
        self.watch_tx.borrow_mut().send(()).log_err();
        Task::ready(Ok(()))
    }

    fn selected_model(&self, cx: &mut App) -> Task<Result<AgentModelInfo>> {
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let selected_models = self.selected_models.clone();
        let default_model = self.default_model.clone();
        cx.foreground_executor().spawn(async move {
            let response = client
                .send_request("model/list", model_list_params())
                .await?;
            let models = agent_model_infos_from_response(&response);
            let selected_model = selected_models.borrow().get(&session_id).cloned();
            selected_agent_model(&models, selected_model.as_ref(), default_model.as_ref())
                .cloned()
                .or_else(|| {
                    selected_model
                        .or(default_model)
                        .map(|model_id| fallback_agent_model_info(model_id))
                })
                .context("Codex model list was empty")
        })
    }

    fn watch(&self, _cx: &mut App) -> Option<watch::Receiver<()>> {
        Some(self.watch_rx.clone())
    }
}

impl CodexNativeConfigOptionsState {
    fn new(options: Vec<acp::SessionConfigOption>) -> Rc<Self> {
        let (watch_tx, watch_rx) = watch::channel(());
        Rc::new(Self {
            options: RefCell::new(options),
            watch_tx: Rc::new(RefCell::new(watch_tx)),
            watch_rx,
        })
    }
}

impl AgentSessionConfigOptions for CodexNativeConfigOptions {
    fn config_options(&self) -> Vec<acp::SessionConfigOption> {
        self.state.options.borrow().clone()
    }

    fn set_config_option(
        &self,
        config_id: acp::SessionConfigId,
        value: acp::SessionConfigValueId,
        cx: &mut App,
    ) -> Task<Result<Vec<acp::SessionConfigOption>>> {
        let client = self.client.clone();
        let state = self.state.clone();
        cx.foreground_executor().spawn(async move {
            client
                .send_request(
                    "config/value/write",
                    json!({
                        "keyPath": config_id.to_string(),
                        "value": config_value_from_config_id(&config_id, value.to_string()),
                        "mergeStrategy": "upsert",
                    }),
                )
                .await
                .with_context(|| {
                    format!("failed to write native Codex config option `{config_id}`")
                })?;
            let response = client
                .send_request("config/read", json!({ "includeLayers": false }))
                .await
                .context("failed to refresh native Codex configuration")?;
            update_config_options_from_read_response(&state, &response);
            let options = state.options.borrow().clone();
            state.watch_tx.borrow_mut().send(()).log_err();
            Ok(options)
        })
    }

    fn watch(&self, cx: &mut App) -> Option<watch::Receiver<()>> {
        if self.refresh_task.borrow().is_none() {
            self.start_refresh(cx);
        }
        Some(self.state.watch_rx.clone())
    }
}

impl CodexNativeConfigOptions {
    fn start_refresh(&self, cx: &mut App) {
        let client = self.client.clone();
        let state = self.state.clone();
        let task = cx.foreground_executor().spawn(async move {
            match client
                .send_request("config/read", json!({ "includeLayers": false }))
                .await
            {
                Ok(response) => {
                    update_config_options_from_read_response(&state, &response);
                    state.watch_tx.borrow_mut().send(()).log_err();
                }
                Err(error) => {
                    log::warn!("failed to refresh native Codex configuration: {error:#}");
                }
            }
        });
        *self.refresh_task.borrow_mut() = Some(task);
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
    state: Rc<RefCell<CodexNativeState>>,
    selected_models: Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    model_watch_tx: Rc<RefCell<watch::Sender<()>>>,
    model_watch_rx: watch::Receiver<()>,
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
    let state = Rc::new(RefCell::new(CodexNativeState::default()));
    let selected_models = Rc::new(RefCell::new(HashMap::new()));
    let (model_watch_tx, model_watch_rx) = watch::channel(());
    let model_watch_tx = Rc::new(RefCell::new(model_watch_tx));

    let dispatch_task = cx.spawn({
        let client = client.clone();
        let sessions = sessions.clone();
        let pending_turn_starts = pending_turn_starts.clone();
        let pending_turns = pending_turns.clone();
        let completed_turns = completed_turns.clone();
        let session_list = session_list.clone();
        let state = state.clone();
        let selected_models = selected_models.clone();
        let model_watch_tx = model_watch_tx.clone();
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
                    &state,
                    &selected_models,
                    &model_watch_tx,
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
        state,
        selected_models,
        model_watch_tx,
        model_watch_rx,
        _dispatch_task: dispatch_task,
    }) as Rc<dyn AgentConnection>)
}

impl CodexNativeConnection {
    fn open_existing_session(
        self: Rc<Self>,
        session_id: acp::SessionId,
        project: Entity<Project>,
        work_dirs: PathList,
        title: Option<SharedString>,
        load_history: bool,
        cx: &mut App,
    ) -> Task<Result<Entity<AgentThread>>> {
        let Some(cwd) = work_dirs.ordered_paths().next().cloned() else {
            return Task::ready(Err(anyhow!("Working directory cannot be empty")));
        };

        cx.spawn(async move |cx| {
            let mut params = serde_json::Map::new();
            params.insert("threadId".into(), json!(session_id.to_string()));
            params.insert("cwd".into(), json!(cwd));
            if let Some(default_model) = &self.default_model {
                params.insert("model".into(), json!(default_model.to_string()));
            }

            let response = self
                .client
                .send_request("thread/resume", Value::Object(params))
                .await
                .context("failed to resume native Codex thread")?;
            remember_selected_model_from_thread(
                &session_id,
                &response,
                &self.selected_models,
                &self.model_watch_tx,
            );
            let title = title.or_else(|| {
                response
                    .get("thread")
                    .and_then(|thread| thread.get("name"))
                    .and_then(Value::as_str)
                    .map(SharedString::from)
            });
            let read_response = if load_history {
                Some(
                    self.client
                        .send_request(
                            "thread/read",
                            json!({
                                "threadId": session_id.to_string(),
                                "includeTurns": true,
                            }),
                        )
                        .await
                        .context("failed to read native Codex thread history")?,
                )
            } else {
                None
            };

            let action_log = cx.new(|_| ActionLog::new(project.clone()));
            let thread = cx.new(|cx| {
                AgentThread::new(
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

            let history_updates = read_response
                .as_ref()
                .map(|response| history_updates_from_thread_read(&session_id, response))
                .unwrap_or_default();
            thread
                .update(cx, |thread, cx| {
                    for update in history_updates {
                        if let Err(error) = register_native_terminal_from_update_sync(thread, &update, cx) {
                            log::error!(
                                "Failed to register native Codex history terminal for {:?}: {error:?}",
                                session_id
                            );
                        }
                        thread.handle_session_update(update.clone(), cx)?;
                        stream_native_terminal_update_sync(thread, &update, cx);
                    }
                    thread.handle_session_update(
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(native_available_commands()),
                        ),
                        cx,
                    )
                })
                .context("failed to install native Codex history and command list")?;

            Ok(thread)
        })
    }
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
    ) -> Task<Result<Entity<AgentThread>>> {
        let Some(cwd) = work_dirs.ordered_paths().next().cloned() else {
            return Task::ready(Err(anyhow!("Working directory cannot be empty")));
        };

        cx.spawn(async move |cx| {
            let mut params = serde_json::Map::new();
            params.insert("cwd".into(), json!(cwd));
            params.insert("threadSource".into(), json!("appServer"));
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
            remember_selected_model_from_thread(
                &session_id,
                &response,
                &self.selected_models,
                &self.model_watch_tx,
            );
            let title = response
                .get("thread")
                .and_then(|thread| thread.get("name"))
                .and_then(Value::as_str)
                .map(SharedString::from);

            let action_log = cx.new(|_| ActionLog::new(project.clone()));
            let thread = cx.new(|cx| {
                AgentThread::new(
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

    fn supports_load_session(&self) -> bool {
        true
    }

    fn load_session(
        self: Rc<Self>,
        session_id: acp::SessionId,
        project: Entity<Project>,
        work_dirs: PathList,
        title: Option<SharedString>,
        cx: &mut App,
    ) -> Task<Result<Entity<AgentThread>>> {
        self.open_existing_session(session_id, project, work_dirs, title, true, cx)
    }

    fn resume_session(
        self: Rc<Self>,
        session_id: acp::SessionId,
        project: Entity<Project>,
        work_dirs: PathList,
        title: Option<SharedString>,
        cx: &mut App,
    ) -> Task<Result<Entity<AgentThread>>> {
        self.open_existing_session(session_id, project, work_dirs, title, false, cx)
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
        let selected_model = self
            .selected_models
            .borrow()
            .get(&session_id)
            .cloned()
            .or_else(|| self.default_model.clone());
        let session_list = self.session_list.clone();

        cx.spawn(async move |cx| {
            let input = prompt_blocks_to_codex_input(params.prompt)?;
            let Some(command) = native_slash_command(&input)? else {
                let mut turn_params = serde_json::Map::new();
                turn_params.insert("threadId".into(), json!(session_id.to_string()));
                turn_params.insert("input".into(), json!(input));
                if let Some(model) = selected_model {
                    turn_params.insert("model".into(), json!(model.to_string()));
                }
                let response = client
                    .send_request("turn/start", Value::Object(turn_params))
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
                &session_list,
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

    fn session_config_options(
        &self,
        session_id: &acp::SessionId,
        _cx: &App,
    ) -> Option<Rc<dyn AgentSessionConfigOptions>> {
        let state = self
            .state
            .borrow_mut()
            .config_options
            .entry(session_id.clone())
            .or_insert_with(|| {
                CodexNativeConfigOptionsState::new(default_config_options_for_session())
            })
            .clone();
        Some(Rc::new(CodexNativeConfigOptions {
            client: self.client.clone(),
            state,
            refresh_task: Rc::new(RefCell::new(None)),
        }))
    }

    fn model_selector(&self, session_id: &acp::SessionId) -> Option<Rc<dyn AgentModelSelector>> {
        Some(Rc::new(CodexNativeModelSelector {
            client: self.client.clone(),
            session_id: session_id.clone(),
            selected_models: self.selected_models.clone(),
            default_model: self.default_model.clone(),
            watch_tx: self.model_watch_tx.clone(),
            watch_rx: self.model_watch_rx.clone(),
        }))
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
}

impl CodexAppServerRpc for CodexAppServerTransport {
    fn send_request(&self, method: &str, params: Value) -> BoxFuture<'static, Result<Value>> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (response_tx, response_rx) = async_channel::bounded(1);
        self.pending
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

        if let Err(error) = self.outbound_tx.send(line) {
            self.pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&id);
            let error = anyhow!("failed to send Codex request `{method}`: {error}");
            return async move { Err(error) }.boxed();
        }

        async move {
            response_rx
                .recv()
                .await
                .context("Codex app-server response channel closed")?
                .map_err(anyhow::Error::from)
        }
        .boxed()
    }

    fn send_response(&self, id: Value, result: Value) -> Result<()> {
        let line = json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        })
        .to_string();
        self.outbound_tx
            .send(line)
            .context("failed to send Codex app-server response")
    }

    fn send_error_response(&self, id: Value, message: String) -> Result<()> {
        let line = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": message,
            },
        })
        .to_string();
        self.outbound_tx
            .send(line)
            .context("failed to send Codex app-server error response")
    }
}

impl CodexAppServerClient {
    fn send_request(&self, method: &str, params: Value) -> BoxFuture<'static, Result<Value>> {
        self.inner.send_request(method, params)
    }

    fn send_response(&self, id: Value, result: Value) -> Result<()> {
        self.inner.send_response(id, result)
    }

    fn send_error_response(&self, id: Value, message: impl Into<String>) -> Result<()> {
        self.inner.send_error_response(id, message.into())
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
    state: &Rc<RefCell<CodexNativeState>>,
    selected_models: &Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    model_watch_tx: &Rc<RefCell<watch::Sender<()>>>,
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
                state,
                selected_models,
                model_watch_tx,
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
    state: &Rc<RefCell<CodexNativeState>>,
    selected_models: &Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    model_watch_tx: &Rc<RefCell<watch::Sender<()>>>,
    cx: &mut AsyncApp,
) {
    if method == "turn/started" {
        start_turn(params, pending_turn_starts);
    }

    if method == "turn/completed" {
        complete_turn(params, sessions, pending_turns, completed_turns);
    }

    if matches!(
        method,
        "thread/archived" | "thread/unarchived" | "thread/deleted"
    ) {
        session_list.notify_refresh();
    }

    refresh_config_options_from_notification(
        method,
        params,
        state,
        selected_models,
        model_watch_tx,
    );

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

        register_native_terminal_from_update(&thread, &update, cx);
        let update_for_thread = update.clone();
        thread
            .update(cx, |thread, cx| {
                thread.handle_session_update(update_for_thread, cx)
            })
            .log_err();
        stream_native_terminal_update(&thread, &update, cx);
    }
}

fn register_native_terminal_from_update(
    thread: &WeakEntity<AgentThread>,
    update: &acp::SessionUpdate,
    cx: &mut AsyncApp,
) {
    let Some((terminal_id, label, cwd)) = native_terminal_info_from_update(update) else {
        return;
    };

    thread
        .update(cx, |thread, cx| {
            register_native_terminal(thread, terminal_id, label, cwd, cx)
        })
        .log_err();
}

fn register_native_terminal_from_update_sync(
    thread: &mut AgentThread,
    update: &acp::SessionUpdate,
    cx: &mut gpui::Context<AgentThread>,
) -> Result<()> {
    let Some((terminal_id, label, cwd)) = native_terminal_info_from_update(update) else {
        return Ok(());
    };
    register_native_terminal(thread, terminal_id, label, cwd, cx)
}

fn native_terminal_info_from_update(
    update: &acp::SessionUpdate,
) -> Option<(acp::TerminalId, String, Option<PathBuf>)> {
    let acp::SessionUpdate::ToolCall(tool_call) = update else {
        return None;
    };
    let Some(terminal_info) = tool_call
        .meta
        .as_ref()
        .and_then(|meta| meta.get("terminal_info"))
    else {
        return None;
    };
    let Some(terminal_id) = terminal_info
        .get("terminal_id")
        .and_then(Value::as_str)
        .map(acp::TerminalId::new)
    else {
        return None;
    };
    let cwd = terminal_info
        .get("cwd")
        .and_then(|value| value.as_str().map(PathBuf::from));
    let label = tool_call.title.clone();
    Some((terminal_id, label, cwd))
}

fn register_native_terminal(
    thread: &mut AgentThread,
    terminal_id: acp::TerminalId,
    label: String,
    cwd: Option<PathBuf>,
    cx: &mut gpui::Context<AgentThread>,
) -> Result<()> {
    let builder = TerminalBuilder::new_display_only(
        CursorShape::default(),
        AlternateScroll::On,
        None,
        0,
        cx.background_executor(),
        thread.project().read(cx).path_style(cx),
    )?;
    let terminal = cx.new(|cx| builder.subscribe(cx));
    thread.on_terminal_provider_event(
        TerminalProviderEvent::Created {
            terminal_id,
            label,
            cwd,
            output_byte_limit: None,
            terminal,
        },
        cx,
    );
    Ok(())
}

fn stream_native_terminal_update(
    thread: &WeakEntity<AgentThread>,
    update: &acp::SessionUpdate,
    cx: &mut AsyncApp,
) {
    let acp::SessionUpdate::ToolCallUpdate(tool_call_update) = update else {
        return;
    };
    let Some(meta) = tool_call_update.meta.as_ref() else {
        return;
    };

    if let Some(terminal_output) = meta.get("terminal_output")
        && let Some(terminal_id) = terminal_output
            .get("terminal_id")
            .and_then(Value::as_str)
            .map(acp::TerminalId::new)
        && let Some(data) = terminal_output.get("data").and_then(Value::as_str)
    {
        let data = data.as_bytes().to_vec();
        thread
            .update(cx, |thread, cx| {
                thread.on_terminal_provider_event(
                    TerminalProviderEvent::Output { terminal_id, data },
                    cx,
                );
            })
            .log_err();
    }

    if let Some(terminal_exit) = meta.get("terminal_exit")
        && let Some(terminal_id) = terminal_exit
            .get("terminal_id")
            .and_then(Value::as_str)
            .map(acp::TerminalId::new)
    {
        let status = acp::TerminalExitStatus::new()
            .exit_code(
                terminal_exit
                    .get("exit_code")
                    .and_then(Value::as_u64)
                    .map(|exit_code| exit_code as u32),
            )
            .signal(
                terminal_exit
                    .get("signal")
                    .and_then(|value| value.as_str().map(ToOwned::to_owned)),
            );

        thread
            .update(cx, |thread, cx| {
                thread.on_terminal_provider_event(
                    TerminalProviderEvent::Exit {
                        terminal_id,
                        status,
                    },
                    cx,
                );
            })
            .log_err();
    }
}

fn stream_native_terminal_update_sync(
    thread: &mut AgentThread,
    update: &acp::SessionUpdate,
    cx: &mut gpui::Context<AgentThread>,
) {
    let acp::SessionUpdate::ToolCallUpdate(tool_call_update) = update else {
        return;
    };
    let Some(meta) = tool_call_update.meta.as_ref() else {
        return;
    };

    if let Some(terminal_output) = meta.get("terminal_output")
        && let Some(terminal_id) = terminal_output
            .get("terminal_id")
            .and_then(Value::as_str)
            .map(acp::TerminalId::new)
        && let Some(data) = terminal_output.get("data").and_then(Value::as_str)
    {
        thread.on_terminal_provider_event(
            TerminalProviderEvent::Output {
                terminal_id,
                data: data.as_bytes().to_vec(),
            },
            cx,
        );
    }

    if let Some(terminal_exit) = meta.get("terminal_exit")
        && let Some(terminal_id) = terminal_exit
            .get("terminal_id")
            .and_then(Value::as_str)
            .map(acp::TerminalId::new)
    {
        let status = acp::TerminalExitStatus::new()
            .exit_code(
                terminal_exit
                    .get("exit_code")
                    .and_then(Value::as_u64)
                    .map(|exit_code| exit_code as u32),
            )
            .signal(
                terminal_exit
                    .get("signal")
                    .and_then(|value| value.as_str().map(ToOwned::to_owned)),
            );
        thread.on_terminal_provider_event(
            TerminalProviderEvent::Exit {
                terminal_id,
                status,
            },
            cx,
        );
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
        "item/tool/requestUserInput" => handle_user_input_request(params, sessions, cx).await,
        "mcpServer/elicitation/request" => {
            handle_mcp_elicitation_request(params, sessions, cx).await
        }
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
    let options = approval_permission_options(&params, allow_decision, deny_decision);
    let tool_call = acp::ToolCallUpdate::new(
        acp::ToolCallId::new(item_id),
        acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Execute)
            .status(acp::ToolCallStatus::Pending)
            .title(title)
            .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(content)),
            ))])
            .raw_input(params.clone()),
    );

    let authorization_task = thread
        .update(cx, |thread, cx| {
            thread.request_tool_call_authorization(tool_call, options, cx)
        })
        .context("failed to request native Codex approval")??;

    let outcome = authorization_task.await;
    let decision = match outcome {
        agent_thread::RequestPermissionOutcome::Selected(selected) => {
            approval_decision_from_selected(&params, &selected, allow_decision)
        }
        agent_thread::RequestPermissionOutcome::Cancelled => json!("cancel"),
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
        acp::PermissionOption::new(
            "allow-for-session",
            "Allow for session",
            acp::PermissionOptionKind::AllowAlways,
        ),
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
    let (permissions, scope) = match outcome {
        agent_thread::RequestPermissionOutcome::Selected(selected)
            if selected.option_kind == acp::PermissionOptionKind::AllowOnce
                || selected.option_kind == acp::PermissionOptionKind::AllowAlways =>
        {
            (
                params
                    .get("permissions")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                if selected.option_kind == acp::PermissionOptionKind::AllowAlways {
                    "session"
                } else {
                    "turn"
                },
            )
        }
        _ => (json!({}), "turn"),
    };

    Ok(json!({
        "permissions": permissions,
        "scope": scope,
    }))
}

fn approval_decision_from_selected(
    params: &Value,
    selected: &agent_thread::SelectedPermissionOutcome,
    allow_decision: &str,
) -> Value {
    let option_id = selected.option_id.to_string();
    if let Some(index) = option_id
        .strip_prefix("applyNetworkPolicyAmendment:")
        .and_then(|index| index.parse::<usize>().ok())
        && let Some(network_policy_amendment) = params
            .get("proposedNetworkPolicyAmendments")
            .and_then(Value::as_array)
            .and_then(|amendments| amendments.get(index))
            .cloned()
    {
        return json!({
            "applyNetworkPolicyAmendment": {
                "network_policy_amendment": network_policy_amendment
            }
        });
    }

    if selected.option_kind == acp::PermissionOptionKind::AllowAlways && allow_decision == "accept"
    {
        match option_id.as_str() {
            "acceptWithExecpolicyAmendment" => params
                .get("proposedExecpolicyAmendment")
                .cloned()
                .map(|execpolicy_amendment| {
                    json!({
                        "acceptWithExecpolicyAmendment": {
                            "execpolicy_amendment": execpolicy_amendment
                        }
                    })
                })
                .unwrap_or_else(|| json!("acceptForSession")),
            _ => json!("acceptForSession"),
        }
    } else {
        json!(option_id)
    }
}

fn approval_permission_options(
    params: &Value,
    allow_decision: &str,
    deny_decision: &str,
) -> PermissionOptions {
    let allow_once = acp::PermissionOption::new(
        allow_decision.to_owned(),
        "Allow",
        acp::PermissionOptionKind::AllowOnce,
    );
    let allow_session = acp::PermissionOption::new(
        "acceptForSession",
        "Allow for session",
        acp::PermissionOptionKind::AllowAlways,
    );
    let deny = acp::PermissionOption::new(
        deny_decision.to_owned(),
        "Deny",
        acp::PermissionOptionKind::RejectOnce,
    );

    if let Some(execpolicy_amendment) = params
        .get("proposedExecpolicyAmendment")
        .and_then(Value::as_array)
        && !execpolicy_amendment.is_empty()
    {
        return PermissionOptions::Flat(vec![
            allow_once,
            allow_session,
            acp::PermissionOption::new(
                "acceptWithExecpolicyAmendment",
                "Allow and remember this command pattern",
                acp::PermissionOptionKind::AllowAlways,
            ),
            deny,
            acp::PermissionOption::new("cancel", "Cancel", acp::PermissionOptionKind::RejectOnce),
        ]);
    }

    if let Some(network_options) = network_approval_options(params) {
        return PermissionOptions::Flat(
            [
                vec![allow_once, allow_session],
                network_options,
                vec![
                    deny,
                    acp::PermissionOption::new(
                        "cancel",
                        "Cancel",
                        acp::PermissionOptionKind::RejectOnce,
                    ),
                ],
            ]
            .into_iter()
            .flatten()
            .collect(),
        );
    }

    PermissionOptions::Flat(vec![
        allow_once,
        allow_session,
        deny,
        acp::PermissionOption::new("cancel", "Cancel", acp::PermissionOptionKind::RejectOnce),
    ])
}

fn network_approval_options(params: &Value) -> Option<Vec<acp::PermissionOption>> {
    let amendments = params
        .get("proposedNetworkPolicyAmendments")
        .and_then(Value::as_array)?;
    if amendments.is_empty() {
        return None;
    }

    let options = amendments
        .iter()
        .enumerate()
        .filter_map(|(index, amendment)| {
            let host = amendment.get("host").and_then(Value::as_str)?;
            let action = amendment
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or("allow");
            let (name, kind) = if action == "deny" {
                (
                    format!("Block `{host}` for future requests"),
                    acp::PermissionOptionKind::RejectAlways,
                )
            } else {
                (
                    format!("Allow `{host}` for future requests"),
                    acp::PermissionOptionKind::AllowAlways,
                )
            };
            Some(acp::PermissionOption::new(
                format!("applyNetworkPolicyAmendment:{index}"),
                name,
                kind,
            ))
        })
        .collect::<Vec<_>>();

    (!options.is_empty()).then_some(options)
}

async fn handle_user_input_request(
    params: Value,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    cx: &mut AsyncApp,
) -> Result<Value> {
    let thread_id = required_string(&params, "threadId")?;
    let item_id = required_string(&params, "itemId")?;
    let session_id = acp::SessionId::new(thread_id);
    let thread = sessions
        .borrow()
        .get(&session_id)
        .map(|session| session.thread.clone())
        .context("user input request for unknown Codex thread")?;

    let options = user_input_permission_options(&params);
    let tool_call = acp::ToolCallUpdate::new(
        acp::ToolCallId::new(item_id.clone()),
        acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Other)
            .status(acp::ToolCallStatus::Pending)
            .title("Answer Codex prompt")
            .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(user_input_markdown(&params))),
            ))])
            .raw_input(params.clone()),
    );

    let authorization_task = thread
        .update(cx, |thread, cx| {
            thread.request_tool_call_authorization(tool_call, PermissionOptions::Flat(options), cx)
        })
        .context("failed to request native Codex user input")??;

    let outcome = authorization_task.await;
    let (response, status, summary) = user_input_response_from_outcome(&params, outcome);
    update_permission_tool_call(thread, item_id, status, summary, cx).await?;
    Ok(response)
}

async fn handle_mcp_elicitation_request(
    params: Value,
    sessions: &Rc<RefCell<HashMap<acp::SessionId, CodexNativeSession>>>,
    cx: &mut AsyncApp,
) -> Result<Value> {
    let thread_id = required_string(&params, "threadId")?;
    let session_id = acp::SessionId::new(thread_id);
    let thread = sessions
        .borrow()
        .get(&session_id)
        .map(|session| session.thread.clone())
        .context("MCP elicitation request for unknown Codex thread")?;

    let tool_call_id = mcp_elicitation_tool_call_id(&params);
    let choices = mcp_elicitation_choices(&params);
    let options = choices
        .iter()
        .map(|choice| {
            acp::PermissionOption::new(choice.option_id.clone(), choice.label.clone(), choice.kind)
        })
        .collect();
    let tool_call = acp::ToolCallUpdate::new(
        acp::ToolCallId::new(tool_call_id.clone()),
        acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Other)
            .status(acp::ToolCallStatus::Pending)
            .title("MCP elicitation")
            .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(mcp_elicitation_markdown(&params))),
            ))])
            .raw_input(params.clone()),
    );

    let authorization_task = thread
        .update(cx, |thread, cx| {
            thread.request_tool_call_authorization(tool_call, PermissionOptions::Flat(options), cx)
        })
        .context("failed to request native Codex MCP elicitation")??;

    let outcome = authorization_task.await;
    let selected_option_id = match outcome {
        agent_thread::RequestPermissionOutcome::Selected(selected) => {
            Some(selected.option_id.to_string())
        }
        agent_thread::RequestPermissionOutcome::Cancelled => None,
    };
    let (response, status, summary, url_to_open) =
        mcp_elicitation_response(&params, &choices, selected_option_id.as_deref());
    if let Some(url) = url_to_open {
        cx.update(|cx| cx.open_url(&url));
    }
    update_permission_tool_call(thread, tool_call_id, status, summary, cx).await?;
    Ok(response)
}

async fn update_permission_tool_call(
    thread: WeakEntity<AgentThread>,
    item_id: String,
    status: acp::ToolCallStatus,
    summary: String,
    cx: &mut AsyncApp,
) -> Result<()> {
    let update = acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
        acp::ToolCallId::new(item_id),
        acp::ToolCallUpdateFields::new()
            .status(status)
            .content(vec![acp::ToolCallContent::Content(acp::Content::new(
                acp::ContentBlock::Text(acp::TextContent::new(summary)),
            ))]),
    ));
    thread
        .update(cx, |thread, cx| thread.handle_session_update(update, cx))?
        .map_err(anyhow::Error::from)
}

fn user_input_permission_options(params: &Value) -> Vec<acp::PermissionOption> {
    let mut options = user_input_selectable_options(params)
        .into_iter()
        .map(|(index, label)| {
            acp::PermissionOption::new(
                format!("answer:{index}"),
                label,
                acp::PermissionOptionKind::AllowOnce,
            )
        })
        .collect::<Vec<_>>();
    if options.is_empty() {
        options.push(acp::PermissionOption::new(
            "submit-empty",
            "Submit empty response",
            acp::PermissionOptionKind::AllowOnce,
        ));
    }
    options.push(acp::PermissionOption::new(
        "cancel",
        "Cancel",
        acp::PermissionOptionKind::RejectOnce,
    ));
    options
}

fn user_input_selectable_options(params: &Value) -> Vec<(usize, String)> {
    let questions = params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let Some(question) = questions.first() else {
        return Vec::new();
    };
    if questions.len() != 1
        || question
            .get("isSecret")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Vec::new();
    }
    question
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, option)| {
            let label = option.get("label").and_then(Value::as_str)?;
            Some((index, label.to_owned()))
        })
        .collect()
}

fn user_input_markdown(params: &Value) -> String {
    let mut lines = vec!["Codex requested input.".to_owned()];
    for question in params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(header) = question.get("header").and_then(Value::as_str)
            && !header.trim().is_empty()
        {
            lines.push(format!("\n**{}**", header.trim()));
        }
        if let Some(text) = question.get("question").and_then(Value::as_str)
            && !text.trim().is_empty()
        {
            lines.push(text.trim().to_owned());
        }
        if question
            .get("isSecret")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            lines.push(
                "_Secret/freeform entry is not supported in this Zed bridge yet._".to_owned(),
            );
        }
        let options = question
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|option| {
                let label = option.get("label").and_then(Value::as_str)?;
                let description = option
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                Some(if description.is_empty() {
                    format!("- {label}")
                } else {
                    format!("- {label}: {description}")
                })
            })
            .collect::<Vec<_>>();
        lines.extend(options);
    }
    lines.join("\n")
}

fn user_input_response_from_outcome(
    params: &Value,
    outcome: agent_thread::RequestPermissionOutcome,
) -> (Value, acp::ToolCallStatus, String) {
    let option_id = match outcome {
        agent_thread::RequestPermissionOutcome::Selected(selected)
            if selected.option_kind == acp::PermissionOptionKind::AllowOnce
                || selected.option_kind == acp::PermissionOptionKind::AllowAlways =>
        {
            selected.option_id.to_string()
        }
        _ => {
            return (
                json!({ "answers": {} }),
                acp::ToolCallStatus::Failed,
                "User input request cancelled.".to_owned(),
            );
        }
    };

    if let Some(index) = option_id
        .strip_prefix("answer:")
        .and_then(|index| index.parse::<usize>().ok())
        && let Some((question_id, label)) = user_input_answer_at(params, index)
    {
        let mut answers = serde_json::Map::new();
        answers.insert(question_id, json!({ "answers": [label] }));
        return (
            json!({ "answers": answers }),
            acp::ToolCallStatus::Completed,
            "Answered Codex prompt.".to_owned(),
        );
    }

    (
        empty_user_input_response(params),
        acp::ToolCallStatus::Completed,
        "Submitted empty response to Codex prompt.".to_owned(),
    )
}

fn user_input_answer_at(params: &Value, index: usize) -> Option<(String, String)> {
    let question = params
        .get("questions")
        .and_then(Value::as_array)
        .and_then(|questions| questions.first())?;
    let question_id = question.get("id").and_then(Value::as_str)?;
    let label = question
        .get("options")
        .and_then(Value::as_array)
        .and_then(|options| options.get(index))
        .and_then(|option| option.get("label"))
        .and_then(Value::as_str)?;
    Some((question_id.to_owned(), label.to_owned()))
}

fn empty_user_input_response(params: &Value) -> Value {
    let answers = params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|question| {
            let id = question.get("id").and_then(Value::as_str)?;
            Some((id.to_owned(), json!({ "answers": [] })))
        })
        .collect::<serde_json::Map<_, _>>();
    json!({ "answers": answers })
}

#[derive(Clone)]
struct McpElicitationChoice {
    option_id: String,
    label: String,
    kind: acp::PermissionOptionKind,
    action: McpElicitationChoiceAction,
}

#[derive(Clone)]
enum McpElicitationChoiceAction {
    OpenUrl(String),
    FormValue { property: String, value: Value },
    Decline,
    Cancel,
}

fn mcp_elicitation_choices(params: &Value) -> Vec<McpElicitationChoice> {
    if params.get("mode").and_then(Value::as_str) == Some("url") {
        let url = params
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        return vec![
            McpElicitationChoice {
                option_id: "open".to_owned(),
                label: "Open".to_owned(),
                kind: acp::PermissionOptionKind::AllowOnce,
                action: McpElicitationChoiceAction::OpenUrl(url),
            },
            McpElicitationChoice {
                option_id: "decline".to_owned(),
                label: "Decline".to_owned(),
                kind: acp::PermissionOptionKind::RejectOnce,
                action: McpElicitationChoiceAction::Decline,
            },
            McpElicitationChoice {
                option_id: "cancel".to_owned(),
                label: "Cancel".to_owned(),
                kind: acp::PermissionOptionKind::RejectOnce,
                action: McpElicitationChoiceAction::Cancel,
            },
        ];
    }

    let mut choices = mcp_simple_form_choices(params);
    choices.push(McpElicitationChoice {
        option_id: "decline".to_owned(),
        label: "Decline".to_owned(),
        kind: acp::PermissionOptionKind::RejectOnce,
        action: McpElicitationChoiceAction::Decline,
    });
    choices.push(McpElicitationChoice {
        option_id: "cancel".to_owned(),
        label: "Cancel".to_owned(),
        kind: acp::PermissionOptionKind::RejectOnce,
        action: McpElicitationChoiceAction::Cancel,
    });
    choices
}

fn mcp_simple_form_choices(params: &Value) -> Vec<McpElicitationChoice> {
    let Some((property_name, property_schema)) = single_mcp_form_property(params) else {
        return Vec::new();
    };
    if property_schema.get("type").and_then(Value::as_str) == Some("boolean") {
        return [true, false]
            .into_iter()
            .map(|value| McpElicitationChoice {
                option_id: format!("form:{property_name}:{value}"),
                label: value.to_string(),
                kind: acp::PermissionOptionKind::AllowOnce,
                action: McpElicitationChoiceAction::FormValue {
                    property: property_name.clone(),
                    value: json!(value),
                },
            })
            .collect();
    }

    property_schema
        .get("enum")
        .and_then(Value::as_array)
        .or_else(|| {
            property_schema
                .get("anyOf")
                .or_else(|| property_schema.get("oneOf"))
                .and_then(Value::as_array)
        })
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, value)| {
            let value = value
                .get("const")
                .or_else(|| value.get("enum").and_then(Value::as_array)?.first())
                .unwrap_or(value);
            Some(McpElicitationChoice {
                option_id: format!("form:{property_name}:{index}"),
                label: property_schema
                    .get("anyOf")
                    .or_else(|| property_schema.get("oneOf"))
                    .and_then(Value::as_array)
                    .and_then(|values| values.get(index))
                    .and_then(|entry| entry.get("title"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| value_to_inline_string(value)),
                kind: acp::PermissionOptionKind::AllowOnce,
                action: McpElicitationChoiceAction::FormValue {
                    property: property_name.clone(),
                    value: value.clone(),
                },
            })
        })
        .collect()
}

fn single_mcp_form_property(params: &Value) -> Option<(String, &Value)> {
    if params.get("mode").and_then(Value::as_str) != Some("form") {
        return None;
    }
    let properties = params
        .get("requestedSchema")
        .and_then(|schema| schema.get("properties"))
        .and_then(Value::as_object)?;
    if properties.len() != 1 {
        return None;
    }
    properties
        .iter()
        .next()
        .map(|(name, schema)| (name.clone(), schema))
}

fn mcp_elicitation_markdown(params: &Value) -> String {
    let server = params
        .get("serverName")
        .and_then(Value::as_str)
        .unwrap_or("MCP server");
    let message = params
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("MCP server requested input.");
    let mut lines = vec![format!("{server} requested input."), message.to_owned()];
    if let Some(url) = params.get("url").and_then(Value::as_str) {
        lines.push(format!("URL: {url}"));
    }
    if params.get("mode").and_then(Value::as_str) == Some("form") {
        let field_names = params
            .get("requestedSchema")
            .and_then(|schema| schema.get("properties"))
            .and_then(Value::as_object)
            .map(|properties| properties.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        if !field_names.is_empty() {
            lines.push(format!("Fields: {}", field_names.join(", ")));
        }
        if mcp_simple_form_choices(params).is_empty() {
            lines.push(
                "This form schema cannot be collected natively yet; decline or cancel to continue."
                    .to_owned(),
            );
        }
    }
    lines.join("\n")
}

fn mcp_elicitation_response(
    params: &Value,
    choices: &[McpElicitationChoice],
    selected_option_id: Option<&str>,
) -> (Value, acp::ToolCallStatus, String, Option<String>) {
    let Some(choice) = selected_option_id
        .and_then(|selected| choices.iter().find(|choice| choice.option_id == selected))
    else {
        return (
            json!({ "action": "cancel", "content": null }),
            acp::ToolCallStatus::Failed,
            "MCP elicitation cancelled.".to_owned(),
            None,
        );
    };

    match &choice.action {
        McpElicitationChoiceAction::OpenUrl(url) => (
            mcp_elicitation_response_value("accept", Value::Null, params),
            acp::ToolCallStatus::Completed,
            "Opened MCP elicitation URL.".to_owned(),
            Some(url.clone()),
        ),
        McpElicitationChoiceAction::FormValue { property, value } => {
            let mut content = serde_json::Map::new();
            content.insert(property.clone(), value.clone());
            (
                mcp_elicitation_response_value("accept", Value::Object(content), params),
                acp::ToolCallStatus::Completed,
                "Answered MCP elicitation.".to_owned(),
                None,
            )
        }
        McpElicitationChoiceAction::Decline => (
            json!({ "action": "decline", "content": null }),
            acp::ToolCallStatus::Failed,
            "MCP elicitation declined.".to_owned(),
            None,
        ),
        McpElicitationChoiceAction::Cancel => (
            json!({ "action": "cancel", "content": null }),
            acp::ToolCallStatus::Failed,
            "MCP elicitation cancelled.".to_owned(),
            None,
        ),
    }
}

fn mcp_elicitation_response_value(action: &str, content: Value, params: &Value) -> Value {
    let mut response = serde_json::Map::new();
    response.insert("action".to_owned(), json!(action));
    response.insert(
        "content".to_owned(),
        if content.is_null() {
            Value::Null
        } else {
            content
        },
    );
    if let Some(meta) = params.get("_meta") {
        response.insert("_meta".to_owned(), meta.clone());
    }
    Value::Object(response)
}

fn mcp_elicitation_tool_call_id(params: &Value) -> String {
    let server = params
        .get("serverName")
        .and_then(Value::as_str)
        .unwrap_or("mcp");
    let suffix = params
        .get("elicitationId")
        .or_else(|| params.get("turnId"))
        .and_then(Value::as_str)
        .unwrap_or("request");
    format!("mcp-elicitation:{server}:{suffix}")
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
        "model/rerouted" => model_rerouted_update(params),
        "model/verification" => model_verification_update(params),
        "model/safetyBuffering/updated" => model_safety_buffering_update(params),
        "configWarning" => config_warning_update(params),
        "warning" => warning_update(params),
        "guardianWarning" => guardian_warning_update(params),
        "error" => error_update(params),
        "mcpServer/startupStatus/updated" => mcp_startup_status_update(params),
        "item/commandExecution/outputDelta" => command_output_delta_update(params, tool_outputs),
        "item/commandExecution/terminalInteraction" => terminal_interaction_update(params),
        "item/fileChange/outputDelta" => tool_output_delta_update(params, tool_outputs),
        "item/fileChange/patchUpdated" => file_change_patch_update(params),
        "item/mcpToolCall/progress" => mcp_tool_progress_update(params),
        "item/started" => item_lifecycle_update(params, false, tool_outputs),
        "item/completed" => item_lifecycle_update(params, true, tool_outputs),
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

fn model_rerouted_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let from_model = params
        .get("fromModel")
        .and_then(Value::as_str)
        .unwrap_or("current model");
    let to_model = params
        .get("toModel")
        .and_then(Value::as_str)
        .unwrap_or("fallback model");
    let reason = params
        .get("reason")
        .map(value_to_inline_string)
        .unwrap_or_else(|| "unspecified".to_owned());
    text_message_update(
        thread_id,
        format!("Codex rerouted this turn from `{from_model}` to `{to_model}` ({reason})."),
    )
}

fn model_verification_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let count = params
        .get("verifications")
        .and_then(Value::as_array)
        .map(|verifications| verifications.len())
        .unwrap_or(0);
    if count == 0 {
        return Vec::new();
    }
    text_message_update(
        thread_id,
        format!("Codex is buffering model safety verification for {count} check(s)."),
    )
}

fn model_safety_buffering_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    if !params
        .get("showBufferingUi")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Vec::new();
    }
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("selected model");
    let faster_model = params.get("fasterModel").and_then(Value::as_str);
    let reasons = params
        .get("reasons")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .take(3)
        .collect::<Vec<_>>();
    let reason_suffix = if reasons.is_empty() {
        String::new()
    } else {
        format!(": {}", reasons.join(", "))
    };
    let faster_suffix = faster_model
        .map(|faster_model| format!(" A faster fallback may use `{faster_model}`."))
        .unwrap_or_default();
    text_message_update(
        thread_id,
        format!("Codex is buffering safety checks for `{model}`{reason_suffix}.{faster_suffix}"),
    )
}

fn config_warning_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let summary = params
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("Codex configuration warning");
    let details = params
        .get("details")
        .and_then(Value::as_str)
        .filter(|details| !details.trim().is_empty())
        .map(|details| format!("\n{details}"))
        .unwrap_or_default();
    text_message_update(
        thread_id,
        format!("Configuration warning: {summary}{details}"),
    )
}

fn warning_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let message = params
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Codex warning");
    text_message_update(thread_id, format!("Codex warning: {message}"))
}

fn guardian_warning_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let message = params
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Codex safety warning");
    text_message_update(thread_id, format!("Codex safety warning: {message}"))
}

fn error_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let error = params.get("error").unwrap_or(&Value::Null);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Codex error");
    let details = error
        .get("additionalDetails")
        .and_then(Value::as_str)
        .filter(|details| !details.trim().is_empty())
        .map(|details| format!("\n{details}"))
        .unwrap_or_default();
    let retry = if params
        .get("willRetry")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "\nCodex will retry."
    } else {
        ""
    };
    text_message_update(thread_id, format!("Codex error: {message}{details}{retry}"))
}

fn mcp_startup_status_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let status = params.get("status").and_then(Value::as_str);
    if !matches!(status, Some("failed" | "cancelled")) {
        return Vec::new();
    }
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("MCP server");
    let error = params
        .get("error")
        .and_then(Value::as_str)
        .filter(|error| !error.trim().is_empty())
        .map(|error| format!(": {error}"))
        .unwrap_or_default();
    text_message_update(
        thread_id,
        format!(
            "Codex MCP server `{name}` {}{error}.",
            status.unwrap_or("failed")
        ),
    )
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
    let meta = agent_thread::meta_with_session_token_usage(agent_thread::SessionTokenUsageMeta {
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

fn command_output_delta_update(
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

    let mut update = acp::ToolCallUpdate::new(
        item_id.to_owned(),
        acp::ToolCallUpdateFields::new().raw_output(json!({ "output": output.clone() })),
    );
    update.meta = Some(acp::Meta::from_iter([(
        "terminal_output".to_owned(),
        json!({
            "terminal_id": command_terminal_id(item_id).to_string(),
            "data": delta,
        }),
    )]));

    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::ToolCallUpdate(update),
    )]
}

fn terminal_interaction_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(stdin) = params.get("stdin").and_then(Value::as_str) else {
        return Vec::new();
    };

    let mut update = acp::ToolCallUpdate::new(item_id.to_owned(), acp::ToolCallUpdateFields::new());
    update.meta = Some(acp::Meta::from_iter([(
        "terminal_output".to_owned(),
        json!({
            "terminal_id": command_terminal_id(item_id).to_string(),
            "data": format!("\n{stdin}\n"),
        }),
    )]));

    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::ToolCallUpdate(update),
    )]
}

fn file_change_patch_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let content = file_change_contents(params.get("changes"));
    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
            item_id.to_owned(),
            acp::ToolCallUpdateFields::new()
                .kind(acp::ToolKind::Edit)
                .content(content),
        )),
    )]
}

fn mcp_tool_progress_update(params: &Value) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(message) = params.get("message").and_then(Value::as_str) else {
        return Vec::new();
    };

    vec![(
        acp::SessionId::new(thread_id.to_owned()),
        acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
            item_id.to_owned(),
            acp::ToolCallUpdateFields::new().content(vec![acp::ToolCallContent::Content(
                acp::Content::new(acp::ContentBlock::Text(acp::TextContent::new(
                    message.to_owned(),
                ))),
            )]),
        )),
    )]
}

fn item_lifecycle_update(
    params: &Value,
    completed: bool,
    tool_outputs: &HashMap<String, String>,
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
        "commandExecution" => command_lifecycle_update(&session_id, item, completed, tool_outputs),
        "fileChange" => file_change_lifecycle_update(&session_id, item, completed),
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

fn file_change_lifecycle_update(
    session_id: &acp::SessionId,
    item: &Value,
    completed: bool,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(item_id) = item.get("id").and_then(Value::as_str) else {
        return Vec::new();
    };
    let status = tool_status_from_item(item, completed);
    if completed {
        vec![(
            session_id.clone(),
            acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(
                item_id.to_owned(),
                acp::ToolCallUpdateFields::new()
                    .kind(acp::ToolKind::Edit)
                    .status(status)
                    .content(file_change_contents(item.get("changes")))
                    .raw_output(item.clone()),
            )),
        )]
    } else {
        vec![(
            session_id.clone(),
            acp::SessionUpdate::ToolCall(
                acp::ToolCall::new(item_id.to_owned(), tool_title(item))
                    .kind(acp::ToolKind::Edit)
                    .status(status)
                    .raw_input(item.clone()),
            ),
        )]
    }
}

fn command_lifecycle_update(
    session_id: &acp::SessionId,
    item: &Value,
    completed: bool,
    tool_outputs: &HashMap<String, String>,
) -> Vec<(acp::SessionId, acp::SessionUpdate)> {
    let Some(item_id) = item.get("id").and_then(Value::as_str) else {
        return Vec::new();
    };
    let status = tool_status_from_item(item, completed);
    if completed {
        let mut fields = acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Execute)
            .status(status);
        let output = tool_item_content(item);
        if let Some(content) = output.as_deref() {
            fields = fields.raw_output(json!({ "output": content }));
        }
        let mut update = acp::ToolCallUpdate::new(item_id.to_owned(), fields);
        update.meta = command_terminal_completion_meta(
            item_id,
            item,
            command_completion_output_delta(
                output.as_deref(),
                tool_outputs.get(item_id).map(String::as_str),
            ),
        );
        vec![(
            session_id.clone(),
            acp::SessionUpdate::ToolCallUpdate(update),
        )]
    } else {
        let mut tool_call = acp::ToolCall::new(item_id.to_owned(), tool_title(item))
            .kind(acp::ToolKind::Execute)
            .status(status)
            .content(vec![acp::ToolCallContent::Terminal(acp::Terminal::new(
                command_terminal_id(item_id),
            ))])
            .raw_input(item.clone());
        tool_call.meta = command_terminal_info_meta(item_id, item);
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

fn history_updates_from_thread_read(
    _session_id: &acp::SessionId,
    response: &Value,
) -> Vec<acp::SessionUpdate> {
    response
        .get("thread")
        .and_then(|thread| thread.get("turns"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.get("items")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .flat_map(history_update_from_item)
        })
        .collect()
}

fn history_update_from_item(item: &Value) -> Vec<acp::SessionUpdate> {
    match item.get("type").and_then(Value::as_str) {
        Some("userMessage") => item
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(user_input_to_content_block)
            .map(|content| acp::SessionUpdate::UserMessageChunk(acp::ContentChunk::new(content)))
            .collect(),
        Some("agentMessage") => item
            .get("text")
            .and_then(Value::as_str)
            .map(|text| {
                vec![acp::SessionUpdate::AgentMessageChunk(
                    acp::ContentChunk::new(acp::ContentBlock::Text(acp::TextContent::new(text))),
                )]
            })
            .unwrap_or_default(),
        Some("reasoning") => reasoning_history_updates(item),
        Some(
            "commandExecution"
            | "fileChange"
            | "mcpToolCall"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "webSearch"
            | "imageGeneration",
        ) => tool_history_updates(item),
        _ => Vec::new(),
    }
}

fn reasoning_history_updates(item: &Value) -> Vec<acp::SessionUpdate> {
    item.get("summary")
        .or_else(|| item.get("content"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|text| {
            acp::SessionUpdate::AgentThoughtChunk(acp::ContentChunk::new(acp::ContentBlock::Text(
                acp::TextContent::new(text),
            )))
        })
        .collect()
}

fn tool_history_updates(item: &Value) -> Vec<acp::SessionUpdate> {
    let Some(item_id) = item.get("id").and_then(Value::as_str) else {
        return Vec::new();
    };
    if item.get("type").and_then(Value::as_str) == Some("commandExecution") {
        let mut tool_call = acp::ToolCall::new(item_id.to_owned(), tool_title(item))
            .kind(acp::ToolKind::Execute)
            .status(tool_status_from_item(item, true))
            .content(vec![acp::ToolCallContent::Terminal(acp::Terminal::new(
                command_terminal_id(item_id),
            ))])
            .raw_input(item.clone());
        tool_call.meta = command_terminal_info_meta(item_id, item);

        let mut fields = acp::ToolCallUpdateFields::new()
            .kind(acp::ToolKind::Execute)
            .status(tool_status_from_item(item, true));
        let output = tool_item_content(item);
        if let Some(content) = output.as_deref() {
            fields = fields.raw_output(json!({ "output": content }));
        }
        let mut update = acp::ToolCallUpdate::new(item_id.to_owned(), fields);
        update.meta = command_terminal_completion_meta(item_id, item, output.as_deref());

        return vec![
            acp::SessionUpdate::ToolCall(tool_call),
            acp::SessionUpdate::ToolCallUpdate(update),
        ];
    }

    if item.get("type").and_then(Value::as_str) == Some("fileChange") {
        let tool_call = acp::ToolCall::new(item_id.to_owned(), tool_title(item))
            .kind(acp::ToolKind::Edit)
            .status(tool_status_from_item(item, true))
            .raw_input(item.clone());
        let update = acp::ToolCallUpdate::new(
            item_id.to_owned(),
            acp::ToolCallUpdateFields::new()
                .kind(acp::ToolKind::Edit)
                .status(tool_status_from_item(item, true))
                .content(file_change_contents(item.get("changes")))
                .raw_output(item.clone()),
        );

        return vec![
            acp::SessionUpdate::ToolCall(tool_call),
            acp::SessionUpdate::ToolCallUpdate(update),
        ];
    }

    let mut tool_call = acp::ToolCall::new(item_id.to_owned(), tool_title(item))
        .kind(tool_kind_for_item_type(
            item.get("type").and_then(Value::as_str).unwrap_or_default(),
        ))
        .status(tool_status_from_item(item, true))
        .raw_input(item.clone());
    tool_call.meta = collab_tool_call_meta(item);
    let mut updates = vec![acp::SessionUpdate::ToolCall(tool_call)];
    let mut fields = acp::ToolCallUpdateFields::new().status(tool_status_from_item(item, true));
    if let Some(content) = tool_item_content(item) {
        fields = fields.content(vec![acp::ToolCallContent::Content(acp::Content::new(
            acp::ContentBlock::Text(acp::TextContent::new(content)),
        ))]);
    }
    let mut update = acp::ToolCallUpdate::new(item_id.to_owned(), fields);
    update.meta = collab_tool_call_meta(item);
    updates.push(acp::SessionUpdate::ToolCallUpdate(update));
    updates
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
    let message = if let Some(goal) = response.get("goal") {
        goal_message(goal)
    } else {
        "No active goal.".to_owned()
    };
    show_thread_message(session_id, sessions, message, cx)
}

fn show_thread_message(
    session_id: &acp::SessionId,
    sessions: &HashMap<acp::SessionId, CodexNativeSession>,
    message: String,
    cx: &mut AsyncApp,
) -> Result<()> {
    let Some(thread) = sessions
        .get(session_id)
        .map(|session| session.thread.clone())
    else {
        return Ok(());
    };
    let update = acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
        acp::ContentBlock::Text(acp::TextContent::new(message)),
    ));
    thread
        .update(cx, |thread, cx| thread.handle_session_update(update, cx))?
        .map_err(anyhow::Error::from)
}

fn model_list_params() -> Value {
    json!({
        "cursor": null,
        "limit": null,
        "includeHidden": true,
    })
}

fn agent_model_infos_from_response(response: &Value) -> Vec<AgentModelInfo> {
    response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(agent_model_info_from_value)
        .collect()
}

fn agent_model_info_from_value(value: &Value) -> Option<AgentModelInfo> {
    let model_id = value
        .get("model")
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)?;
    let name = value
        .get("displayName")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .unwrap_or(model_id);
    let description = value
        .get("description")
        .and_then(Value::as_str)
        .filter(|description| !description.is_empty())
        .map(SharedString::from);
    Some(AgentModelInfo {
        id: acp::ModelId::new(model_id.to_owned()),
        name: SharedString::from(name.to_owned()),
        description,
        icon: None,
        is_latest: value
            .get("isDefault")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        cost: None,
    })
}

fn fallback_agent_model_info(model_id: acp::ModelId) -> AgentModelInfo {
    AgentModelInfo {
        name: SharedString::from(model_id.to_string()),
        id: model_id,
        description: None,
        icon: None,
        is_latest: false,
        cost: None,
    }
}

fn selected_agent_model<'a>(
    models: &'a [AgentModelInfo],
    selected_model: Option<&acp::ModelId>,
    default_model: Option<&acp::ModelId>,
) -> Option<&'a AgentModelInfo> {
    selected_model
        .and_then(|selected_model| models.iter().find(|model| &model.id == selected_model))
        .or_else(|| {
            default_model
                .and_then(|default_model| models.iter().find(|model| &model.id == default_model))
        })
        .or_else(|| models.iter().find(|model| model.is_latest))
        .or_else(|| models.first())
}

fn default_config_options_for_session() -> Vec<acp::SessionConfigOption> {
    vec![
        codex_select_config_option(
            "model_reasoning_effort",
            "Reasoning",
            "xhigh",
            ["low", "medium", "high", "xhigh"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        )
        .category(acp::SessionConfigOptionCategory::ThoughtLevel),
        codex_select_config_option(
            "approval_policy",
            "Approvals",
            "never",
            ["untrusted", "on-failure", "on-request", "never"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        ),
        codex_select_config_option(
            "sandbox_mode",
            "Sandbox",
            "danger-full-access",
            ["read-only", "workspace-write", "danger-full-access"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        ),
    ]
}

fn config_options_from_read_response(response: &Value) -> Vec<acp::SessionConfigOption> {
    let config = response.get("config").unwrap_or(&Value::Null);
    let reasoning_effort = config
        .get("model_reasoning_effort")
        .and_then(Value::as_str)
        .unwrap_or("xhigh")
        .to_owned();
    let approval_policy = config
        .get("approval_policy")
        .and_then(Value::as_str)
        .unwrap_or("never")
        .to_owned();
    let sandbox_mode = config
        .get("sandbox_mode")
        .and_then(Value::as_str)
        .unwrap_or("danger-full-access")
        .to_owned();

    let mut options = vec![
        codex_select_config_option(
            "model_reasoning_effort",
            "Reasoning",
            reasoning_effort,
            ["low", "medium", "high", "xhigh"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        )
        .category(acp::SessionConfigOptionCategory::ThoughtLevel),
        codex_select_config_option(
            "approval_policy",
            "Approvals",
            approval_policy,
            ["untrusted", "on-failure", "on-request", "never"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        ),
        codex_select_config_option(
            "sandbox_mode",
            "Sandbox",
            sandbox_mode,
            ["read-only", "workspace-write", "danger-full-access"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        ),
    ];

    if let Some(verbosity) = config
        .get("model_verbosity")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    {
        options.push(codex_select_config_option(
            "model_verbosity",
            "Verbosity",
            verbosity,
            ["low", "medium", "high"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        ));
    }
    if let Some(web_search) = config
        .get("web_search")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    {
        options.push(codex_select_config_option(
            "web_search",
            "Web search",
            web_search,
            ["disabled", "cached", "indexed", "live"]
                .into_iter()
                .map(|value| (value.to_owned(), None))
                .collect(),
        ));
    }
    options
}

fn update_config_options_from_read_response(
    state: &Rc<CodexNativeConfigOptionsState>,
    response: &Value,
) {
    let incoming = config_options_from_read_response(response);
    let mut options = state.options.borrow_mut();
    for incoming_option in incoming {
        upsert_config_option(&mut options, incoming_option);
    }
}

fn refresh_config_options_from_notification(
    method: &str,
    params: &Value,
    state: &Rc<RefCell<CodexNativeState>>,
    selected_models: &Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    model_watch_tx: &Rc<RefCell<watch::Sender<()>>>,
) {
    if method != "thread/settings/updated" {
        return;
    }
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return;
    };
    let session_id = acp::SessionId::new(thread_id.to_owned());
    let Some(config_options) = state.borrow().config_options.get(&session_id).cloned() else {
        return;
    };
    update_config_options_from_thread_settings(
        &session_id,
        &config_options,
        params.get("threadSettings").unwrap_or(&Value::Null),
        selected_models,
        model_watch_tx,
    );
    config_options.watch_tx.borrow_mut().send(()).log_err();
}

fn update_config_options_from_thread_settings(
    session_id: &acp::SessionId,
    state: &Rc<CodexNativeConfigOptionsState>,
    thread_settings: &Value,
    selected_models: &Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    model_watch_tx: &Rc<RefCell<watch::Sender<()>>>,
) {
    if let Some(model) = thread_settings.get("model").and_then(Value::as_str) {
        selected_models
            .borrow_mut()
            .insert(session_id.clone(), acp::ModelId::new(model.to_owned()));
        model_watch_tx.borrow_mut().send(()).log_err();
    }
    if let Some(effort) = thread_settings.get("effort").and_then(Value::as_str) {
        set_config_option_current_value(
            state,
            &acp::SessionConfigId::new("model_reasoning_effort"),
            &acp::SessionConfigValueId::new(effort.to_owned()),
        );
    }
    if let Some(approval_policy) =
        approval_policy_config_value(thread_settings.get("approvalPolicy"))
    {
        set_config_option_current_value(
            state,
            &acp::SessionConfigId::new("approval_policy"),
            &acp::SessionConfigValueId::new(approval_policy),
        );
    }
    if let Some(sandbox_mode) = sandbox_policy_config_value(thread_settings.get("sandboxPolicy")) {
        set_config_option_current_value(
            state,
            &acp::SessionConfigId::new("sandbox_mode"),
            &acp::SessionConfigValueId::new(sandbox_mode),
        );
    }
}

fn approval_policy_config_value(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(policy) => Some(policy.clone()),
        Value::Object(object) if object.contains_key("granular") => Some("on-request".to_owned()),
        _ => None,
    }
}

fn sandbox_policy_config_value(value: Option<&Value>) -> Option<String> {
    match value?.get("type").and_then(Value::as_str)? {
        "dangerFullAccess" => Some("danger-full-access".to_owned()),
        "readOnly" => Some("read-only".to_owned()),
        "workspaceWrite" => Some("workspace-write".to_owned()),
        _ => None,
    }
}

fn remember_selected_model_from_thread(
    session_id: &acp::SessionId,
    response: &Value,
    selected_models: &Rc<RefCell<HashMap<acp::SessionId, acp::ModelId>>>,
    model_watch_tx: &Rc<RefCell<watch::Sender<()>>>,
) {
    let Some(model) = response
        .get("thread")
        .and_then(|thread| thread.get("settings"))
        .or_else(|| response.get("threadSettings"))
        .and_then(|settings| settings.get("model"))
        .and_then(Value::as_str)
    else {
        return;
    };
    selected_models
        .borrow_mut()
        .insert(session_id.clone(), acp::ModelId::new(model.to_owned()));
    model_watch_tx.borrow_mut().send(()).log_err();
}

fn upsert_config_option(
    options: &mut Vec<acp::SessionConfigOption>,
    option: acp::SessionConfigOption,
) {
    if let Some(existing) = options.iter_mut().find(|existing| existing.id == option.id) {
        *existing = option;
    } else {
        options.push(option);
    }
}

fn set_config_option_current_value(
    state: &Rc<CodexNativeConfigOptionsState>,
    config_id: &acp::SessionConfigId,
    value: &acp::SessionConfigValueId,
) {
    let mut options = state.options.borrow_mut();
    for option in options.iter_mut() {
        if &option.id == config_id
            && let acp::SessionConfigKind::Select(select) = &mut option.kind
        {
            select.current_value = value.clone();
        }
    }
}

fn codex_select_config_option(
    id: impl Into<acp::SessionConfigId>,
    name: impl Into<String>,
    current_value: impl Into<acp::SessionConfigValueId>,
    values: Vec<(String, Option<String>)>,
) -> acp::SessionConfigOption {
    let options = values
        .into_iter()
        .map(|(value, description)| {
            acp::SessionConfigSelectOption::new(value.clone(), config_value_label(&value))
                .description(description)
        })
        .collect::<Vec<_>>();
    acp::SessionConfigOption::select(id, name, current_value, options)
}

fn config_value_label(value: &str) -> String {
    match value {
        "gpt-5.5" => "GPT-5.5".to_owned(),
        "gpt-5.4" => "GPT-5.4".to_owned(),
        "gpt-5.4-mini" => "GPT-5.4 Mini".to_owned(),
        "xhigh" => "Extra High".to_owned(),
        "on-request" => "On Request".to_owned(),
        "on-failure" => "On Failure".to_owned(),
        "read-only" => "Read Only".to_owned(),
        "workspace-write" => "Workspace Write".to_owned(),
        "danger-full-access" => "Danger Full Access".to_owned(),
        other => {
            let mut label = String::new();
            for (index, part) in other.split(['_', '-']).enumerate() {
                if index > 0 {
                    label.push(' ');
                }
                let mut chars = part.chars();
                if let Some(first) = chars.next() {
                    label.extend(first.to_uppercase());
                    label.push_str(chars.as_str());
                }
            }
            if label.is_empty() {
                other.to_owned()
            } else {
                label
            }
        }
    }
}

fn config_value_from_config_id(config_id: &acp::SessionConfigId, value: String) -> Value {
    match config_id.0.as_ref() {
        "model_context_window" | "model_auto_compact_token_limit" => value
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(value)),
        _ => Value::String(value),
    }
}

fn format_model_list(response: &Value) -> String {
    let models = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if models.is_empty() {
        return "Codex models: no models returned.".to_owned();
    }

    let mut lines = vec!["Codex models:".to_owned()];
    for model in models.iter().take(30) {
        let id = model
            .get("model")
            .or_else(|| model.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let name = model
            .get("displayName")
            .or_else(|| model.get("name"))
            .and_then(Value::as_str)
            .unwrap_or(id);
        let default_suffix = if model
            .get("isDefault")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            " (default)"
        } else {
            ""
        };
        let description = model
            .get("description")
            .and_then(Value::as_str)
            .filter(|description| !description.trim().is_empty())
            .map(|description| format!(" — {}", description.trim()))
            .unwrap_or_default();
        lines.push(format!("- {name} (`{id}`){default_suffix}{description}"));
    }
    if models.len() > 30 {
        lines.push(format!("- … {} more", models.len() - 30));
    }
    lines.push(
        "Use the model selector to choose the model for future turns in this thread.".to_owned(),
    );
    lines.join("\n")
}

fn format_config_read(response: &Value) -> String {
    let Some(config) = response.get("config").and_then(Value::as_object) else {
        return "Codex configuration: no config returned.".to_owned();
    };
    let mut lines = vec!["Codex configuration:".to_owned()];
    for (key, value) in config.iter().filter(|(_, value)| !value.is_null()).take(30) {
        lines.push(format!("- `{key}`: {}", value_to_inline_string(value)));
    }
    if lines.len() == 1 {
        lines.push("- No non-null config values returned.".to_owned());
    }
    lines.join("\n")
}

fn format_skills_list(response: &Value) -> String {
    let entries = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return "Codex skills: no skills returned.".to_owned();
    }

    let mut lines = vec!["Codex skills:".to_owned()];
    for entry in entries.iter().take(10) {
        if let Some(cwd) = entry.get("cwd").and_then(Value::as_str) {
            lines.push(format!("- `{cwd}`"));
        }
        let skills = entry
            .get("skills")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for skill in skills.iter().take(20) {
            let name = skill.get("name").and_then(Value::as_str).unwrap_or("skill");
            let description = skill
                .get("description")
                .and_then(Value::as_str)
                .filter(|description| !description.trim().is_empty())
                .map(|description| format!(" — {}", description.trim()))
                .unwrap_or_default();
            lines.push(format!("  - {name}{description}"));
        }
        if skills.len() > 20 {
            lines.push(format!("  - … {} more", skills.len() - 20));
        }
        append_list_errors(&mut lines, entry, "errors");
    }
    lines.join("\n")
}

fn format_hooks_list(response: &Value) -> String {
    let entries = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return "Codex hooks: no hooks returned.".to_owned();
    }

    let mut lines = vec!["Codex hooks:".to_owned()];
    for entry in entries.iter().take(10) {
        if let Some(cwd) = entry.get("cwd").and_then(Value::as_str) {
            lines.push(format!("- `{cwd}`"));
        }
        let hooks = entry
            .get("hooks")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for hook in hooks.iter().take(20) {
            let key = hook.get("key").and_then(Value::as_str).unwrap_or("hook");
            let event = hook
                .get("eventName")
                .and_then(Value::as_str)
                .unwrap_or("event");
            let enabled = hook
                .get("enabled")
                .and_then(Value::as_bool)
                .map(|enabled| if enabled { "enabled" } else { "disabled" })
                .unwrap_or("unknown");
            lines.push(format!("  - {key} ({event}, {enabled})"));
        }
        if hooks.len() > 20 {
            lines.push(format!("  - … {} more", hooks.len() - 20));
        }
        append_list_errors(&mut lines, entry, "warnings");
        append_list_errors(&mut lines, entry, "errors");
    }
    lines.join("\n")
}

fn format_plugin_list(response: &Value) -> String {
    let marketplaces = response
        .get("marketplaces")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if marketplaces.is_empty() {
        return "Codex plugins: no marketplaces returned.".to_owned();
    }

    let mut lines = vec!["Codex plugins:".to_owned()];
    for marketplace in marketplaces.iter().take(10) {
        let marketplace_name = marketplace
            .get("interface")
            .and_then(|interface| interface.get("displayName"))
            .and_then(Value::as_str)
            .or_else(|| marketplace.get("name").and_then(Value::as_str))
            .unwrap_or("marketplace");
        lines.push(format!("- {marketplace_name}"));
        let plugins = marketplace
            .get("plugins")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for plugin in plugins.iter().take(20) {
            let name = plugin
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("plugin");
            let id = plugin.get("id").and_then(Value::as_str).unwrap_or(name);
            let enabled = plugin
                .get("enabled")
                .and_then(Value::as_bool)
                .map(|enabled| if enabled { "enabled" } else { "disabled" })
                .unwrap_or("unknown");
            let installed = plugin
                .get("installed")
                .and_then(Value::as_bool)
                .map(|installed| {
                    if installed {
                        "installed"
                    } else {
                        "not installed"
                    }
                })
                .unwrap_or("unknown install state");
            lines.push(format!("  - {name} (`{id}`): {installed}, {enabled}"));
        }
        if plugins.len() > 20 {
            lines.push(format!("  - … {} more", plugins.len() - 20));
        }
    }
    append_list_errors(&mut lines, response, "marketplaceLoadErrors");
    lines.join("\n")
}

fn format_mcp_status_list(response: &Value) -> String {
    let servers = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return "Codex MCP servers: no servers returned.".to_owned();
    }

    let mut lines = vec!["Codex MCP servers:".to_owned()];
    for server in servers.iter().take(30) {
        let name = server
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("server");
        let tool_count = server
            .get("tools")
            .and_then(Value::as_object)
            .map(|tools| tools.len())
            .unwrap_or(0);
        let auth = server
            .get("authStatus")
            .map(value_to_inline_string)
            .unwrap_or_else(|| "unknown".to_owned());
        lines.push(format!("- {name}: {tool_count} tool(s), auth {auth}"));
    }
    if servers.len() > 30 {
        lines.push(format!("- … {} more", servers.len() - 30));
    }
    lines.join("\n")
}

fn format_thread_fork(response: &Value) -> String {
    let thread = response.get("thread").unwrap_or(&Value::Null);
    let id = thread
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let name = thread
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Untitled thread");
    format!("Forked native Codex thread `{id}`: {name}\nOpen it from thread history/sidebar.")
}

fn format_thread_history(response: &Value) -> String {
    let threads = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if threads.is_empty() {
        return "Native Codex history: no threads returned.".to_owned();
    }

    let mut lines = vec!["Native Codex history:".to_owned()];
    for thread in threads.iter().take(20) {
        let id = thread
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let name = thread
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("Untitled thread");
        let cwd = thread.get("cwd").and_then(Value::as_str).unwrap_or("");
        if cwd.is_empty() {
            lines.push(format!("- {name} (`{id}`)"));
        } else {
            lines.push(format!("- {name} (`{id}`) — `{cwd}`"));
        }
    }
    if threads.len() > 20 {
        lines.push(format!("- … {} more", threads.len() - 20));
    }
    lines.join("\n")
}

fn append_list_errors(lines: &mut Vec<String>, value: &Value, key: &str) {
    let errors = value
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for error in errors.iter().take(5) {
        lines.push(format!("  - {key}: {}", value_to_inline_string(error)));
    }
    if errors.len() > 5 {
        lines.push(format!("  - {key}: … {} more", errors.len() - 5));
    }
}

fn value_to_inline_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "null".to_owned(),
        Value::Array(_) | Value::Object(_) => match serde_json::to_string(value) {
            Ok(text) => text,
            Err(error) => format!("<failed to format JSON: {error}>"),
        },
    }
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
        "model" => Ok(Some(NativeSlashCommand::Model)),
        "config" => Ok(Some(NativeSlashCommand::Config)),
        "skills" => Ok(Some(NativeSlashCommand::Skills)),
        "plugins" => Ok(Some(NativeSlashCommand::Plugins)),
        "hooks" => Ok(Some(NativeSlashCommand::Hooks)),
        "mcp" => Ok(Some(NativeSlashCommand::Mcp)),
        "fork" => Ok(Some(NativeSlashCommand::Fork)),
        "history" => Ok(Some(NativeSlashCommand::History)),
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
    session_list: &Rc<CodexNativeSessionList>,
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
        NativeSlashCommand::Model => {
            let response = client
                .send_request("model/list", model_list_params())
                .await
                .context("failed to list native Codex models")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_model_list(&response),
                cx,
            )
            .context("failed to show native Codex model list")?;
        }
        NativeSlashCommand::Config => {
            let response = client
                .send_request("config/read", json!({ "includeLayers": false }))
                .await
                .context("failed to read native Codex configuration")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_config_read(&response),
                cx,
            )
            .context("failed to show native Codex configuration")?;
        }
        NativeSlashCommand::Skills => {
            let response = client
                .send_request(
                    "skills/list",
                    json!({
                        "cwds": [],
                        "forceReload": false,
                    }),
                )
                .await
                .context("failed to list native Codex skills")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_skills_list(&response),
                cx,
            )
            .context("failed to show native Codex skills")?;
        }
        NativeSlashCommand::Plugins => {
            let response = client
                .send_request(
                    "plugin/list",
                    json!({
                        "cwds": null,
                        "marketplaceKinds": null,
                    }),
                )
                .await
                .context("failed to list native Codex plugins")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_plugin_list(&response),
                cx,
            )
            .context("failed to show native Codex plugins")?;
        }
        NativeSlashCommand::Hooks => {
            let response = client
                .send_request("hooks/list", json!({ "cwds": [] }))
                .await
                .context("failed to list native Codex hooks")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_hooks_list(&response),
                cx,
            )
            .context("failed to show native Codex hooks")?;
        }
        NativeSlashCommand::Mcp => {
            let response = client
                .send_request(
                    "mcpServerStatus/list",
                    json!({
                        "cursor": null,
                        "limit": null,
                        "detail": "toolsAndAuthOnly",
                    }),
                )
                .await
                .context("failed to list native Codex MCP servers")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_mcp_status_list(&response),
                cx,
            )
            .context("failed to show native Codex MCP servers")?;
        }
        NativeSlashCommand::Fork => {
            let response = client
                .send_request(
                    "thread/fork",
                    json!({
                        "threadId": session_id.to_string(),
                        "threadSource": "appServer",
                    }),
                )
                .await
                .context("failed to fork native Codex thread")?;
            session_list.notify_refresh();
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_thread_fork(&response),
                cx,
            )
            .context("failed to show native Codex fork result")?;
        }
        NativeSlashCommand::History => {
            let response = client
                .send_request(
                    "thread/list",
                    json!({
                        "sourceKinds": ["appServer"],
                        "limit": 20,
                    }),
                )
                .await
                .context("failed to list native Codex history")?;
            show_thread_message(
                session_id,
                &sessions.borrow(),
                format_thread_history(&response),
                cx,
            )
            .context("failed to show native Codex history")?;
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

fn command_terminal_id(item_id: &str) -> acp::TerminalId {
    acp::TerminalId::new(format!("{CODEX_NATIVE_COMMAND_TERMINAL_PREFIX}:{item_id}"))
}

fn command_terminal_info_meta(item_id: &str, item: &Value) -> Option<acp::Meta> {
    Some(acp::Meta::from_iter([(
        "terminal_info".to_owned(),
        json!({
            "terminal_id": command_terminal_id(item_id).to_string(),
            "cwd": item
                .get("cwd")
                .or_else(|| item.get("cd"))
                .or_else(|| item.get("workingDirectory"))
                .and_then(Value::as_str),
        }),
    )]))
}

fn command_terminal_completion_meta(
    item_id: &str,
    item: &Value,
    completion_output_delta: Option<&str>,
) -> Option<acp::Meta> {
    let mut meta = acp::Meta::new();
    if let Some(content) = completion_output_delta {
        meta.insert(
            "terminal_output".to_owned(),
            json!({
                "terminal_id": command_terminal_id(item_id).to_string(),
                "data": content,
            }),
        );
    }
    meta.insert(
        "terminal_exit".to_owned(),
        json!({
            "terminal_id": command_terminal_id(item_id).to_string(),
            "exit_code": item
                .get("exitCode")
                .or_else(|| item.get("exit_code"))
                .and_then(Value::as_u64),
            "signal": item.get("signal").and_then(Value::as_str),
        }),
    );
    Some(meta)
}

fn command_completion_output_delta<'a>(
    completed_output: Option<&'a str>,
    streamed_output: Option<&str>,
) -> Option<&'a str> {
    let completed_output = completed_output.filter(|output| !output.is_empty())?;
    let streamed_output = streamed_output.unwrap_or_default();
    if streamed_output.is_empty() {
        Some(completed_output)
    } else {
        completed_output
            .strip_prefix(streamed_output)
            .filter(|delta| !delta.is_empty())
    }
}

fn collab_tool_call_meta(item: &Value) -> Option<acp::Meta> {
    if item.get("type").and_then(Value::as_str) != Some("collabAgentToolCall") {
        return None;
    }

    let mut meta = agent_thread::meta_with_tool_name(ZED_SPAWN_AGENT_TOOL_NAME);
    let receiver_thread_ids = item
        .get("receiverThreadIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if let Some(session_id) = receiver_thread_ids.first() {
        meta.insert(
            agent_thread::SUBAGENT_SESSION_INFO_META_KEY.to_owned(),
            json!({
                "session_id": session_id,
                "message_start_index": 0,
                "message_end_index": null,
            }),
        );
    }
    meta.insert(
        CODEX_NATIVE_COLLAB_META_KEY.to_owned(),
        json!({
            "tool": item.get("tool").cloned().unwrap_or(Value::Null),
            "name": collab_agent_name(item),
            "status": item.get("status").cloned().unwrap_or(Value::Null),
            "receiver_thread_ids": receiver_thread_ids,
            "summary": item.get("summary").cloned().unwrap_or(Value::Null),
            "output": item
                .get("aggregatedOutput")
                .or_else(|| item.get("output"))
                .cloned()
                .unwrap_or(Value::Null),
        }),
    );
    Some(meta)
}

fn collab_agent_name(item: &Value) -> Option<String> {
    item.get("name")
        .or_else(|| item.get("agentName"))
        .or_else(|| item.get("subagentName"))
        .or_else(|| item.get("agent"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
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
        Some("collabAgentToolCall") => {
            let tool = item.get("tool").and_then(Value::as_str);
            let name = collab_agent_name(item);
            match (name, tool) {
                (Some(name), Some(tool)) => format!("Subagent {name}: {tool}"),
                (Some(name), None) => format!("Subagent: {name}"),
                (None, Some(tool)) => format!("Subagent: {tool}"),
                (None, None) => "Subagent".to_owned(),
            }
        }
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
    if let Some(error) = item.get("error")
        && !error.is_null()
    {
        return Some(format!("Error: {error}"));
    }
    None
}

fn file_change_contents(changes: Option<&Value>) -> Vec<acp::ToolCallContent> {
    let Some(change_array) = changes.and_then(Value::as_array) else {
        return file_change_summary_content(changes);
    };

    let content = change_array
        .iter()
        .flat_map(file_change_content)
        .collect::<Vec<_>>();
    if content.is_empty() {
        file_change_summary_content_from_array(change_array)
    } else {
        content
    }
}

fn file_change_summary_content(changes: Option<&Value>) -> Vec<acp::ToolCallContent> {
    vec![acp::ToolCallContent::Content(acp::Content::new(
        acp::ContentBlock::Text(acp::TextContent::new(summarize_file_changes(changes))),
    ))]
}

fn file_change_summary_content_from_array(changes: &[Value]) -> Vec<acp::ToolCallContent> {
    vec![acp::ToolCallContent::Content(acp::Content::new(
        acp::ContentBlock::Text(acp::TextContent::new(summarize_file_changes_from_array(
            changes,
        ))),
    ))]
}

fn file_change_content(change: &Value) -> Vec<acp::ToolCallContent> {
    let Some(path) = change
        .get("path")
        .or_else(|| change.get("file"))
        .or_else(|| change.get("absPath"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };

    let Some(diff) = change
        .get("diff")
        .or_else(|| change.get("unified_diff"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };

    let kind = change_kind_type(change);
    match kind {
        Some("add") => {
            let new_text = text_from_unified_diff(diff, false).unwrap_or_else(|| diff.to_owned());
            vec![acp::ToolCallContent::Diff(acp::Diff::new(path, new_text))]
        }
        Some("delete") => {
            let old_text = text_from_unified_diff(diff, true).unwrap_or_else(|| diff.to_owned());
            vec![acp::ToolCallContent::Diff(
                acp::Diff::new(path, String::new()).old_text(old_text),
            )]
        }
        Some("update") | None => {
            let path = change
                .get("kind")
                .and_then(|kind| kind.get("move_path"))
                .and_then(Value::as_str)
                .unwrap_or(path);
            content_from_unified_diff(PathBuf::from(path), diff)
        }
        _ => Vec::new(),
    }
}

fn change_kind_type(change: &Value) -> Option<&str> {
    match change.get("kind")? {
        Value::String(kind) => Some(kind.as_str()),
        Value::Object(kind) => kind.get("type").and_then(Value::as_str),
        _ => None,
    }
}

fn content_from_unified_diff(path: PathBuf, unified_diff: &str) -> Vec<acp::ToolCallContent> {
    let Ok(patch) = Patch::from_str(unified_diff) else {
        return vec![acp::ToolCallContent::Content(acp::Content::new(
            acp::ContentBlock::Text(acp::TextContent::new(unified_diff.to_owned())),
        ))];
    };

    let diffs = patch
        .hunks()
        .iter()
        .map(|hunk| {
            let mut old_text = String::new();
            let mut new_text = String::new();

            for line in hunk.lines() {
                match line {
                    diffy::Line::Context(text) => {
                        old_text.push_str(text);
                        new_text.push_str(text);
                    }
                    diffy::Line::Delete(text) => old_text.push_str(text),
                    diffy::Line::Insert(text) => new_text.push_str(text),
                }
            }

            acp::ToolCallContent::Diff(acp::Diff::new(path.clone(), new_text).old_text(old_text))
        })
        .collect::<Vec<_>>();

    if diffs.is_empty() {
        vec![acp::ToolCallContent::Content(acp::Content::new(
            acp::ContentBlock::Text(acp::TextContent::new(unified_diff.to_owned())),
        ))]
    } else {
        diffs
    }
}

fn text_from_unified_diff(unified_diff: &str, old_text: bool) -> Option<String> {
    let patch = Patch::from_str(unified_diff).ok()?;
    let mut text = String::new();
    for hunk in patch.hunks() {
        for line in hunk.lines() {
            match line {
                diffy::Line::Context(line) => text.push_str(line),
                diffy::Line::Delete(line) if old_text => text.push_str(line),
                diffy::Line::Insert(line) if !old_text => text.push_str(line),
                _ => {}
            }
        }
    }
    Some(text)
}

fn summarize_file_changes(changes: Option<&Value>) -> String {
    let Some(changes) = changes.and_then(Value::as_array) else {
        return "File changes updated.".to_owned();
    };
    summarize_file_changes_from_array(changes)
}

fn summarize_file_changes_from_array(changes: &[Value]) -> String {
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
    use std::collections::VecDeque;

    struct FakeCodexAppServerRpc {
        requests: Arc<Mutex<Vec<(String, Value)>>>,
        responses: Arc<Mutex<VecDeque<Result<Value, String>>>>,
    }

    impl FakeCodexAppServerRpc {
        fn new(responses: Vec<Result<Value, String>>) -> (Self, Arc<Mutex<Vec<(String, Value)>>>) {
            let requests = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    requests: requests.clone(),
                    responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                },
                requests,
            )
        }
    }

    impl CodexAppServerRpc for FakeCodexAppServerRpc {
        fn send_request(&self, method: &str, params: Value) -> BoxFuture<'static, Result<Value>> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((method.to_owned(), params));
            let response = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_else(|| Err(format!("missing fake response for `{method}`")));
            async move { response.map_err(anyhow::Error::msg) }.boxed()
        }

        fn send_response(&self, _id: Value, _result: Value) -> Result<()> {
            Ok(())
        }

        fn send_error_response(&self, _id: Value, _message: String) -> Result<()> {
            Ok(())
        }
    }

    fn test_codex_connection(
        responses: Vec<Result<Value, String>>,
    ) -> (Rc<CodexNativeConnection>, Arc<Mutex<Vec<(String, Value)>>>) {
        let (rpc, requests) = FakeCodexAppServerRpc::new(responses);
        let client = CodexAppServerClient {
            inner: Arc::new(rpc),
        };
        let session_list = Rc::new(CodexNativeSessionList::new(client.clone()));
        let (model_watch_tx, model_watch_rx) = watch::channel(());
        (
            Rc::new(CodexNativeConnection {
                id: AgentId::new("codex-acp"),
                telemetry_id: CODEX_NATIVE_TELEMETRY_ID.into(),
                client,
                sessions: Rc::new(RefCell::new(HashMap::new())),
                pending_turn_starts: Rc::new(RefCell::new(HashMap::new())),
                pending_turns: Rc::new(RefCell::new(HashMap::new())),
                completed_turns: Rc::new(RefCell::new(HashMap::new())),
                auth_methods: Vec::new(),
                default_model: None,
                session_list,
                state: Rc::new(RefCell::new(CodexNativeState::default())),
                selected_models: Rc::new(RefCell::new(HashMap::new())),
                model_watch_tx: Rc::new(RefCell::new(model_watch_tx)),
                model_watch_rx,
                _dispatch_task: Task::ready(Ok(())),
            }),
            requests,
        )
    }

    async fn test_project(cx: &mut gpui::TestAppContext) -> Entity<Project> {
        cx.update(|cx| {
            let store = settings::SettingsStore::test(cx);
            cx.set_global(store);
        });
        let fs = fs::FakeFs::new(cx.executor());
        fs.insert_tree("/project", json!({ "file.txt": "" })).await;
        Project::test(fs, [Path::new("/project")], cx).await
    }

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
    fn maps_command_start_to_terminal_tool_call() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/started",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "status": "inProgress",
                    "command": "echo hi",
                    "cwd": "/tmp"
                }
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0.to_string(), "thread-1");
        match &updates[0].1 {
            acp::SessionUpdate::ToolCall(tool_call) => {
                assert_eq!(tool_call.kind, acp::ToolKind::Execute);
                assert_eq!(tool_call.status, acp::ToolCallStatus::InProgress);
                assert_eq!(tool_call.content.len(), 1);
                match &tool_call.content[0] {
                    acp::ToolCallContent::Terminal(terminal) => {
                        assert_eq!(
                            terminal.terminal_id.to_string(),
                            "codex-native-command:cmd-1"
                        );
                    }
                    other => panic!("unexpected tool call content: {other:?}"),
                }

                let terminal_info = tool_call
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("terminal_info"))
                    .expect("terminal info meta should exist");
                assert_eq!(
                    terminal_info.get("terminal_id").and_then(Value::as_str),
                    Some("codex-native-command:cmd-1")
                );
                assert_eq!(
                    terminal_info.get("cwd").and_then(Value::as_str),
                    Some("/tmp")
                );
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_command_output_delta_to_terminal_output_meta() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/commandExecution/outputDelta",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "cmd-1",
                "delta": "hello\n"
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0.to_string(), "thread-1");
        match &updates[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                assert_eq!(update.tool_call_id.to_string(), "cmd-1");
                assert!(update.fields.content.is_none());
                assert_eq!(
                    update
                        .fields
                        .raw_output
                        .as_ref()
                        .and_then(|raw_output| raw_output.get("output"))
                        .and_then(Value::as_str),
                    Some("hello\n")
                );

                let terminal_output = update
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("terminal_output"))
                    .expect("terminal output meta should exist");
                assert_eq!(
                    terminal_output.get("terminal_id").and_then(Value::as_str),
                    Some("codex-native-command:cmd-1")
                );
                assert_eq!(
                    terminal_output.get("data").and_then(Value::as_str),
                    Some("hello\n")
                );
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_command_completion_to_terminal_exit_without_replacing_terminal() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/completed",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "status": "completed",
                    "command": "echo hi",
                    "aggregatedOutput": "hello\n",
                    "exitCode": 0
                }
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0.to_string(), "thread-1");
        match &updates[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                assert_eq!(update.tool_call_id.to_string(), "cmd-1");
                assert_eq!(update.fields.kind, Some(acp::ToolKind::Execute));
                assert_eq!(update.fields.status, Some(acp::ToolCallStatus::Completed));
                assert!(update.fields.content.is_none());
                assert_eq!(
                    update
                        .fields
                        .raw_output
                        .as_ref()
                        .and_then(|raw_output| raw_output.get("output"))
                        .and_then(Value::as_str),
                    Some("hello\n")
                );

                let terminal_exit = update
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("terminal_exit"))
                    .expect("terminal exit meta should exist");
                assert_eq!(
                    terminal_exit.get("terminal_id").and_then(Value::as_str),
                    Some("codex-native-command:cmd-1")
                );
                assert_eq!(
                    terminal_exit.get("exit_code").and_then(Value::as_u64),
                    Some(0)
                );

                let terminal_output = update
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("terminal_output"))
                    .expect("completion should replay output when no deltas were seen");
                assert_eq!(
                    terminal_output.get("terminal_id").and_then(Value::as_str),
                    Some("codex-native-command:cmd-1")
                );
                assert_eq!(
                    terminal_output.get("data").and_then(Value::as_str),
                    Some("hello\n")
                );
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_command_completion_to_missing_terminal_output_suffix() {
        let mut tool_outputs = HashMap::from([("cmd-1".to_owned(), "hello".to_owned())]);
        let updates = session_updates_from_notification(
            "item/completed",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "status": "completed",
                    "command": "printf hello",
                    "aggregatedOutput": "hello\n",
                    "exitCode": 0
                }
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                let terminal_output = update
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("terminal_output"))
                    .expect("completion should stream only the missing suffix");
                assert_eq!(
                    terminal_output.get("data").and_then(Value::as_str),
                    Some("\n")
                );
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_terminal_interaction_to_terminal_output_meta() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/commandExecution/terminalInteraction",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "cmd-1",
                "processId": "process-1",
                "stdin": "y"
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                assert!(update.fields.content.is_none());
                let terminal_output = update
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("terminal_output"))
                    .expect("terminal interaction should stream to the terminal");
                assert_eq!(
                    terminal_output.get("terminal_id").and_then(Value::as_str),
                    Some("codex-native-command:cmd-1")
                );
                assert_eq!(
                    terminal_output.get("data").and_then(Value::as_str),
                    Some("\ny\n")
                );
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_file_change_patch_update_to_diff_content() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/fileChange/patchUpdated",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "edit-1",
                "changes": [{
                    "path": "src/main.rs",
                    "kind": {"type": "update"},
                    "diff": "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-old\n+new\n"
                }]
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                assert_eq!(update.fields.kind, Some(acp::ToolKind::Edit));
                let content = update
                    .fields
                    .content
                    .as_ref()
                    .expect("file changes should render as diff content");
                assert_eq!(content.len(), 1);
                match &content[0] {
                    acp::ToolCallContent::Diff(diff) => {
                        assert_eq!(diff.path, PathBuf::from("src/main.rs"));
                        assert_eq!(diff.old_text.as_deref(), Some("old\n"));
                        assert_eq!(diff.new_text, "new\n");
                    }
                    other => panic!("expected diff content, got {other:?}"),
                }
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_file_change_completion_and_history_to_diff_content() {
        let file_change = json!({
            "id": "edit-1",
            "type": "fileChange",
            "status": "completed",
            "changes": [{
                "path": "new.txt",
                "kind": {"type": "add"},
                "diff": "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+created\n"
            }]
        });
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "item/completed",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": file_change
            }),
            &mut tool_outputs,
        );

        assert_eq!(updates.len(), 1);
        assert_file_change_diff(&updates[0].1, "new.txt", None, "created\n");

        let history_updates = tool_history_updates(&json!({
            "id": "edit-1",
            "type": "fileChange",
            "status": "completed",
            "changes": [{
                "path": "new.txt",
                "kind": {"type": "add"},
                "diff": "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+created\n"
            }]
        }));
        assert_eq!(history_updates.len(), 2);
        assert_file_change_diff(&history_updates[1], "new.txt", None, "created\n");
    }

    #[test]
    fn maps_mcp_progress_and_turn_error_to_visible_updates() {
        let mut tool_outputs = HashMap::new();
        let progress = session_updates_from_notification(
            "item/mcpToolCall/progress",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "mcp-1",
                "message": "Reading resource..."
            }),
            &mut tool_outputs,
        );
        match &progress[0].1 {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                assert_text_content(update.fields.content.as_ref(), "Reading resource...");
            }
            other => panic!("unexpected update: {other:?}"),
        }

        let error = session_updates_from_notification(
            "error",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "willRetry": true,
                "error": {
                    "message": "network failed",
                    "additionalDetails": "connection reset"
                }
            }),
            &mut tool_outputs,
        );
        match &error[0].1 {
            acp::SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
                acp::ContentBlock::Text(text) => {
                    assert!(text.text.contains("network failed"));
                    assert!(text.text.contains("connection reset"));
                    assert!(text.text.contains("will retry"));
                }
                other => panic!("unexpected content block: {other:?}"),
            },
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_native_approval_allow_always_to_session_decisions() {
        let selected = agent_thread::SelectedPermissionOutcome::new(
            acp::PermissionOptionId::new("acceptForSession"),
            acp::PermissionOptionKind::AllowAlways,
        );
        assert_eq!(
            approval_decision_from_selected(&json!({}), &selected, "accept"),
            json!("acceptForSession")
        );

        let selected = agent_thread::SelectedPermissionOutcome::new(
            acp::PermissionOptionId::new("acceptWithExecpolicyAmendment"),
            acp::PermissionOptionKind::AllowAlways,
        );
        assert_eq!(
            approval_decision_from_selected(
                &json!({"proposedExecpolicyAmendment": ["cargo test"]}),
                &selected,
                "accept"
            ),
            json!({
                "acceptWithExecpolicyAmendment": {
                    "execpolicy_amendment": ["cargo test"]
                }
            })
        );

        let options = approval_permission_options(
            &json!({"proposedExecpolicyAmendment": ["cargo test"]}),
            "accept",
            "decline",
        );
        match options {
            PermissionOptions::Flat(options) => {
                assert!(options.iter().any(|option| {
                    option.option_id.to_string() == "acceptWithExecpolicyAmendment"
                        && option.kind == acp::PermissionOptionKind::AllowAlways
                }));
            }
            _ => panic!("execpolicy approvals should use flat options"),
        }

        let selected = agent_thread::SelectedPermissionOutcome::new(
            acp::PermissionOptionId::new("applyNetworkPolicyAmendment:0"),
            acp::PermissionOptionKind::AllowAlways,
        );
        assert_eq!(
            approval_decision_from_selected(
                &json!({
                    "proposedNetworkPolicyAmendments": [{
                        "action": "allow",
                        "host": "api.example.test"
                    }]
                }),
                &selected,
                "accept"
            ),
            json!({
                "applyNetworkPolicyAmendment": {
                    "network_policy_amendment": {
                        "action": "allow",
                        "host": "api.example.test"
                    }
                }
            })
        );
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
                let usage = agent_thread::session_token_usage_from_meta(&update.meta)
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

        for (text, expected) in [
            ("/model", NativeSlashCommand::Model),
            ("/config", NativeSlashCommand::Config),
            ("/skills", NativeSlashCommand::Skills),
            ("/plugins", NativeSlashCommand::Plugins),
            ("/hooks", NativeSlashCommand::Hooks),
            ("/mcp", NativeSlashCommand::Mcp),
            ("/fork", NativeSlashCommand::Fork),
            ("/history", NativeSlashCommand::History),
        ] {
            let parsed = native_slash_command(&[json!({
                "type": "text",
                "text": text,
            })])
            .expect("slash parsing should succeed")
            .expect("command should be native");
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn formats_model_list_for_thread_entry() {
        let message = format_model_list(&json!({
            "data": [
                {
                    "id": "gpt-5.4",
                    "model": "gpt-5.4",
                    "displayName": "GPT-5.4",
                    "description": "best default",
                    "isDefault": true
                }
            ]
        }));
        assert!(message.contains("Codex models:"));
        assert!(message.contains("GPT-5.4"));
        assert!(message.contains("(default)"));
    }

    #[test]
    fn builds_config_options_from_config_read_response() {
        let options = config_options_from_read_response(&json!({
            "config": {
                "model": "gpt-5.4",
                "model_reasoning_effort": "high",
                "approval_policy": "never",
                "sandbox_mode": "danger-full-access",
                "model_verbosity": "high",
                "web_search": "live"
            }
        }));

        assert!(
            !options
                .iter()
                .any(|option| option.category == Some(acp::SessionConfigOptionCategory::Model))
        );
        assert_eq!(
            current_config_value_for_test(&options, "model_reasoning_effort").as_deref(),
            Some("high")
        );
        assert_eq!(
            current_config_value_for_test(&options, "approval_policy").as_deref(),
            Some("never")
        );
        assert_eq!(
            current_config_value_for_test(&options, "sandbox_mode").as_deref(),
            Some("danger-full-access")
        );
        assert_eq!(
            current_config_value_for_test(&options, "model_verbosity").as_deref(),
            Some("high")
        );
        assert_eq!(
            current_config_value_for_test(&options, "web_search").as_deref(),
            Some("live")
        );
    }

    #[test]
    fn defaults_config_options_to_codex_full_access_mode() {
        let options = default_config_options_for_session();
        assert_eq!(
            current_config_value_for_test(&options, "model_reasoning_effort").as_deref(),
            Some("xhigh")
        );
        assert_eq!(
            current_config_value_for_test(&options, "approval_policy").as_deref(),
            Some("never")
        );
        assert_eq!(
            current_config_value_for_test(&options, "sandbox_mode").as_deref(),
            Some("danger-full-access")
        );

        let options = config_options_from_read_response(&json!({ "config": {} }));
        assert_eq!(
            current_config_value_for_test(&options, "approval_policy").as_deref(),
            Some("never")
        );
        assert_eq!(
            current_config_value_for_test(&options, "sandbox_mode").as_deref(),
            Some("danger-full-access")
        );
    }

    #[test]
    fn updates_config_options_from_thread_settings_notification_shape() {
        let session_id = acp::SessionId::new("thread-1");
        let state = CodexNativeConfigOptionsState::new(default_config_options_for_session());
        let selected_models = Rc::new(RefCell::new(HashMap::new()));
        let (model_watch_tx, _model_watch_rx) = watch::channel(());
        let model_watch_tx = Rc::new(RefCell::new(model_watch_tx));

        update_config_options_from_thread_settings(
            &session_id,
            &state,
            &json!({
                "model": "gpt-5.4",
                "effort": "xhigh",
                "approvalPolicy": "on-failure",
                "sandboxPolicy": {
                    "type": "workspaceWrite"
                }
            }),
            &selected_models,
            &model_watch_tx,
        );

        assert_eq!(
            selected_models
                .borrow()
                .get(&session_id)
                .map(ToString::to_string)
                .as_deref(),
            Some("gpt-5.4")
        );
        let options = state.options.borrow().clone();
        assert_eq!(
            current_config_value_for_test(&options, "model_reasoning_effort").as_deref(),
            Some("xhigh")
        );
        assert_eq!(
            current_config_value_for_test(&options, "approval_policy").as_deref(),
            Some("on-failure")
        );
        assert_eq!(
            current_config_value_for_test(&options, "sandbox_mode").as_deref(),
            Some("workspace-write")
        );
    }

    #[test]
    fn builds_user_input_answer_response_after_explicit_choice() {
        let params = json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "input-1",
            "questions": [
                {
                    "id": "choice",
                    "header": "Pick",
                    "question": "Which one?",
                    "isOther": false,
                    "isSecret": false,
                    "options": [
                        {"label": "A", "description": "first"},
                        {"label": "B", "description": "second"}
                    ]
                }
            ]
        });
        let outcome = agent_thread::RequestPermissionOutcome::Selected(
            agent_thread::SelectedPermissionOutcome::new(
                acp::PermissionOptionId::new("answer:1"),
                acp::PermissionOptionKind::AllowOnce,
            ),
        );
        let (response, status, _) = user_input_response_from_outcome(&params, outcome);
        assert_eq!(status, acp::ToolCallStatus::Completed);
        assert_eq!(
            response
                .get("answers")
                .and_then(|answers| answers.get("choice"))
                .and_then(|answer| answer.get("answers"))
                .and_then(Value::as_array)
                .and_then(|answers| answers.first())
                .and_then(Value::as_str),
            Some("B")
        );
    }

    #[test]
    fn builds_mcp_form_elicitation_response_for_simple_boolean() {
        let params = json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "serverName": "server",
            "mode": "form",
            "message": "Confirm?",
            "requestedSchema": {
                "type": "object",
                "properties": {
                    "confirmed": {"type": "boolean"}
                },
                "required": ["confirmed"]
            }
        });
        let choices = mcp_elicitation_choices(&params);
        assert!(
            choices
                .iter()
                .any(|choice| choice.option_id == "form:confirmed:true")
        );
        let (response, status, _, _) =
            mcp_elicitation_response(&params, &choices, Some("form:confirmed:true"));
        assert_eq!(status, acp::ToolCallStatus::Completed);
        assert_eq!(
            response
                .get("content")
                .and_then(|content| content.get("confirmed"))
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn maps_model_reroute_to_visible_message() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "model/rerouted",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "fromModel": "gpt-5.4",
                "toModel": "gpt-5.4-codex",
                "reason": "highRiskCyberActivity"
            }),
            &mut tool_outputs,
        );
        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
                acp::ContentBlock::Text(text) => {
                    assert!(text.text.contains("rerouted"));
                    assert!(text.text.contains("gpt-5.4-codex"));
                }
                other => panic!("unexpected content block: {other:?}"),
            },
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn maps_model_safety_buffering_to_visible_message() {
        let mut tool_outputs = HashMap::new();
        let updates = session_updates_from_notification(
            "model/safetyBuffering/updated",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "model": "gpt-5.4",
                "fasterModel": "gpt-5.4-mini",
                "showBufferingUi": true,
                "reasons": ["policy", "latency"],
                "useCases": []
            }),
            &mut tool_outputs,
        );
        assert_eq!(updates.len(), 1);
        match &updates[0].1 {
            acp::SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
                acp::ContentBlock::Text(text) => {
                    assert!(text.text.contains("buffering safety checks"));
                    assert!(text.text.contains("gpt-5.4-mini"));
                }
                other => panic!("unexpected content block: {other:?}"),
            },
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn builds_history_updates_from_thread_read_response() {
        let updates = history_updates_from_thread_read(
            &acp::SessionId::new("thread-1"),
            &json!({
                "thread": {
                    "turns": [
                        {
                            "items": [
                                {
                                    "type": "userMessage",
                                    "content": [{"type": "text", "text": "hello"}]
                                },
                                {
                                    "type": "agentMessage",
                                    "text": "hi"
                                },
                                {
                                    "id": "cmd-1",
                                    "type": "commandExecution",
                                    "status": "completed",
                                    "command": "echo hi",
                                    "aggregatedOutput": "hi\n"
                                }
                            ]
                        }
                    ]
                }
            }),
        );

        assert!(matches!(
            updates.first(),
            Some(acp::SessionUpdate::UserMessageChunk(_))
        ));
        assert!(
            updates
                .iter()
                .any(|update| matches!(update, acp::SessionUpdate::AgentMessageChunk(_)))
        );
        assert!(
            updates
                .iter()
                .any(|update| matches!(update, acp::SessionUpdate::ToolCall(_)))
        );
        assert!(
            updates
                .iter()
                .any(|update| matches!(update, acp::SessionUpdate::ToolCallUpdate(_)))
        );
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
                    meta.get(agent_thread::TOOL_NAME_META_KEY),
                    Some(&json!(ZED_SPAWN_AGENT_TOOL_NAME))
                );
                assert_eq!(
                    meta.get(agent_thread::SUBAGENT_SESSION_INFO_META_KEY)
                        .and_then(|value| value.get("session_id"))
                        .and_then(Value::as_str),
                    Some("subagent-1")
                );
                assert_eq!(
                    meta.get(CODEX_NATIVE_COLLAB_META_KEY)
                        .and_then(|value| value.get("receiver_thread_ids"))
                        .and_then(Value::as_array)
                        .and_then(|ids| ids.first())
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

    #[gpui::test]
    async fn native_connection_starts_resumes_prompts_and_cancels_agent_thread(
        cx: &mut gpui::TestAppContext,
    ) {
        let (connection, requests) = test_codex_connection(vec![
            Ok(json!({"thread": {"id": "started-thread", "name": "Started thread"}})),
            Ok(json!({"thread": {"id": "resumed-thread", "name": "Resumed thread"}})),
            Ok(json!({"thread": {"id": "loaded-thread", "name": "Loaded thread"}})),
            Ok(json!({
                "thread": {
                    "turns": [{
                        "items": [
                            {"type": "userMessage", "content": [{"type": "text", "text": "Earlier prompt"}]},
                            {"type": "agentMessage", "text": "Earlier response"},
                        ],
                    }],
                },
            })),
            Ok(json!({"turn": {"id": "turn-1"}})),
            Ok(json!({})),
        ]);
        let project = test_project(cx).await;
        let work_dirs = PathList::new(&[Path::new("/project")]);

        let started_thread = cx
            .update(|cx| {
                connection
                    .clone()
                    .new_session(project.clone(), work_dirs.clone(), cx)
            })
            .await
            .expect("native Codex start should create an agent thread");
        assert_eq!(
            started_thread.read_with(cx, |thread, _| thread.session_id().to_string()),
            "started-thread"
        );
        assert_eq!(
            started_thread
                .read_with(cx, |thread, _| thread.title())
                .as_deref(),
            Some("Started thread")
        );
        assert!(started_thread.read_with(cx, |thread, _| thread.available_commands().len()) > 0);

        let resumed_thread = cx
            .update(|cx| {
                connection.clone().resume_session(
                    acp::SessionId::new("resumed-thread"),
                    project.clone(),
                    work_dirs.clone(),
                    Some("Existing title".into()),
                    cx,
                )
            })
            .await
            .expect("native Codex resume should create an agent thread");
        assert_eq!(
            resumed_thread.read_with(cx, |thread, _| thread.session_id().to_string()),
            "resumed-thread"
        );
        assert!(
            resumed_thread.read_with(cx, |thread, _| thread.entries().is_empty()),
            "resume without history should not replay transcript entries"
        );

        let loaded_thread = cx
            .update(|cx| {
                connection.clone().load_session(
                    acp::SessionId::new("loaded-thread"),
                    project.clone(),
                    work_dirs.clone(),
                    None,
                    cx,
                )
            })
            .await
            .expect("native Codex load should replay history into an agent thread");
        assert_eq!(
            loaded_thread.read_with(cx, |thread, _| thread.entries().len()),
            2
        );

        let prompt = acp::PromptRequest::new(
            started_thread.read_with(cx, |thread, _| thread.session_id().clone()),
            vec![acp::ContentBlock::Text(acp::TextContent::new(
                "Continue".to_owned(),
            ))],
        );
        let prompt_task =
            cx.update(|cx| connection.clone().prompt(UserMessageId::new(), prompt, cx));
        cx.run_until_parked();

        let started_session_id =
            started_thread.read_with(cx, |thread, _| thread.session_id().clone());
        assert_eq!(
            connection
                .sessions
                .borrow()
                .get(&started_session_id)
                .and_then(|session| session.active_turn_id.as_deref()),
            Some("turn-1")
        );
        cx.update(|cx| connection.cancel(&started_session_id, cx));
        complete_turn(
            &json!({
                "threadId": started_session_id.to_string(),
                "turn": {
                    "id": "turn-1",
                    "status": "interrupted",
                }
            }),
            &connection.sessions,
            &connection.pending_turns,
            &connection.completed_turns,
        );
        let prompt_response = prompt_task
            .await
            .expect("native Codex prompt should resolve after turn completion");
        assert_eq!(prompt_response.stop_reason, acp::StopReason::Cancelled);

        let methods = requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(method, _)| method.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec![
                "thread/start",
                "thread/resume",
                "thread/resume",
                "thread/read",
                "turn/start",
                "turn/interrupt",
            ]
        );
    }

    #[test]
    fn resolves_codex_path_from_explicit_setting() {
        let path = PathBuf::from("/tmp/custom-codex");
        let resolved = resolve_codex_path(Some(path.clone()), None).expect("path should resolve");
        assert_eq!(resolved, path);
    }

    fn current_config_value_for_test(
        options: &[acp::SessionConfigOption],
        config_id: &str,
    ) -> Option<String> {
        options
            .iter()
            .find(|option| option.id.to_string() == config_id)
            .and_then(|option| match &option.kind {
                acp::SessionConfigKind::Select(select) => Some(select.current_value.to_string()),
                _ => None,
            })
    }

    fn assert_text_content(content: Option<&Vec<acp::ToolCallContent>>, expected: &str) {
        let content = content.expect("content should exist");
        assert_eq!(content.len(), 1);
        match &content[0] {
            acp::ToolCallContent::Content(content) => match &content.content {
                acp::ContentBlock::Text(text) => assert_eq!(text.text, expected),
                other => panic!("unexpected content block: {other:?}"),
            },
            other => panic!("unexpected tool content: {other:?}"),
        }
    }

    fn assert_file_change_diff(
        update: &acp::SessionUpdate,
        expected_path: &str,
        expected_old_text: Option<&str>,
        expected_new_text: &str,
    ) {
        match update {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                let content = update
                    .fields
                    .content
                    .as_ref()
                    .expect("diff content should exist");
                assert_eq!(content.len(), 1);
                match &content[0] {
                    acp::ToolCallContent::Diff(diff) => {
                        assert_eq!(diff.path, PathBuf::from(expected_path));
                        assert_eq!(diff.old_text.as_deref(), expected_old_text);
                        assert_eq!(diff.new_text, expected_new_text);
                    }
                    other => panic!("expected diff content, got {other:?}"),
                }
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }
}

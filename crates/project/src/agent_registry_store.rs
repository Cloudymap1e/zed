use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use collections::HashMap;
use fs::Fs;
use futures::{AsyncReadExt, future::join_all};
use gpui::{
    App, AppContext as _, BackgroundExecutor, Context, Entity, FutureExt as _, Global,
    SharedString, Task, TaskExt,
};
use http_client::{AsyncBody, HttpClient, StatusCode};
use serde::Deserialize;
use settings::Settings as _;
use util::ResultExt;

use crate::{AgentId, DisableAiSettings, canonical_agent_id};

const BUNDLED_REGISTRY_SNAPSHOT_B64: &str = include_str!("agent_registry_snapshot.b64");
const REFRESH_THROTTLE_DURATION: Duration = Duration::from_secs(60 * 60);
// Bound the full request lifecycle, including response body reads; the shared
// HTTP client only has a connect timeout.
const REGISTRY_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRY_ICON_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct RegistryAgentMetadata {
    pub id: AgentId,
    pub name: SharedString,
    pub description: SharedString,
    pub version: SharedString,
    pub repository: Option<SharedString>,
    pub website: Option<SharedString>,
    pub icon_path: Option<SharedString>,
}

#[derive(Clone, Debug)]
pub struct RegistryBinaryAgent {
    pub metadata: RegistryAgentMetadata,
    pub targets: HashMap<String, RegistryTargetConfig>,
    pub supports_current_platform: bool,
}

#[derive(Clone, Debug)]
pub struct RegistryNpxAgent {
    pub metadata: RegistryAgentMetadata,
    pub package: SharedString,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub enum RegistryAgent {
    Binary(RegistryBinaryAgent),
    Npx(RegistryNpxAgent),
}

impl RegistryAgent {
    pub fn metadata(&self) -> &RegistryAgentMetadata {
        match self {
            RegistryAgent::Binary(agent) => &agent.metadata,
            RegistryAgent::Npx(agent) => &agent.metadata,
        }
    }

    pub fn id(&self) -> &AgentId {
        &self.metadata().id
    }

    pub fn name(&self) -> &SharedString {
        &self.metadata().name
    }

    pub fn description(&self) -> &SharedString {
        &self.metadata().description
    }

    pub fn version(&self) -> &SharedString {
        &self.metadata().version
    }

    pub fn repository(&self) -> Option<&SharedString> {
        self.metadata().repository.as_ref()
    }

    pub fn website(&self) -> Option<&SharedString> {
        self.metadata().website.as_ref()
    }

    pub fn icon_path(&self) -> Option<&SharedString> {
        self.metadata().icon_path.as_ref()
    }

    pub fn supports_current_platform(&self) -> bool {
        match self {
            RegistryAgent::Binary(agent) => agent.supports_current_platform,
            RegistryAgent::Npx(_) => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RegistryTargetConfig {
    pub archive: String,
    pub cmd: String,
    pub args: Vec<String>,
    pub sha256: Option<String>,
    pub env: HashMap<String, String>,
}

struct GlobalAgentRegistryStore(Entity<AgentRegistryStore>);

impl Global for GlobalAgentRegistryStore {}

pub struct AgentRegistryStore {
    fs: Arc<dyn Fs>,
    http_client: Arc<dyn HttpClient>,
    agents: Vec<RegistryAgent>,
    is_fetching: bool,
    fetch_error: Option<SharedString>,
    pending_refresh: Option<Task<()>>,
    last_refresh: Option<Instant>,
}

impl AgentRegistryStore {
    /// Initialize the global AgentRegistryStore.
    ///
    /// This loads the cached registry from disk, falling back to the bundled
    /// external-agent registry snapshot.
    pub fn init_global(
        cx: &mut App,
        fs: Arc<dyn Fs>,
        http_client: Arc<dyn HttpClient>,
    ) -> Entity<Self> {
        if let Some(store) = Self::try_global(cx) {
            return store;
        }

        let store = cx.new(|cx| Self::new(fs, http_client, cx));
        cx.set_global(GlobalAgentRegistryStore(store.clone()));

        store
    }

    pub fn global(cx: &App) -> Entity<Self> {
        cx.global::<GlobalAgentRegistryStore>().0.clone()
    }

    pub fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalAgentRegistryStore>()
            .map(|store| store.0.clone())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn init_test_global(cx: &mut App, agents: Vec<RegistryAgent>) -> Entity<Self> {
        let fs: Arc<dyn Fs> = fs::FakeFs::new(cx.background_executor().clone());
        let store = cx.new(|_cx| Self {
            fs,
            http_client: http_client::FakeHttpClient::with_404_response(),
            agents,
            is_fetching: false,
            fetch_error: None,
            pending_refresh: None,
            last_refresh: None,
        });
        cx.set_global(GlobalAgentRegistryStore(store.clone()));
        store
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn set_agents(&mut self, agents: Vec<RegistryAgent>, cx: &mut Context<Self>) {
        self.agents = agents;
        cx.notify();
    }

    pub fn agents(&self) -> &[RegistryAgent] {
        &self.agents
    }

    pub fn agent(&self, id: &AgentId) -> Option<&RegistryAgent> {
        let id = canonical_agent_id(id.as_ref());
        self.agents.iter().find(|agent| agent.id().as_ref() == id)
    }

    pub fn is_fetching(&self) -> bool {
        self.is_fetching
    }

    pub fn fetch_error(&self) -> Option<SharedString> {
        self.fetch_error.clone()
    }

    /// Refresh the registry from the network.
    ///
    /// This will fetch the latest registry data and update the cache.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.pending_refresh.is_some() {
            return;
        }

        if DisableAiSettings::get_global(cx).disable_ai {
            return;
        }

        if remote_registry_url().is_none() && !self.agents.is_empty() {
            return;
        }

        self.is_fetching = true;
        self.fetch_error = None;
        self.last_refresh = Some(Instant::now());
        cx.notify();

        let fs = self.fs.clone();
        let http_client = self.http_client.clone();
        let executor = cx.background_executor().clone();

        self.pending_refresh = Some(cx.spawn(async move |this, cx| {
            let result = match fetch_registry_index(http_client.clone(), &executor).await {
                Ok(data) => {
                    build_registry_agents(
                        fs.clone(),
                        http_client,
                        data.index,
                        data.raw_body,
                        data.update_cache,
                        &executor,
                    )
                    .await
                }
                Err(error) => {
                    log::error!("AgentRegistryStore::refresh: fetch failed: {error:#}");
                    Err(error)
                }
            };

            this.update(cx, |this, cx| {
                this.pending_refresh = None;
                this.is_fetching = false;
                match result {
                    Ok(agents) => {
                        this.agents = agents;
                        this.fetch_error = None;
                    }
                    Err(error) => {
                        this.fetch_error = Some(SharedString::from(format!("{error:#}")));
                    }
                }
                cx.notify();
            })
            .ok();
        }));
    }

    /// Refresh the registry if it hasn't been refreshed recently.
    ///
    /// This is useful to call when using a registry-based agent to check for
    /// updates without making too many network requests. The refresh is
    /// throttled to at most once per hour.
    pub fn refresh_if_stale(&mut self, cx: &mut Context<Self>) {
        let should_refresh = self
            .last_refresh
            .map(|last| last.elapsed() >= REFRESH_THROTTLE_DURATION)
            .unwrap_or(true);

        if should_refresh {
            self.refresh(cx);
        }
    }

    fn new(fs: Arc<dyn Fs>, http_client: Arc<dyn HttpClient>, cx: &mut Context<Self>) -> Self {
        let mut store = Self {
            fs: fs.clone(),
            http_client,
            agents: Vec::new(),
            is_fetching: false,
            fetch_error: None,
            pending_refresh: None,
            last_refresh: None,
        };

        store.load_cached_or_bundled_registry(fs, store.http_client.clone(), cx);

        store
    }

    fn load_cached_or_bundled_registry(
        &mut self,
        fs: Arc<dyn Fs>,
        http_client: Arc<dyn HttpClient>,
        cx: &mut Context<Self>,
    ) {
        if DisableAiSettings::get_global(cx).disable_ai {
            return;
        }

        cx.spawn(async move |this, cx| -> Result<()> {
            let cache_path = registry_cache_path();
            let (index, bytes) = if fs.is_file(&cache_path).await {
                match fs.load_bytes(&cache_path).await {
                    Ok(bytes) => match parse_registry_index(bytes, "cached registry") {
                        Ok(data) => data,
                        Err(error) => {
                            log::warn!("failed to load cached external-agent registry: {error:#}");
                            bundled_registry_index()?
                        }
                    },
                    Err(error) => {
                        log::warn!("failed to read cached external-agent registry: {error:#}");
                        bundled_registry_index()?
                    }
                }
            } else {
                bundled_registry_index()?
            };

            let executor = cx.background_executor().clone();
            let agents =
                build_registry_agents(fs, http_client, index, bytes, false, &executor).await?;

            this.update(cx, |this, cx| {
                this.agents = agents;
                cx.notify();
            })?;

            Ok(())
        })
        .detach_and_log_err(cx);
    }
}

struct RegistryFetchResult {
    index: RegistryIndex,
    raw_body: Vec<u8>,
    update_cache: bool,
}

async fn fetch_registry_index(
    http_client: Arc<dyn HttpClient>,
    executor: &BackgroundExecutor,
) -> Result<RegistryFetchResult> {
    let Some(registry_url) = remote_registry_url() else {
        let (index, raw_body) = bundled_registry_index()?;
        return Ok(RegistryFetchResult {
            index,
            raw_body,
            update_cache: false,
        });
    };

    let (status, body) =
        fetch_url_body(http_client, registry_url, REGISTRY_FETCH_TIMEOUT, executor)
            .await
            .context("fetching agent registry")?;

    if status.is_client_error() {
        let text = String::from_utf8_lossy(body.as_slice());
        bail!(
            "registry status error {}, response: {text:?}",
            status.as_u16()
        );
    }

    let (index, body) = parse_registry_index(body, "agent registry")?;
    Ok(RegistryFetchResult {
        index,
        raw_body: body,
        update_cache: true,
    })
}

fn remote_registry_url() -> Option<&'static str> {
    None
}

fn bundled_registry_index() -> Result<(RegistryIndex, Vec<u8>)> {
    let bytes = BASE64
        .decode(BUNDLED_REGISTRY_SNAPSHOT_B64.lines().collect::<String>())
        .context("decoding bundled external-agent registry")?;
    parse_registry_index(bytes, "bundled external-agent registry")
}

fn parse_registry_index(bytes: Vec<u8>, context: &'static str) -> Result<(RegistryIndex, Vec<u8>)> {
    let index: RegistryIndex =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {context}"))?;
    Ok((index, bytes))
}

async fn build_registry_agents(
    fs: Arc<dyn Fs>,
    http_client: Arc<dyn HttpClient>,
    index: RegistryIndex,
    raw_body: Vec<u8>,
    update_cache: bool,
    executor: &BackgroundExecutor,
) -> Result<Vec<RegistryAgent>> {
    let cache_dir = registry_cache_dir();
    fs.create_dir(&cache_dir).await?;

    let cache_path = cache_dir.join("registry.json");
    if update_cache {
        fs.write(&cache_path, &raw_body).await?;
    }

    let icons_dir = cache_dir.join("icons");
    if update_cache {
        fs.create_dir(&icons_dir).await?;
    }

    let current_platform = current_platform_key();
    let icon_paths = resolve_icon_paths(
        &index.agents,
        &icons_dir,
        update_cache,
        fs.clone(),
        http_client.clone(),
        executor,
    )
    .await;

    let mut agents: Vec<RegistryAgent> = Vec::new();
    for (entry, icon_path) in index.agents.into_iter().zip(icon_paths) {
        let raw_id = entry.id.clone();
        let metadata = RegistryAgentMetadata {
            id: AgentId::new(entry.id),
            name: entry.name.into(),
            description: entry.description.into(),
            version: entry.version.into(),
            repository: entry.repository.map(Into::into),
            website: entry.website.map(Into::into),
            icon_path,
        };

        let binary_agent = entry.distribution.binary.as_ref().and_then(|binary| {
            if binary.is_empty() {
                return None;
            }

            let mut targets = HashMap::default();
            for (platform, target) in binary.iter() {
                targets.insert(
                    platform.clone(),
                    RegistryTargetConfig {
                        archive: target.archive.clone(),
                        cmd: target.cmd.clone(),
                        args: target.args.clone(),
                        sha256: None,
                        env: target.env.clone(),
                    },
                );
            }

            let supports_current_platform = current_platform
                .as_ref()
                .is_some_and(|platform| targets.contains_key(*platform));

            Some(RegistryBinaryAgent {
                metadata: metadata.clone(),
                targets,
                supports_current_platform,
            })
        });

        let npx_agent = entry.distribution.npx.as_ref().map(|npx| RegistryNpxAgent {
            metadata: metadata.clone(),
            package: npx.package.clone().into(),
            args: npx.args.clone(),
            env: npx.env.clone(),
        });

        let agent = match (binary_agent, npx_agent) {
            (Some(binary_agent), Some(npx_agent)) => {
                if binary_agent.supports_current_platform {
                    RegistryAgent::Binary(binary_agent)
                } else {
                    RegistryAgent::Npx(npx_agent)
                }
            }
            (Some(binary_agent), None) => RegistryAgent::Binary(binary_agent),
            (None, Some(npx_agent)) => RegistryAgent::Npx(npx_agent),
            (None, None) => continue,
        };

        if let Some(existing_ix) = agents
            .iter()
            .position(|existing| existing.id() == agent.id())
        {
            if raw_id == agent.id().as_ref() {
                agents[existing_ix] = agent;
            }
            continue;
        }

        agents.push(agent);
    }

    Ok(agents)
}

async fn resolve_icon_paths(
    entries: &[RegistryEntry],
    icons_dir: &Path,
    update_cache: bool,
    fs: Arc<dyn Fs>,
    http_client: Arc<dyn HttpClient>,
    executor: &BackgroundExecutor,
) -> Vec<Option<SharedString>> {
    join_all(entries.iter().map(|entry| {
        let fs = fs.clone();
        let http_client = http_client.clone();
        async move {
            resolve_icon_path(entry, icons_dir, update_cache, fs, http_client, executor)
                .await
                .log_err()
                .flatten()
        }
    }))
    .await
}

async fn resolve_icon_path(
    entry: &RegistryEntry,
    icons_dir: &Path,
    update_cache: bool,
    fs: Arc<dyn Fs>,
    http_client: Arc<dyn HttpClient>,
    executor: &BackgroundExecutor,
) -> Result<Option<SharedString>> {
    let icon_url = resolve_icon_url(entry);
    let Some(icon_url) = icon_url else {
        return Ok(None);
    };

    let icon_path = icons_dir.join(format!("{}.svg", entry.id));
    if update_cache && !fs.is_file(&icon_path).await {
        if let Err(error) = download_icon(fs.clone(), http_client, &icon_url, entry, executor).await
        {
            log::warn!(
                "Failed to download agent registry icon for {}: {error:#}",
                entry.id
            );
        }
    }

    if fs.is_file(&icon_path).await {
        Ok(Some(SharedString::from(
            icon_path.to_string_lossy().into_owned(),
        )))
    } else {
        Ok(None)
    }
}

async fn download_icon(
    fs: Arc<dyn Fs>,
    http_client: Arc<dyn HttpClient>,
    icon_url: &str,
    entry: &RegistryEntry,
    executor: &BackgroundExecutor,
) -> Result<()> {
    let (status, body) =
        fetch_url_body(http_client, icon_url, REGISTRY_ICON_FETCH_TIMEOUT, executor)
            .await
            .with_context(|| format!("fetching icon for {}", entry.id))?;

    if status.is_client_error() {
        let text = String::from_utf8_lossy(body.as_slice());
        bail!("icon status error {}, response: {text:?}", status.as_u16());
    }

    let icon_path = registry_cache_dir()
        .join("icons")
        .join(format!("{}.svg", entry.id));
    fs.write(&icon_path, &body).await?;
    Ok(())
}

async fn fetch_url_body(
    http_client: Arc<dyn HttpClient>,
    url: &str,
    timeout: Duration,
    executor: &BackgroundExecutor,
) -> Result<(StatusCode, Vec<u8>)> {
    async {
        let mut response = http_client
            .get(url, AsyncBody::default(), true)
            .await
            .with_context(|| format!("requesting {url}"))?;

        let status = response.status();
        let mut body = Vec::new();
        response
            .body_mut()
            .read_to_end(&mut body)
            .await
            .with_context(|| format!("reading response from {url}"))?;

        Ok((status, body))
    }
    .with_timeout(timeout, executor)
    .await
    .map_err(|_| {
        anyhow!(
            "timed out after {}s while fetching {url}",
            timeout.as_secs()
        )
    })?
}

fn resolve_icon_url(entry: &RegistryEntry) -> Option<String> {
    let icon = entry.icon.as_ref()?;
    if icon.starts_with("https://") || icon.starts_with("http://") {
        return Some(icon.to_string());
    }

    None
}

fn current_platform_key() -> Option<&'static str> {
    let os = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        return None;
    };

    let arch = if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else {
        return None;
    };

    Some(match os {
        "darwin" => match arch {
            "aarch64" => "darwin-aarch64",
            "x86_64" => "darwin-x86_64",
            _ => return None,
        },
        "linux" => match arch {
            "aarch64" => "linux-aarch64",
            "x86_64" => "linux-x86_64",
            _ => return None,
        },
        "windows" => match arch {
            "aarch64" => "windows-aarch64",
            "x86_64" => "windows-x86_64",
            _ => return None,
        },
        _ => return None,
    })
}

fn registry_cache_dir() -> PathBuf {
    paths::external_agents_dir().join("registry")
}

fn registry_cache_path() -> PathBuf {
    registry_cache_dir().join("registry.json")
}

#[derive(Deserialize)]
struct RegistryIndex {
    #[serde(rename = "version")]
    _version: String,
    agents: Vec<RegistryEntry>,
}

#[derive(Deserialize)]
struct RegistryEntry {
    id: String,
    name: String,
    version: String,
    description: String,
    #[serde(default)]
    repository: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    icon: Option<String>,
    distribution: RegistryDistribution,
}

#[derive(Deserialize)]
struct RegistryDistribution {
    #[serde(default)]
    binary: Option<HashMap<String, RegistryBinaryTarget>>,
    #[serde(default)]
    npx: Option<RegistryNpxDistribution>,
}

#[derive(Deserialize)]
struct RegistryBinaryTarget {
    archive: String,
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
}

#[derive(Deserialize)]
struct RegistryNpxDistribution {
    package: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::FakeFs;
    use gpui::TestAppContext;
    use http_client::FakeHttpClient;

    fn registry_entry(id: &str, package: &str, version: &str) -> RegistryEntry {
        RegistryEntry {
            id: id.to_string(),
            name: format!("{id} name"),
            version: version.to_string(),
            description: format!("{id} description"),
            repository: None,
            website: None,
            icon: None,
            distribution: RegistryDistribution {
                binary: None,
                npx: Some(RegistryNpxDistribution {
                    package: package.to_string(),
                    args: Vec::new(),
                    env: HashMap::default(),
                }),
            },
        }
    }

    #[test]
    fn resolve_icon_url_keeps_absolute_urls() {
        let mut entry = registry_entry("test-agent", "@example/test-agent", "1.0.0");
        entry.icon = Some("https://example.com/test-agent.svg".to_string());

        assert_eq!(
            resolve_icon_url(&entry).expect("resolve absolute icon url"),
            "https://example.com/test-agent.svg"
        );
    }

    #[test]
    fn resolve_icon_url_ignores_relative_urls_without_remote_registry() {
        let mut entry = registry_entry("test-agent", "@example/test-agent", "1.0.0");
        entry.icon = Some("./icon.svg".to_string());

        assert_eq!(resolve_icon_url(&entry), None);
    }

    #[gpui::test]
    async fn build_registry_agents_canonicalizes_legacy_ids(cx: &mut TestAppContext) {
        let executor = cx.executor();
        let fs = FakeFs::new(executor.clone());
        let http_client = FakeHttpClient::with_404_response();
        let agents = build_registry_agents(
            fs,
            http_client,
            RegistryIndex {
                _version: "1.0.0".into(),
                agents: vec![registry_entry(
                    crate::LEGACY_CODEX_AGENT_ID,
                    "@example/codex",
                    "1.0.0",
                )],
            },
            Vec::new(),
            false,
            &executor,
        )
        .await
        .expect("build registry agents");

        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].id(), &AgentId::new(crate::CODEX_AGENT_ID));
    }

    #[gpui::test]
    async fn build_registry_agents_prefers_neutral_id_over_legacy_duplicate(
        cx: &mut TestAppContext,
    ) {
        let executor = cx.executor();
        let fs = FakeFs::new(executor.clone());
        let http_client = FakeHttpClient::with_404_response();
        let agents = build_registry_agents(
            fs,
            http_client,
            RegistryIndex {
                _version: "1.0.0".into(),
                agents: vec![
                    registry_entry(crate::LEGACY_CODEX_AGENT_ID, "@example/legacy", "1.0.0"),
                    registry_entry(crate::CODEX_AGENT_ID, "@example/neutral", "2.0.0"),
                ],
            },
            Vec::new(),
            false,
            &executor,
        )
        .await
        .expect("build registry agents");

        assert_eq!(agents.len(), 1);
        match &agents[0] {
            RegistryAgent::Npx(agent) => {
                assert_eq!(agent.metadata.id, AgentId::new(crate::CODEX_AGENT_ID));
                assert_eq!(agent.package.as_ref(), "@example/neutral");
                assert_eq!(agent.metadata.version.as_ref(), "2.0.0");
            }
            RegistryAgent::Binary(_) => panic!("expected npx registry agent"),
        }
    }
}

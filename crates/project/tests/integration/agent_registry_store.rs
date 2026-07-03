use std::{future, sync::Arc};

use fs::{FakeFs, Fs as _};
use gpui::TestAppContext;
use http_client::{AsyncBody, FakeHttpClient, HttpClient, Response};
use project::AgentRegistryStore;
use serde_json::json;

use crate::init_test;

#[gpui::test]
async fn registry_initializes_from_bundled_snapshot_without_network(cx: &mut TestAppContext) {
    init_test(cx);

    let fs = FakeFs::new(cx.executor());
    let http_client =
        FakeHttpClient::create(|_| future::pending::<anyhow::Result<Response<AsyncBody>>>())
            as Arc<dyn HttpClient>;

    let registry_store =
        cx.update(|cx| AgentRegistryStore::init_global(cx, fs.clone(), http_client));
    cx.run_until_parked();

    registry_store.update(cx, |store, _| {
        assert!(!store.is_fetching());
        assert_eq!(store.fetch_error(), None);
        assert!(
            store
                .agents()
                .iter()
                .any(|agent| agent.id().as_ref() == "codex")
        );
        assert!(
            store
                .agents()
                .iter()
                .any(|agent| agent.id().as_ref() == "claude")
        );
    });
}

#[gpui::test]
async fn registry_refresh_uses_bundled_snapshot_without_network(cx: &mut TestAppContext) {
    init_test(cx);

    let fs = FakeFs::new(cx.executor());
    let http_client =
        FakeHttpClient::create(|_| future::pending::<anyhow::Result<Response<AsyncBody>>>())
            as Arc<dyn HttpClient>;

    let registry_store =
        cx.update(|cx| AgentRegistryStore::init_global(cx, fs.clone(), http_client));
    cx.run_until_parked();

    registry_store.update(cx, |store, cx| store.refresh(cx));
    cx.run_until_parked();

    registry_store.update(cx, |store, _| {
        assert!(!store.is_fetching());
        assert_eq!(store.fetch_error(), None);
        assert!(
            store
                .agents()
                .iter()
                .any(|agent| agent.id().as_ref() == "codex")
        );
    });
}

#[gpui::test]
async fn registry_loads_cached_registry_without_network(cx: &mut TestAppContext) {
    init_test(cx);

    let fs = FakeFs::new(cx.executor());
    let cache_dir = paths::external_agents_dir().join("registry");
    fs.create_dir(&cache_dir).await.unwrap();
    fs.write(
        &cache_dir.join("registry.json"),
        serde_json::to_string(&json!({
            "version": "1",
            "agents": [
                {
                    "id": "cached-agent-a",
                    "name": "Cached Agent A",
                    "version": "1.0.0",
                    "description": "An agent loaded from cache.",
                    "distribution": {
                        "npx": {
                            "package": "cached-agent-a"
                        }
                    }
                },
                {
                    "id": "cached-agent-b",
                    "name": "Cached Agent B",
                    "version": "1.0.0",
                    "description": "Another cached agent.",
                    "distribution": {
                        "npx": {
                            "package": "cached-agent-b"
                        }
                    }
                },
                {
                    "id": "cached-agent-c",
                    "name": "Cached Agent C",
                    "version": "1.0.0",
                    "description": "A third cached agent.",
                    "distribution": {
                        "npx": {
                            "package": "cached-agent-c"
                        }
                    }
                }
            ]
        }))
        .unwrap()
        .as_bytes(),
    )
    .await
    .unwrap();

    let http_client =
        FakeHttpClient::create(|_| future::pending::<anyhow::Result<Response<AsyncBody>>>())
            as Arc<dyn HttpClient>;

    let registry_store =
        cx.update(|cx| AgentRegistryStore::init_global(cx, fs.clone(), http_client));
    cx.run_until_parked();

    registry_store.update(cx, |store, _| {
        assert!(!store.is_fetching());
        assert_eq!(store.agents().len(), 3);
        assert_eq!(store.agents()[0].id().as_ref(), "cached-agent-a");
        assert_eq!(store.agents()[1].id().as_ref(), "cached-agent-b");
        assert_eq!(store.agents()[2].id().as_ref(), "cached-agent-c");
        assert_eq!(store.fetch_error(), None);
    });
}

use anyhow::Result;
use serde_json::Value;

use crate::migrations::migrate_settings;

const AGENT_SERVERS_KEY: &str = "agent_servers";
pub(crate) const LEGACY_CLAUDE_REGISTRY_KEY: &str = "claude-acp";
pub(crate) const LEGACY_CODEX_REGISTRY_KEY: &str = "codex-acp";
const CLAUDE_REGISTRY_KEY: &str = "claude";
const CODEX_REGISTRY_KEY: &str = "codex";

const LEGACY_MAPPINGS: &[(&str, &str)] = &[
    (LEGACY_CLAUDE_REGISTRY_KEY, CLAUDE_REGISTRY_KEY),
    (LEGACY_CODEX_REGISTRY_KEY, CODEX_REGISTRY_KEY),
];

pub fn migrate_legacy_agent_registry_ids(value: &mut Value) -> Result<()> {
    migrate_settings(value, &mut migrate_one)
}

fn migrate_one(obj: &mut serde_json::Map<String, Value>) -> Result<()> {
    let Some(agent_servers) = obj.get_mut(AGENT_SERVERS_KEY) else {
        return Ok(());
    };
    let Some(servers_map) = agent_servers.as_object_mut() else {
        return Ok(());
    };

    for (legacy_key, registry_key) in LEGACY_MAPPINGS {
        if servers_map.contains_key(*registry_key) {
            servers_map.remove(*legacy_key);
        } else if let Some(value) = servers_map.remove(*legacy_key) {
            servers_map.insert((*registry_key).to_string(), value);
        }
    }

    Ok(())
}

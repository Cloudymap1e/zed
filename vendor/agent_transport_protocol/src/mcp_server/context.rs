use crate::{ConnectionTo, role::Role};

/// Context about the agent transport and MCP connection available to an MCP server.
#[derive(Clone, Debug)]
pub struct McpConnectionTo<Counterpart: Role> {
    pub(super) agent_transport_id: String,
    pub(super) connection: ConnectionTo<Counterpart>,
}

impl<Counterpart: Role> McpConnectionTo<Counterpart> {
    /// The agent transport identifier for this MCP server (e.g., `"agent-transport:UUID"`).
    pub fn agent_transport_id(&self) -> String {
        self.agent_transport_id.clone()
    }

    /// The `agent-transport:UUID` that was given.
    #[deprecated(since = "0.12.0", note = "renamed to `agent_transport_id()`")]
    pub fn agent_transport_url(&self) -> String {
        self.agent_transport_id()
    }

    /// The host connection context.
    ///
    /// If this MCP server is hosted inside of an agent transport context, this will be the agent transport connection context.
    pub fn connection_to(&self) -> ConnectionTo<Counterpart> {
        self.connection.clone()
    }
}

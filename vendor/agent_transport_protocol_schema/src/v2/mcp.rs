//! MCP-over-agent-transport transport types.

use std::sync::Arc;

use derive_more::{Display, From};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_with::skip_serializing_none;

use super::{McpServerAgentTransportId, Meta};
use crate::IntoOption;

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// A unique identifier for an active MCP-over-agent-transport connection.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash, Display, From)]
#[serde(transparent)]
#[from(Arc<str>, String, &'static str)]
#[non_exhaustive]
pub struct McpConnectionId(pub Arc<str>);

impl McpConnectionId {
    /// Wraps a protocol string as a typed [`McpConnectionId`].
    #[must_use]
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Request parameters for `mcp/connect`.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-side" = "client", "x-method" = MCP_CONNECT_METHOD_NAME))]
#[non_exhaustive]
pub struct ConnectMcpRequest {
    /// The agent transport MCP server ID that was provided by the component declaring the MCP server.
    #[serde(rename = "agentTransportId")]
    pub agent_transport_id: McpServerAgentTransportId,
    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[serde(rename = "_meta")]
    pub meta: Option<Meta>,
}

impl ConnectMcpRequest {
    /// Builds [`ConnectMcpRequest`] with the required request fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new(agent_transport_id: impl Into<McpServerAgentTransportId>) -> Self {
        Self {
            agent_transport_id: agent_transport_id.into(),
            meta: None,
        }
    }

    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[must_use]
    pub fn meta(mut self, meta: impl IntoOption<Meta>) -> Self {
        self.meta = meta.into_option();
        self
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Response to `mcp/connect`.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-side" = "client", "x-method" = MCP_CONNECT_METHOD_NAME))]
#[non_exhaustive]
pub struct ConnectMcpResponse {
    /// The unique identifier for this MCP-over-agent-transport connection.
    pub connection_id: McpConnectionId,
    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[serde(rename = "_meta")]
    pub meta: Option<Meta>,
}

impl ConnectMcpResponse {
    /// Builds [`ConnectMcpResponse`] with the required response fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new(connection_id: impl Into<McpConnectionId>) -> Self {
        Self {
            connection_id: connection_id.into(),
            meta: None,
        }
    }

    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[must_use]
    pub fn meta(mut self, meta: impl IntoOption<Meta>) -> Self {
        self.meta = meta.into_option();
        self
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Request parameters for `mcp/message`.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-side" = "both", "x-method" = MCP_MESSAGE_METHOD_NAME))]
#[non_exhaustive]
pub struct MessageMcpRequest {
    /// The MCP-over-agent-transport connection this message is sent on.
    pub connection_id: McpConnectionId,
    /// The inner MCP method name.
    pub method: String,
    /// Optional inner MCP params.
    ///
    /// If omitted or set to `null`, the inner MCP message has no params.
    #[serde(default)]
    pub params: Option<serde_json::Map<String, serde_json::Value>>,
    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[serde(rename = "_meta")]
    pub meta: Option<Meta>,
}

impl MessageMcpRequest {
    /// Builds [`MessageMcpRequest`] with the required request fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new(connection_id: impl Into<McpConnectionId>, method: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            method: method.into(),
            params: None,
            meta: None,
        }
    }

    /// Optional inner MCP params.
    ///
    /// If omitted or set to `null`, the inner MCP message has no params.
    #[must_use]
    pub fn params(
        mut self,
        params: impl IntoOption<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        self.params = params.into_option();
        self
    }

    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[must_use]
    pub fn meta(mut self, meta: impl IntoOption<Meta>) -> Self {
        self.meta = meta.into_option();
        self
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Notification parameters for `mcp/message`.
///
/// This is used when the wrapped MCP message is a notification and the outer JSON-RPC
/// envelope has no `id`.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-side" = "both", "x-method" = MCP_MESSAGE_METHOD_NAME))]
#[non_exhaustive]
pub struct MessageMcpNotification {
    /// The MCP-over-agent-transport connection this message is sent on.
    pub connection_id: McpConnectionId,
    /// The inner MCP method name.
    pub method: String,
    /// Optional inner MCP params.
    ///
    /// If omitted or set to `null`, the inner MCP message has no params.
    #[serde(default)]
    pub params: Option<serde_json::Map<String, serde_json::Value>>,
    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[serde(rename = "_meta")]
    pub meta: Option<Meta>,
}

impl MessageMcpNotification {
    /// Builds [`MessageMcpNotification`] with the required notification fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new(connection_id: impl Into<McpConnectionId>, method: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            method: method.into(),
            params: None,
            meta: None,
        }
    }

    /// Optional inner MCP params.
    ///
    /// If omitted or set to `null`, the inner MCP message has no params.
    #[must_use]
    pub fn params(
        mut self,
        params: impl IntoOption<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        self.params = params.into_option();
        self
    }

    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[must_use]
    pub fn meta(mut self, meta: impl IntoOption<Meta>) -> Self {
        self.meta = meta.into_option();
        self
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Response to `mcp/message`.
///
/// This is the inner MCP response result payload. Any JSON value is valid.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, From)]
#[serde(transparent)]
#[schemars(extend("x-side" = "both", "x-method" = MCP_MESSAGE_METHOD_NAME))]
#[non_exhaustive]
pub struct MessageMcpResponse(#[schemars(with = "serde_json::Value")] pub Arc<RawValue>);

impl MessageMcpResponse {
    /// Builds [`MessageMcpResponse`] with the required response fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new(result: Arc<RawValue>) -> Self {
        Self(result)
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Request parameters for `mcp/disconnect`.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-side" = "client", "x-method" = MCP_DISCONNECT_METHOD_NAME))]
#[non_exhaustive]
pub struct DisconnectMcpRequest {
    /// The MCP-over-agent-transport connection to close.
    pub connection_id: McpConnectionId,
    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[serde(rename = "_meta")]
    pub meta: Option<Meta>,
}

impl DisconnectMcpRequest {
    /// Builds [`DisconnectMcpRequest`] with the required request fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new(connection_id: impl Into<McpConnectionId>) -> Self {
        Self {
            connection_id: connection_id.into(),
            meta: None,
        }
    }

    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[must_use]
    pub fn meta(mut self, meta: impl IntoOption<Meta>) -> Self {
        self.meta = meta.into_option();
        self
    }
}

/// **UNSTABLE**
///
/// This capability is not part of the spec yet, and may be removed or changed at any point.
///
/// Response to `mcp/disconnect`.
#[skip_serializing_none]
#[derive(Default, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-side" = "client", "x-method" = MCP_DISCONNECT_METHOD_NAME))]
#[non_exhaustive]
pub struct DisconnectMcpResponse {
    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[serde(rename = "_meta")]
    pub meta: Option<Meta>,
}

impl DisconnectMcpResponse {
    /// Builds [`DisconnectMcpResponse`] with the required response fields set; optional fields start unset or empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The _meta property is reserved by agent transport to allow clients and agents to attach additional
    /// metadata to their interactions. Implementations MUST NOT make assumptions about values at
    /// these keys.
    ///
    /// See protocol docs: [Extensibility](https://agent-transport-protocol/protocol/extensibility)
    #[must_use]
    pub fn meta(mut self, meta: impl IntoOption<Meta>) -> Self {
        self.meta = meta.into_option();
        self
    }
}

/// Method name for opening an MCP-over-agent-transport connection.
pub(crate) const MCP_CONNECT_METHOD_NAME: &str = "mcp/connect";
/// Method name for exchanging MCP-over-agent-transport messages.
pub(crate) const MCP_MESSAGE_METHOD_NAME: &str = "mcp/message";
/// Method name for closing an MCP-over-agent-transport connection.
pub(crate) const MCP_DISCONNECT_METHOD_NAME: &str = "mcp/disconnect";

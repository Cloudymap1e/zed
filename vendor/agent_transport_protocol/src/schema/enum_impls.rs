//! JsonRpcMessage and JsonRpcNotification/JsonRpcRequest implementations for
//! the agent transport enum types from agent-transport-protocol-schema.

use crate::schema::v1::{
    AgentNotification, AgentRequest, AgentResponse, ClientNotification, ClientRequest,
    ClientResponse,
};

// ============================================================================
// Agent side (messages that agents receive)
// ============================================================================

impl_jsonrpc_request_enum!(ClientRequest {
    InitializeRequest => "initialize",
    AuthenticateRequest => "authenticate",
    LogoutRequest => "logout",
    NewSessionRequest => "session/new",
    LoadSessionRequest => "session/load",
    ListSessionsRequest => "session/list",
    DeleteSessionRequest => "session/delete",
    #[cfg(feature = "unstable_session_fork")]
    ForkSessionRequest => "session/fork",
    ResumeSessionRequest => "session/resume",
    CloseSessionRequest => "session/close",
    SetSessionModeRequest => "session/set_mode",
    SetSessionConfigOptionRequest => "session/set_config_option",
    PromptRequest => "session/prompt",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    MessageMcpRequest => "mcp/message",
    [ext] ExtMethodRequest,
});

impl_jsonrpc_response_enum!(AgentResponse {
    InitializeResponse => "initialize",
    AuthenticateResponse => "authenticate",
    LogoutResponse => "logout",
    NewSessionResponse => "session/new",
    LoadSessionResponse => "session/load",
    ListSessionsResponse => "session/list",
    DeleteSessionResponse => "session/delete",
    #[cfg(feature = "unstable_session_fork")]
    ForkSessionResponse => "session/fork",
    ResumeSessionResponse => "session/resume",
    CloseSessionResponse => "session/close",
    SetSessionModeResponse => "session/set_mode",
    SetSessionConfigOptionResponse => "session/set_config_option",
    PromptResponse => "session/prompt",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    MessageMcpResponse => "mcp/message",
    [ext] ExtMethodResponse,
});

impl_jsonrpc_notification_enum!(ClientNotification {
    CancelNotification => "session/cancel",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    MessageMcpNotification => "mcp/message",
    [ext] ExtNotification,
});

// ============================================================================
// Client side (messages that clients/editors receive)
// ============================================================================

impl_jsonrpc_request_enum!(AgentRequest {
    WriteTextFileRequest => "fs/write_text_file",
    ReadTextFileRequest => "fs/read_text_file",
    RequestPermissionRequest => "session/request_permission",
    CreateTerminalRequest => "terminal/create",
    TerminalOutputRequest => "terminal/output",
    ReleaseTerminalRequest => "terminal/release",
    WaitForTerminalExitRequest => "terminal/wait_for_exit",
    KillTerminalRequest => "terminal/kill",
    #[cfg(feature = "unstable_elicitation")]
    CreateElicitationRequest => "elicitation/create",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    ConnectMcpRequest => "mcp/connect",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    MessageMcpRequest => "mcp/message",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    DisconnectMcpRequest => "mcp/disconnect",
    [ext] ExtMethodRequest,
});

impl_jsonrpc_response_enum!(ClientResponse {
    WriteTextFileResponse => "fs/write_text_file",
    ReadTextFileResponse => "fs/read_text_file",
    RequestPermissionResponse => "session/request_permission",
    CreateTerminalResponse => "terminal/create",
    TerminalOutputResponse => "terminal/output",
    ReleaseTerminalResponse => "terminal/release",
    WaitForTerminalExitResponse => "terminal/wait_for_exit",
    KillTerminalResponse => "terminal/kill",
    #[cfg(feature = "unstable_elicitation")]
    CreateElicitationResponse => "elicitation/create",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    ConnectMcpResponse => "mcp/connect",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    MessageMcpResponse => "mcp/message",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    DisconnectMcpResponse => "mcp/disconnect",
    [ext] ExtMethodResponse,
});

impl_jsonrpc_notification_enum!(AgentNotification {
    SessionNotification => "session/update",
    #[cfg(feature = "unstable_elicitation")]
    CompleteElicitationNotification => "elicitation/complete",
    #[cfg(feature = "unstable_mcp_over_agent_transport")]
    MessageMcpNotification => "mcp/message",
    [ext] ExtNotification,
});

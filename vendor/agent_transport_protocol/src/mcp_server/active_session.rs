use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use rustc_hash::FxHashMap;

use crate::mcp_server::{McpConnectionTo, McpServerConnect};
use crate::role;
use crate::role::HasPeer;
use crate::schema::{
    McpConnectRequest, McpConnectResponse, McpDisconnectNotification, McpOverAgentTransportMessage,
};
use crate::util::MatchDispatchFrom;
use crate::{
    Agent, Channel, ConnectTo, ConnectionTo, Dispatch, HandleDispatchFrom, Handled, Responder,
    Role, UntypedMessage,
};
use std::sync::Arc;

/// The message handler for an MCP server offered to a particular session.
/// This is added as a 'dynamic' handler to the connection context
/// (see [`ConnectionTo::add_dynamic_handler`]) and handles MCP-over-agent-transport messages
/// with the appropriate agent transport URL.
pub(super) struct McpActiveSession<Counterpart: Role> {
    /// The agent transport identifier created for this session
    agent_transport_id: String,

    /// The MCP server we are managing
    mcp_connect: Arc<dyn McpServerConnect<Counterpart>>,

    /// Active connections to MCP server tasks
    connections: FxHashMap<String, mpsc::Sender<Dispatch>>,
}

impl<Counterpart: Role> McpActiveSession<Counterpart>
where
    Counterpart: HasPeer<Agent>,
{
    pub fn new(
        agent_transport_id: String,
        mcp_connect: Arc<dyn McpServerConnect<Counterpart>>,
    ) -> Self {
        Self {
            agent_transport_id,
            mcp_connect,
            connections: FxHashMap::default(),
        }
    }

    /// Handle connection requests for our MCP server by creating a new connection.
    /// A *connection* is an actual running instance of this MCP server.
    fn handle_connect_request(
        &mut self,
        request: McpConnectRequest,
        responder: Responder<McpConnectResponse>,
        agent_transport_connection: &ConnectionTo<Counterpart>,
    ) -> Result<Handled<(McpConnectRequest, Responder<McpConnectResponse>)>, crate::Error> {
        // Check that this is for our MCP server
        if request.agent_transport_id != self.agent_transport_id {
            return Ok(Handled::No {
                message: (request, responder),
                retry: false,
            });
        }

        // Create a unique connection ID and a channel for future communication
        let connection_id = format!(
            "mcp-over-agent-transport-connection:{}",
            uuid::Uuid::new_v4()
        );
        let (mcp_server_tx, mut mcp_server_rx) = mpsc::channel(128);
        self.connections
            .insert(connection_id.clone(), mcp_server_tx);

        // Create connected channel pair for client-server communication
        let (client_channel, server_channel) = Channel::duplex();

        // Create client-side handler that wraps messages and forwards to successor
        let client_component = {
            let connection_id = connection_id.clone();
            let agent_transport_connection = agent_transport_connection.clone();

            role::mcp::Client
                .builder()
                .on_receive_dispatch(
                    async move |message: Dispatch, _mcp_connection| {
                        // Wrap the message in McpOverAgentTransport{Request,Notification} and forward to successor
                        let wrapped = message.map(
                            |request, responder| {
                                (
                                    McpOverAgentTransportMessage {
                                        connection_id: connection_id.clone(),
                                        message: request,
                                        meta: None,
                                    },
                                    responder,
                                )
                            },
                            |notification| McpOverAgentTransportMessage {
                                connection_id: connection_id.clone(),
                                message: notification,
                                meta: None,
                            },
                        );
                        agent_transport_connection.send_proxied_message_to(Agent, wrapped)
                    },
                    crate::on_receive_dispatch!(),
                )
                .with_spawned(move |mcp_connection| async move {
                    // Messages we pull off this channel were sent from the agent.
                    // Forward them back to the MCP server.
                    while let Some(msg) = mcp_server_rx.next().await {
                        mcp_connection.send_proxied_message_to(role::mcp::Server, msg)?;
                    }
                    Ok(())
                })
        };

        // Get the MCP server component
        let spawned_server = self.mcp_connect.connect(McpConnectionTo {
            agent_transport_id: request.agent_transport_id.clone(),
            connection: agent_transport_connection.clone(),
        });

        // Spawn both sides of the connection
        let spawn_results = agent_transport_connection
            .spawn(async move { client_component.connect_to(client_channel).await })
            .and_then(|()| {
                // Spawn the MCP server serving the server channel
                agent_transport_connection
                    .spawn(async move { spawned_server.connect_to(server_channel).await })
            });

        match spawn_results {
            Ok(()) => {
                responder.respond(McpConnectResponse {
                    connection_id,
                    meta: None,
                })?;
                Ok(Handled::Yes)
            }

            Err(err) => {
                responder.respond_with_error(err)?;
                Ok(Handled::Yes)
            }
        }
    }

    /// Forward MCP-over-agent-transport requests to the connection.
    async fn handle_mcp_over_agent_transport_request(
        &mut self,
        request: McpOverAgentTransportMessage<UntypedMessage>,
        responder: Responder<serde_json::Value>,
    ) -> Result<
        Handled<(
            McpOverAgentTransportMessage<UntypedMessage>,
            Responder<serde_json::Value>,
        )>,
        crate::Error,
    > {
        // Check if we have a registered server with the given URL. If not, don't try to handle the request.
        let Some(mcp_server_tx) = self.connections.get_mut(&request.connection_id) else {
            return Ok(Handled::No {
                message: (request, responder),
                retry: false,
            });
        };

        mcp_server_tx
            .send(Dispatch::Request(request.message, responder))
            .await
            .map_err(crate::Error::into_internal_error)?;

        Ok(Handled::Yes)
    }

    /// Forward MCP-over-agent-transport notifications to the connection.
    async fn handle_mcp_over_agent_transport_notification(
        &mut self,
        notification: McpOverAgentTransportMessage<UntypedMessage>,
    ) -> Result<Handled<McpOverAgentTransportMessage<UntypedMessage>>, crate::Error> {
        // Check if we have a registered server with the given URL. If not, don't try to handle the request.
        let Some(mcp_server_tx) = self.connections.get_mut(&notification.connection_id) else {
            return Ok(Handled::No {
                message: notification,
                retry: false,
            });
        };

        mcp_server_tx
            .send(Dispatch::Notification(notification.message))
            .await
            .map_err(crate::Error::into_internal_error)?;

        Ok(Handled::Yes)
    }

    /// Disconnect a connection.
    fn handle_mcp_disconnect_notification(
        &mut self,
        successor_notification: McpDisconnectNotification,
    ) -> Handled<McpDisconnectNotification> {
        // Remove connection if we have it. Otherwise, do not handle the notification.
        if self
            .connections
            .remove(&successor_notification.connection_id)
            .is_some()
        {
            Handled::Yes
        } else {
            Handled::No {
                message: successor_notification,
                retry: false,
            }
        }
    }
}

impl<Counterpart: Role> HandleDispatchFrom<Counterpart> for McpActiveSession<Counterpart>
where
    Counterpart: HasPeer<Agent>,
{
    fn describe_chain(&self) -> impl std::fmt::Debug {
        "McpServerSession"
    }

    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        connection: ConnectionTo<Counterpart>,
    ) -> Result<Handled<Dispatch>, crate::Error> {
        MatchDispatchFrom::new(message, &connection)
            // MCP connect requests come from the Agent direction (wrapped in SuccessorMessage)
            .if_request_from(Agent, async |request, responder| {
                self.handle_connect_request(request, responder, &connection)
            })
            .await
            // MCP over agent transport requests come from the Agent direction
            .if_request_from(Agent, async |request, responder| {
                self.handle_mcp_over_agent_transport_request(request, responder)
                    .await
            })
            .await
            // MCP over agent transport notifications come from the Agent direction
            .if_notification_from(Agent, async |notification| {
                self.handle_mcp_over_agent_transport_notification(notification)
                    .await
            })
            .await
            // MCP disconnect notifications come from the Agent direction
            .if_notification_from(Agent, async |notification| {
                Ok(self.handle_mcp_disconnect_notification(notification))
            })
            .await
            .done()
    }
}

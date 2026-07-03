#![cfg_attr(docsrs, feature(doc_cfg))]

//! # agent transport protocol Schema
//!
//! Strongly-typed Rust definitions of the agent transport protocol (agent transport) wire
//! format. agent transport is a JSON-RPC based protocol that standardizes communication
//! between code editors (IDEs, text-editors, etc.) and coding agents
//! (programs that use generative AI to autonomously modify code).
//!
//! This crate is **only** the schema: the request, response, and
//! notification types, plus serde plumbing and JSON Schema generation. For
//! the runtime pieces (transport, connection setup, the `Agent` / `Client`
//! traits, etc.) use the higher-level [`agent-transport-protocol`] crate, which
//! builds on top of these types.
//!
//! [`agent-transport-protocol`]: https://crates.io/crates/agent-transport-protocol
//!
//! ## What's in this crate
//!
//! - Versioned wire-format types for every agent transport method: request, response, and
//!   notification structs grouped by which side handles them, currently under
//!   the [`v1`] module.
//! - JSON-RPC envelope and routing types: [`v1::JsonRpcMessage`],
//!   [`rpc::JsonRpcBatch`], [`v1::Request`], [`v1::Response`],
//!   [`v1::Notification`], [`v1::RequestId`], [`v1::Error`].
//! - Aggregated routing enums: [`v1::AgentRequest`], [`v1::AgentResponse`],
//!   [`v1::AgentNotification`], and the matching client-side trio used by SDK
//!   crates to dispatch incoming JSON-RPC messages.
//!
//! ## Versioning
//!
//! Stable protocol types are exposed through explicit version modules. For
//! example, use `agent_transport_protocol_schema::v1::SessionId` for agent transport protocol
//! version 1 types.
//!
//! For the complete protocol specification and documentation, visit
//! <https://agent-transport-protocol>.

pub mod rpc;
mod serde_util;
pub mod v1;
#[cfg(feature = "unstable_protocol_v2")]
pub mod v2;
mod version;

pub(crate) use serde_util::SkipListener;
pub use serde_util::{IntoMaybeUndefined, IntoOption, MaybeUndefined};
pub use version::*;

#[cfg(test)]
mod serde_json_feature_tests {
    use serde_json::Value;

    #[test]
    fn serde_json_values_preserve_object_key_order() {
        let Value::Object(object) =
            serde_json::from_str::<Value>(r#"{"z":1,"a":2,"m":3}"#).unwrap()
        else {
            panic!("expected JSON object");
        };

        let keys = object.keys().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(keys, ["z", "a", "m"]);
    }
}

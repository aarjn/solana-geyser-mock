use async_trait::async_trait;
use futures::Stream;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeUpdate};

/// Source of Yellowstone gRPC `SubscribeUpdate`s.
///
/// Implementations: real `yellowstone-grpc-client` adapter, in-memory mock,
/// replay-from-file, etc. Consumers write their pipelines against this trait
/// and stay agnostic to the source.
#[async_trait]
pub trait GeyserSource: Send {
    type Stream: Stream<Item = Result<SubscribeUpdate, GeyserError>> + Send + Unpin + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Open a subscription with the given request and return the update stream.
    async fn subscribe(&mut self, request: SubscribeRequest) -> Result<Self::Stream, Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserError {
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("subscribe failed: {0}")]
    Subscribe(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("stream closed")]
    Closed,
}

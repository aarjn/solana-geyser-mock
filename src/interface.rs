use async_trait::async_trait;
use futures::{Sink, Stream};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeUpdate};
use yellowstone_grpc_proto::tonic::Status;

/// Source of Yellowstone gRPC `SubscribeUpdate`s.
///
/// Implementations: real `yellowstone-grpc-client` adapter, in-memory mock,
/// replay-from-file, etc.
#[async_trait]
pub trait GeyserSource: Send {
    /// Error returned when opening a subscription fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Error returned by the sink. Decoupled from the trait so impls can use
    /// `futures::mpsc`, `tokio::mpsc`, or a custom channel.
    type SinkError: std::error::Error + Send + Sync + 'static;

    /// Sink for client-to-server requests after subscription (filter updates, pings).
    type Sink: Sink<SubscribeRequest, Error = Self::SinkError> + Send + Unpin + 'static;

    /// Stream of server-to-client updates.
    type Stream: Stream<Item = Result<SubscribeUpdate, Status>> + Send + Unpin + 'static;

    /// Open a subscription. `None` defers the initial request — send it later via the sink.
    async fn subscribe(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> Result<(Self::Sink, Self::Stream), Self::Error>;
}

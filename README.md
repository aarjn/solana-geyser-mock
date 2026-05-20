# solana-yellowstone-grpc-mock

A `GeyserSource` trait and in-process mock for testing Solana applications
that consume [Yellowstone gRPC](https://github.com/rpcpool/yellowstone-grpc)
geyser streams.

Write your trading bot, indexer, or monitoring service against `GeyserSource`,
swap in `MockGeyserClient` for tests, and exercise your pipeline against
randomized event streams without spinning up a real validator.

## Why

Testing code that consumes a live geyser stream is painful:

- Spinning up a real validator with yellowstone is slow and fragile.
- Hitting a public RPC during tests is rate-limited, non-deterministic, and ties
  CI to external infrastructure.
- Stubbing `Stream<Item = Result<SubscribeUpdate, Status>>` directly works for
  trivial cases but doesn't let you exercise reconnect logic, sink-side filter
  updates, or realistic slot cadence.

This crate gives you a small trait — `GeyserSource` — that both the real
`yellowstone-grpc-client` and an in-memory mock implement. Your consumer code
stays agnostic to the source.

## Quick start

```toml
[dependencies]
solana-yellowstone-grpc-mock = "0.1"
yellowstone-grpc-proto = "..."

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
futures = "0.3"
```

Write your pipeline against the trait:

```rust
use solana_yellowstone_grpc_mock::interface::GeyserSource;
use futures::StreamExt;
use yellowstone_grpc_proto::geyser::SubscribeRequest;

async fn run_pipeline<C: GeyserSource>(mut client: C, request: SubscribeRequest)
    -> Result<(), C::Error>
{
    let (_sink, mut stream) = client.subscribe(Some(request)).await?;
    while let Some(update) = stream.next().await {
        match update {
            Ok(u) => handle_update(u),
            Err(status) => tracing::warn!(?status, "stream error"),
        }
    }
    Ok(())
}
```

In production, pass a real `GeyserGrpcClient`:

```rust
let client = GeyserGrpcClient::build_from_shared(endpoint)?
    .x_token(Some(token))?
    .connect()
    .await?;
run_pipeline(client, my_subscription).await?;
```

In tests, pass the mock:

```rust
use solana_yellowstone_grpc_mock::MockGeyserClient;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn pipeline_handles_account_updates() {
    let shutdown_token = CancellationToken::new();
    let mock = MockGeyserClient::new(0, Some(Duration::from_millis(2)), shutdown_token);
    run_pipeline(mock, my_subscription).await.unwrap();
}
```

## The trait

```rust
#[async_trait]
pub trait GeyserSource: Send {
    type Error: std::error::Error + Send + Sync + 'static;
    type SinkError: std::error::Error + Send + Sync + 'static;
    type Sink: Sink<SubscribeRequest, Error = Self::SinkError> + Send + Unpin + 'static;
    type Stream: Stream<Item = Result<SubscribeUpdate, Status>> + Send + Unpin + 'static;

    async fn subscribe(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> Result<(Self::Sink, Self::Stream), Self::Error>;
}
```

- **`Stream`** carries server-to-client updates. `tonic::Status` is reached
  through `yellowstone-grpc-proto`'s re-export to keep tonic versions aligned.
- **`Sink`** is for client-to-server messages after the subscription is open —
  dynamic filter updates, keep-alive pings. `SinkError` is a separate
  associated type so impls can use `futures::mpsc`, `tokio::mpsc`, or a custom
  channel without forcing a single error type.
- **`request: Option<SubscribeRequest>`** — pass `Some(req)` to send an initial
  request immediately, or `None` to defer and send the first request via the
  sink.

## What the mock does

`MockGeyserClient` runs a tokio task that emits synthetic
`SubscribeUpdate`s on two cadences:

- **Slot boundary** (every ~400ms) — one block / blockmeta update per
  configured filter, plus three slot status transitions (Processed →
  Confirmed → Finalized).
- **Intra-slot** (every ~10ms, configurable) — one randomized transaction,
  transaction-status, or account update, picked uniformly from the kinds the
  request actually subscribed to.

The mock honors `SubscribeRequest` constraints:

- Account updates respect `account` / `owner` pubkey lists, `datasize` filters,
  and `memcmp` filters — emitting accounts that would actually have matched.
- Transaction updates respect `vote` and `failed` flags.
- Slot updates respect `filter_by_commitment` and the request's commitment
  level.

If the request doesn't subscribe to any intra-slot kinds, the mock falls back
to fully random events so the stream stays lively.

## Configuration

```rust
MockGeyserClient::new(
    0,                                  // start_slot
    Some(Duration::from_millis(2)),     // intra-slot interval (faster for tests)
    shutdown_token,
);
```

Default intra-slot interval is `10ms`.

## License

MIT or Apache-2.0, at your option.

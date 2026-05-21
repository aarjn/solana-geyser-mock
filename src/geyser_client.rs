use crate::fixtures::{FixtureError, FixtureEvent};
use crate::geyser_events::{map_random_intraslot_event, map_slot_boundary_events};
use crate::interface::GeyserSource;
use async_trait::async_trait;
use futures::Stream;
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use yellowstone_grpc_client::{
    GeyserGrpcClient, GeyserGrpcClientError, GeyserStream, SubscribeRequestSink,
    SubscribeRequestSinkError,
};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeUpdate};
use yellowstone_grpc_proto::tonic::{self};

// Real Solana slot time is ~400ms.
const SLOT_TICK_INTERVAL: Duration = Duration::from_millis(400);
// Intra-slot events (tx, account) fire much faster than slot boundaries.
const INTRA_SLOT_MOCK_EVENT_INTERVAL_FALLBACK: Duration = Duration::from_millis(10);
const DOWNSTREAM_SEND_TIMEOUT: Duration = Duration::from_millis(50);
const DOWNSTREAM_CHANNEL_CAPACITY: usize = 1024;

/// Cheap, cloneable factory. Configures how the mock will behave; doesn't
/// spawn anything until [`GeyserSource::subscribe`] is called.
#[derive(Debug, Clone)]
pub struct MockGeyserClient {
    start_slot: u64,
    intraslot_mock_event_interval: Option<Duration>,
    shutdown_token: CancellationToken,
    fixture_path: Option<PathBuf>,
}

impl MockGeyserClient {
    pub fn new(
        start_slot: u64,
        intraslot_mock_event_interval: Option<Duration>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            start_slot,
            intraslot_mock_event_interval,
            shutdown_token,
            fixture_path: None,
        }
    }

    /// Use a fixture that replays events from a
    /// JSON file with random mock data
    pub fn with_fixture(mut self, path: impl Into<PathBuf>) -> Self {
        self.fixture_path = Some(path.into());
        self
    }

    /// Spawn the mock task with full access to the handle (events_rx,
    /// JoinHandle). Use this directly in tests when you need to inject events;
    /// use [`GeyserSource::subscribe`] when consuming behind the trait.
    fn start(&self, subscription: SubscribeRequest) -> MockGeyserHandle {
        spawn_mock_task(
            subscription,
            self.start_slot,
            self.intraslot_mock_event_interval,
            self.fixture_path.clone(),
            self.shutdown_token.clone(),
        )
    }
}

struct MockGeyserHandle {
    pub jh: JoinHandle<()>,
    pub events_rx: mpsc::Receiver<Box<SubscribeUpdate>>,
}

/// Stream returned to consumers via [`GeyserSource::subscribe`].
pub struct MockGeyserStream {
    inner: ReceiverStream<Box<SubscribeUpdate>>,
}

impl Stream for MockGeyserStream {
    type Item = Result<SubscribeUpdate, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(boxed)) => Poll::Ready(Some(Ok(*boxed))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Errors produced by [`MockGeyserClient`].
#[derive(Debug, Error)]
pub enum MockGeyserError {
    #[error("mock configured to fail subscribe: {reason}")]
    SubscribeFailed { reason: String },

    #[error("mock requires an initial SubscribeRequest")]
    MissingInitialRequest,

    #[error("mock worker unavailable")]
    WorkerUnavailable,

    #[error("mock has already been used")]
    AlreadyConsumed,
}

#[async_trait]
impl GeyserSource for MockGeyserClient {
    type Error = MockGeyserError;
    type SinkError = futures::channel::mpsc::SendError;
    type Sink = futures::channel::mpsc::Sender<SubscribeRequest>;
    type Stream = MockGeyserStream;

    async fn subscribe(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> Result<(Self::Sink, Self::Stream), Self::Error> {
        let request = request.ok_or(MockGeyserError::MissingInitialRequest)?;

        let MockGeyserHandle { jh: _jh, events_rx } = self.start(request);

        let (sink, _sink_rx) = futures::channel::mpsc::channel::<SubscribeRequest>(1024);

        let stream = MockGeyserStream {
            inner: ReceiverStream::new(events_rx),
        };

        Ok((sink, stream))
    }
}

#[async_trait]
impl GeyserSource for GeyserGrpcClient {
    type Error = GeyserGrpcClientError;
    type SinkError = SubscribeRequestSinkError;
    type Sink = SubscribeRequestSink;
    type Stream = GeyserStream;

    async fn subscribe(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> Result<(Self::Sink, Self::Stream), Self::Error> {
        self.subscribe_with_request(request).await
    }
}

fn try_load_fixture(path: &PathBuf) -> Result<HashMap<u64, Vec<FixtureEvent>>, FixtureError> {
    let content = std::fs::read_to_string(path)?;
    let events: Vec<FixtureEvent> = serde_json::from_str(&content)?;

    let mut fixture_event_map: HashMap<u64, Vec<FixtureEvent>> = HashMap::new();
    for event in events {
        fixture_event_map.entry(event.slot).or_default().push(event);
    }
    Ok(fixture_event_map)
}

fn spawn_mock_task(
    subscription: SubscribeRequest,
    start_slot: u64,
    intraslot_mock_events_interval: Option<Duration>,
    fixture_path: Option<PathBuf>,
    shutdown_token: CancellationToken,
) -> MockGeyserHandle {
    let mut current_slot = start_slot;

    let (downstream_tx, downstream_rx) = mpsc::channel(DOWNSTREAM_CHANNEL_CAPACITY);

    let mut fixture_by_slot = match &fixture_path {
        Some(path) => match try_load_fixture(path) {
            Ok(map) => map,
            Err(err) => {
                warn!("failed to load fixture: {err}");
                HashMap::new()
            }
        },
        None => HashMap::new(),
    };

    let intraslot_interval =
        intraslot_mock_events_interval.unwrap_or(INTRA_SLOT_MOCK_EVENT_INTERVAL_FALLBACK);
    let mut intraslot_tick = tokio::time::interval(intraslot_interval);
    let mut slot_boundary_tick = tokio::time::interval(SLOT_TICK_INTERVAL);

    let jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;

                _ = shutdown_token.cancelled() => {
                    info!("shutting down grpc mock task on signal");
                    break;
                }

                _ = intraslot_tick.tick() => {
                    let event = map_random_intraslot_event(&subscription, current_slot);
                    if let Err(err) = downstream_tx
                        .send_timeout(event, DOWNSTREAM_SEND_TIMEOUT)
                        .await
                    {
                        warn!("failed to send intra-slot mock event downstream: {err}");
                    }

                }

                _ = slot_boundary_tick.tick() => {
                    if let Some(fixture_events) = fixture_by_slot.remove(&current_slot) {
                        for fixture_event in fixture_events {
                            let update: Box<SubscribeUpdate> = match fixture_event.try_into() {
                                Ok(u) => u,
                                Err(err) => {
                                    warn!("failed to convert fixture event: {err}");
                                    continue;
                                }
                            };
                            if let Err(err) = downstream_tx
                                .send_timeout(update, DOWNSTREAM_SEND_TIMEOUT)
                                .await
                            {
                                warn!("failed to send fixture event downstream: {err}");
                            }
                        }
                    }

                    let events = map_slot_boundary_events(&subscription, current_slot);
                    current_slot = current_slot.saturating_add(1);
                    for event in events {
                        if let Err(err) = downstream_tx
                            .send_timeout(event, DOWNSTREAM_SEND_TIMEOUT)
                            .await
                        {
                            warn!("failed to send slot-boundary mock event downstream: {err}");
                        }
                    }
                }
            }
        }
    });

    MockGeyserHandle {
        jh,
        events_rx: downstream_rx,
    }
}

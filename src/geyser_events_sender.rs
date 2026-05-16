use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::interface::{GeyserError, GeyserSource};
use rand::Rng;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
    SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
    subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction, Message, MessageHeader, Transaction, TransactionStatusMeta,
};

const MOCK_EVENT_BUFFER_LOOKUP_INTERVAL: Duration = Duration::from_millis(400);
const RANDOM_EVENTS_SEND_INTERVAL_FALLBACK: Duration = Duration::from_millis(10);
const SLOT_TICK_INTERVAL: Duration = Duration::from_millis(400);
const DOWNSTREAM_SEND_TIMEOUT: Duration = Duration::from_millis(50);
const DOWNSTREAM_CHANNEL_CAPACITY: usize = 1024;
const INJECT_CHANNEL_CAPACITY: usize = 1024;

/// Cheap, cloneable factory. Configures how the mock will behave; doesn't
/// spawn anything until [`GeyserSource::subscribe`] is called.
#[derive(Debug, Clone)]
pub struct MockGeyserEventSender {
    start_slot: u64,
    random_events_send_interval: Option<Duration>,
}

impl Default for MockGeyserEventSender {
    fn default() -> Self {
        Self {
            start_slot: 1,
            random_events_send_interval: None,
        }
    }
}

impl MockGeyserEventSender {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_start_slot(mut self, slot: u64) -> Self {
        self.start_slot = slot;
        self
    }

    pub fn with_random_events_interval(mut self, interval: Duration) -> Self {
        self.random_events_send_interval = Some(interval);
        self
    }

    /// Spawn the mock task with full access to the handle (events_rx, inject_tx,
    /// JoinHandle). Use this directly in tests when you need to inject events;
    /// use [`GeyserSource::subscribe`] when consuming behind the trait.
    pub fn spawn(
        &self,
        subscription: SubscribeRequest,
        shutdown_token: CancellationToken,
    ) -> MockGeyserHandle {
        spawn_mock_task(
            subscription,
            self.start_slot,
            self.random_events_send_interval,
            shutdown_token,
        )
    }
}

pub struct MockGeyserHandle {
    pub jh: JoinHandle<()>,
    pub events_rx: mpsc::Receiver<Box<SubscribeUpdate>>,
    pub inject_tx: mpsc::Sender<Box<SubscribeUpdate>>,
}

/// Stream returned to consumers via [`GeyserSource::subscribe`].
/// Cancels the underlying task when dropped.
pub struct MockGeyserStream {
    inner: ReceiverStream<Box<SubscribeUpdate>>,
    _cancel_on_drop: DropGuard,
}

struct DropGuard(CancellationToken);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl Stream for MockGeyserStream {
    type Item = Result<SubscribeUpdate, GeyserError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(boxed)) => Poll::Ready(Some(Ok(*boxed))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait]
impl GeyserSource for MockGeyserEventSender {
    type Stream = MockGeyserStream;
    type Error = GeyserError;

    async fn subscribe(&mut self, request: SubscribeRequest) -> Result<Self::Stream, Self::Error> {
        let shutdown = CancellationToken::new();
        let MockGeyserHandle {
            jh: _jh,
            events_rx,
            inject_tx: _inject_tx,
        } = self.spawn(request, shutdown.clone());

        Ok(MockGeyserStream {
            inner: ReceiverStream::new(events_rx),
            _cancel_on_drop: DropGuard(shutdown),
        })
    }
}

fn spawn_mock_task(
    subscription: SubscribeRequest,
    start_slot: u64,
    random_events_send_interval: Option<Duration>,
    shutdown_token: CancellationToken,
) -> MockGeyserHandle {
    let mut current_slot = start_slot;
    let mut mock_events_buffer: Vec<Box<SubscribeUpdate>> = Vec::new();

    let (inject_tx, mut inject_rx) = mpsc::channel(INJECT_CHANNEL_CAPACITY);
    let (downstream_tx, downstream_rx) = mpsc::channel(DOWNSTREAM_CHANNEL_CAPACITY);

    let mut mock_buffer_lookup_tick = tokio::time::interval(MOCK_EVENT_BUFFER_LOOKUP_INTERVAL);
    let random_event_arrival_interval =
        random_events_send_interval.unwrap_or(RANDOM_EVENTS_SEND_INTERVAL_FALLBACK);
    let mut random_event_tick = tokio::time::interval(random_event_arrival_interval);
    let mut slot_update_event_tick = tokio::time::interval(SLOT_TICK_INTERVAL);

    let jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;

                _ = shutdown_token.cancelled() => {
                    info!("shutting down grpc mock task on signal");
                    break;
                }

                Some(msg) = inject_rx.recv() => {
                    mock_events_buffer.push(msg);
                }

                _ = mock_buffer_lookup_tick.tick() => {
                    if mock_events_buffer.is_empty() {
                        continue;
                    }
                    debug!(
                        count = mock_events_buffer.len(),
                        "flushing buffered updates to downstream consumer",
                    );
                    for e in mock_events_buffer.drain(..) {
                        if let Err(err) = downstream_tx
                            .send_timeout(e, DOWNSTREAM_SEND_TIMEOUT)
                            .await
                        {
                            error!("failed to flush buffered mock event downstream: {err}");
                        }
                    }
                }

                _ = random_event_tick.tick() => {
                    let event = map_random_event(&subscription, current_slot);
                    if let Err(err) = downstream_tx
                        .send_timeout(event, DOWNSTREAM_SEND_TIMEOUT)
                        .await
                    {
                        warn!("failed to send random mock event downstream: {err}");
                    }
                }

                _ = slot_update_event_tick.tick() => {
                    let event = map_slot_event(current_slot);
                    current_slot = current_slot.saturating_add(1);
                    if let Err(err) = downstream_tx
                        .send_timeout(event, DOWNSTREAM_SEND_TIMEOUT)
                        .await
                    {
                        warn!("failed to send slot mock event downstream: {err}");
                    }
                }
            }
        }
    });

    MockGeyserHandle {
        jh,
        events_rx: downstream_rx,
        inject_tx,
    }
}

/// Build a random event based on subscription
pub(crate) fn map_random_event(req: &SubscribeRequest, slot: u64) -> Box<SubscribeUpdate> {
    if let Some((filter, _)) = req.transactions.iter().next() {
        return Box::new(random_transaction_update(filter.clone(), slot));
    }
    if let Some((filter, _)) = req.accounts.iter().next() {
        return Box::new(random_account_update(filter.clone(), slot));
    }
    // Default: emit another slot update so the stream is never silent.
    Box::new(slot_update(slot))
}

/// Build a `SubscribeUpdate` carrying a slot status change.
pub(crate) fn map_slot_event(slot: u64) -> Box<SubscribeUpdate> {
    Box::new(slot_update(slot))
}

fn slot_update(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["slot".to_string()],
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: Some(slot.saturating_sub(1)),
            // 0 = Processed, 1 = Confirmed, 2 = Finalized in the proto enum.
            // Cycle through them so client exercise all branches.
            status: (slot % 3) as i32,
            dead_error: None,
        })),
        created_at: None,
    }
}

fn random_account_update(filter: String, slot: u64) -> SubscribeUpdate {
    let mut rng = rand::thread_rng();
    let pubkey = Pubkey::new_unique().to_bytes().to_vec();
    let owner = Pubkey::new_unique().to_bytes().to_vec();
    let data: Vec<u8> = (0..rng.gen_range(0..256)).map(|_| rng.r#gen()).collect();

    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey,
                lamports: rng.gen_range(0..1_000_000_000),
                owner,
                executable: false,
                rent_epoch: 0,
                data,
                write_version: rng.r#gen(),
                txn_signature: None,
            }),
            slot,
            is_startup: false,
        })),
        created_at: None,
    }
}

fn random_transaction_update(filter: String, slot: u64) -> SubscribeUpdate {
    let mut rng = rand::thread_rng();
    let sig_bytes: [u8; 64] = std::array::from_fn(|_| rng.r#gen());
    let signature = Signature::from(sig_bytes).as_ref().to_vec();

    let payer = Pubkey::new_unique().to_bytes().to_vec();
    let program_id = Pubkey::new_unique().to_bytes().to_vec();
    let recent_blockhash: Vec<u8> = {
        let bh: [u8; 32] = std::array::from_fn(|_| rng.r#gen());
        bh.to_vec()
    };

    let message = Message {
        header: Some(MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        }),
        account_keys: vec![payer, program_id],
        recent_blockhash,
        instructions: vec![CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: (0..rng.gen_range(0..32)).map(|_| rng.r#gen()).collect(),
        }],
        versioned: false,
        address_table_lookups: vec![],
    };

    let transaction = Transaction {
        signatures: vec![signature.clone()],
        message: Some(message),
    };

    let meta = TransactionStatusMeta {
        err: None,
        fee: rng.gen_range(5_000..50_000),
        pre_balances: vec![1_000_000_000, 0],
        post_balances: vec![999_995_000, 0],
        inner_instructions: vec![],
        inner_instructions_none: true,
        log_messages: vec![],
        log_messages_none: true,
        pre_token_balances: vec![],
        post_token_balances: vec![],
        rewards: vec![],
        loaded_writable_addresses: vec![],
        loaded_readonly_addresses: vec![],
        return_data: None,
        return_data_none: true,
        compute_units_consumed: Some(rng.gen_range(1_000..200_000)),
        cost_units: None,
    };

    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature,
                is_vote: false,
                transaction: Some(transaction),
                meta: Some(meta),
                index: rng.gen_range(0..1024),
            }),
            slot,
        })),
        created_at: None,
    }
}

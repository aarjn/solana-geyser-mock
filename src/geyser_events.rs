use base64::Engine;
use rand::Rng;
use rand::seq::IteratorRandom;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter::Filter;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
    SubscribeUpdateBlockMeta, SubscribeUpdateSlot, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo, SubscribeUpdateTransactionStatus,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction, Message, MessageHeader, Transaction, TransactionStatusMeta,
};

pub enum GeyserEventUpdate {
    Account,
    Transaction,
    TransactionStatus,
    Slot,
    Block,
    BlockMeta,
    Ping,
}

/// Pick a random intra-slot event (tx, tx_status, account) consistent with the
/// subscription. If none of these kinds are subscribed, emits a fully random
/// event so the stream stays lively.
pub(crate) fn map_random_intraslot_event(
    req: &SubscribeRequest,
    slot: u64,
) -> Box<SubscribeUpdate> {
    let mut rng = rand::thread_rng();

    #[derive(Clone, Copy)]
    enum Kind {
        Tx,
        TxStatus,
        Account,
    }

    let mut kinds: Vec<Kind> = Vec::with_capacity(3);
    if !req.transactions.is_empty() {
        kinds.push(Kind::Tx);
    }
    if !req.transactions_status.is_empty() {
        kinds.push(Kind::TxStatus);
    }
    if !req.accounts.is_empty() {
        kinds.push(Kind::Account);
    }

    // No intra-slot kinds subscribed — emit a fully random event.
    if kinds.is_empty() {
        return random_intraslot_event_unconstrained(slot, &mut rng);
    }

    let kind = kinds.into_iter().choose(&mut rng).unwrap();

    match kind {
        Kind::Tx => {
            let filter = req.transactions.keys().choose(&mut rng).cloned().unwrap();
            Box::new(random_transaction_update(filter, slot))
        }
        Kind::TxStatus => {
            let filter = req
                .transactions_status
                .keys()
                .choose(&mut rng)
                .cloned()
                .unwrap();
            Box::new(random_transaction_status_update(filter, slot))
        }
        Kind::Account => {
            let (filter_name, cfg) = req
                .accounts
                .iter()
                .choose(&mut rng)
                .map(|(k, v)| (k.clone(), v))
                .unwrap();
            Box::new(random_account_update(filter_name, cfg, slot))
        }
    }
}

/// Build a fully random intra-slot event with no subscription constraints.
/// Used as a fallback when the request has no tx/tx_status/account filters.
fn random_intraslot_event_unconstrained<R: Rng>(slot: u64, rng: &mut R) -> Box<SubscribeUpdate> {
    match rng.gen_range(0..3) {
        0 => Box::new(random_transaction_update(
            "__mock_random_tx".to_string(),
            slot,
        )),
        1 => Box::new(random_transaction_status_update(
            "__mock_random_tx_status".to_string(),
            slot,
        )),
        _ => Box::new(random_account_update(
            "__mock_random_account".to_string(),
            &SubscribeRequestFilterAccounts::default(),
            slot,
        )),
    }
}

/// Emit the once-per-slot events: block, blockmeta, and slot status transitions
/// (Processed → Confirmed → Finalized). Real geyser staggers Confirmed and
/// Finalized; the mock emits all three at the boundary for simplicity.
///
/// One update is emitted per registered filter in each kind, mirroring how
/// real geyser fans out to every matching subscription.
pub(crate) fn map_slot_boundary_events(
    req: &SubscribeRequest,
    slot: u64,
) -> Vec<Box<SubscribeUpdate>> {
    let mut events = Vec::with_capacity(
        req.blocks.len() + req.blocks_meta.len() + req.slots.len().saturating_mul(3).max(1),
    );

    for filter in req.blocks.keys() {
        events.push(Box::new(random_block_update(filter.clone(), slot)));
    }

    for filter in req.blocks_meta.keys() {
        events.push(Box::new(random_block_meta_update(filter.clone(), slot)));
    }

    if req.slots.is_empty() {
        // No slot subscription, but keep the stream alive with a default slot tick.
        events.push(Box::new(slot_update_with_filter(
            "slot".to_string(),
            slot,
            (slot % 3) as i32,
        )));
    } else {
        // 0 = Processed, 1 = Confirmed, 2 = Finalized
        for filter in req.slots.keys() {
            for status in 0..=2 {
                events.push(Box::new(slot_update_with_filter(
                    filter.clone(),
                    slot,
                    status,
                )));
            }
        }
    }

    events
}

fn slot_update_with_filter(filter: String, slot: u64, status: i32) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: Some(slot.saturating_sub(1)),
            status,
            dead_error: None,
        })),
        created_at: None,
    }
}

fn random_account_update(
    filter: String,
    cfg: &SubscribeRequestFilterAccounts,
    slot: u64,
) -> SubscribeUpdate {
    let mut rng = rand::thread_rng();

    // Pubkey: pick from `cfg.account` if non-empty, else random.
    let pubkey = pick_pubkey_or_random(&cfg.account, &mut rng);

    // Owner: pick from `cfg.owner` if non-empty, else random.
    let owner = pick_pubkey_or_random(&cfg.owner, &mut rng);

    // Data: shape according to `cfg.filters` (datasize / memcmp) if specified.
    let data = build_account_data(&cfg.filters, &mut rng);

    // txn_signature: respect `nonempty_txn_signature` if set.
    let txn_signature = match cfg.nonempty_txn_signature {
        Some(true) => Some(random_signature_bytes(&mut rng)),
        Some(false) => None,
        None => {
            // Unspecified — randomize.
            if rng.gen_bool(0.5) {
                Some(random_signature_bytes(&mut rng))
            } else {
                None
            }
        }
    };

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
                txn_signature,
            }),
            slot,
            is_startup: false,
        })),
        created_at: None,
    }
}

/// Pick a pubkey from a configured list (decoding base58), or generate a random one.
fn pick_pubkey_or_random<R: Rng>(configured: &[String], rng: &mut R) -> Vec<u8> {
    if let Some(key_str) = configured.iter().choose(rng) {
        if let Ok(pk) = Pubkey::from_str(key_str) {
            return pk.to_bytes().to_vec();
        }
        // Malformed config — fall through to random rather than panic.
    }
    Pubkey::new_unique().to_bytes().to_vec()
}

fn random_signature_bytes<R: Rng>(rng: &mut R) -> Vec<u8> {
    let bytes: [u8; 64] = std::array::from_fn(|_| rng.r#gen());
    bytes.to_vec()
}

/// Build account data honoring datasize and memcmp constraints from the filter.
/// If multiple filters are present, all of them are applied; conflicting
/// filters (e.g. two different datasizes) — last write wins, which is fine
/// for a randomized mock.
fn build_account_data<R: Rng>(
    filters: &[SubscribeRequestFilterAccountsFilter],
    rng: &mut R,
) -> Vec<u8> {
    // Determine target size: explicit datasize wins; otherwise random 0..256.
    let datasize = filters
        .iter()
        .find_map(|f| match &f.filter {
            Some(Filter::Datasize(n)) => Some(*n as usize),
            _ => None,
        })
        .unwrap_or_else(|| rng.gen_range(0..256));

    let mut data: Vec<u8> = (0..datasize).map(|_| rng.r#gen()).collect();

    // Apply memcmp constraints: write each `bytes` at its `offset`.
    for f in filters {
        if let Some(Filter::Memcmp(memcmp)) = &f.filter {
            let offset = memcmp.offset as usize;
            // Memcmp data is in the `data` field of the memcmp oneof.
            if let Some(memcmp_data) = memcmp_bytes(memcmp) {
                // Grow data if memcmp extends past current size.
                let end = offset + memcmp_data.len();
                if end > data.len() {
                    data.resize(end, 0);
                }
                data[offset..end].copy_from_slice(&memcmp_data);
            }
        }
    }

    data
}

/// Extract the bytes from a memcmp filter, handling both `bytes` and `base58`/`base64` encodings.
fn memcmp_bytes(
    memcmp: &yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccountsFilterMemcmp,
) -> Option<Vec<u8>> {
    use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter_memcmp::Data;
    match &memcmp.data {
        Some(Data::Bytes(b)) => Some(b.clone()),
        Some(Data::Base58(s)) => bs58::decode(s).into_vec().ok(),
        Some(Data::Base64(s)) => base64::engine::general_purpose::STANDARD.decode(s).ok(),
        None => None,
    }
}

fn random_transaction_update(filter: String, slot: u64) -> SubscribeUpdate {
    let info = random_transaction_info();
    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(info),
            slot,
        })),
        created_at: None,
    }
}

fn random_transaction_status_update(filter: String, slot: u64) -> SubscribeUpdate {
    let mut rng = rand::thread_rng();
    let sig_bytes: [u8; 64] = std::array::from_fn(|_| rng.r#gen());
    let signature = Signature::from(sig_bytes).as_ref().to_vec();

    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::TransactionStatus(
            SubscribeUpdateTransactionStatus {
                slot,
                signature,
                is_vote: false,
                index: rng.gen_range(0..1024),
                err: None,
            },
        )),
        created_at: None,
    }
}

fn random_block_update(filter: String, slot: u64) -> SubscribeUpdate {
    let mut rng = rand::thread_rng();
    let blockhash: [u8; 32] = std::array::from_fn(|_| rng.r#gen());

    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
            slot,
            blockhash: bs58::encode(blockhash).into_string(),
            rewards: None,
            block_time: None,
            block_height: None,
            parent_slot: slot.saturating_sub(1),
            parent_blockhash: String::new(),
            executed_transaction_count: 0,
            transactions: vec![],
            updated_account_count: 0,
            accounts: vec![],
            entries_count: 0,
            entries: vec![],
        })),
        created_at: None,
    }
}

fn random_block_meta_update(filter: String, slot: u64) -> SubscribeUpdate {
    let mut rng = rand::thread_rng();
    let blockhash: [u8; 32] = std::array::from_fn(|_| rng.r#gen());

    SubscribeUpdate {
        filters: vec![filter],
        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: bs58::encode(blockhash).into_string(),
            rewards: None,
            block_time: None,
            block_height: None,
            parent_slot: slot.saturating_sub(1),
            parent_blockhash: String::new(),
            executed_transaction_count: 0,
            entries_count: 0,
        })),
        created_at: None,
    }
}

fn random_transaction_info() -> SubscribeUpdateTransactionInfo {
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

    SubscribeUpdateTransactionInfo {
        signature,
        is_vote: false,
        transaction: Some(transaction),
        meta: Some(meta),
        index: rng.gen_range(0..1024),
    }
}

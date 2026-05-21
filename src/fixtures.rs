use base64::Engine;
use serde::Deserialize;
use thiserror::Error;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
    SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::prelude::{Transaction, TransactionStatusMeta, UnixTimestamp};
use yellowstone_grpc_proto::prost::Message;

#[derive(Debug, Error)]
pub enum FixtureError {
    #[error("Payload mismatch")]
    PayloadMismatch,

    #[error("Base58 decode error")]
    Base58Decode(#[from] bs58::decode::Error),

    #[error("Base64 decode error")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("Proto decode error")]
    ProtoDecode(#[from] yellowstone_grpc_proto::prost::DecodeError),

    #[error("Unknown update_type")]
    UnknownUpdateType,

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error")]
    JsonParse(#[from] serde_json::Error),
}

#[derive(Debug, Deserialize)]
pub struct FixtureEvent {
    pub slot: u64,
    pub update_type: String,
    pub payload: Payload,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Payload {
    Account(AccountPayload),
    Slot(SlotPayload),
    Transaction(TransactionPayload),
    BlockMeta(BlockMetaPayload),
}

#[derive(Debug, Deserialize)]
pub struct AccountPayload {
    pub pubkey: String,
    pub owner: String,
    pub lamports: u64,
    pub data: String,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    #[serde(default)]
    pub is_startup: bool,
}

#[derive(Debug, Deserialize)]
pub struct SlotPayload {
    pub parent: Option<u64>,
    pub status: i32,
}

#[derive(Debug, Deserialize)]
pub struct TransactionPayload {
    pub signature: String,
    pub is_vote: bool,
    pub transaction: String,
    pub meta: String,
    pub index: u64,
}

#[derive(Debug, Deserialize)]
pub struct BlockMetaPayload {
    pub blockhash: String,
    pub block_time: Option<i64>,
    pub executed_transaction_count: u64,
}

impl TryFrom<FixtureEvent> for Box<SubscribeUpdate> {
    type Error = FixtureError;

    fn try_from(event: FixtureEvent) -> Result<Self, Self::Error> {
        match event.update_type.as_str() {
            "account" => {
                let payload = match event.payload {
                    Payload::Account(p) => p,
                    _ => {
                        return Err(FixtureError::PayloadMismatch);
                    }
                };

                let pubkey_bytes = bs58::decode(&payload.pubkey).into_vec()?;
                let owner_bytes = bs58::decode(&payload.owner).into_vec()?;
                let data_bytes = base64::engine::general_purpose::STANDARD.decode(&payload.data)?;

                Ok(Box::new(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                        account: Some(SubscribeUpdateAccountInfo {
                            pubkey: pubkey_bytes,
                            owner: owner_bytes,
                            lamports: payload.lamports,
                            data: data_bytes,
                            executable: payload.executable,
                            rent_epoch: payload.rent_epoch,
                            write_version: payload.write_version,
                            txn_signature: None,
                        }),
                        slot: event.slot,
                        is_startup: payload.is_startup,
                    })),
                    created_at: None,
                }))
            }

            "slot" => {
                let payload = match event.payload {
                    Payload::Slot(p) => p,
                    _ => {
                        return Err(FixtureError::PayloadMismatch);
                    }
                };

                Ok(Box::new(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot: event.slot,
                        parent: payload.parent.or(Some(event.slot.saturating_sub(1))),
                        status: payload.status,
                        dead_error: None,
                    })),
                    created_at: None,
                }))
            }

            "transaction" => {
                let payload = match event.payload {
                    Payload::Transaction(p) => p,
                    _ => {
                        return Err(FixtureError::PayloadMismatch);
                    }
                };

                let sig_bytes = bs58::decode(&payload.signature).into_vec()?;
                let tx_bytes =
                    base64::engine::general_purpose::STANDARD.decode(&payload.transaction)?;
                let meta_bytes = base64::engine::general_purpose::STANDARD.decode(&payload.meta)?;

                let tx = Transaction::decode(tx_bytes.as_slice())?;
                let meta = TransactionStatusMeta::decode(meta_bytes.as_slice())?;

                Ok(Box::new(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        transaction: Some(SubscribeUpdateTransactionInfo {
                            signature: sig_bytes,
                            is_vote: payload.is_vote,
                            transaction: Some(tx),
                            meta: Some(meta),
                            index: payload.index,
                        }),
                        slot: event.slot,
                    })),
                    created_at: None,
                }))
            }

            "block_meta" => {
                let payload = match event.payload {
                    Payload::BlockMeta(p) => p,
                    _ => {
                        return Err(FixtureError::PayloadMismatch);
                    }
                };

                Ok(Box::new(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: event.slot,
                        blockhash: payload.blockhash,
                        rewards: None,
                        block_time: payload.block_time.map(|t| UnixTimestamp { timestamp: t }),
                        block_height: None,
                        parent_slot: event.slot.saturating_sub(1),
                        parent_blockhash: String::new(),
                        executed_transaction_count: payload.executed_transaction_count,
                        entries_count: 0,
                    })),
                    created_at: None,
                }))
            }

            _ => Err(FixtureError::UnknownUpdateType),
        }
    }
}

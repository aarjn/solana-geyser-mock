use futures::StreamExt;
use solana_pubkey::Pubkey;
use solana_yellowstone_grpc_mock::geyser_client::MockGeyserClient;
use solana_yellowstone_grpc_mock::interface::GeyserSource;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter::Filter as AccountFilterOneof;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter_memcmp::Data as MemcmpData;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp,
};

/// Collect up to `n` account updates from the mock stream, with a hard timeout
/// so tests fail fast rather than hang if the mock stops producing.
async fn collect_account_updates(
    req: SubscribeRequest,
    n: usize,
    timeout: Duration,
) -> Vec<yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo> {
    let mut mock =
        MockGeyserClient::new(0, Some(Duration::from_millis(2)), CancellationToken::new());
    let (_sink, mut stream) = mock.subscribe(Some(req)).await.expect("subscribe");

    let mut collected = Vec::with_capacity(n);
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if collected.len() >= n {
            return collected;
        }

        tokio::select! {
            biased;

            _ = &mut deadline => {
                panic!("timed out collecting {n} account updates, got {}", collected.len());
            }

            next = stream.next() => {
                match next {
                    Some(Ok(update)) => {
                        if let Some(UpdateOneof::Account(acc)) = update.update_oneof {
                            if let Some(info) = acc.account {
                                collected.push(info);
                            }
                        }
                    }
                    Some(Err(e)) => panic!("stream error: {e}"),
                    None => panic!("stream ended unexpectedly"),
                }
            }
        }
    }
}

#[tokio::test]
async fn account_update_respects_configured_pubkey() {
    let watched = Pubkey::new_unique();
    let mut accounts = HashMap::new();
    accounts.insert(
        "my_filter".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![watched.to_string()],
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: None,
        },
    );
    let req = SubscribeRequest {
        accounts,
        ..Default::default()
    };

    let updates = collect_account_updates(req, 10, Duration::from_secs(5)).await;

    for info in &updates {
        let pk = Pubkey::try_from(info.pubkey.as_slice()).expect("valid pubkey bytes");
        assert_eq!(
            pk, watched,
            "account update pubkey should match the single configured account",
        );
    }
}

#[tokio::test]
async fn account_update_respects_configured_owner() {
    let owner = Pubkey::new_unique();
    let mut accounts = HashMap::new();
    accounts.insert(
        "owner_filter".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![owner.to_string()],
            filters: vec![],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: None,
        },
    );
    let req = SubscribeRequest {
        accounts,
        ..Default::default()
    };

    let updates = collect_account_updates(req, 10, Duration::from_secs(5)).await;

    for info in &updates {
        let actual_owner = Pubkey::try_from(info.owner.as_slice()).expect("valid owner bytes");
        assert_eq!(
            actual_owner, owner,
            "account update owner should match the configured owner",
        );
    }
}

#[tokio::test]
async fn account_update_respects_datasize_filter() {
    const SIZE: usize = 165; // SPL Token account size
    let mut accounts = HashMap::new();
    accounts.insert(
        "size_filter".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![SubscribeRequestFilterAccountsFilter {
                filter: Some(AccountFilterOneof::Datasize(SIZE as u64)),
            }],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: None,
        },
    );
    let req = SubscribeRequest {
        accounts,
        ..Default::default()
    };

    let updates = collect_account_updates(req, 10, Duration::from_secs(5)).await;

    for info in &updates {
        assert_eq!(
            info.data.len(),
            SIZE,
            "account data length should match datasize filter",
        );
    }
}

#[tokio::test]
async fn account_update_respects_memcmp_filter() {
    let mint = Pubkey::new_unique();
    let mint_bytes = mint.to_bytes().to_vec();
    let offset = 0_u64;

    let mut accounts = HashMap::new();
    accounts.insert(
        "memcmp_filter".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![SubscribeRequestFilterAccountsFilter {
                filter: Some(AccountFilterOneof::Memcmp(
                    SubscribeRequestFilterAccountsFilterMemcmp {
                        offset,
                        data: Some(MemcmpData::Bytes(mint_bytes.clone())),
                    },
                )),
            }],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: None,
        },
    );
    let req = SubscribeRequest {
        accounts,
        ..Default::default()
    };

    let updates = collect_account_updates(req, 10, Duration::from_secs(5)).await;

    for info in &updates {
        let end = offset as usize + mint_bytes.len();
        assert!(
            info.data.len() >= end,
            "account data too short to contain memcmp bytes (len={}, need={})",
            info.data.len(),
            end,
        );
        assert_eq!(
            &info.data[offset as usize..end],
            mint_bytes.as_slice(),
            "memcmp bytes should appear at the configured offset",
        );
    }
}

#[tokio::test]
async fn account_update_filter_name_propagates() {
    let mut accounts = HashMap::new();
    accounts.insert(
        "named_filter_abc".to_string(),
        SubscribeRequestFilterAccounts::default(),
    );
    let req = SubscribeRequest {
        accounts,
        ..Default::default()
    };

    let mut mock =
        MockGeyserClient::new(0, Some(Duration::from_millis(2)), CancellationToken::new());
    let (_sink, mut stream) = mock.subscribe(Some(req)).await.expect("subscribe");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut seen_account = false;
    while tokio::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        if let Ok(Some(Ok(update))) = next {
            if matches!(update.update_oneof, Some(UpdateOneof::Account(_))) {
                assert_eq!(update.filters, vec!["named_filter_abc".to_string()]);
                seen_account = true;
                break;
            }
        }
    }
    assert!(
        seen_account,
        "did not observe an account update within timeout"
    );
}

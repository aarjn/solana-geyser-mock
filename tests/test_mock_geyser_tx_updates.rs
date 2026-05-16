use futures::StreamExt;
use solana_geyser_mock::geyser_events_stream::MockGeyserClient;
use solana_geyser_mock::interface::GeyserSource;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterTransactions};

async fn collect_tx_updates(
    req: SubscribeRequest,
    n: usize,
    timeout: Duration,
) -> Vec<yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo> {
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
                panic!("timed out collecting {n} tx updates, got {}", collected.len());
            }

            next = stream.next() => {
                match next {
                    Some(Ok(update)) => {
                        if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof {
                            if let Some(info) = tx.transaction {
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
async fn tx_update_has_well_formed_shape() {
    let mut transactions = HashMap::new();
    transactions.insert(
        "all_txs".to_string(),
        SubscribeRequestFilterTransactions::default(),
    );
    let req = SubscribeRequest {
        transactions,
        ..Default::default()
    };

    let updates = collect_tx_updates(req, 5, Duration::from_secs(5)).await;

    for info in &updates {
        assert_eq!(info.signature.len(), 64, "tx signature must be 64 bytes");
        let tx = info.transaction.as_ref().expect("transaction present");
        assert!(
            !tx.signatures.is_empty(),
            "tx must have at least one signature"
        );
        let msg = tx.message.as_ref().expect("message present");
        assert!(
            !msg.account_keys.is_empty(),
            "message must have account keys"
        );
        assert!(msg.header.is_some(), "message header must be populated");
        let meta = info.meta.as_ref().expect("tx meta present");
        assert_eq!(
            meta.pre_balances.len(),
            msg.account_keys.len(),
            "pre_balances must match account_keys length",
        );
        assert_eq!(
            meta.post_balances.len(),
            msg.account_keys.len(),
            "post_balances must match account_keys length",
        );
    }
}

#[tokio::test]
async fn tx_update_respects_vote_false() {
    let mut transactions = HashMap::new();
    transactions.insert(
        "no_votes".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );
    let req = SubscribeRequest {
        transactions,
        ..Default::default()
    };

    let updates = collect_tx_updates(req, 10, Duration::from_secs(5)).await;

    for info in &updates {
        assert!(!info.is_vote, "vote=Some(false) must not emit vote txs");
    }
}

#[tokio::test]
async fn tx_update_respects_failed_false() {
    let mut transactions = HashMap::new();
    transactions.insert(
        "no_failed".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: Some(false),
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );
    let req = SubscribeRequest {
        transactions,
        ..Default::default()
    };

    let updates = collect_tx_updates(req, 10, Duration::from_secs(5)).await;

    for info in &updates {
        let meta = info.meta.as_ref().expect("tx meta present");
        assert!(
            meta.err.is_none(),
            "failed=Some(false) must not emit failed txs"
        );
    }
}

#[tokio::test]
async fn tx_update_filter_name_propagates() {
    let mut transactions = HashMap::new();
    transactions.insert(
        "named_tx_filter".to_string(),
        SubscribeRequestFilterTransactions::default(),
    );
    let req = SubscribeRequest {
        transactions,
        ..Default::default()
    };

    let mut mock =
        MockGeyserClient::new(0, Some(Duration::from_millis(2)), CancellationToken::new());
    let (_sink, mut stream) = mock.subscribe(Some(req)).await.expect("subscribe");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut seen = false;
    while tokio::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        if let Ok(Some(Ok(update))) = next {
            if matches!(update.update_oneof, Some(UpdateOneof::Transaction(_))) {
                assert_eq!(update.filters, vec!["named_tx_filter".to_string()]);
                seen = true;
                break;
            }
        }
    }
    assert!(seen, "did not observe a tx update within timeout");
}

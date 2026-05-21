use futures::StreamExt;
use solana_yellowstone_grpc_mock::geyser_client::MockGeyserClient;
use solana_yellowstone_grpc_mock::interface::GeyserSource;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions,
};

#[derive(Debug, PartialEq)]
enum ExpectedEvent {
    Slot {
        slot: u64,
        status: i32,
        parent: u64,
    },
    Account {
        slot: u64,
        pubkey: String,
        lamports: u64,
    },
    Transaction {
        slot: u64,
        signature: String,
    },
    BlockMeta {
        slot: u64,
        blockhash: String,
        tx_count: u64,
    },
}

#[tokio::test]
async fn test_fixture_replay() {
    let shutdown = CancellationToken::new();
    let mut client = MockGeyserClient::new(0, Some(Duration::from_millis(5)), shutdown.clone())
        .with_fixture("fixture/sample.json");

    let req = SubscribeRequest {
        slots: HashMap::from([("all".to_string(), SubscribeRequestFilterSlots::default())]),
        accounts: HashMap::from([("all".to_string(), SubscribeRequestFilterAccounts::default())]),
        transactions: HashMap::from([(
            "all".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        ..Default::default()
    };
    let (_sink, mut stream) = client.subscribe(Some(req)).await.unwrap();

    // matches fixture/sample.json exactly
    let expected: Vec<ExpectedEvent> = vec![
        ExpectedEvent::Slot { slot: 2, status: 1, parent: 100 },
        ExpectedEvent::Account {
            slot: 2,
            pubkey: "5YNmS1R9nNSCDzb5a7mMJ1dwK9uHeAAF4CerTf3CSEJM".to_string(),
            lamports: 2039280,
        },
        ExpectedEvent::Account {
            slot: 2,
            pubkey: "9aE476sH92Vz7DMPyq5WLPkrKWivxeuTKEFKd2sZZcjk".to_string(),
            lamports: 2039280,
        },
        ExpectedEvent::Transaction {
            slot: 5,
            signature: "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2".to_string(),
        },
        ExpectedEvent::Account {
            slot: 5,
            pubkey: "3Jq2GDvhPMBAEJEFr61sRNFCm35C6mTQBBRVMhEJr6Mq".to_string(),
            lamports: 2039280,
        },
        ExpectedEvent::BlockMeta {
            slot: 8,
            blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
            tx_count: 1500,
        },
    ];

    let mut replayed: Vec<ExpectedEvent> = vec![];

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            update = stream.next() => {
                match update {
                    None => break,
                    Some(Err(_)) => break,
                    Some(Ok(u)) => match u.update_oneof {
                        Some(UpdateOneof::Slot(slot)) => {
                            if slot.slot == 2 && slot.status == 1 && slot.parent == Some(100) {
                                replayed.push(ExpectedEvent::Slot {
                                    slot: slot.slot,
                                    status: slot.status,
                                    parent: slot.parent.unwrap(),
                                });
                            }
                        }
                        Some(UpdateOneof::Account(acct)) => {
                            let info = acct.account.as_ref().unwrap();
                            let pubkey = bs58::encode(&info.pubkey).into_string();
                            if expected.iter().any(|e| matches!(e, ExpectedEvent::Account { pubkey: pk, .. } if pk == &pubkey)) {
                                replayed.push(ExpectedEvent::Account {
                                    slot: acct.slot,
                                    pubkey,
                                    lamports: info.lamports,
                                });
                            }
                        }
                        Some(UpdateOneof::Transaction(tx)) => {
                            let info = tx.transaction.as_ref().unwrap();
                            let sig = bs58::encode(&info.signature).into_string();
                            if expected.iter().any(|e| matches!(e, ExpectedEvent::Transaction { signature: s, .. } if s == &sig)) {
                                replayed.push(ExpectedEvent::Transaction {
                                    slot: tx.slot,
                                    signature: sig,
                                });
                            }
                        }
                        Some(UpdateOneof::BlockMeta(bm)) => {
                            if expected.iter().any(|e| matches!(e, ExpectedEvent::BlockMeta { blockhash: bh, .. } if bh == &bm.blockhash)) {
                                replayed.push(ExpectedEvent::BlockMeta {
                                    slot: bm.slot,
                                    blockhash: bm.blockhash.clone(),
                                    tx_count: bm.executed_transaction_count,
                                });
                            }
                        }
                        _ => {}
                    },
                }

                if replayed.len() >= expected.len() {
                    break;
                }
            }
        }
    }

    shutdown.cancel();

    assert_eq!(
        replayed.len(),
        expected.len(),
        "expected {} fixture events but got {}",
        expected.len(),
        replayed.len()
    );

    for event in &expected {
        assert!(replayed.contains(event), "missing fixture event: {event:?}");
    }
}

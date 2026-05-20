use futures::StreamExt;
use solana_yellowstone_grpc_mock::geyser_client::MockGeyserClient;
use solana_yellowstone_grpc_mock::interface::GeyserSource;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots};

// Proto encodes slot status as i32:
//   0 = Processed, 1 = Confirmed, 2 = Finalized
const STATUS_PROCESSED: i32 = 0;
const STATUS_CONFIRMED: i32 = 1;
const STATUS_FINALIZED: i32 = 2;

/// Collect up to `n` slot updates from the mock stream with a hard timeout.
async fn collect_slot_updates(
    req: SubscribeRequest,
    n: usize,
    timeout: Duration,
) -> Vec<(
    Vec<String>,
    yellowstone_grpc_proto::geyser::SubscribeUpdateSlot,
)> {
    let mut mock = MockGeyserClient::new(0, None, CancellationToken::new());
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
                panic!("timed out collecting {n} slot updates, got {}", collected.len());
            }

            next = stream.next() => {
                match next {
                    Some(Ok(update)) => {
                        if let Some(UpdateOneof::Slot(s)) = update.update_oneof {
                            collected.push((update.filters, s));
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
async fn slot_update_emits_all_three_status_transitions_per_slot() {
    let mut slots = HashMap::new();
    slots.insert(
        "slot_filter".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        },
    );
    let req = SubscribeRequest {
        slots,
        ..Default::default()
    };

    // 3 statuses × at least 2 distinct slots = 6 updates.
    let updates = collect_slot_updates(req, 6, Duration::from_secs(3)).await;

    // Group emitted statuses by slot.
    let mut by_slot: HashMap<u64, Vec<i32>> = HashMap::new();
    for (_filters, s) in &updates {
        by_slot.entry(s.slot).or_default().push(s.status);
    }

    // At least one slot should have produced all three transitions.
    let any_complete = by_slot.values().any(|statuses| {
        statuses.contains(&STATUS_PROCESSED)
            && statuses.contains(&STATUS_CONFIRMED)
            && statuses.contains(&STATUS_FINALIZED)
    });
    assert!(
        any_complete,
        "expected at least one slot to emit Processed, Confirmed, and Finalized; got {by_slot:?}",
    );
}

#[tokio::test]
async fn slot_update_carries_configured_filter_name() {
    let mut slots = HashMap::new();
    slots.insert(
        "my_slot_filter".to_string(),
        SubscribeRequestFilterSlots::default(),
    );
    let req = SubscribeRequest {
        slots,
        ..Default::default()
    };

    let updates = collect_slot_updates(req, 3, Duration::from_secs(2)).await;

    for (filters, _s) in &updates {
        assert_eq!(
            filters,
            &vec!["my_slot_filter".to_string()],
            "slot update should carry the configured filter name",
        );
    }
}

#[tokio::test]
async fn slot_update_parent_linkage_is_correct() {
    let mut slots = HashMap::new();
    slots.insert(
        "slot_filter".to_string(),
        SubscribeRequestFilterSlots::default(),
    );
    let req = SubscribeRequest {
        slots,
        ..Default::default()
    };

    let updates = collect_slot_updates(req, 5, Duration::from_secs(3)).await;

    for (_filters, s) in &updates {
        match s.parent {
            Some(parent) => assert_eq!(
                parent,
                s.slot.saturating_sub(1),
                "parent slot should be exactly one less than the slot",
            ),
            None => panic!("slot update should always set a parent"),
        }
    }
}

#[tokio::test]
async fn slot_update_starts_from_configured_slot() {
    const START: u64 = 1000;
    let mut slots = HashMap::new();
    slots.insert(
        "slot_filter".to_string(),
        SubscribeRequestFilterSlots::default(),
    );
    let req = SubscribeRequest {
        slots,
        ..Default::default()
    };

    let mut mock = MockGeyserClient::new(START, None, CancellationToken::new());
    let (_sink, mut stream) = mock.subscribe(Some(req)).await.expect("subscribe");

    // Take the first slot update we see — it should be at the configured start slot.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        if let Ok(Some(Ok(update))) = next {
            if let Some(UpdateOneof::Slot(s)) = update.update_oneof {
                assert_eq!(
                    s.slot, START,
                    "first slot update should match configured start_slot"
                );
                return;
            }
        }
    }
    panic!("never observed a slot update");
}

#[tokio::test]
async fn multiple_slot_filters_each_receive_updates() {
    let mut slots = HashMap::new();
    slots.insert(
        "filter_a".to_string(),
        SubscribeRequestFilterSlots::default(),
    );
    slots.insert(
        "filter_b".to_string(),
        SubscribeRequestFilterSlots::default(),
    );
    let req = SubscribeRequest {
        slots,
        ..Default::default()
    };

    // Collect enough updates to be confident both filters appear.
    let updates = collect_slot_updates(req, 12, Duration::from_secs(5)).await;

    let seen_a = updates
        .iter()
        .any(|(f, _)| f.contains(&"filter_a".to_string()));
    let seen_b = updates
        .iter()
        .any(|(f, _)| f.contains(&"filter_b".to_string()));

    assert!(seen_a, "filter_a should have received slot updates");
    assert!(seen_b, "filter_b should have received slot updates");
}

#[tokio::test]
async fn slot_update_emitted_even_without_slot_subscription() {
    // No slot filter, but the stream should still emit fallback slot updates
    // to keep itself alive.
    let req = SubscribeRequest::default();

    let updates = collect_slot_updates(req, 3, Duration::from_secs(3)).await;

    assert!(
        !updates.is_empty(),
        "stream should emit fallback slot updates when no slot filter is configured",
    );
}

use crate::{
    distributed::{
        cleanup_redis_node, distributed_user_data, is_valid_distributed_message,
        process_redis_message, reconcile_redis_node, schedule_empty_room_cleanup,
    },
    state::*,
};
use futures::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

const PROTOCOL_VERSION: u8 = 1;
const REDIS_CHUNK_SIZE: usize = 256 * 1024;
const MAX_REDIS_FRAME_SIZE: usize = DISTRIBUTED_MAX_MESSAGE_SIZE;
const MAX_REDIS_CHUNKS: usize = MAX_REDIS_FRAME_SIZE.div_ceil(REDIS_CHUNK_SIZE);
const RECONNECT_MAX_SECS: u64 = 30;
const CHUNK_TTL_SECS: u64 = 60;
const MAX_PENDING_CHUNK_ASSEMBLIES: usize = 256;
// Incomplete transmissions are controlled by Redis publishers, so limiting
// only their count would still permit several gigabytes of buffered chunks.
const MAX_PENDING_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_SNAPSHOTS: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum EnvelopeKind {
    Event,
    Heartbeat,
    SnapshotRequest,
    SnapshotStart,
    SnapshotEnd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RedisEnvelope {
    version: u8,
    source_node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<String>,
    kind: EnvelopeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<DistributedMessage>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RedisChunk {
    version: u8,
    sender: String,
    transmission_id: String,
    index: usize,
    total: usize,
    payload: String,
}

struct ChunkAssembly {
    last_activity: Instant,
    total: usize,
    bytes: usize,
    parts: Vec<Option<String>>,
}

struct SnapshotAssembly {
    last_activity: Instant,
    users: HashSet<RemoteUserKey>,
    // Live events can arrive through a faster Redis endpoint while an older
    // snapshot is still streaming through another one. Patch them into every
    // in-flight snapshot so SnapshotEnd cannot resurrect a departed user or
    // remove a user who joined after SnapshotStart.
    removed_users: HashSet<RemoteUserKey>,
    // A delayed snapshot must not recreate a channel that a newer live
    // rename/delete already superseded through another Redis path.
    superseded_channels: HashSet<(String, String)>,
}

type LastSeen = HashMap<String, Instant>;
type Assemblies = HashMap<(String, String), ChunkAssembly>;
type Snapshots = HashMap<(String, String), SnapshotAssembly>;

#[derive(Clone)]
struct RedisRuntime {
    last_seen: Arc<Mutex<LastSeen>>,
    assemblies: Arc<Mutex<Assemblies>>,
    snapshots: Arc<Mutex<Snapshots>>,
    snapshot_permits: Arc<Semaphore>,
}

pub(crate) fn parse_redis_urls<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut urls = HashSet::new();
    for value in values {
        urls.extend(
            value
                .split([',', ';', '\n'])
                .map(str::trim)
                .filter(|url| !url.is_empty())
                .map(str::to_string),
        );
    }
    let mut urls: Vec<_> = urls.into_iter().collect();
    urls.sort();
    urls
}

pub(crate) fn spawn_redis_distributed(state: AppState, redis_urls: Vec<String>, prefix: String) {
    let prefix = prefix.trim().trim_end_matches(':');
    let prefix = if prefix.is_empty() {
        "rustrooms"
    } else {
        prefix
    };
    let channel = format!("{prefix}:events:v1");
    let runtime = RedisRuntime {
        last_seen: Arc::new(Mutex::new(HashMap::new())),
        assemblies: Arc::new(Mutex::new(HashMap::new())),
        snapshots: Arc::new(Mutex::new(HashMap::new())),
        // Bound profile cloning and Redis writes when many nodes request a
        // snapshot at once.
        snapshot_permits: Arc::new(Semaphore::new(2)),
    };

    for (index, redis_url) in redis_urls.into_iter().enumerate() {
        let client = match redis::Client::open(redis_url.as_str()) {
            Ok(client) => client,
            Err(error) => {
                eprintln!(
                    "REDIS DISTRIBUTED: Invalid Redis endpoint {}: {error}",
                    index + 1
                );
                continue;
            }
        };
        let label = redis_endpoint_label(&redis_url, index);
        let state = state.clone();
        let channel = channel.clone();
        let runtime = runtime.clone();
        let mut distributed_rx = state.distributed_tx.subscribe();
        tokio::spawn(async move {
            let mut retry_secs = 1u64;
            loop {
                match run_session(
                    &client,
                    &channel,
                    &label,
                    &state,
                    &mut distributed_rx,
                    &runtime,
                    &mut retry_secs,
                )
                .await
                {
                    Ok(()) => {
                        eprintln!("REDIS DISTRIBUTED [{label}]: Connection closed; reconnecting")
                    }
                    Err(error) => eprintln!(
                        "REDIS DISTRIBUTED [{label}]: Connection unavailable: {error}; retrying in {retry_secs}s"
                    ),
                }
                tokio::time::sleep(Duration::from_secs(retry_secs)).await;
                retry_secs = (retry_secs * 2).min(RECONNECT_MAX_SECS);
            }
        });
    }
}

fn redis_endpoint_label(redis_url: &str, index: usize) -> String {
    url::Url::parse(redis_url)
        .ok()
        .and_then(|url| {
            let host = url.host_str()?;
            Some(match url.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_string(),
            })
        })
        .unwrap_or_else(|| format!("endpoint-{}", index + 1))
}

async fn run_session(
    client: &redis::Client,
    channel: &str,
    label: &str,
    state: &AppState,
    distributed_rx: &mut tokio::sync::broadcast::Receiver<String>,
    runtime: &RedisRuntime,
    retry_secs: &mut u64,
) -> redis::RedisResult<()> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    let mut publisher = client.get_multiplexed_async_connection().await?;

    // Events queued while this endpoint was disconnected are stale relative
    // to the authoritative snapshot below (a queued join after a later leave
    // can otherwise resurrect a ghost). Start at the live tail instead.
    *distributed_rx = state.distributed_tx.subscribe();

    println!("REDIS DISTRIBUTED [{label}]: Connected and subscribed to {channel}");
    *retry_secs = 1;
    publish_envelope(
        &mut publisher,
        channel,
        &RedisEnvelope {
            version: PROTOCOL_VERSION,
            source_node_id: state.node_id.clone(),
            target_node_id: None,
            snapshot_id: None,
            kind: EnvelopeKind::Heartbeat,
            message: None,
        },
    )
    .await?;
    publish_envelope(
        &mut publisher,
        channel,
        &RedisEnvelope {
            version: PROTOCOL_VERSION,
            source_node_id: state.node_id.clone(),
            target_node_id: None,
            snapshot_id: None,
            kind: EnvelopeKind::SnapshotRequest,
            message: None,
        },
    )
    .await?;
    // Re-announce this node as well. Publish in the background: a large
    // snapshot must not stop this session from reading heartbeats/events (and
    // falsely timing out healthy peers) while Redis accepts the payload.
    spawn_local_snapshot(
        publisher.clone(),
        channel.to_string(),
        state.clone(),
        None,
        runtime.snapshot_permits.clone(),
    );

    let mut heartbeat = tokio::time::interval(Duration::from_secs(REDIS_HEARTBEAT_SECS));
    heartbeat.tick().await;
    let mut liveness = tokio::time::interval(Duration::from_secs(REDIS_HEARTBEAT_SECS));
    liveness.tick().await;
    let mut messages = pubsub.on_message();

    loop {
        tokio::select! {
            received = messages.next() => {
                let Some(received) = received else {
                    return Ok(());
                };
                let raw: String = match received.get_payload() {
                    Ok(raw) => raw,
                    Err(error) => {
                        eprintln!("REDIS DISTRIBUTED [{label}]: Ignoring invalid Pub/Sub payload: {error}");
                        continue;
                    }
                };
                if let Some(envelope) = accept_chunk(&raw, &runtime.assemblies).await {
                    handle_envelope(
                        envelope,
                        &mut publisher,
                        channel,
                        state,
                        &runtime.last_seen,
                        &runtime.snapshots,
                        &runtime.snapshot_permits,
                    ).await?;
                }
            }
            event = distributed_rx.recv() => {
                match event {
                    Ok(raw) => {
                        let Ok(message) = serde_json::from_str::<DistributedMessage>(&raw) else {
                            continue;
                        };
                        publish_envelope(
                            &mut publisher,
                            channel,
                            &RedisEnvelope {
                                version: PROTOCOL_VERSION,
                                source_node_id: state.node_id.clone(),
                                target_node_id: None,
                                snapshot_id: None,
                                kind: EnvelopeKind::Event,
                                message: Some(message),
                            },
                        ).await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!("REDIS DISTRIBUTED [{label}]: Outbound queue lagged by {skipped} event(s); re-announcing local state");
                        // This receiver skipped this node's outbound events.
                        // Asking peers for their snapshots would not repair
                        // their stale view of us; publish our authoritative
                        // local snapshot instead.
                        spawn_local_snapshot(
                            publisher.clone(),
                            channel.to_string(),
                            state.clone(),
                            None,
                            runtime.snapshot_permits.clone(),
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            _ = heartbeat.tick() => {
                publish_envelope(
                    &mut publisher,
                    channel,
                    &RedisEnvelope {
                        version: PROTOCOL_VERSION,
                        source_node_id: state.node_id.clone(),
                        target_node_id: None,
                        snapshot_id: None,
                        kind: EnvelopeKind::Heartbeat,
                        message: None,
                    },
                ).await?;
            }
            _ = liveness.tick() => {
                cleanup_stale_nodes(state, &runtime.last_seen, &runtime.snapshots).await;
                runtime.assemblies.lock().await.retain(|_, assembly| {
                    assembly.last_activity.elapsed() < Duration::from_secs(CHUNK_TTL_SECS)
                });
                runtime.snapshots.lock().await.retain(|_, snapshot| {
                    snapshot.last_activity.elapsed() < Duration::from_secs(CHUNK_TTL_SECS)
                });
            }
        }
    }
}

async fn handle_envelope(
    envelope: RedisEnvelope,
    publisher: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    state: &AppState,
    last_seen: &Arc<Mutex<LastSeen>>,
    snapshots: &Arc<Mutex<Snapshots>>,
    snapshot_permits: &Arc<Semaphore>,
) -> redis::RedisResult<()> {
    if envelope.version != PROTOCOL_VERSION
        || envelope.source_node_id == state.node_id
        || Uuid::parse_str(&envelope.source_node_id).is_err()
    {
        return Ok(());
    }

    // Targeted snapshot traffic still proves that its source is alive.
    last_seen
        .lock()
        .await
        .insert(envelope.source_node_id.clone(), Instant::now());
    if envelope
        .target_node_id
        .as_ref()
        .is_some_and(|target| target != &state.node_id)
    {
        return Ok(());
    }

    match envelope.kind {
        EnvelopeKind::Heartbeat => {}
        EnvelopeKind::SnapshotRequest => {
            spawn_local_snapshot(
                publisher.clone(),
                channel.to_string(),
                state.clone(),
                Some(envelope.source_node_id.clone()),
                snapshot_permits.clone(),
            );
        }
        EnvelopeKind::SnapshotStart => {
            let Some(snapshot_id) = valid_snapshot_id(envelope.snapshot_id) else {
                return Ok(());
            };
            let mut snapshots = snapshots.lock().await;
            let key = (envelope.source_node_id, snapshot_id);
            // Duplicate starts can be delivered by redundant Redis paths. Do
            // not reset records already assembled from the faster path.
            if let Some(snapshot) = snapshots.get_mut(&key) {
                snapshot.last_activity = Instant::now();
            } else {
                if snapshots.len() >= MAX_PENDING_SNAPSHOTS
                    && let Some(oldest) = snapshots
                        .iter()
                        .min_by_key(|(_, snapshot)| snapshot.last_activity)
                        .map(|(key, _)| key.clone())
                {
                    snapshots.remove(&oldest);
                }
                snapshots.insert(
                    key,
                    SnapshotAssembly {
                        last_activity: Instant::now(),
                        users: HashSet::new(),
                        removed_users: HashSet::new(),
                        superseded_channels: HashSet::new(),
                    },
                );
            }
        }
        EnvelopeKind::SnapshotEnd => {
            let Some(snapshot_id) = valid_snapshot_id(envelope.snapshot_id) else {
                return Ok(());
            };
            // Keep the snapshot mutex until reconciliation finishes. A live
            // join otherwise could slip between remove() and reconcile() and
            // be incorrectly treated as missing from the older snapshot.
            let mut snapshot_guard = snapshots.lock().await;
            let snapshot = snapshot_guard.remove(&(envelope.source_node_id.clone(), snapshot_id));
            let Some(snapshot) = snapshot else {
                return Ok(());
            };
            let affected_rooms =
                reconcile_redis_node(state, &envelope.source_node_id, &snapshot.users).await;
            drop(snapshot_guard);
            for room_id in affected_rooms {
                schedule_empty_room_cleanup(state, &room_id).await;
            }
        }
        EnvelopeKind::Event => {
            let Some(message) = envelope.message else {
                return Ok(());
            };
            if !is_valid_distributed_message(&message) {
                return Ok(());
            }
            let user_key = (
                message.room_id.clone(),
                message.channel_id.clone(),
                message.user_id.clone(),
            );
            if let Some(snapshot_id) = envelope.snapshot_id {
                let Some(snapshot_id) = valid_snapshot_id(Some(snapshot_id)) else {
                    return Ok(());
                };
                let mut snapshots = snapshots.lock().await;
                let Some(snapshot) =
                    snapshots.get_mut(&(envelope.source_node_id.clone(), snapshot_id))
                else {
                    return Ok(());
                };
                snapshot.last_activity = Instant::now();
                if snapshot
                    .superseded_channels
                    .contains(&(message.room_id.clone(), message.channel_id.clone()))
                {
                    return Ok(());
                }
                if message.msg_type == "user-joined" {
                    if snapshot.removed_users.contains(&user_key) {
                        // A live leave newer than this snapshot record already
                        // arrived through another endpoint.
                        return Ok(());
                    }
                    snapshot.users.insert(user_key);
                }
            } else {
                // Fold live changes into snapshots currently being assembled.
                // Independent Redis paths can have very different latency.
                let is_presence = matches!(
                    message.msg_type.as_str(),
                    "user-joined" | "user-left" | "user-kicked" | "user-banned"
                );
                let is_channel_change = matches!(
                    message.msg_type.as_str(),
                    "rename-channel" | "delete-channel"
                );
                if is_presence || is_channel_change {
                    let mut snapshots = snapshots.lock().await;
                    for ((source, _), snapshot) in snapshots.iter_mut() {
                        if is_presence {
                            let applies = matches!(message.msg_type.as_str(), "user-kicked" | "user-banned")
                                || source == &envelope.source_node_id;
                            if applies {
                                snapshot.last_activity = Instant::now();
                                if message.msg_type == "user-joined" {
                                    snapshot.removed_users.remove(&user_key);
                                    snapshot.users.insert(user_key.clone());
                                } else {
                                    snapshot.users.remove(&user_key);
                                    snapshot.removed_users.insert(user_key.clone());
                                }
                            }
                        }
                        if is_channel_change {
                            snapshot.last_activity = Instant::now();
                            let old_channel = (message.room_id.clone(), message.channel_id.clone());
                            snapshot.superseded_channels.insert(old_channel.clone());
                            if message.msg_type == "rename-channel"
                                && let Some(new_channel) = message
                                    .data
                                    .as_ref()
                                    .and_then(|data| data.get("newName"))
                                    .and_then(serde_json::Value::as_str)
                                    .and_then(normalize_channel_id)
                            {
                                let moved: Vec<_> = snapshot
                                    .users
                                    .iter()
                                    .filter(|(rid, cid, _)| {
                                        rid == &old_channel.0 && cid == &old_channel.1
                                    })
                                    .cloned()
                                    .collect();
                                for old_key in moved {
                                    snapshot.users.remove(&old_key);
                                    snapshot.users.insert((
                                        old_key.0,
                                        new_channel.clone(),
                                        old_key.2,
                                    ));
                                }
                            } else {
                                snapshot.users.retain(|(rid, cid, _)| {
                                    rid != &old_channel.0 || cid != &old_channel.1
                                });
                            }
                        }
                    }
                }
            }
            process_redis_message(message, &envelope.source_node_id, state).await;
        }
    }
    Ok(())
}

fn valid_snapshot_id(snapshot_id: Option<String>) -> Option<String> {
    snapshot_id.filter(|id| Uuid::parse_str(id).is_ok())
}

async fn publish_envelope(
    connection: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    envelope: &RedisEnvelope,
) -> redis::RedisResult<()> {
    let payload = serde_json::to_string(envelope).map_err(|error| {
        redis::RedisError::from((
            redis::ErrorKind::TypeError,
            "failed to serialize distributed message",
            error.to_string(),
        ))
    })?;
    if payload.len() > MAX_REDIS_FRAME_SIZE {
        eprintln!(
            "REDIS DISTRIBUTED: Dropping oversized distributed frame ({} bytes)",
            payload.len()
        );
        return Ok(());
    }

    let transmission_id = Uuid::new_v4().to_string();
    let parts = split_string(&payload, REDIS_CHUNK_SIZE);
    let total = parts.len();
    for (index, part) in parts.into_iter().enumerate() {
        let wire = serde_json::to_string(&RedisChunk {
            version: PROTOCOL_VERSION,
            sender: envelope.source_node_id.clone(),
            transmission_id: transmission_id.clone(),
            index,
            total,
            payload: part,
        })
        .map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "failed to serialize Redis chunk",
                error.to_string(),
            ))
        })?;
        let _: i64 = connection.publish(channel, wire).await?;
    }
    Ok(())
}

async fn accept_chunk(raw: &str, assemblies: &Arc<Mutex<Assemblies>>) -> Option<RedisEnvelope> {
    let chunk: RedisChunk = serde_json::from_str(raw).ok()?;
    if chunk.version != PROTOCOL_VERSION
        || Uuid::parse_str(&chunk.sender).is_err()
        || Uuid::parse_str(&chunk.transmission_id).is_err()
        || chunk.total == 0
        || chunk.total > MAX_REDIS_CHUNKS
        || chunk.index >= chunk.total
        || chunk.payload.len() > REDIS_CHUNK_SIZE
    {
        return None;
    }

    let key = (chunk.sender.clone(), chunk.transmission_id.clone());
    let mut pending = assemblies.lock().await;
    if !pending.contains_key(&key)
        && pending.len() >= MAX_PENDING_CHUNK_ASSEMBLIES
        && let Some(oldest) = pending
            .iter()
            .min_by_key(|(_, assembly)| assembly.last_activity)
            .map(|(key, _)| key.clone())
    {
        pending.remove(&oldest);
    }
    if pending
        .get(&key)
        .is_some_and(|assembly| assembly.total != chunk.total)
    {
        pending.remove(&key);
        return None;
    }

    let is_new_part = pending
        .get(&key)
        .is_none_or(|assembly| assembly.parts[chunk.index].is_none());
    if is_new_part {
        while pending
            .values()
            .fold(0usize, |total, assembly| {
                total.saturating_add(assembly.bytes)
            })
            .saturating_add(chunk.payload.len())
            > MAX_PENDING_CHUNK_BYTES
        {
            let oldest = pending
                .iter()
                .filter(|(candidate, _)| *candidate != &key)
                .min_by_key(|(_, assembly)| assembly.last_activity)
                .map(|(candidate, _)| candidate.clone());
            let Some(oldest) = oldest else {
                pending.remove(&key);
                return None;
            };
            pending.remove(&oldest);
        }
    }

    let assembly = pending.entry(key.clone()).or_insert_with(|| ChunkAssembly {
        last_activity: Instant::now(),
        total: chunk.total,
        bytes: 0,
        parts: (0..chunk.total).map(|_| None).collect(),
    });
    assembly.last_activity = Instant::now();
    if is_new_part {
        assembly.bytes += chunk.payload.len();
        if assembly.bytes > MAX_REDIS_FRAME_SIZE {
            pending.remove(&key);
            return None;
        }
        assembly.parts[chunk.index] = Some(chunk.payload);
    }
    if assembly.parts.iter().any(Option::is_none) {
        return None;
    }

    let completed = pending.remove(&key)?;
    drop(pending);
    let mut payload = String::with_capacity(completed.bytes);
    for part in completed.parts {
        payload.push_str(part.as_deref()?);
    }
    let envelope: RedisEnvelope = serde_json::from_str(&payload).ok()?;
    (envelope.source_node_id == chunk.sender).then_some(envelope)
}

fn split_string(value: &str, max_bytes: usize) -> Vec<String> {
    if value.is_empty() {
        return vec![String::new()];
    }
    let mut parts = Vec::new();
    let mut start = 0;
    while start < value.len() {
        let mut end = (start + max_bytes).min(value.len());
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        parts.push(value[start..end].to_string());
        start = end;
    }
    parts
}

fn spawn_local_snapshot(
    mut publisher: redis::aio::MultiplexedConnection,
    channel: String,
    state: AppState,
    target_node_id: Option<String>,
    snapshot_permits: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        let Ok(_permit) = snapshot_permits.acquire_owned().await else {
            return;
        };
        if let Err(error) =
            publish_local_snapshot(&mut publisher, &channel, &state, target_node_id).await
        {
            eprintln!("REDIS DISTRIBUTED: Failed to publish local snapshot: {error}");
        }
    });
}

async fn publish_local_snapshot(
    publisher: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    state: &AppState,
    target_node_id: Option<String>,
) -> redis::RedisResult<()> {
    let snapshot_id = Uuid::new_v4().to_string();
    publish_envelope(
        publisher,
        channel,
        &RedisEnvelope {
            version: PROTOCOL_VERSION,
            source_node_id: state.node_id.clone(),
            target_node_id: target_node_id.clone(),
            snapshot_id: Some(snapshot_id.clone()),
            kind: EnvelopeKind::SnapshotStart,
            message: None,
        },
    )
    .await?;

    let mut snapshot = local_snapshot_stream(state.clone());
    while let Some(message) = snapshot.recv().await {
        publish_envelope(
            publisher,
            channel,
            &RedisEnvelope {
                version: PROTOCOL_VERSION,
                source_node_id: state.node_id.clone(),
                target_node_id: target_node_id.clone(),
                snapshot_id: Some(snapshot_id.clone()),
                kind: EnvelopeKind::Event,
                message: Some(message),
            },
        )
        .await?;
    }
    publish_envelope(
        publisher,
        channel,
        &RedisEnvelope {
            version: PROTOCOL_VERSION,
            source_node_id: state.node_id.clone(),
            target_node_id,
            snapshot_id: Some(snapshot_id),
            kind: EnvelopeKind::SnapshotEnd,
            message: None,
        },
    )
    .await?;
    Ok(())
}

fn local_snapshot_stream(state: AppState) -> tokio::sync::mpsc::Receiver<DistributedMessage> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        // Clone rooms before metadata. A join creates its channel while holding
        // the room lock and then records its timestamp, so this order ensures
        // every room in the snapshot also has its completed metadata update.
        // Both locks are released before any Redis I/O.
        let rooms = state.rooms.lock().await.clone();
        let times = state.channel_creation_times.lock().await.clone();
        for (room_id, room) in &rooms {
            for (channel_id, channel) in room {
                let created_at = times
                    .get(room_id)
                    .and_then(|channels| channels.get(channel_id))
                    .copied()
                    .filter(|created_at| *created_at > 0)
                    .unwrap_or_else(current_unix_secs);
                let channel_message = DistributedMessage {
                    msg_type: "channel-upsert".into(),
                    room_id: room_id.clone(),
                    channel_id: channel_id.clone(),
                    user_id: state.node_id.clone(),
                    msg_id: Uuid::new_v4().to_string(),
                    status: None,
                    data: Some(serde_json::json!({ "createdAt": created_at })),
                    signal_msg: None,
                };
                if tx.send(channel_message).await.is_err() {
                    return;
                }

                for (user_id, (_, status)) in channel {
                    let user_message = DistributedMessage {
                        msg_type: "user-joined".into(),
                        room_id: room_id.clone(),
                        channel_id: channel_id.clone(),
                        user_id: user_id.clone(),
                        msg_id: Uuid::new_v4().to_string(),
                        status: Some(status.clone()),
                        data: Some(distributed_user_data(status, created_at)),
                        signal_msg: None,
                    };
                    if tx.send(user_message).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    rx
}

async fn cleanup_stale_nodes(
    state: &AppState,
    last_seen: &Arc<Mutex<LastSeen>>,
    snapshots: &Arc<Mutex<Snapshots>>,
) {
    let stale: Vec<String> = {
        let mut last_seen = last_seen.lock().await;
        let stale: Vec<_> = last_seen
            .iter()
            .filter(|(_, seen)| seen.elapsed() > Duration::from_secs(REDIS_NODE_TIMEOUT_SECS))
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node_id in &stale {
            last_seen.remove(node_id);
        }
        stale
    };

    for node_id in stale {
        snapshots
            .lock()
            .await
            .retain(|(source_node_id, _), _| source_node_id != &node_id);
        let affected_rooms = cleanup_redis_node(state, &node_id).await;
        for room_id in affected_rooms {
            schedule_empty_room_cleanup(state, &room_id).await;
        }
        eprintln!("REDIS DISTRIBUTED: Node {node_id} timed out; removed its stale presence");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_url_lists_are_split_trimmed_and_deduplicated() {
        let urls = parse_redis_urls([
            "rediss://one.example:6379, rediss://two.example:6379",
            "rediss://one.example:6379;\nredis://three.example:6379",
        ]);
        assert_eq!(
            urls,
            vec![
                "redis://three.example:6379",
                "rediss://one.example:6379",
                "rediss://two.example:6379",
            ]
        );
    }

    #[test]
    fn endpoint_labels_never_include_redis_credentials() {
        let label = redis_endpoint_label("rediss://default:very-secret@redis.example.com:6380", 0);
        assert_eq!(label, "redis.example.com:6380");
    }

    #[test]
    fn chunks_round_trip_unicode_without_splitting_code_points() {
        let input = format!("{}🙂{}", "a".repeat(10), "b".repeat(10));
        let chunks = split_string(&input, 12);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 12));
        assert_eq!(chunks.concat(), input);
    }
}

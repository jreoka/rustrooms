use crate::state::*;
use axum::extract::ws::Message;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(crate) fn distributed_user_data(status: &UserStatus, created_at: u64) -> serde_json::Value {
    serde_json::json!({
        "nickname": status.nickname,
        "avatar": status.avatar,
        "isGif": status.is_gif,
        "staticFrame": status.static_frame,
        "isMuted": status.is_muted,
        "isDeafened": status.is_deafened,
        "screenEnabled": status.is_screen_sharing,
        "isLowBandwidthMode": status.is_low_bandwidth_mode,
        "isMobile": status.is_mobile,
        "profileRev": status.profile_rev,
        "createdAt": created_at,
    })
}

pub(crate) async fn process_redis_message(
    message: DistributedMessage,
    source_node_id: &str,
    state: &AppState,
) -> bool {
    if !is_valid_distributed_message(&message)
        || !remember_distributed_message(&message, state).await
        || !update_remote_ownership(&message, source_node_id, &state.remote_user_owners).await
    {
        return false;
    }
    apply_distributed_message(&message, &state.rooms, &state.remote_users, state).await;
    true
}

pub(crate) fn is_valid_distributed_message(msg: &DistributedMessage) -> bool {
    // Room-scoped admin messages carry no channel or user of their own.
    let room_scoped = matches!(msg.msg_type.as_str(), "admin-update" | "knock-update");
    let channel_ok = room_scoped
        || msg.msg_type == "knock-response"
        || normalize_channel_id(&msg.channel_id).as_deref() == Some(msg.channel_id.as_str());
    let user_ok = room_scoped || Uuid::parse_str(&msg.user_id).is_ok();
    if !is_valid_room_id(&msg.room_id)
        || !channel_ok
        || !user_ok
        || Uuid::parse_str(&msg.msg_id).is_err()
        || msg
            .data
            .as_ref()
            .is_some_and(|data| data.to_string().len() > MAX_DISTRIBUTED_DATA_LEN)
        || msg
            .signal_msg
            .as_ref()
            .is_some_and(|signal| signal.len() > MAX_DISTRIBUTED_DATA_LEN)
    {
        return false;
    }

    let valid_status = msg.status.as_ref().is_none_or(|status| {
        status.nickname.chars().count() <= MAX_NICKNAME_LEN
            && status
                .avatar
                .as_ref()
                .is_none_or(|avatar| avatar.len() <= MAX_AVATAR_DATA_LEN)
            && status
                .static_frame
                .as_ref()
                .is_none_or(|frame| frame.len() <= MAX_STATIC_FRAME_DATA_LEN)
    });
    if !valid_status {
        return false;
    }

    match msg.msg_type.as_str() {
        "user-joined" | "user-update" => msg.status.is_some(),
        "user-left" | "user-kicked" | "user-banned" | "delete-channel" | "user-resumed" => true,
        "channel-upsert" => msg
            .data
            .as_ref()
            .and_then(|data| data.get("createdAt"))
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|created_at| created_at > 0),
        "rename-channel" => msg
            .data
            .as_ref()
            .and_then(|data| data.get("newName"))
            .and_then(serde_json::Value::as_str)
            .and_then(normalize_channel_id)
            .is_some(),
        "cam-toggle" | "screen-toggle" => msg.data.is_some(),
        "admin-update" | "knock-update" | "knock-response" => msg.data.is_some(),
        "mod-action" => msg.signal_msg.is_some(),
        "signal" => msg.signal_msg.as_ref().is_some_and(|raw| {
            serde_json::from_str::<SignalMessage>(raw)
                .ok()
                .and_then(|signal| signal.target)
                .is_some_and(|target| Uuid::parse_str(&target).is_ok())
        }),
        _ => false,
    }
}

async fn update_remote_ownership(
    msg: &DistributedMessage,
    source_node_id: &str,
    owners: &RemoteUserOwnersMap,
) -> bool {
    let key = (
        msg.room_id.clone(),
        msg.channel_id.clone(),
        msg.user_id.clone(),
    );
    let mut owners = owners.lock().await;
    match msg.msg_type.as_str() {
        "user-joined" => match owners.get(&key) {
            // First owner wins. UUID collisions are extremely unlikely, but a
            // delayed snapshot must not steal a live identity from another node.
            Some(owner) => owner == source_node_id,
            None => {
                owners.insert(key, source_node_id.to_string());
                true
            }
        },
        "user-left" => {
            if owners
                .get(&key)
                .is_some_and(|owner| owner == source_node_id)
            {
                owners.remove(&key);
                true
            } else {
                // A delayed leave from an old owner must not remove the current
                // owner's presence.
                false
            }
        }
        "user-update" | "cam-toggle" | "screen-toggle" => owners
            .get(&key)
            .is_some_and(|owner| owner == source_node_id),
        // A kick is a deployment-wide moderation event and can originate on
        // a node other than the one hosting the target.
        "user-kicked" | "user-banned" => {
            owners.remove(&key);
            true
        }
        // A resume moves the user's home node: the resuming node
        // becomes the owner so its subsequent presence traffic is
        // accepted everywhere.
        "user-resumed" => {
            owners.insert(key, source_node_id.to_string());
            true
        }
        _ => true,
    }
}

pub(crate) async fn rename_remote_user_owners(
    owners: &RemoteUserOwnersMap,
    room_id: &str,
    old_channel_id: &str,
    new_channel_id: &str,
) {
    let mut owners = owners.lock().await;
    let moved: Vec<_> = owners
        .keys()
        .filter(|(rid, cid, _)| rid == room_id && cid == old_channel_id)
        .cloned()
        .collect();
    for old_key in moved {
        if let Some(owner) = owners.remove(&old_key) {
            owners
                .entry((old_key.0, new_channel_id.to_string(), old_key.2))
                .or_insert(owner);
        }
    }
}

pub(crate) async fn remove_remote_channel_owners(
    owners: &RemoteUserOwnersMap,
    room_id: &str,
    channel_id: &str,
) {
    owners
        .lock()
        .await
        .retain(|(rid, cid, _), _| rid != room_id || cid != channel_id);
}

pub(crate) async fn remove_remote_user_owner(
    owners: &RemoteUserOwnersMap,
    room_id: &str,
    channel_id: &str,
    user_id: &str,
) {
    owners.lock().await.remove(&(
        room_id.to_string(),
        channel_id.to_string(),
        user_id.to_string(),
    ));
}

pub(crate) async fn cleanup_redis_node(state: &AppState, source_node_id: &str) -> HashSet<String> {
    reconcile_redis_node(state, source_node_id, &HashSet::new()).await
}

pub(crate) async fn reconcile_redis_node(
    state: &AppState,
    source_node_id: &str,
    retained: &HashSet<RemoteUserKey>,
) -> HashSet<String> {
    let dead: Vec<_> = {
        let mut owners = state.remote_user_owners.lock().await;
        let dead: Vec<_> = owners
            .iter()
            .filter(|(key, owner)| owner.as_str() == source_node_id && !retained.contains(*key))
            .map(|(key, _)| key.clone())
            .collect();
        for key in &dead {
            owners.remove(key);
        }
        dead
    };

    let mut affected_rooms = HashSet::new();
    for (room_id, channel_id, user_id) in dead {
        let removed_channels = {
            // Keep ownership locked through removal. A user with the same ID
            // may join on another node immediately after the stale owner was
            // removed; cleanup must not delete that replacement's presence.
            let owners = state.remote_user_owners.lock().await;
            let key = (room_id.clone(), channel_id.clone(), user_id.clone());
            if owners.contains_key(&key) {
                Vec::new()
            } else {
                let mut remote = state.remote_users.lock().await;
                if remove_remote_user(&mut remote, &room_id, &channel_id, &user_id) {
                    vec![channel_id]
                } else {
                    // A channel rename may have moved the remote record just
                    // before its ownership key was moved. Prune any matching
                    // record that has no owner instead of leaving a permanent
                    // ghost in the renamed channel.
                    let orphan_channels: Vec<String> = remote
                        .get(&room_id)
                        .into_iter()
                        .flat_map(|room| room.iter())
                        .filter(|(cid, users)| {
                            users.contains_key(&user_id)
                                && !owners.contains_key(&(
                                    room_id.clone(),
                                    (*cid).clone(),
                                    user_id.clone(),
                                ))
                        })
                        .map(|(cid, _)| cid.clone())
                        .collect();
                    for cid in &orphan_channels {
                        remove_remote_user(&mut remote, &room_id, cid, &user_id);
                    }
                    orphan_channels
                }
            }
        };
        if removed_channels.is_empty() {
            continue;
        }
        let notify = serde_json::to_string(&SignalMessage {
            msg_type: "user-left".into(),
            user_id: Some(user_id),
            target: None,
            data: None,
        })
        .unwrap();
        let rooms = state.rooms.lock().await;
        if let Some(room) = rooms.get(&room_id) {
            for channel_id in removed_channels {
                if let Some(channel) = room.get(&channel_id) {
                    for (tx, _) in channel.values() {
                        let _ = tx.try_send(Ok(Message::Text(notify.clone().into())));
                    }
                }
            }
        }
        drop(rooms);
        affected_rooms.insert(room_id);
    }

    for room_id in &affected_rooms {
        broadcast_channel_list(
            &state.rooms,
            &state.remote_users,
            &state.channel_creation_times,
            room_id,
        )
        .await;
    }
    affected_rooms
}

pub(crate) async fn schedule_empty_room_cleanup(state: &AppState, room_id: &str) {
    let has_remote_users = state
        .remote_users
        .lock()
        .await
        .get(room_id)
        .is_some_and(|room| room.values().any(|channel| !channel.is_empty()));

    let local_is_empty = state
        .rooms
        .lock()
        .await
        .get(room_id)
        .is_none_or(|room| room.values().all(HashMap::is_empty));
    if !local_is_empty || has_remote_users {
        return;
    }

    let generation = {
        let mut generations = state.room_cleanup_generations.lock().await;
        let next = generations.get(room_id).copied().unwrap_or(0) + 1;
        generations.insert(room_id.to_string(), next);
        next
    };

    let state = state.clone();
    let room_id = room_id.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(ROOM_EMPTY_GRACE_SECS)).await;
        if state
            .room_cleanup_generations
            .lock()
            .await
            .get(&room_id)
            .copied()
            != Some(generation)
        {
            return;
        }

        let has_remote_users = state
            .remote_users
            .lock()
            .await
            .get(&room_id)
            .is_some_and(|room| room.values().any(|channel| !channel.is_empty()));
        if has_remote_users {
            return;
        }

        let removed = {
            let mut rooms = state.rooms.lock().await;
            let is_empty = rooms
                .get(&room_id)
                .is_none_or(|room| room.values().all(HashMap::is_empty));
            if is_empty {
                rooms.remove(&room_id);
                true
            } else {
                false
            }
        };
        if removed {
            state.channel_creation_times.lock().await.remove(&room_id);
            state.admin_rooms.lock().await.remove(&room_id);
            crate::admin::delete_admin_redis(&state, &room_id).await;
            crate::admin::persist_state(&state).await;
            let mut generations = state.room_cleanup_generations.lock().await;
            if generations.get(&room_id).copied() == Some(generation) {
                generations.remove(&room_id);
            }
        }
    });
}

pub(crate) fn remove_remote_user(
    remote_users: &mut HashMap<String, HashMap<String, HashMap<String, UserStatus>>>,
    room_id: &str,
    channel_id: &str,
    user_id: &str,
) -> bool {
    let mut removed = false;
    let mut remove_room = false;
    if let Some(room) = remote_users.get_mut(room_id) {
        let mut remove_channel = false;
        if let Some(channel) = room.get_mut(channel_id) {
            removed = channel.remove(user_id).is_some();
            remove_channel = channel.is_empty();
        }
        if remove_channel {
            room.remove(channel_id);
        }
        remove_room = room.is_empty();
    }
    if remove_room {
        remote_users.remove(room_id);
    }
    removed
}

async fn remember_distributed_message(msg: &DistributedMessage, state: &AppState) -> bool {
    let mut ids = state.recent_distributed_msg_ids.lock().await;
    if !ids.insert(msg.msg_id.clone()) {
        return false;
    }

    let mut history = state.distributed_msg_history.lock().await;
    history.push_back(msg.msg_id.clone());
    // This must comfortably exceed the outbound queue: independent Redis
    // paths can have very different latency and deliver an old duplicate well
    // after the fast path. Keep the cache bounded to avoid unbounded growth.
    if history.len() > 16_384
        && let Some(oldest) = history.pop_front()
    {
        ids.remove(&oldest);
    }
    true
}

#[cfg(test)]
async fn handle_distributed_message(
    msg: &DistributedMessage,
    rooms: &RoomMap,
    remote_users: &RemoteUsersMap,
    state: &AppState,
) {
    if remember_distributed_message(msg, state).await {
        apply_distributed_message(msg, rooms, remote_users, state).await;
    }
}

async fn apply_distributed_message(
    msg: &DistributedMessage,
    rooms: &RoomMap,
    remote_users: &RemoteUsersMap,
    state: &AppState,
) {
    match msg.msg_type.as_str() {
        "channel-upsert" => {
            let created_at = msg
                .data
                .as_ref()
                .and_then(|data| data.get("createdAt"))
                .and_then(serde_json::Value::as_u64)
                .filter(|created_at| *created_at > 0)
                .unwrap_or_else(current_unix_secs);
            let mut times = state.channel_creation_times.lock().await;
            let stored = times
                .entry(msg.room_id.clone())
                .or_default()
                .entry(msg.channel_id.clone())
                .or_insert(created_at);
            *stored = (*stored).min(created_at);
            drop(times);
            broadcast_channel_list(
                rooms,
                remote_users,
                &state.channel_creation_times,
                &msg.room_id,
            )
            .await;
            // A node can first discover a room from an empty snapshot and then
            // miss the originating node's local grace-period deletion (Redis
            // Pub/Sub has no retained delete event). Give metadata-only rooms
            // the same bounded lifetime; a following user-joined cancels it.
            schedule_empty_room_cleanup(state, &msg.room_id).await;
        }
        "user-joined" => {
            if let Some(ref status) = msg.status {
                // Normalize at the ingest boundary so a remote instance can
                // never store a nickname the owner's own client doesn't show.
                let mut status = status.clone();
                status.nickname = normalize_nickname(&status.nickname);
                normalize_profile_images(&mut status);
                state
                    .room_cleanup_generations
                    .lock()
                    .await
                    .remove(&msg.room_id);
                let is_new_user = {
                    let mut rl = remote_users.lock().await;
                    rl.entry(msg.room_id.clone())
                        .or_default()
                        .entry(msg.channel_id.clone())
                        .or_default()
                        .insert(msg.user_id.clone(), status.clone())
                        .is_none()
                };
                {
                    let created_at = msg
                        .data
                        .as_ref()
                        .and_then(|data| data.get("createdAt"))
                        .and_then(serde_json::Value::as_u64)
                        .filter(|created_at| *created_at > 0)
                        .unwrap_or_else(current_unix_secs);
                    let mut times = state.channel_creation_times.lock().await;
                    let stored = times
                        .entry(msg.room_id.clone())
                        .or_default()
                        .entry(msg.channel_id.clone())
                        .or_insert(created_at);
                    *stored = (*stored).min(created_at);
                }
                if is_new_user {
                    let rooms_lock = rooms.lock().await;
                    if let Some(room) = rooms_lock.get(&msg.room_id)
                        && let Some(channel) = room.get(&msg.channel_id)
                    {
                        let notify = serde_json::to_string(&SignalMessage {
                            msg_type: "user-joined".into(),
                            user_id: Some(msg.user_id.clone()),
                            target: None,
                            data: msg.data.clone(),
                        })
                        .unwrap();
                        for (_, (tx, _)) in channel.iter() {
                            let _ = tx.try_send(Ok(Message::Text(notify.clone().into())));
                        }
                    }
                }
                broadcast_channel_list(
                    rooms,
                    remote_users,
                    &state.channel_creation_times,
                    &msg.room_id,
                )
                .await;
            }
        }
        "user-left" | "user-kicked" | "user-banned" => {
            if msg.msg_type == "user-banned" {
                let mut lock = state.admin_rooms.lock().await;
                lock.entry(msg.room_id.clone())
                    .or_default()
                    .banned
                    .insert(msg.user_id.clone());
            }
            let removed_remote = {
                // user-left authorization removes the old ownership record
                // before this state update. Hold the map while removing so a
                // concurrent replacement join cannot be deleted by the old
                // connection's delayed leave.
                let owners = if msg.msg_type == "user-left" {
                    Some(state.remote_user_owners.lock().await)
                } else {
                    None
                };
                let key = (
                    msg.room_id.clone(),
                    msg.channel_id.clone(),
                    msg.user_id.clone(),
                );
                if owners
                    .as_ref()
                    .is_some_and(|owners| owners.contains_key(&key))
                {
                    false
                } else {
                    let mut rl = remote_users.lock().await;
                    remove_remote_user(&mut rl, &msg.room_id, &msg.channel_id, &msg.user_id)
                }
            };
            // A kick may target a user hosted on this node (cross-node kick):
            // remove them from the local channel and close their socket.
            let removed_local = if matches!(msg.msg_type.as_str(), "user-kicked" | "user-banned") {
                let mut rooms_lock = rooms.lock().await;
                let mut removed_local = false;
                let mut victim_tx = None;
                if let Some(room) = rooms_lock.get_mut(&msg.room_id)
                    && let Some(channel) = room.get_mut(&msg.channel_id)
                    && let Some((tx, _)) = channel.remove(&msg.user_id)
                {
                    removed_local = true;
                    victim_tx = Some(tx);
                }
                if removed_local {
                    let kick_notify = serde_json::to_string(&SignalMessage {
                        msg_type: msg.msg_type.clone(),
                        user_id: Some(msg.user_id.clone()),
                        target: None,
                        data: None,
                    })
                    .unwrap();
                    // Reuse the lock already held above: re-acquiring the
                    // same tokio Mutex would deadlock the message loop.
                    if let Some(room) = rooms_lock.get(&msg.room_id)
                        && let Some(channel) = room.get(&msg.channel_id)
                    {
                        for (_, (tx, _)) in channel.iter() {
                            let _ = tx.try_send(Ok(Message::Text(kick_notify.clone().into())));
                        }
                    }
                    if let Some(victim_tx) = victim_tx {
                        let _ = victim_tx.try_send(Ok(Message::Text(kick_notify.into())));
                        let _ = victim_tx.try_send(Ok(Message::Close(None)));
                    }
                }
                removed_local
            } else {
                false
            };

            if !removed_remote && !removed_local {
                return;
            }
            // When the victim was local, the removal block above already
            // notified the channel; only notify again for remote removals.
            if !removed_local {
                let mtype = if msg.msg_type == "user-kicked" {
                    "user-kicked"
                } else if msg.msg_type == "user-banned" {
                    "user-banned"
                } else {
                    "user-left"
                };
                let rooms_lock = rooms.lock().await;
                if let Some(room) = rooms_lock.get(&msg.room_id)
                    && let Some(channel) = room.get(&msg.channel_id)
                {
                    let notify = serde_json::to_string(&SignalMessage {
                        msg_type: mtype.into(),
                        user_id: Some(msg.user_id.clone()),
                        target: None,
                        data: None,
                    })
                    .unwrap();
                    for (_, (tx, _)) in channel.iter() {
                        let _ = tx.try_send(Ok(Message::Text(notify.clone().into())));
                    }
                }
            }
            broadcast_channel_list(
                rooms,
                remote_users,
                &state.channel_creation_times,
                &msg.room_id,
            )
            .await;
            schedule_empty_room_cleanup(state, &msg.room_id).await;
        }
        "user-update" => {
            if let Some(ref status) = msg.status {
                let mut status = status.clone();
                status.nickname = normalize_nickname(&status.nickname);
                normalize_profile_images(&mut status);
                {
                    let mut rl = remote_users.lock().await;
                    if let Some(room) = rl.get_mut(&msg.room_id)
                        && let Some(channel) = room.get_mut(&msg.channel_id)
                        && let Some(existing) = channel.get_mut(&msg.user_id)
                    {
                        *existing = status.clone();
                    }
                }
                {
                    let rooms_lock = rooms.lock().await;
                    if let Some(room) = rooms_lock.get(&msg.room_id)
                        && let Some(channel) = room.get(&msg.channel_id)
                    {
                        let full_data = serde_json::to_value(status).unwrap();
                        let notify = serde_json::to_string(&SignalMessage {
                            msg_type: "user-update".into(),
                            user_id: Some(msg.user_id.clone()),
                            target: None,
                            data: Some(full_data),
                        })
                        .unwrap();
                        for (_, (tx, _)) in channel.iter() {
                            let _ = tx.try_send(Ok(Message::Text(notify.clone().into())));
                        }
                    }
                }
                broadcast_channel_list(
                    rooms,
                    remote_users,
                    &state.channel_creation_times,
                    &msg.room_id,
                )
                .await;
            }
        }
        "cam-toggle" | "screen-toggle" => {
            if msg.msg_type == "screen-toggle"
                && let Some(enabled) = msg
                    .data
                    .as_ref()
                    .and_then(|d| d.get("enabled"))
                    .and_then(|v| v.as_bool())
            {
                let mut rl = remote_users.lock().await;
                if let Some(room) = rl.get_mut(&msg.room_id)
                    && let Some(channel) = room.get_mut(&msg.channel_id)
                    && let Some(s) = channel.get_mut(&msg.user_id)
                {
                    s.is_screen_sharing = enabled;
                }
            }
            {
                let rooms_lock = rooms.lock().await;
                if let Some(room) = rooms_lock.get(&msg.room_id)
                    && let Some(channel) = room.get(&msg.channel_id)
                {
                    let notify = serde_json::to_string(&SignalMessage {
                        msg_type: msg.msg_type.clone(),
                        user_id: Some(msg.user_id.clone()),
                        target: None,
                        data: msg.data.clone(),
                    })
                    .unwrap();
                    for (_, (tx, _)) in channel.iter() {
                        let _ = tx.try_send(Ok(Message::Text(notify.clone().into())));
                    }
                }
            }
            if msg.msg_type == "screen-toggle" {
                broadcast_channel_list(
                    rooms,
                    remote_users,
                    &state.channel_creation_times,
                    &msg.room_id,
                )
                .await;
            }
        }
        "rename-channel" => {
            if let Some(ref data) = msg.data {
                let new_name = data
                    .get("newName")
                    .and_then(|v| v.as_str())
                    .and_then(normalize_channel_id);
                if let Some(new_name) = new_name {
                    let old_name = msg.channel_id.clone();

                    // Rename the local channel too if this node hosts it (the
                    // initiating node may be acting on a channel it only sees
                    // in its remote view). Refuse when the channel is occupied
                    // or the target name already exists here, so state stays
                    // consistent with the initiating node's checks.
                    {
                        let mut rooms_lock = rooms.lock().await;
                        if let Some(room) = rooms_lock.get_mut(&msg.room_id)
                            && let Some(channel) = room.get(&msg.channel_id)
                        {
                            if channel.is_empty() && !room.contains_key(&new_name) {
                                if let Some(ch) = room.remove(&msg.channel_id) {
                                    room.insert(new_name.clone(), ch);
                                }
                            } else {
                                return;
                            }
                        }
                    }

                    let rename_notify = serde_json::to_string(&SignalMessage {
                        msg_type: "rename-channel".into(),
                        user_id: Some(msg.user_id.clone()),
                        target: None,
                        data: Some(serde_json::json!({
                            "roomId": msg.room_id,
                            "oldName": old_name,
                            "newName": new_name,
                        })),
                    })
                    .unwrap();

                    let mut rl = remote_users.lock().await;
                    if let Some(room) = rl.get_mut(&msg.room_id)
                        && let Some(channel_data) = room.remove(&msg.channel_id)
                    {
                        // Merge, don't replace: another node may already host a
                        // channel with the target name, and wholesale replacement
                        // would drop its users from this node's view.
                        let target = room.entry(new_name.clone()).or_default();
                        for (uid, status) in channel_data {
                            target.entry(uid).or_insert(status);
                        }
                    }
                    drop(rl);
                    rename_remote_user_owners(
                        &state.remote_user_owners,
                        &msg.room_id,
                        &msg.channel_id,
                        &new_name,
                    )
                    .await;

                    {
                        let mut times = state.channel_creation_times.lock().await;
                        if let Some(room_times) = times.get_mut(&msg.room_id)
                            && let Some(created_at) = room_times.remove(&msg.channel_id)
                        {
                            room_times.insert(new_name.clone(), created_at);
                        }
                    }

                    // Forward rename-channel to local WebSocket clients in this room
                    let rooms_lock = rooms.lock().await;
                    if let Some(room) = rooms_lock.get(&msg.room_id) {
                        for (_ch_name, channel) in room.iter() {
                            for (_uid, (tx, _)) in channel.iter() {
                                let _ =
                                    tx.try_send(Ok(Message::Text(rename_notify.clone().into())));
                            }
                        }
                    }
                    drop(rooms_lock);

                    broadcast_channel_list(
                        rooms,
                        remote_users,
                        &state.channel_creation_times,
                        &msg.room_id,
                    )
                    .await;
                }
            }
        }
        "delete-channel" => {
            // Remove the local channel too if this node hosts it. Only empty
            // channels can be deleted (clients have no handler for being
            // evicted from a channel), so abort otherwise.
            {
                let mut rooms_lock = rooms.lock().await;
                if let Some(room) = rooms_lock.get_mut(&msg.room_id)
                    && let Some(channel) = room.get(&msg.channel_id)
                {
                    if channel.is_empty() {
                        room.remove(&msg.channel_id);
                    } else {
                        return;
                    }
                }
            }
            let mut rl = remote_users.lock().await;
            if let Some(room) = rl.get_mut(&msg.room_id) {
                room.remove(&msg.channel_id);
                if room.is_empty() {
                    rl.remove(&msg.room_id);
                }
            }
            drop(rl);
            remove_remote_channel_owners(&state.remote_user_owners, &msg.room_id, &msg.channel_id)
                .await;
            {
                let mut times = state.channel_creation_times.lock().await;
                if let Some(room_times) = times.get_mut(&msg.room_id) {
                    room_times.remove(&msg.channel_id);
                    if room_times.is_empty() {
                        times.remove(&msg.room_id);
                    }
                }
            }
            broadcast_channel_list(
                rooms,
                remote_users,
                &state.channel_creation_times,
                &msg.room_id,
            )
            .await;
        }
        "admin-update" => {
            if let Some(ref data) = msg.data {
                {
                    let mut lock = state.admin_rooms.lock().await;
                    lock.entry(msg.room_id.clone())
                        .or_default()
                        .apply_public_state(data);
                }
                let mut client_data = data.clone();
                if let Some(obj) = client_data.as_object_mut() {
                    obj.remove("keyHash");
                }
                let text = crate::admin::signal_text("admin-update", None, Some(client_data));
                let rooms_lock = rooms.lock().await;
                if let Some(room) = rooms_lock.get(&msg.room_id) {
                    for (_, channel) in room.iter() {
                        for (_, (tx, _)) in channel.iter() {
                            let _ = tx.try_send(Ok(Message::Text(text.clone().into())));
                        }
                    }
                }
            }
        }
        "user-resumed" => {
            // If this node holds a phantom seat for the user, drop it
            // silently: they live on the resuming node now, and a
            // user-left from here would tear down their healthy call.
            crate::rooms::finalize_held_seat(state, &msg.room_id, &msg.user_id, None, false)
                .await;
        }
        "knock-update" => {
            if let Some(ref data) = msg.data {
                let admin_ids = {
                    let lock = state.admin_rooms.lock().await;
                    lock.get(&msg.room_id)
                        .map(|a| a.admins.clone())
                        .unwrap_or_default()
                };
                if !admin_ids.is_empty() {
                    let text =
                        crate::admin::signal_text("knock-update", None, Some(data.clone()));
                    let rooms_lock = rooms.lock().await;
                    if let Some(room) = rooms_lock.get(&msg.room_id) {
                        for (_, channel) in room.iter() {
                            for (uid, (tx, _)) in channel.iter() {
                                if admin_ids.contains(uid) {
                                    let _ =
                                        tx.try_send(Ok(Message::Text(text.clone().into())));
                                }
                            }
                        }
                    }
                }
            }
        }
        "mod-action" => {
            if let Some(ref text) = msg.signal_msg {
                let rooms_lock = rooms.lock().await;
                if let Some(room) = rooms_lock.get(&msg.room_id) {
                    for (_, channel) in room.iter() {
                        if let Some((target_tx, _)) = channel.get(&msg.user_id) {
                            let _ =
                                target_tx.try_send(Ok(Message::Text(text.clone().into())));
                            break;
                        }
                    }
                }
            }
        }
        "knock-response" => {
            let approve = msg
                .data
                .as_ref()
                .and_then(|d| d.get("approve"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let block = msg
                .data
                .as_ref()
                .and_then(|d| d.get("block"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let pending_tx = {
                let mut lock = state.admin_rooms.lock().await;
                lock.get_mut(&msg.room_id).and_then(|a| {
                    let removed = a.pending.remove(&msg.user_id);
                    if approve && removed.is_some() {
                        a.approved.insert(msg.user_id.clone());
                    }
                    if block && removed.is_some() {
                        a.banned.insert(msg.user_id.clone());
                        a.approved.remove(&msg.user_id);
                    }
                    removed.map(|p| p.tx)
                })
            };
            if let Some(ptx) = pending_tx {
                if approve {
                    let text = crate::admin::signal_text("knock-approved", None, None);
                    let _ = ptx.try_send(Ok(Message::Text(text.into())));
                } else {
                    let text = crate::admin::signal_text("knock-denied", None, None);
                    let _ = ptx.try_send(Ok(Message::Text(text.into())));
                    let _ = ptx.try_send(Ok(Message::Close(None)));
                }
                crate::admin::send_knock_update(state, &msg.room_id).await;
                if block {
                    crate::admin::delete_resume_token(state, &msg.room_id, &msg.user_id)
                        .await;
                    state
                        .away_seats
                        .lock()
                        .await
                        .remove(&(msg.room_id.clone(), msg.user_id.clone()));
                    crate::admin::broadcast_admin_update(state, &msg.room_id).await;
                }
            }
        }
        "signal" => {
            if let Some(ref signal_json) = msg.signal_msg
                && let Ok(signal) = serde_json::from_str::<SignalMessage>(signal_json)
            {
                let target_uid = signal.target.as_ref().cloned().unwrap_or_default();
                if !target_uid.is_empty() {
                    let rooms_lock = rooms.lock().await;
                    if let Some(room) = rooms_lock.get(&msg.room_id)
                        && let Some(channel) = room.get(&msg.channel_id)
                        && let Some((target_tx, _)) = channel.get(&target_uid)
                    {
                        let forwarded = serde_json::to_string(&signal).unwrap();
                        let _ = target_tx.try_send(Ok(Message::Text(forwarded.into())));
                    }
                }
            }
        }
        _ => {}
    }
}

pub(crate) async fn distributed_broadcast(state: &AppState, msg: &DistributedMessage) {
    let mut message = msg.clone();
    if message.msg_id.is_empty() {
        message.msg_id = Uuid::new_v4().to_string();
    }
    if let Ok(json) = serde_json::to_string(&message) {
        let _ = state.distributed_tx.send(json);
    }
}

pub(crate) async fn broadcast_channel_upsert(state: &AppState, room_id: &str, channel_id: &str) {
    let created_at = state
        .channel_creation_times
        .lock()
        .await
        .get(room_id)
        .and_then(|channels| channels.get(channel_id))
        .copied()
        .unwrap_or_else(current_unix_secs);
    distributed_broadcast(
        state,
        &DistributedMessage {
            msg_type: "channel-upsert".into(),
            room_id: room_id.to_string(),
            channel_id: channel_id.to_string(),
            user_id: state.node_id.clone(),
            msg_id: Uuid::new_v4().to_string(),
            status: None,
            data: Some(serde_json::json!({ "createdAt": created_at })),
            signal_msg: None,
        },
    )
    .await;
}

fn normalize_profile_images(status: &mut UserStatus) {
    if status.avatar.is_none() {
        status.is_gif = false;
        status.static_frame = None;
    } else if !status.is_gif {
        status.static_frame = None;
    }
}

// Sidebar presence list. Avatars are excluded: they're delivered once via
// existing-users / user-joined / identify, and re-serializing multi-MB
// data URLs here on every event would amplify into a DoS.
fn presence_status(status: &UserStatus) -> UserStatus {
    UserStatus {
        nickname: status.nickname.clone(),
        avatar: None,
        is_gif: false,
        static_frame: None,
        is_muted: status.is_muted,
        is_deafened: status.is_deafened,
        is_screen_sharing: status.is_screen_sharing,
        is_low_bandwidth_mode: status.is_low_bandwidth_mode,
        is_mobile: status.is_mobile,
        profile_rev: status.profile_rev,
    }
}

pub(crate) async fn broadcast_channel_list(
    rooms: &RoomMap,
    remote_users: &RemoteUsersMap,
    times: &ChannelCreationTimesMap,
    room_id: &str,
) {
    let rooms_lock = rooms.lock().await;
    let remote_lock = remote_users.lock().await;
    let times_lock = times.lock().await;

    let local_room = rooms_lock.get(room_id);
    let remote_room = remote_lock.get(room_id);

    let metadata_room = times_lock.get(room_id);
    if local_room.is_none() && remote_room.is_none() && metadata_room.is_none() {
        return;
    }

    let mut channel_list: HashMap<String, RoomStatus> = HashMap::new();
    if let Some(channels) = metadata_room {
        for (cid, created_at) in channels {
            channel_list.insert(
                cid.clone(),
                RoomStatus {
                    name: cid.clone(),
                    users: HashMap::new(),
                    created_at: *created_at,
                    kind: String::new(),
                    speakers: Vec::new(),
                },
            );
        }
    }

    if let Some(room) = local_room {
        for (cid, users) in room.iter() {
            let mut user_map = HashMap::new();
            for (user_id, (_, status)) in users.iter() {
                user_map.insert(user_id.clone(), presence_status(status));
            }
            let created_at = times_lock
                .get(room_id)
                .and_then(|t| t.get(cid))
                .copied()
                .unwrap_or(0);
            channel_list.insert(
                cid.clone(),
                RoomStatus {
                    name: cid.clone(),
                    users: user_map,
                    created_at,
                    kind: String::new(),
                    speakers: Vec::new(),
                },
            );
        }
    }

    if let Some(remote_room) = remote_room {
        for (cid, users) in remote_room.iter() {
            let created_at = times_lock
                .get(room_id)
                .and_then(|t| t.get(cid))
                .copied()
                .unwrap_or(0);
            let entry = channel_list
                .entry(cid.clone())
                .or_insert_with(|| RoomStatus {
                    name: cid.clone(),
                    users: HashMap::new(),
                    created_at,
                    kind: String::new(),
                    speakers: Vec::new(),
                });
            for (user_id, status) in users.iter() {
                entry.users.insert(user_id.clone(), presence_status(status));
            }
        }
    }

    let msg = serde_json::to_string(&SignalMessage {
        msg_type: "room-list".into(),
        target: None,
        user_id: None,
        data: Some(serde_json::to_value(channel_list).unwrap()),
    })
    .unwrap();

    if let Some(room) = local_room {
        for users in room.values() {
            for (tx, _) in users.values() {
                let _ = tx.try_send(Ok(Message::Text(msg.clone().into())));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn test_distributed_message(status: UserStatus) -> DistributedMessage {
        DistributedMessage {
            msg_type: "user-joined".to_string(),
            room_id: "room".to_string(),
            channel_id: "General".to_string(),
            user_id: Uuid::new_v4().to_string(),
            msg_id: Uuid::new_v4().to_string(),
            status: Some(status),
            data: None,
            signal_msg: None,
        }
    }

    fn test_user_status() -> UserStatus {
        UserStatus {
            nickname: "Guest".to_string(),
            avatar: None,
            is_gif: false,
            static_frame: None,
            is_muted: false,
            is_deafened: false,
            is_screen_sharing: false,
            is_low_bandwidth_mode: false,
            is_mobile: false,
            profile_rev: 0,
        }
    }

    #[test]
    fn distributed_messages_reject_oversized_profile_images() {
        let mut status = test_user_status();
        assert!(is_valid_distributed_message(&test_distributed_message(
            status.clone()
        )));

        status.avatar = Some("x".repeat(MAX_AVATAR_DATA_LEN + 1));
        assert!(!is_valid_distributed_message(&test_distributed_message(
            status
        )));

        let mut status = test_user_status();
        status.static_frame = Some("x".repeat(MAX_STATIC_FRAME_DATA_LEN + 1));
        assert!(!is_valid_distributed_message(&test_distributed_message(
            status
        )));
    }

    #[test]
    fn channel_upserts_require_a_real_creation_timestamp() {
        let mut message = DistributedMessage {
            msg_type: "channel-upsert".to_string(),
            room_id: "room".to_string(),
            channel_id: "General".to_string(),
            user_id: Uuid::new_v4().to_string(),
            msg_id: Uuid::new_v4().to_string(),
            status: None,
            data: Some(serde_json::json!({ "createdAt": 0 })),
            signal_msg: None,
        };
        assert!(!is_valid_distributed_message(&message));
        message.data = Some(serde_json::json!({ "createdAt": 1 }));
        assert!(is_valid_distributed_message(&message));
    }

    #[test]
    fn removing_the_last_remote_user_prunes_empty_parents() {
        let mut remote_users = HashMap::new();
        remote_users
            .entry("room".to_string())
            .or_insert_with(HashMap::new)
            .entry("General".to_string())
            .or_insert_with(HashMap::new)
            .insert(Uuid::nil().to_string(), test_user_status());

        assert!(remove_remote_user(
            &mut remote_users,
            "room",
            "General",
            &Uuid::nil().to_string()
        ));
        assert!(remote_users.is_empty());
    }

    #[test]
    fn presence_status_strips_avatar_payloads_but_keeps_status_flags() {
        let mut status = test_user_status();
        status.nickname = "Alice".to_string();
        status.avatar = Some("x".repeat(MAX_AVATAR_DATA_LEN));
        status.static_frame = Some("y".repeat(MAX_STATIC_FRAME_DATA_LEN));
        status.is_gif = true;
        status.is_muted = true;
        status.is_screen_sharing = true;
        status.is_mobile = true;

        let presence = presence_status(&status);
        assert_eq!(presence.nickname, "Alice");
        assert!(presence.avatar.is_none());
        assert!(presence.static_frame.is_none());
        assert!(!presence.is_gif);
        assert!(presence.is_muted);
        assert!(presence.is_deafened == status.is_deafened);
        assert!(presence.is_screen_sharing);
        assert!(presence.is_low_bandwidth_mode == status.is_low_bandwidth_mode);
        assert!(presence.is_mobile);
    }

    #[test]
    fn update_user_changes_are_detected_for_skip_on_noop() {
        let mut status = test_user_status();
        status.is_muted = true;
        let unchanged = status.clone();
        assert_eq!(status, unchanged);

        status.nickname = "Bob".to_string();
        assert_ne!(status, unchanged);
    }

    fn test_state() -> AppState {
        AppState {
            rooms: Arc::new(Mutex::new(HashMap::new())),
            room_cleanup_generations: Arc::new(Mutex::new(HashMap::new())),
            room_creation_password: None,
            distributed_tx: tokio::sync::broadcast::channel(DISTRIBUTED_BROADCAST_CAPACITY).0,
            remote_users: Arc::new(Mutex::new(HashMap::new())),
            remote_user_owners: Arc::new(Mutex::new(HashMap::new())),
            channel_creation_times: Arc::new(Mutex::new(HashMap::new())),
            allowed_url: None,
            recent_distributed_msg_ids: Arc::new(Mutex::new(HashSet::new())),
            distributed_msg_history: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            node_id: Uuid::new_v4().to_string(),
            admin_rooms: Arc::new(Mutex::new(HashMap::new())),
admin_redis: None,
redis_prefix: "rustrooms".to_string(),
resume_records: Arc::new(Mutex::new(HashMap::new())),
away_seats: Arc::new(Mutex::new(HashMap::new())),
departure_gen: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            server_secret: Arc::new(vec![7u8; 32]),
        }
    }

    fn test_distributed_message_typed(
        msg_type: &str,
        room: &str,
        channel: &str,
        user: &str,
    ) -> DistributedMessage {
        DistributedMessage {
            msg_type: msg_type.to_string(),
            room_id: room.to_string(),
            channel_id: channel.to_string(),
            user_id: user.to_string(),
            msg_id: Uuid::new_v4().to_string(),
            status: None,
            data: None,
            signal_msg: None,
        }
    }

    fn test_spy_channel() -> (
        UserTx,
        tokio::sync::mpsc::Receiver<Result<Message, axum::Error>>,
    ) {
        tokio::sync::mpsc::channel(OUTBOUND_QUEUE_CAPACITY)
    }

    #[tokio::test]
    async fn redis_messages_record_the_hosting_node() {
        let state = test_state();
        let owner = Uuid::new_v4().to_string();
        let uid = Uuid::new_v4().to_string();
        let mut joined = test_distributed_message_typed("user-joined", "room", "General", &uid);
        joined.status = Some(test_user_status());

        process_redis_message(joined, &owner, &state).await;

        let key = ("room".to_string(), "General".to_string(), uid.clone());
        assert_eq!(
            state.remote_user_owners.lock().await.get(&key),
            Some(&owner)
        );
        assert!(
            state
                .remote_users
                .lock()
                .await
                .get("room")
                .unwrap()
                .get("General")
                .unwrap()
                .contains_key(&uid)
        );
    }

    #[tokio::test]
    async fn stale_leave_from_a_non_owner_does_not_remove_the_live_user() {
        let state = test_state();
        let owner = Uuid::new_v4().to_string();
        let stale_node = Uuid::new_v4().to_string();
        let uid = Uuid::new_v4().to_string();
        let mut joined = test_distributed_message_typed("user-joined", "room", "General", &uid);
        joined.status = Some(test_user_status());
        assert!(process_redis_message(joined, &owner, &state).await);

        let left = test_distributed_message_typed("user-left", "room", "General", &uid);
        assert!(!process_redis_message(left, &stale_node, &state).await);

        let key = ("room".to_string(), "General".to_string(), uid.clone());
        assert_eq!(
            state.remote_user_owners.lock().await.get(&key),
            Some(&owner)
        );
        assert!(
            state
                .remote_users
                .lock()
                .await
                .get("room")
                .unwrap()
                .get("General")
                .unwrap()
                .contains_key(&uid)
        );
    }

    #[tokio::test]
    async fn remote_owner_keys_follow_channel_renames() {
        let state = test_state();
        let owner = Uuid::new_v4().to_string();
        let uid = Uuid::new_v4().to_string();
        let old_key = ("room".to_string(), "old".to_string(), uid.clone());
        state
            .remote_user_owners
            .lock()
            .await
            .insert(old_key.clone(), owner.clone());

        rename_remote_user_owners(&state.remote_user_owners, "room", "old", "new").await;

        let owners = state.remote_user_owners.lock().await;
        assert!(!owners.contains_key(&old_key));
        assert_eq!(
            owners.get(&("room".to_string(), "new".to_string(), uid)),
            Some(&owner)
        );
    }

    #[tokio::test]
    async fn redis_timeout_removes_only_users_owned_by_the_dead_node() {
        let state = test_state();
        let dead_owner = Uuid::new_v4().to_string();
        let live_owner = Uuid::new_v4().to_string();
        let dead_uid = Uuid::new_v4().to_string();
        let live_uid = Uuid::new_v4().to_string();

        for (owner, uid) in [(&dead_owner, &dead_uid), (&live_owner, &live_uid)] {
            let mut joined = test_distributed_message_typed("user-joined", "room", "General", uid);
            joined.status = Some(test_user_status());
            process_redis_message(joined, owner, &state).await;
        }

        let affected = cleanup_redis_node(&state, &dead_owner).await;
        let remote = state.remote_users.lock().await;
        let users = remote.get("room").unwrap().get("General").unwrap();
        assert!(!users.contains_key(&dead_uid));
        assert!(users.contains_key(&live_uid));
        assert!(affected.contains("room"));
    }

    #[tokio::test]
    async fn stale_node_cleanup_prunes_a_user_moved_before_its_owner_key() {
        let state = test_state();
        let owner = Uuid::new_v4().to_string();
        let uid = Uuid::new_v4().to_string();
        state.remote_user_owners.lock().await.insert(
            ("room".to_string(), "old".to_string(), uid.clone()),
            owner.clone(),
        );
        state
            .remote_users
            .lock()
            .await
            .entry("room".to_string())
            .or_default()
            .entry("new".to_string())
            .or_default()
            .insert(uid, test_user_status());

        let affected = cleanup_redis_node(&state, &owner).await;

        assert!(state.remote_users.lock().await.get("room").is_none());
        assert!(affected.contains("room"));
    }

    #[tokio::test]
    async fn authoritative_snapshot_removes_only_users_missing_from_it() {
        let state = test_state();
        let owner = Uuid::new_v4().to_string();
        let retained_uid = Uuid::new_v4().to_string();
        let stale_uid = Uuid::new_v4().to_string();

        for uid in [&retained_uid, &stale_uid] {
            let mut joined = test_distributed_message_typed("user-joined", "room", "General", uid);
            joined.status = Some(test_user_status());
            process_redis_message(joined, &owner, &state).await;
        }

        let retained = HashSet::from([(
            "room".to_string(),
            "General".to_string(),
            retained_uid.clone(),
        )]);
        reconcile_redis_node(&state, &owner, &retained).await;

        let remote = state.remote_users.lock().await;
        let users = remote.get("room").unwrap().get("General").unwrap();
        assert!(users.contains_key(&retained_uid));
        assert!(!users.contains_key(&stale_uid));
    }

    #[tokio::test]
    async fn duplicate_distributed_messages_are_deduplicated() {
        let state = test_state();
        let uid = Uuid::new_v4().to_string();
        let mut joined = test_distributed_message_typed("user-joined", "room", "General", &uid);
        joined.status = Some(test_user_status());
        let shared_id = Uuid::new_v4().to_string();
        joined.msg_id = shared_id.clone();

        handle_distributed_message(&joined, &state.rooms, &state.remote_users, &state).await;
        handle_distributed_message(&joined, &state.rooms, &state.remote_users, &state).await;

        let rl = state.remote_users.lock().await;
        let users = rl.get("room").unwrap().get("General").unwrap();
        assert_eq!(users.len(), 1);
        assert!(users.contains_key(&uid));
    }

    #[tokio::test]
    async fn user_joined_broadcast_notifies_local_members_and_populates_remote_users() {
        let state = test_state();
        let (local_tx, mut local_rx) = test_spy_channel();
        let local_uid = Uuid::new_v4().to_string();
        state
            .rooms
            .lock()
            .await
            .entry("room".to_string())
            .or_default()
            .entry("General".to_string())
            .or_default()
            .insert(local_uid.clone(), (local_tx, test_user_status()));

        let remote_uid = Uuid::new_v4().to_string();
        let mut joined =
            test_distributed_message_typed("user-joined", "room", "General", &remote_uid);
        joined.status = Some(test_user_status());
        handle_distributed_message(&joined, &state.rooms, &state.remote_users, &state).await;

        assert!(
            state
                .remote_users
                .lock()
                .await
                .get("room")
                .unwrap()
                .get("General")
                .unwrap()
                .contains_key(&remote_uid)
        );
        let msg = local_rx.try_recv().unwrap().unwrap();
        match msg {
            Message::Text(t) => assert!(t.contains("user-joined")),
            _ => panic!("expected user-joined text message"),
        }
    }

    #[tokio::test]
    async fn cross_node_kick_removes_local_victim_and_closes_their_socket() {
        let state = test_state();
        let (victim_tx, mut victim_rx) = test_spy_channel();
        let victim_uid = Uuid::new_v4().to_string();
        let (member_tx, mut member_rx) = test_spy_channel();
        let member_uid = Uuid::new_v4().to_string();
        {
            let mut rooms = state.rooms.lock().await;
            let channel = rooms.entry("room".to_string()).or_default();
            channel
                .entry("General".to_string())
                .or_default()
                .insert(victim_uid.clone(), (victim_tx, test_user_status()));
            channel
                .entry("General".to_string())
                .or_default()
                .insert(member_uid.clone(), (member_tx, test_user_status()));
        }

        let kick = test_distributed_message_typed("user-kicked", "room", "General", &victim_uid);
        handle_distributed_message(&kick, &state.rooms, &state.remote_users, &state).await;

        assert!(
            !state
                .rooms
                .lock()
                .await
                .get("room")
                .unwrap()
                .get("General")
                .unwrap()
                .contains_key(&victim_uid)
        );

        let victim_first = victim_rx.try_recv().unwrap().unwrap();
        match victim_first {
            Message::Text(t) => assert!(t.contains("user-kicked")),
            _ => panic!("expected user-kicked text message"),
        }
        let victim_second = victim_rx.try_recv().unwrap().unwrap();
        assert!(matches!(victim_second, Message::Close(_)));

        let member_msg = member_rx.try_recv().unwrap().unwrap();
        match member_msg {
            Message::Text(t) => assert!(t.contains("user-kicked")),
            _ => panic!("expected user-kicked text message"),
        }
    }

    #[tokio::test]
    async fn remote_rename_merges_users_instead_of_dropping_them() {
        let state = test_state();
        let a_uid = Uuid::new_v4().to_string();
        let c_uid = Uuid::new_v4().to_string();
        {
            let mut rl = state.remote_users.lock().await;
            let room = rl.entry("room".to_string()).or_default();
            room.entry("old".to_string())
                .or_default()
                .insert(a_uid.clone(), test_user_status());
            room.entry("new".to_string())
                .or_default()
                .insert(c_uid.clone(), test_user_status());
        }

        let mut rename = test_distributed_message_typed(
            "rename-channel",
            "room",
            "old",
            &Uuid::new_v4().to_string(),
        );
        rename.data = Some(serde_json::json!({ "newName": "new" }));
        handle_distributed_message(&rename, &state.rooms, &state.remote_users, &state).await;

        let rl = state.remote_users.lock().await;
        let room = rl.get("room").unwrap();
        assert!(!room.contains_key("old"));
        let merged = room.get("new").unwrap();
        assert!(merged.contains_key(&a_uid));
        assert!(merged.contains_key(&c_uid));
    }

    #[tokio::test]
    async fn distributed_rename_renames_empty_local_channel_and_notifies_members() {
        let state = test_state();
        let (spy_tx, mut spy_rx) = test_spy_channel();
        let spy_uid = Uuid::new_v4().to_string();
        {
            let mut rooms = state.rooms.lock().await;
            let room = rooms.entry("room".to_string()).or_default();
            room.entry("General".to_string())
                .or_default()
                .insert(spy_uid.clone(), (spy_tx, test_user_status()));
            room.entry("old".to_string()).or_default();
        }

        let mut rename = test_distributed_message_typed(
            "rename-channel",
            "room",
            "old",
            &Uuid::new_v4().to_string(),
        );
        rename.data = Some(serde_json::json!({ "newName": "new" }));
        handle_distributed_message(&rename, &state.rooms, &state.remote_users, &state).await;

        {
            let rooms = state.rooms.lock().await;
            let room = rooms.get("room").unwrap();
            assert!(!room.contains_key("old"));
            assert!(room.contains_key("new"));
        }
        let msg = spy_rx.try_recv().unwrap().unwrap();
        match msg {
            Message::Text(t) => assert!(t.contains("rename-channel")),
            _ => panic!("expected rename-channel text message"),
        }
    }

    #[tokio::test]
    async fn distributed_rename_refuses_occupied_local_channel() {
        let state = test_state();
        let occupant_uid = Uuid::new_v4().to_string();
        {
            let mut rooms = state.rooms.lock().await;
            let room = rooms.entry("room".to_string()).or_default();
            room.entry("old".to_string()).or_default().insert(
                occupant_uid.clone(),
                (test_spy_channel().0, test_user_status()),
            );
        }

        let mut rename = test_distributed_message_typed(
            "rename-channel",
            "room",
            "old",
            &Uuid::new_v4().to_string(),
        );
        rename.data = Some(serde_json::json!({ "newName": "new" }));
        handle_distributed_message(&rename, &state.rooms, &state.remote_users, &state).await;

        let rooms = state.rooms.lock().await;
        let room = rooms.get("room").unwrap();
        assert!(room.contains_key("old"));
        assert!(!room.contains_key("new"));
    }

    #[tokio::test]
    async fn distributed_delete_removes_empty_local_channel() {
        let state = test_state();
        let (spy_tx, mut spy_rx) = test_spy_channel();
        let spy_uid = Uuid::new_v4().to_string();
        {
            let mut rooms = state.rooms.lock().await;
            let room = rooms.entry("room".to_string()).or_default();
            room.entry("General".to_string())
                .or_default()
                .insert(spy_uid.clone(), (spy_tx, test_user_status()));
            room.entry("old".to_string()).or_default();
        }

        let del = test_distributed_message_typed(
            "delete-channel",
            "room",
            "old",
            &Uuid::new_v4().to_string(),
        );
        handle_distributed_message(&del, &state.rooms, &state.remote_users, &state).await;

        {
            let rooms = state.rooms.lock().await;
            let room = rooms.get("room").unwrap();
            assert!(!room.contains_key("old"));
            assert!(room.contains_key("General"));
        }
        let msg = spy_rx.try_recv().unwrap().unwrap();
        assert!(matches!(msg, Message::Text(_)));
    }

    #[tokio::test]
    async fn distributed_delete_refuses_occupied_local_channel() {
        let state = test_state();
        let occupant_uid = Uuid::new_v4().to_string();
        {
            let mut rooms = state.rooms.lock().await;
            let room = rooms.entry("room".to_string()).or_default();
            room.entry("old".to_string()).or_default().insert(
                occupant_uid.clone(),
                (test_spy_channel().0, test_user_status()),
            );
        }

        let del = test_distributed_message_typed(
            "delete-channel",
            "room",
            "old",
            &Uuid::new_v4().to_string(),
        );
        handle_distributed_message(&del, &state.rooms, &state.remote_users, &state).await;

        let rooms = state.rooms.lock().await;
        assert!(rooms.get("room").unwrap().contains_key("old"));
    }
}

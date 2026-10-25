use crate::{
    distributed::{
        broadcast_channel_list, broadcast_channel_upsert, distributed_broadcast,
        remove_remote_channel_owners, remove_remote_user, remove_remote_user_owner,
        rename_remote_user_owners, schedule_empty_room_cleanup,
    },
    routes::host_is_allowed,
    state::*,
};
use axum::{
    extract::{
        Path, State,
        ws::{CloseFrame, Message, WebSocket},
    },
    response::IntoResponse,
};
use futures::{sink::SinkExt, stream::StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

// The room-creation password gates the *creation* of a room, never joining
// an existing one: if the room already has members on this node or any
// distributed instance, no password is required.
fn room_creation_needs_password(
    required: Option<&str>,
    exists_locally: bool,
    exists_remotely: bool,
    provided: Option<&str>,
) -> bool {
    let Some(required) = required else {
        return false;
    };
    if exists_locally || exists_remotely {
        return false;
    }
    provided != Some(required)
}


// Marks a dropped participant's seat as held and returns the
// departure generation, or None if this connection is not the live
// seat (superseded, kicked, or never joined).
async fn hold_seat(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    tx: &UserTx,
) -> Option<u64> {
    let channel_id = {
        let rooms_lock = state.rooms.lock().await;
        let room = rooms_lock.get(room_id)?;
        let mut found = None;
        for (cid, channel) in room.iter() {
            if let Some((stored_tx, _)) = channel.get(user_id)
                && stored_tx.same_channel(tx)
            {
                found = Some(cid.clone());
                break;
            }
        }
        found?
    };
    let generation = state
        .departure_gen
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    state.away_seats.lock().await.insert(
        (room_id.to_string(), user_id.to_string()),
        AwayMark {
            channel_id,
            generation,
            tx: tx.clone(),
        },
    );
    Some(generation)
}

// Removes a held seat for good. With announce=true this is the
// delayed real departure (user-left locally and cluster-wide, plus
// empty-room cleanup). With announce=false another node has taken
// the user over (user-resumed), so the seat vanishes silently.
// No-ops when the mark is gone or the generation moved on (the
// user resumed in the meantime).
pub(crate) async fn finalize_held_seat(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    generation: Option<u64>,
    announce: bool,
) {
    let key = (room_id.to_string(), user_id.to_string());
    let mark = {
        let mut lock = state.away_seats.lock().await;
        match lock.get(&key) {
            Some(m) if generation.map(|g| g == m.generation).unwrap_or(true) => {
                lock.remove(&key)
            }
            _ => None,
        }
    };
    let Some(mark) = mark else { return };

    let mut removed_from: Option<String> = None;
    let mut room_empty = false;
    {
        let mut rooms_lock = state.rooms.lock().await;
        if let Some(room) = rooms_lock.get_mut(room_id) {
            let mut order: Vec<String> = vec![mark.channel_id.clone()];
            for cid in room.keys() {
                if *cid != mark.channel_id {
                    order.push(cid.clone());
                }
            }
            for cid in order {
                if let Some(channel) = room.get_mut(&cid)
                    && let Some((stored_tx, _)) = channel.get(user_id)
                    && stored_tx.same_channel(&mark.tx)
                {
                    channel.remove(user_id);
                    removed_from = Some(cid);
                    break;
                }
            }
            if let Some(cid) = &removed_from {
                room_empty = room.values().all(|c| c.is_empty());
                if announce && let Some(channel) = room.get(cid) && !channel.is_empty() {
                    let notify_msg = serde_json::to_string(&SignalMessage {
                        msg_type: "user-left".into(),
                        user_id: Some(user_id.to_string()),
                        target: None,
                        data: None,
                    })
                    .unwrap();
                    for (_, (tx, _)) in channel.iter() {
                        let _ = tx.try_send(Ok(Message::Text(notify_msg.clone().into())));
                    }
                }
            }
        }
    }
    let Some(cid) = removed_from else { return };
    if announce {
        distributed_broadcast(
            state,
            &DistributedMessage {
                msg_type: "user-left".into(),
                room_id: room_id.to_string(),
                channel_id: cid,
                user_id: user_id.to_string(),
                msg_id: Uuid::new_v4().to_string(),
                status: None,
                data: None,
                signal_msg: None,
            },
        )
        .await;
    }
    if room_empty {
        on_room_maybe_empty(state, room_id).await;
    }
}

// Schedules the delayed deletion of a room that just became empty
// (shared by the disconnect path and held-seat finalization).
async fn on_room_maybe_empty(state: &AppState, room_id: &str) {
        let next_generation = {
            let mut cleanup_lock = state.room_cleanup_generations.lock().await;
            let next = cleanup_lock.get(room_id).copied().unwrap_or(0) + 1;
            cleanup_lock.insert(room_id.to_string(), next);
            next
        };
        println!(
            "CLEANUP: Room '{}' became empty; scheduling deletion in {}s (generation {})",
            room_id, ROOM_EMPTY_GRACE_SECS, next_generation
        );

        let rooms_clone = state.rooms.clone();
        let cleanup_clone = state.room_cleanup_generations.clone();
        let remote_users_clone = state.remote_users.clone();
        let times_clone = state.channel_creation_times.clone();
        let admin_clone = state.admin_rooms.clone();
        let state_clone = state.clone();
        let room_id_clone = room_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(ROOM_EMPTY_GRACE_SECS)).await;

            let generation_still_current = cleanup_clone
                .lock()
                .await
                .get(&room_id_clone)
                .copied()
                .map(|g| g == next_generation)
                .unwrap_or(false);
            if !generation_still_current {
                return;
            }

            let removed_room = {
                let has_remote = remote_users_clone
                    .lock()
                    .await
                    .get(&room_id_clone)
                    .map(|r| r.values().any(|c| !c.is_empty()))
                    .unwrap_or(false);
                if has_remote {
                    false
                } else {
                    let mut rooms_lock = rooms_clone.lock().await;
                    let should_remove_room = rooms_lock
                        .get(&room_id_clone)
                        .map(|room| room.values().all(|c| c.is_empty()))
                        .unwrap_or(false);
                    if should_remove_room {
                        rooms_lock.remove(&room_id_clone);
                        true
                    } else {
                        false
                    }
                }
            };

            if removed_room {
                times_clone.lock().await.remove(&room_id_clone);
                admin_clone.lock().await.remove(&room_id_clone);
                crate::admin::delete_admin_redis(&state_clone, &room_id_clone).await;
                crate::admin::persist_state(&state_clone).await;
                let mut cleanup_lock = cleanup_clone.lock().await;
                if cleanup_lock.get(&room_id_clone).copied() == Some(next_generation) {
                    cleanup_lock.remove(&room_id_clone);
                }
                println!(
                    "CLEANUP: Removed empty room '{}' after {}s empty",
                    room_id_clone, ROOM_EMPTY_GRACE_SECS
                );
            } else {
                // Room still has remote users or became non-empty; reschedule cleanup.
                let mut cleanup_lock = cleanup_clone.lock().await;
                if cleanup_lock.get(&room_id_clone).copied() == Some(next_generation) {
                    let next_gen = next_generation + 1;
                    cleanup_lock.insert(room_id_clone.clone(), next_gen);
                    let rooms_retry = rooms_clone.clone();
                    let cleanup_retry = cleanup_clone.clone();
                    let remote_retry = remote_users_clone.clone();
                    let times_retry = times_clone.clone();
                    let admin_retry = admin_clone.clone();
                    let state_retry = state_clone.clone();
                    let rid_retry = room_id_clone.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_secs(ROOM_EMPTY_GRACE_SECS))
                            .await;
                        let gen_current =
                            cleanup_retry.lock().await.get(&rid_retry).copied() == Some(next_gen);
                        if !gen_current {
                            return;
                        }
                        let has_remote = remote_retry
                            .lock()
                            .await
                            .get(&rid_retry)
                            .map(|r| r.values().any(|c| !c.is_empty()))
                            .unwrap_or(false);
                        if has_remote {
                            // Still has remote users, clear generation so future activity can re-trigger.
                            let mut cl = cleanup_retry.lock().await;
                            if cl.get(&rid_retry).copied() == Some(next_gen) {
                                cl.remove(&rid_retry);
                            }
                            return;
                        }
                        let removed = {
                            let mut rl = rooms_retry.lock().await;
                            let should = rl
                                .get(&rid_retry)
                                .map(|rm| rm.values().all(|c| c.is_empty()))
                                .unwrap_or(false);
                            if should {
                                rl.remove(&rid_retry);
                                true
                            } else {
                                false
                            }
                        };
                        if removed {
                            times_retry.lock().await.remove(&rid_retry);
                            admin_retry.lock().await.remove(&rid_retry);
                            crate::admin::delete_admin_redis(&state_retry, &rid_retry).await;
                            crate::admin::persist_state(&state_retry).await;
                            let mut cl = cleanup_retry.lock().await;
                            if cl.get(&rid_retry).copied() == Some(next_gen) {
                                cl.remove(&rid_retry);
                            }
                            println!(
                                "CLEANUP: Removed empty room '{}' after rescheduled check",
                                rid_retry
                            );
                        }
                    });
                }
            }
        });
}

pub(crate) async fn handle_socket(
    socket: WebSocket,
    room_id: String,
    channel_id: String,
    state: AppState,
) {
    let rooms = state.rooms.clone();
    let remote_users = state.remote_users.clone();
    let room_cleanup_generations = state.room_cleanup_generations.clone();
    let (mut user_ws_tx, mut user_ws_rx) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel(OUTBOUND_QUEUE_CAPACITY);

    let mut user_id = String::new();
    let mut is_joined = false;
    let mut is_pending_knock = false;
    let mut intentional_leave = false;
    let mut message_window_started = std::time::Instant::now();
    let mut messages_in_window = 0u32;
    let mut bytes_in_window = 0usize;
    let mut last_profile_image_update: Option<std::time::Instant> = None;

    // Server-side ping to detect dead iOS Safari connections
    let tx_ping = tx.clone();
    let (ping_shutdown_tx, mut ping_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (inactivity_tx, mut inactivity_rx) = tokio::sync::oneshot::channel::<()>();
    let join_deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(JOIN_TIMEOUT_SECS);
    let last_activity = Arc::new(tokio::sync::Mutex::new(std::time::Instant::now()));
    let last_activity_writer = last_activity.clone();

    tokio::spawn(async move {
        while let Some(result) = rx.recv().await {
            if let Ok(msg) = result
                && user_ws_tx.send(msg).await.is_err()
            {
                break;
            }
        }
    });

    // Server-side ping task: sends a ping every 5s, closes connection after 10s of silence
    let tx_for_ping = tx_ping.clone();
    let last_activity_for_ping = last_activity.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.tick().await; // skip first immediate tick
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let elapsed = last_activity_for_ping.lock().await.elapsed();
                    if elapsed > std::time::Duration::from_secs(10) {
                        // No activity for 10s, client is likely dead (iOS Safari silent drop)
                        let _ = tx_for_ping.try_send(Ok(Message::Close(Some(CloseFrame {
                            code: 4001,
                            reason: "Inactivity timeout".into(),
                        }))));
                        // Do not rely on a dead client replying to the close
                        // frame: wake the reader so presence is removed now.
                        let _ = inactivity_tx.send(());
                        break;
                    }
                    // Send server-side keepalive
                    let ping_msg = serde_json::to_string(&SignalMessage {
                        msg_type: "keepalive".into(),
                        user_id: None,
                        target: None,
                        data: None,
                    }).unwrap();
                    match tx_for_ping.try_send(Ok(Message::Text(ping_msg.into()))) {
                        Ok(()) => {}
                        // A temporary full queue must not permanently disable
                        // inactivity detection; just skip this keepalive.
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => continue,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                _ = &mut ping_shutdown_rx => {
                    break;
                }
            }
        }
    });

    loop {
        let next_message = if is_joined || is_pending_knock {
            tokio::select! {
                message = user_ws_rx.next() => message,
                _ = &mut inactivity_rx => None,
            }
        } else {
            tokio::select! {
                message = user_ws_rx.next() => message,
                _ = &mut inactivity_rx => None,
                _ = tokio::time::sleep_until(join_deadline) => {
                    let _ = tx.try_send(Ok(Message::Close(Some(CloseFrame {
                        code: 4000,
                        reason: "Join timeout".into(),
                    }))));
                    None
                }
            }
        };
        let Some(result) = next_message else {
            break;
        };

        // Update last activity timestamp on any received message
        *last_activity_writer.lock().await = std::time::Instant::now();
        if let Ok(msg) = result {
            if let Message::Text(text) = msg {
                let now = std::time::Instant::now();
                if now.duration_since(message_window_started)
                    >= std::time::Duration::from_secs(MESSAGE_RATE_WINDOW_SECS)
                {
                    message_window_started = now;
                    messages_in_window = 0;
                    bytes_in_window = 0;
                }
                messages_in_window += 1;
                bytes_in_window += text.len();
                if messages_in_window > MAX_MESSAGES_PER_RATE_WINDOW
                    || bytes_in_window > MAX_BYTES_PER_RATE_WINDOW
                {
                    let _ = tx.try_send(Ok(Message::Close(Some(CloseFrame {
                        code: 4008,
                        reason: "Message rate limit exceeded".into(),
                    }))));
                    break;
                }

                if let Ok(parsed) = serde_json::from_str::<SignalMessage>(&text) {
                    if is_joined {
                        let is_current_connection = {
                            let rooms_lock = rooms.lock().await;
                            rooms_lock
                                .get(&room_id)
                                .and_then(|room| room.get(&channel_id))
                                .and_then(|channel| channel.get(&user_id))
                                .is_some_and(|(stored_tx, _)| stored_tx.same_channel(&tx))
                        };
                        if !is_current_connection {
                            let _ = tx.try_send(Ok(Message::Close(Some(CloseFrame {
                                code: 4002,
                                reason: "User identity is active on another connection".into(),
                            }))));
                            break;
                        }
                    }

                    if is_pending_knock && parsed.msg_type == "knock-cancel" {
                        break;
                    }

                    if parsed.msg_type == "leave" && is_joined {
                        intentional_leave = true;
                        break;
                    }

                    if parsed.msg_type == "ping" {
                        let pong_msg = serde_json::to_string(&SignalMessage {
                            msg_type: "pong".into(),
                            user_id: None,
                            target: None,
                            data: None,
                        })
                        .unwrap();
                        let _ = tx.try_send(Ok(Message::Text(pong_msg.into())));
                        continue;
                    }

                    if !is_joined {
                        if parsed.msg_type == "join" {
                            user_id = normalize_user_id(
                                parsed
                                    .data
                                    .as_ref()
                                    .and_then(|data| data.get("userId"))
                                    .and_then(|value| value.as_str()),
                            );

                            let nickname = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("nickname"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("Guest")
                                .chars()
                                .take(MAX_NICKNAME_LEN)
                                .collect::<String>();

                            let avatar = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("avatar"))
                                .and_then(|v| v.as_str())
                                .filter(|value| value.len() <= MAX_AVATAR_DATA_LEN)
                                .map(|s| s.to_string());

                            let is_muted = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("isMuted"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let is_deafened = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("isDeafened"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let is_screen_sharing = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("screenEnabled"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let is_low_bandwidth_mode = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("isLowBandwidthMode"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let is_mobile = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("isMobile"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let cam_enabled = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("camEnabled"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let screen_has_audio = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("screenAudio"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let mic_track_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("micTrackId"))
                                .and_then(|v| v.as_str())
                                .filter(|value| value.len() <= 256);
                            let screen_audio_track_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("screenAudioTrackId"))
                                .and_then(|v| v.as_str())
                                .filter(|value| value.len() <= 256);
                            let cam_video_track_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("camVideoTrackId"))
                                .and_then(|v| v.as_str())
                                .filter(|value| value.len() <= 256);
                            let screen_video_track_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("screenVideoTrackId"))
                                .and_then(|v| v.as_str())
                                .filter(|value| value.len() <= 256);

                            let mut is_gif = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("isGif"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let mut static_frame = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("staticFrame"))
                                .and_then(|v| v.as_str())
                                .filter(|s| s.len() <= MAX_STATIC_FRAME_DATA_LEN)
                                .map(|s| s.to_string());

                            let profile_rev = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("profileRev"))
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);

                            let admin_key = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("adminKey"))
                                .and_then(|v| v.as_str())
                                .filter(|k| k.len() <= 256)
                                .map(|k| k.to_string());
                            let resume_token = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("resumeToken"))
                                .and_then(|v| v.as_str())
                                .filter(|k| k.len() <= 256)
                                .map(|k| k.to_string());
                            let want_lobby = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("lobby"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let want_kind = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("roomType"))
                                .and_then(|v| v.as_str())
                                .map(RoomKind::from_str)
                                .unwrap_or_default();

                            if avatar.is_none() {
                                is_gif = false;
                                static_frame = None;
                            } else if !is_gif {
                                static_frame = None;
                            }

                            // Canonical status stored for this identity (either
                            // the client's fresh profile or the server's kept
                            // copy on a stale reconnect); announced to other
                            // instances and echoed back via existing-users.
                            let mut joined_status = UserStatus {
                                nickname: String::new(),
                                avatar: None,
                                is_gif: false,
                                static_frame: None,
                                is_muted: false,
                                is_deafened: false,
                                is_screen_sharing: false,
                                is_low_bandwidth_mode: false,
                                is_mobile: false,
                                profile_rev: 0,
                            };
                            let mut issued_admin_key: Option<String> = None;
                            let mut park_for_knock = false;
                            let mut resume_ok = false;
                            let mut warm_resume = false;
                            let mut admin_state_changed = false;
                            {
                                let room_needs_password = {
                                    let exists_locally = rooms.lock().await.contains_key(&room_id);
                                    let exists_remotely =
                                        remote_users.lock().await.contains_key(&room_id)
                                            || state
                                                .channel_creation_times
                                                .lock()
                                                .await
                                                .contains_key(&room_id);
                                    let provided_password = parsed
                                        .data
                                        .as_ref()
                                        .and_then(|d| d.get("password"))
                                        .and_then(|v| v.as_str());
                                    room_creation_needs_password(
                                        state.room_creation_password.as_deref(),
                                        exists_locally,
                                        exists_remotely,
                                        provided_password,
                                    )
                                };

                                if room_needs_password {
                                    let error_msg = serde_json::to_string(&SignalMessage {
                                        msg_type: "error".into(),
                                        user_id: None,
                                        target: None,
                                        data: Some(serde_json::json!({
                                            "code": "PASSWORD_REQUIRED",
                                            "message": "Room creation requires a password."
                                        })),
                                    })
                                    .unwrap();
                                    let _ = tx.send(Ok(Message::Text(error_msg.into()))).await;
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                    return;
                                }

                                let occupied_remote_ids: HashSet<String> = remote_users
                                    .lock()
                                    .await
                                    .get(&room_id)
                                    .and_then(|room| room.get(&channel_id))
                                    .map(|channel| channel.keys().cloned().collect())
                                    .unwrap_or_default();

                                let remote_member = {
                                    let rl = remote_users.lock().await;
                                    rl.get(&room_id)
                                        .map(|room| {
                                            room.values().any(|c| c.contains_key(&user_id))
                                        })
                                        .unwrap_or(false)
                                };
                                let room_preexisting = remote_member
                                    || rooms
                                        .lock()
                                        .await
                                        .get(&room_id)
                                        .map(|r| r.values().any(|c| !c.is_empty()))
                                        .unwrap_or(false);
                                let channel_preexists = {
                                    let local = rooms
                                        .lock()
                                        .await
                                        .get(&room_id)
                                        .map(|r| r.contains_key(&channel_id))
                                        .unwrap_or(false);
                                    let timed = state
                                        .channel_creation_times
                                        .lock()
                                        .await
                                        .get(&room_id)
                                        .map(|t| t.contains_key(&channel_id))
                                        .unwrap_or(false);
                                    let remote = remote_users
                                        .lock()
                                        .await
                                        .get(&room_id)
                                        .map(|r| r.contains_key(&channel_id))
                                        .unwrap_or(false);
                                    local || timed || remote
                                };

                                // In a stage room only admins may create
                                // channels. Checked before the channel entry
                                // is materialized by the insert below.
                                if !channel_preexists && channel_id != "General" {
                                    crate::admin::load_admin_redis(&state, &room_id).await;
                                    let blocked = {
                                        let lock = state.admin_rooms.lock().await;
                                        match lock.get(&room_id) {
                                            Some(a) if a.kind == RoomKind::Stage => {
                                                let key_ok = admin_key
                                                    .as_deref()
                                                    .zip(a.key_hash.as_deref())
                                                    .map(|(k, h)| {
                                                        crate::admin::verify_key(
                                                            &state.server_secret,
                                                            &room_id,
                                                            k,
                                                            h,
                                                        )
                                                    })
                                                    .unwrap_or(false);
                                                !(key_ok || a.is_admin(&user_id))
                                            }
                                            _ => false,
                                        }
                                    };
                                    if blocked {
                                        let error_msg = crate::admin::error_text(
                                            "STAGE_CHANNEL_ADMIN_ONLY",
                                            "Only admins can create channels in a stage room.",
                                        );
                                        let _ = tx.send(Ok(Message::Text(error_msg.into()))).await;
                                        tokio::time::sleep(std::time::Duration::from_millis(500))
                                            .await;
                                        return;
                                    }
                                }

                                let mut rooms_lock = rooms.lock().await;

                                let room = rooms_lock
                                    .entry(room_id.clone())
                                    .or_insert_with(HashMap::new);
                                let already_member = remote_member
                                    || room.values().any(|c| c.contains_key(&user_id));
                                room.entry("General".to_string())
                                    .or_insert_with(HashMap::new);
                                let channel =
                                    room.entry(channel_id.clone()).or_insert_with(HashMap::new);

                                // Resume check: a live token for this
                                // room+channel means this connection is
                                // the same participant returning, not a
                                // new identity to be suffixed.
                                let seat_held = {
                                    let lock = state.away_seats.lock().await;
                                    lock.get(&(room_id.clone(), user_id.clone()))
                                        .is_some_and(|m| m.channel_id == channel_id)
                                };
                                resume_ok = crate::admin::validate_resume_token(
                                    &state,
                                    &room_id,
                                    &user_id,
                                    &channel_id,
                                    resume_token.as_deref(),
                                )
                                .await;
                                // The creator key is also proof of
                                // identity: its holder may reclaim a
                                // held seat (their browser may have lost
                                // the resume token, e.g. a fresh tab).
                                let key_ok_pre = if seat_held || occupied_remote_ids.contains(&user_id) {
                                    crate::admin::load_admin_redis(&state, &room_id).await;
                                    let lock = state.admin_rooms.lock().await;
                                    lock.get(&room_id)
                                        .and_then(|a| {
                                            admin_key
                                                .as_deref()
                                                .zip(a.key_hash.as_deref())
                                                .map(|(k, h)| {
                                                    crate::admin::verify_key(
                                                        &state.server_secret,
                                                        &room_id,
                                                        k,
                                                        h,
                                                    )
                                                })
                                        })
                                        .unwrap_or(false)
                                } else {
                                    false
                                };
                                let takeover = (resume_ok || key_ok_pre)
                                    && (seat_held || occupied_remote_ids.contains(&user_id));
                                warm_resume = takeover;
                                if takeover {
                                    state
                                        .away_seats
                                        .lock()
                                        .await
                                        .remove(&(room_id.clone(), user_id.clone()));
                                } else {
                                    user_id = unique_user_id(user_id, |candidate| {
                                        channel.contains_key(candidate)
                                            || occupied_remote_ids.contains(candidate)
                                    });
                                }

                                // ---- Admin gate: creator creation, bans, lobby ----
                                crate::admin::load_admin_redis(&state, &room_id).await;
                                {
                                    let mut admins_lock = state.admin_rooms.lock().await;
                                    let admin_state =
                                        admins_lock.entry(room_id.clone()).or_default();
                                    if admin_state.creator_id.is_none() && !room_preexisting {
                                        // This join creates the room: the joiner
                                        // becomes its creator and is issued the
                                        // signed key their browser will keep.
                                        let key = crate::admin::issue_key();
                                        admin_state.key_hash = Some(crate::admin::key_hash(
                                            &state.server_secret,
                                            &room_id,
                                            &key,
                                        ));
                                        admin_state.creator_id = Some(user_id.clone());
                                        admin_state.admins.insert(user_id.clone());
                                        admin_state.lobby = want_lobby;
                                        admin_state.kind = want_kind;
                                        issued_admin_key = Some(key);
                                        admin_state_changed = true;
                                    }
                                    if admin_state.banned.contains(&user_id) {
                                        let error_msg = crate::admin::error_text(
                                            "BANNED",
                                            "You are banned from this room.",
                                        );
                                        let _ = tx.send(Ok(Message::Text(error_msg.into()))).await;
                                        tokio::time::sleep(std::time::Duration::from_millis(500))
                                            .await;
                                        return;
                                    }
                                    let key_ok = admin_key
                                        .as_deref()
                                        .zip(admin_state.key_hash.as_deref())
                                        .map(|(k, h)| {
                                            crate::admin::verify_key(
                                                &state.server_secret,
                                                &room_id,
                                                k,
                                                h,
                                            )
                                        })
                                        .unwrap_or(false);
                                    if key_ok {
                                        // The creator is back: their current
                                        // identity becomes the creator id.
                                        admin_state.creator_id = Some(user_id.clone());
                                        admin_state.admins.insert(user_id.clone());
                                        admin_state_changed = true;
                                    }
                                    let bypass_lobby = key_ok
                                        || resume_ok
                                        || admin_state.is_admin(&user_id)
                                        || already_member
                                        || admin_state.approved.remove(&user_id);
                                    if admin_state.lobby && !bypass_lobby {
                                        admin_state.pending.insert(
                                            user_id.clone(),
                                            PendingKnock {
                                                nickname: nickname.clone(),
                                                tx: tx.clone(),
                                            },
                                        );
                                        park_for_knock = true;
                                    }
                                }

                                if !park_for_knock {
                                    {
                                        let mut times = state.channel_creation_times.lock().await;
                                        let room_times =
                                            times.entry(room_id.clone()).or_insert_with(HashMap::new);
                                        room_times
                                            .entry("General".to_string())
                                            .or_insert_with(current_unix_secs);
                                        room_times
                                            .entry(channel_id.clone())
                                            .or_insert_with(current_unix_secs);
                                    }

                                    // If this identity already has a stored status in
                                    // the channel (a reconnect: the old socket's
                                    // death hasn't been noticed yet), the server's
                                    // copy wins unless the client provably saved
                                    // something newer (profileRev). Otherwise a
                                    // client that lost its local storage (frozen tab
                                    // killed by iOS) would rejoin as "Guest" and
                                    // clobber the name everyone else still sees.
                                    // The client reconciles by adopting the status
                                    // echoed back in existing-users.
                                    let client_status = UserStatus {
                                        nickname: nickname.clone(),
                                        avatar: avatar.clone(),
                                        is_gif,
                                        static_frame: static_frame.clone(),
                                        is_muted,
                                        is_deafened,
                                        is_screen_sharing,
                                        is_low_bandwidth_mode,
                                        is_mobile,
                                        profile_rev,
                                    };
                                    joined_status = reconcile_join_status(
                                        channel.get(&user_id).map(|(_, status)| status),
                                        client_status,
                                    );

                                    {
                                        let mut status = joined_status.clone();
                                        let admins_lock = state.admin_rooms.lock().await;
                                        if let Some(a) = admins_lock.get(&room_id) {
                                            if a.kind == RoomKind::Stage && !a.can_speak(&user_id) {
                                                status.is_muted = true;
                                                status.is_screen_sharing = false;
                                            }
                                            if let Some(f) = a.forced.get(&user_id) {
                                                if f.muted {
                                                    status.is_muted = true;
                                                }
                                                if f.deafened {
                                                    status.is_deafened = true;
                                                }
                                                if f.share_off {
                                                    status.is_screen_sharing = false;
                                                }
                                            }
                                        }
                                        joined_status = status;
                                    }

                                    channel
                                        .insert(user_id.clone(), (tx.clone(), joined_status.clone()));
                                }
                            }

                            if park_for_knock {
                                let waiting =
                                    crate::admin::signal_text("knock-waiting", None, None);
                                let _ = tx.try_send(Ok(Message::Text(waiting.into())));
                                crate::admin::send_knock_update(&state, &room_id).await;
                                is_pending_knock = true;
                                continue;
                            }

                            if room_cleanup_generations
                                .lock()
                                .await
                                .remove(&room_id)
                                .is_some()
                            {
                                println!(
                                    "CLEANUP: Canceled pending deletion for room '{}'",
                                    room_id
                                );
                            }
                            is_joined = true;

                            if resume_ok || warm_resume {
                                // Let any node holding this user's held
                                // seat drop it silently; also moves the
                                // presence ownership to this node.
                                distributed_broadcast(
                                    &state,
                                    &DistributedMessage {
                                        msg_type: "user-resumed".into(),
                                        room_id: room_id.clone(),
                                        channel_id: channel_id.clone(),
                                        user_id: user_id.clone(),
                                        msg_id: Uuid::new_v4().to_string(),
                                        status: None,
                                        data: None,
                                        signal_msg: None,
                                    },
                                )
                                .await;
                            }

                            // Send the server-assigned userId back to the client
                            let new_resume_token = crate::admin::issue_resume_token(
                                &state,
                                &room_id,
                                &user_id,
                                &channel_id,
                            )
                            .await;
                            let joined_msg = serde_json::to_string(&SignalMessage {
                                msg_type: "joined".into(),
                                user_id: Some(user_id.clone()),
                                target: None,
                                data: Some(serde_json::json!({
                                    "resumed": resume_ok || warm_resume,
                                    "resumeToken": new_resume_token
                                })),
                            })
                            .unwrap();
                            let _ = tx.try_send(Ok(Message::Text(joined_msg.into())));

                            if let Some(key) = issued_admin_key.take() {
                                let key_msg = crate::admin::signal_text(
                                    "admin-key",
                                    None,
                                    Some(serde_json::json!({ "key": key })),
                                );
                                let _ = tx.try_send(Ok(Message::Text(key_msg.into())));
                            }
                            crate::admin::send_admin_update_to(&state, &room_id, &tx).await;
                            {
                                let (is_admin, forced) = {
                                    let lock = state.admin_rooms.lock().await;
                                    lock.get(&room_id)
                                        .map(|a| {
                                            (a.is_admin(&user_id), a.forced.get(&user_id).copied())
                                        })
                                        .unwrap_or((false, None))
                                };
                                if is_admin {
                                    crate::admin::send_knock_update(&state, &room_id).await;
                                }
                                if let Some(flags) = forced
                                    && flags.any()
                                {
                                    let msg = crate::admin::signal_text(
                                        "mod-state",
                                        None,
                                        Some(serde_json::json!({ "forced": flags })),
                                    );
                                    let _ = tx.try_send(Ok(Message::Text(msg.into())));
                                }
                            }
                            if admin_state_changed {
                                crate::admin::broadcast_admin_update(&state, &room_id).await;
                            }

                            {
                                let mut existing_users: Vec<serde_json::Value> = Vec::new();
                                let mut seen_ids = HashSet::new();
                                seen_ids.insert(user_id.clone());
                                {
                                    let rooms_lock = rooms.lock().await;
                                    if let Some(room) = rooms_lock.get(&room_id)
                                        && let Some(channel) = room.get(&channel_id)
                                    {
                                        for (uid, (_, status)) in channel.iter() {
                                            if seen_ids.insert(uid.clone()) {
                                                existing_users.push(serde_json::json!({
                                                        "id": uid,
                                                        "status": {
                                                            "nickname": status.nickname,
                                                            "avatar": status.avatar,
                                                            "isGif": status.is_gif,
                                                            "staticFrame": status.static_frame,
                                                            "isMuted": status.is_muted,
                                                            "isDeafened": status.is_deafened,
                                                            "isScreenSharing": status.is_screen_sharing,
                                                            "isLowBandwidthMode": status.is_low_bandwidth_mode,
                                                            "isMobile": status.is_mobile,
                                                            "profileRev": status.profile_rev
                                                        }
                                                    }));
                                            }
                                        }
                                    }
                                }
                                {
                                    let remote_lock = remote_users.lock().await;
                                    if let Some(remote_room) = remote_lock.get(&room_id)
                                        && let Some(remote_channel) = remote_room.get(&channel_id)
                                    {
                                        for (uid, status) in remote_channel.iter() {
                                            if seen_ids.insert(uid.clone()) {
                                                existing_users.push(serde_json::json!({
                                                        "id": uid,
                                                        "status": {
                                                            "nickname": status.nickname,
                                                            "avatar": status.avatar,
                                                            "isGif": status.is_gif,
                                                            "staticFrame": status.static_frame,
                                                            "isMuted": status.is_muted,
                                                            "isDeafened": status.is_deafened,
                                                            "isScreenSharing": status.is_screen_sharing,
                                                            "isLowBandwidthMode": status.is_low_bandwidth_mode,
                                                            "isMobile": status.is_mobile,
                                                            "profileRev": status.profile_rev
                                                        }
                                                    }));
                                            }
                                        }
                                    }
                                }
                                let existing_users_msg = serde_json::to_string(&SignalMessage {
                                    msg_type: "existing-users".into(),
                                    user_id: None,
                                    target: None,
                                    data: Some(serde_json::json!({ "users": existing_users })),
                                })
                                .unwrap();
                                let _ = tx.try_send(Ok(Message::Text(existing_users_msg.into())));
                            }

                            // Only forward validated public profile fields. The
                            // room-creation password must never be published to other
                            // instances. Uses the canonical stored status so a stale reconnect
                            // announces the server's copy, not the client's.
                            let created_at = state
                                .channel_creation_times
                                .lock()
                                .await
                                .get(&room_id)
                                .and_then(|channels| channels.get(&channel_id))
                                .copied()
                                .unwrap_or_else(current_unix_secs);
                            let notify_data = Some(serde_json::json!({
                                "nickname": joined_status.nickname,
                                "avatar": joined_status.avatar,
                                "isGif": joined_status.is_gif,
                                "staticFrame": joined_status.static_frame,
                                "isMuted": joined_status.is_muted,
                                "isDeafened": joined_status.is_deafened,
                                "camEnabled": cam_enabled,
                                "screenEnabled": joined_status.is_screen_sharing,
                                "screenAudio": screen_has_audio,
                                "micTrackId": mic_track_id,
                                "camVideoTrackId": cam_video_track_id,
                                "screenVideoTrackId": screen_video_track_id,
                                "screenAudioTrackId": screen_audio_track_id,
                                "isLowBandwidthMode": joined_status.is_low_bandwidth_mode,
                                "isMobile": joined_status.is_mobile,
                                "profileRev": joined_status.profile_rev,
                                "createdAt": created_at
                            }));

                            let notify_msg = serde_json::to_string(&SignalMessage {
                                msg_type: "user-joined".into(),
                                user_id: Some(user_id.clone()),
                                target: None,
                                data: notify_data.clone(),
                            })
                            .unwrap();

                            // Only announce the join if we're still in the channel:
                            // the user may have been kicked between the channel
                            // insert and this point, in which case a stale
                            // user-joined broadcast would resurrect a ghost.
                            let still_in_channel = {
                                let rooms_lock = rooms.lock().await;
                                rooms_lock
                                    .get(&room_id)
                                    .and_then(|room| room.get(&channel_id))
                                    .and_then(|channel| channel.get(&user_id))
                                    .is_some_and(|(stored_tx, _)| stored_tx.same_channel(&tx))
                            };
                            if still_in_channel && !warm_resume {
                                {
                                    let rooms_lock = rooms.lock().await;
                                    if let Some(room) = rooms_lock.get(&room_id)
                                        && let Some(channel) = room.get(&channel_id)
                                    {
                                        for (uid, (tx, _)) in channel.iter() {
                                            if *uid != user_id {
                                                let _ = tx.try_send(Ok(Message::Text(
                                                    notify_msg.clone().into(),
                                                )));
                                            }
                                        }
                                    }
                                }
                                broadcast_channel_upsert(&state, &room_id, "General").await;
                                if channel_id != "General" {
                                    broadcast_channel_upsert(&state, &room_id, &channel_id).await;
                                }
                                distributed_broadcast(
                                    &state,
                                    &DistributedMessage {
                                        msg_type: "user-joined".into(),
                                        room_id: room_id.clone(),
                                        channel_id: channel_id.clone(),
                                        user_id: user_id.clone(),
                                        msg_id: Uuid::new_v4().to_string(),
                                        status: Some(joined_status.clone()),
                                        data: notify_data.clone(),
                                        signal_msg: None,
                                    },
                                )
                                .await;
                                broadcast_channel_list(
                                    &rooms,
                                    &remote_users,
                                    &state.channel_creation_times,
                                    &room_id,
                                )
                                .await;
                            }
                        }
                    } else {
                        if parsed.msg_type == "update-user" {
                            let data = parsed.data.as_ref().and_then(|d| d.as_object());
                            let contains_profile_image = data.is_some_and(|data| {
                                data.contains_key("avatar") || data.contains_key("staticFrame")
                            });
                            if contains_profile_image {
                                let now = std::time::Instant::now();
                                if last_profile_image_update.is_some_and(|last| {
                                    now.duration_since(last)
                                        < std::time::Duration::from_secs(
                                            PROFILE_IMAGE_UPDATE_COOLDOWN_SECS,
                                        )
                                }) {
                                    continue;
                                }
                                last_profile_image_update = Some(now);
                            }

                            let mut full_status = None;
                            {
                                let mut rooms_lock = rooms.lock().await;
                                if let Some(room) = rooms_lock.get_mut(&room_id)
                                    && let Some(channel) = room.get_mut(&channel_id)
                                {
                                    if let Some((_, status)) = channel.get_mut(&user_id) {
                                        let previous = status.clone();
                                        // Drop updates from a stale client copy
                                        // (e.g. one that lost its local storage):
                                        // the join-time echo has already
                                        // reconciled it, and letting an older
                                        // revision overwrite the stored profile
                                        // would re-introduce the desync.
                                        let data_rev = data
                                            .and_then(|d| d.get("profileRev"))
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or(0);
                                        if data_rev >= status.profile_rev {
                                            if let Some(d) = data {
                                                if let Some(n) =
                                                    d.get("nickname").and_then(|v| v.as_str())
                                                {
                                                    status.nickname = normalize_nickname(n);
                                                }
                                                if let Some(a) = d.get("avatar") {
                                                    if a.is_null() {
                                                        status.avatar = None;
                                                        status.is_gif = false;
                                                        status.static_frame = None;
                                                    } else if let Some(a_str) = a.as_str()
                                                        && a_str.len() <= MAX_AVATAR_DATA_LEN
                                                    {
                                                        status.avatar = Some(a_str.to_string());
                                                        // A frame belongs to a specific GIF.
                                                        // Do not retain one from the old avatar
                                                        // unless this update supplies a new one.
                                                        status.static_frame = None;
                                                    }
                                                }
                                                if let Some(g) =
                                                    d.get("isGif").and_then(|v| v.as_bool())
                                                {
                                                    status.is_gif = g;
                                                }
                                                if d.contains_key("staticFrame") {
                                                    let sf = d
                                                        .get("staticFrame")
                                                        .and_then(|v| v.as_str())
                                                        .filter(|s| {
                                                            s.len() <= MAX_STATIC_FRAME_DATA_LEN
                                                        })
                                                        .map(|s| s.to_string());
                                                    if sf.is_some() {
                                                        status.static_frame = sf;
                                                    } else if d
                                                        .get("staticFrame")
                                                        .is_some_and(|v| v.is_null())
                                                    {
                                                        status.static_frame = None;
                                                    }
                                                }
                                                if let Some(m) =
                                                    d.get("isMuted").and_then(|v| v.as_bool())
                                                {
                                                    status.is_muted = m;
                                                }
                                                if let Some(d) =
                                                    d.get("isDeafened").and_then(|v| v.as_bool())
                                                {
                                                    status.is_deafened = d;
                                                }
                                                if let Some(lbm) = d
                                                    .get("isLowBandwidthMode")
                                                    .and_then(|v| v.as_bool())
                                                {
                                                    status.is_low_bandwidth_mode = lbm;
                                                }
                                                if status.avatar.is_none() {
                                                    status.is_gif = false;
                                                    status.static_frame = None;
                                                } else if !status.is_gif {
                                                    status.static_frame = None;
                                                }
                                            }
                                            status.profile_rev = data_rev.max(status.profile_rev);
                                        }
                                        {
                                            let admins_lock = state.admin_rooms.lock().await;
                                            if let Some(a) = admins_lock.get(&room_id) {
                                                if a.kind == RoomKind::Stage
                                                    && !a.can_speak(&user_id)
                                                {
                                                    status.is_muted = true;
                                                    status.is_screen_sharing = false;
                                                }
                                                if let Some(f) = a.forced.get(&user_id) {
                                                    if f.muted {
                                                        status.is_muted = true;
                                                    }
                                                    if f.deafened {
                                                        status.is_deafened = true;
                                                    }
                                                    if f.share_off {
                                                        status.is_screen_sharing = false;
                                                    }
                                                }
                                            }
                                        }
                                        // Only propagate when something actually changed,
                                        // so spammy same-value updates don't trigger a
                                        // room-list rebuild and distributed broadcast.
                                        if status != &previous {
                                            full_status = Some(status.clone());
                                        }
                                    }

                                    if let Some(ref status) = full_status {
                                        let full_data = serde_json::to_value(status).unwrap();

                                        let notify_msg = serde_json::to_string(&SignalMessage {
                                            msg_type: "user-update".into(),
                                            user_id: Some(user_id.clone()),
                                            target: None,
                                            data: Some(full_data),
                                        })
                                        .unwrap();

                                        // Broadcast to everyone INCLUDING the sender:
                                        // the authoritative echo lets the client
                                        // reconcile its local profile (and heal it
                                        // if local storage was lost).
                                        for (_, (tx, _)) in channel.iter() {
                                            let _ = tx.try_send(Ok(Message::Text(
                                                notify_msg.clone().into(),
                                            )));
                                        }
                                    }

                                    if let Some(ref status) = full_status {
                                        distributed_broadcast(
                                            &state,
                                            &DistributedMessage {
                                                msg_type: "user-update".into(),
                                                room_id: room_id.clone(),
                                                channel_id: channel_id.clone(),
                                                user_id: user_id.clone(),
                                                msg_id: Uuid::new_v4().to_string(),
                                                status: Some(status.clone()),
                                                data: None,
                                                signal_msg: None,
                                            },
                                        )
                                        .await;
                                    }
                                }
                            }
                            if full_status.is_some() {
                                broadcast_channel_list(
                                    &rooms,
                                    &remote_users,
                                    &state.channel_creation_times,
                                    &room_id,
                                )
                                .await;
                            }
                        } else if parsed.msg_type == "cam-toggle" {
                            // Stage audience and force-camera-off users may
                            // turn their camera off but never on.
                            let enabling = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("enabled"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let cam_blocked = if enabling {
                                let admins_lock = state.admin_rooms.lock().await;
                                admins_lock
                                    .get(&room_id)
                                    .map(|a| {
                                        (a.kind == RoomKind::Stage && !a.can_speak(&user_id))
                                            || a.forced
                                                .get(&user_id)
                                                .map(|f| f.cam_off)
                                                .unwrap_or(false)
                                    })
                                    .unwrap_or(false)
                            } else {
                                false
                            };
                            // Distributed payloads are amplified to every room member and
                            // every instance, so reject oversized ones.
                            if !cam_blocked && text.len() <= MAX_DISTRIBUTED_DATA_LEN {
                                let rooms_lock = rooms.lock().await;
                                if let Some(room) = rooms_lock.get(&room_id)
                                    && let Some(channel) = room.get(&channel_id)
                                {
                                    let notify_msg = serde_json::to_string(&SignalMessage {
                                        msg_type: "cam-toggle".into(),
                                        user_id: Some(user_id.clone()),
                                        target: None,
                                        data: parsed.data.clone(),
                                    })
                                    .unwrap();

                                    for (uid, (tx, _)) in channel.iter() {
                                        if *uid != user_id {
                                            let _ = tx.try_send(Ok(Message::Text(
                                                notify_msg.clone().into(),
                                            )));
                                        }
                                    }
                                }
                                distributed_broadcast(
                                    &state,
                                    &DistributedMessage {
                                        msg_type: "cam-toggle".into(),
                                        room_id: room_id.clone(),
                                        channel_id: channel_id.clone(),
                                        user_id: user_id.clone(),
                                        msg_id: Uuid::new_v4().to_string(),
                                        status: None,
                                        data: parsed.data.clone(),
                                        signal_msg: None,
                                    },
                                )
                                .await;
                            }
                        } else if parsed.msg_type == "screen-toggle" {
                            let enabling = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("enabled"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let screen_blocked = if enabling {
                                let admins_lock = state.admin_rooms.lock().await;
                                admins_lock
                                    .get(&room_id)
                                    .map(|a| {
                                        (a.kind == RoomKind::Stage && !a.can_speak(&user_id))
                                            || a.forced
                                                .get(&user_id)
                                                .map(|f| f.share_off)
                                                .unwrap_or(false)
                                    })
                                    .unwrap_or(false)
                            } else {
                                false
                            };
                            if !screen_blocked && text.len() <= MAX_DISTRIBUTED_DATA_LEN {
                                {
                                    let mut rooms_lock = rooms.lock().await;
                                    if let Some(room) = rooms_lock.get_mut(&room_id)
                                        && let Some(channel) = room.get_mut(&channel_id)
                                    {
                                        if let Some((_, status)) = channel.get_mut(&user_id)
                                            && let Some(enabled) = parsed
                                                .data
                                                .as_ref()
                                                .and_then(|d| d.get("enabled"))
                                                .and_then(|v| v.as_bool())
                                        {
                                            status.is_screen_sharing = enabled;
                                        }

                                        let notify_msg = serde_json::to_string(&SignalMessage {
                                            msg_type: "screen-toggle".into(),
                                            user_id: Some(user_id.clone()),
                                            target: None,
                                            data: parsed.data.clone(),
                                        })
                                        .unwrap();

                                        for (uid, (tx, _)) in channel.iter() {
                                            if *uid != user_id {
                                                let _ = tx.try_send(Ok(Message::Text(
                                                    notify_msg.clone().into(),
                                                )));
                                            }
                                        }
                                    }
                                }

                                distributed_broadcast(
                                    &state,
                                    &DistributedMessage {
                                        msg_type: "screen-toggle".into(),
                                        room_id: room_id.clone(),
                                        channel_id: channel_id.clone(),
                                        user_id: user_id.clone(),
                                        msg_id: Uuid::new_v4().to_string(),
                                        status: None,
                                        data: parsed.data.clone(),
                                        signal_msg: None,
                                    },
                                )
                                .await;
                                broadcast_channel_list(
                                    &rooms,
                                    &remote_users,
                                    &state.channel_creation_times,
                                    &room_id,
                                )
                                .await;
                            }
                        } else if parsed.msg_type == "kick-user" {
                            let actor_is_admin = {
                                let lock = state.admin_rooms.lock().await;
                                lock.get(&room_id)
                                    .map(|a| a.is_admin(&user_id))
                                    .unwrap_or(false)
                            };
                            if !actor_is_admin {
                                let error_msg = crate::admin::error_text(
                                    "NOT_ADMIN",
                                    "Only admins can kick users.",
                                );
                                let _ = tx.try_send(Ok(Message::Text(error_msg.into())));
                                continue;
                            }

                            let target_user_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("userId"))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());

                            if let Some(kick_uid) = target_user_id {
                                if kick_uid == user_id {
                                    let error_msg = serde_json::to_string(&SignalMessage {
                                        msg_type: "error".into(),
                                        user_id: None,
                                        target: None,
                                        data: Some(serde_json::json!({
                                            "code": "KICK_SELF",
                                            "message": "You cannot kick yourself."
                                        })),
                                    })
                                    .unwrap();
                                    let _ = tx.try_send(Ok(Message::Text(error_msg.into())));
                                    continue;
                                }

                                let mut rooms_lock = rooms.lock().await;
                                let mut kicked = false;
                                let mut kicked_tx = None;

                                if let Some(room) = rooms_lock.get_mut(&room_id)
                                    && let Some(channel) = room.get_mut(&channel_id)
                                    && let Some((tx, _)) = channel.remove(&kick_uid)
                                {
                                    kicked = true;
                                    kicked_tx = Some(tx);
                                }

                                if kicked {
                                    let kick_notify_msg = serde_json::to_string(&SignalMessage {
                                        msg_type: "user-kicked".into(),
                                        user_id: Some(kick_uid.clone()),
                                        target: None,
                                        data: None,
                                    })
                                    .unwrap();

                                    if let Some(room) = rooms_lock.get(&room_id)
                                        && let Some(channel) = room.get(&channel_id)
                                    {
                                        for (_uid, (tx, _)) in channel.iter() {
                                            let _ = tx.try_send(Ok(Message::Text(
                                                kick_notify_msg.clone().into(),
                                            )));
                                        }
                                    }

                                    drop(rooms_lock);
                                    state
                                        .away_seats
                                        .lock()
                                        .await
                                        .remove(&(room_id.clone(), kick_uid.clone()));
                                    crate::admin::delete_resume_token(
                                        &state, &room_id, &kick_uid,
                                    )
                                    .await;

                                    // This node never processes its own
                                    // broadcast, so also drop any duplicate
                                    // remote entry for the same id.
                                    {
                                        let mut rl = remote_users.lock().await;
                                        remove_remote_user(
                                            &mut rl,
                                            &room_id,
                                            &channel_id,
                                            &kick_uid,
                                        );
                                    }
                                    remove_remote_user_owner(
                                        &state.remote_user_owners,
                                        &room_id,
                                        &channel_id,
                                        &kick_uid,
                                    )
                                    .await;

                                    if let Some(kicked_tx) = kicked_tx {
                                        let _ = kicked_tx
                                            .try_send(Ok(Message::Text(kick_notify_msg.into())));

                                        let _ = kicked_tx.try_send(Ok(Message::Close(None)));
                                    }

                                    distributed_broadcast(
                                        &state,
                                        &DistributedMessage {
                                            msg_type: "user-kicked".into(),
                                            room_id: room_id.clone(),
                                            channel_id: channel_id.clone(),
                                            user_id: kick_uid.clone(),
                                            msg_id: Uuid::new_v4().to_string(),
                                            status: None,
                                            data: None,
                                            signal_msg: None,
                                        },
                                    )
                                    .await;
                                    broadcast_channel_list(
                                        &rooms,
                                        &remote_users,
                                        &state.channel_creation_times,
                                        &room_id,
                                    )
                                    .await;
                                    // A kicked user never runs the disconnect
                                    // cleanup, so schedule room cleanup here or
                                    // the empty room would linger forever.
                                    schedule_empty_room_cleanup(&state, &room_id).await;
                                } else {
                                    drop(rooms_lock);
                                    // Target isn't local; publish the kick so the
                                    // hosting instance removes them and closes the socket.
                                    let is_remote = {
                                        let rl = remote_users.lock().await;
                                        rl.get(&room_id)
                                            .and_then(|r| r.get(&channel_id))
                                            .map(|c| c.contains_key(&kick_uid))
                                            .unwrap_or(false)
                                    };
                                    if is_remote {
                                        // Remove from this node's own view: this
                                        // node won't process its own broadcast,
                                        // and the hosting node's cleanup only
                                        // propagates via user-left on disconnect.
                                        {
                                            let mut rl = remote_users.lock().await;
                                            remove_remote_user(
                                                &mut rl,
                                                &room_id,
                                                &channel_id,
                                                &kick_uid,
                                            );
                                        }
                                        remove_remote_user_owner(
                                            &state.remote_user_owners,
                                            &room_id,
                                            &channel_id,
                                            &kick_uid,
                                        )
                                        .await;
                                        broadcast_channel_list(
                                            &rooms,
                                            &remote_users,
                                            &state.channel_creation_times,
                                            &room_id,
                                        )
                                        .await;
                                        distributed_broadcast(
                                            &state,
                                            &DistributedMessage {
                                                msg_type: "user-kicked".into(),
                                                room_id: room_id.clone(),
                                                channel_id: channel_id.clone(),
                                                user_id: kick_uid.clone(),
                                                msg_id: Uuid::new_v4().to_string(),
                                                status: None,
                                                data: None,
                                                signal_msg: None,
                                            },
                                        )
                                        .await;
                                    }
                                }
                            }
                        } else if parsed.msg_type == "admin-action" {
                            let action = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("action"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let target = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("userId"))
                                .and_then(|v| v.as_str())
                                .map(|v| v.to_string());
                            let move_channel = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("channelId"))
                                .and_then(|v| v.as_str())
                                .and_then(normalize_channel_id);
                            let actor_is_admin = {
                                let lock = state.admin_rooms.lock().await;
                                lock.get(&room_id)
                                    .map(|a| a.is_admin(&user_id))
                                    .unwrap_or(false)
                            };
                            if !actor_is_admin {
                                let error_msg = crate::admin::error_text(
                                    "NOT_ADMIN",
                                    "Only admins can do that.",
                                );
                                let _ = tx.try_send(Ok(Message::Text(error_msg.into())));
                                continue;
                            }
                            let Some(target) = target else { continue };

                            match action.as_str() {
                                "promote" | "demote" | "to-stage" | "to-audience" => {
                                    let mut refusal: Option<(&str, &str)> = None;
                                    let mut changed = false;
                                    {
                                        let mut lock = state.admin_rooms.lock().await;
                                        if let Some(a) = lock.get_mut(&room_id) {
                                            match action.as_str() {
                                                "promote" => {
                                                    changed = a.admins.insert(target.clone());
                                                }
                                                "demote" => {
                                                    if a.creator_id.as_deref()
                                                        == Some(target.as_str())
                                                    {
                                                        refusal = Some((
                                                            "CANNOT_DEMOTE_CREATOR",
                                                            "The room creator cannot be demoted.",
                                                        ));
                                                    } else {
                                                        changed =
                                                            a.admins.remove(&target);
                                                    }
                                                }
                                                "to-stage" => {
                                                    changed =
                                                        a.speakers.insert(target.clone());
                                                }
                                                "to-audience" => {
                                                    if a.is_admin(&target) {
                                                        refusal = Some((
                                                            "MUST_DEMOTE_FIRST",
                                                            "Demote this admin before moving them to the audience.",
                                                        ));
                                                    } else {
                                                        changed =
                                                            a.speakers.remove(&target);
                                                    }
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    if let Some((code, message)) = refusal {
                                        let error_msg =
                                            crate::admin::error_text(code, message);
                                        let _ =
                                            tx.try_send(Ok(Message::Text(error_msg.into())));
                                        continue;
                                    }
                                    if changed {
                                        crate::admin::broadcast_admin_update(&state, &room_id)
                                            .await;
                                    }
                                    if action == "to-audience" {
                                        // Silence the demoted user's stored
                                        // status immediately; their client
                                        // also self-mutes on admin-update.
                                        let mut updated_status = None;
                                        {
                                            let mut rooms_lock = rooms.lock().await;
                                            if let Some(room) = rooms_lock.get_mut(&room_id) {
                                                for (_, channel) in room.iter_mut() {
                                                    if let Some((_, status)) =
                                                        channel.get_mut(&target)
                                                    {
                                                        status.is_muted = true;
                                                        status.is_screen_sharing = false;
                                                        updated_status =
                                                            Some(status.clone());
                                                        break;
                                                    }
                                                }
                                            }
                                            if let Some(status) = &updated_status {
                                                let notify_msg =
                                                    serde_json::to_string(&SignalMessage {
                                                        msg_type: "user-update".into(),
                                                        user_id: Some(target.clone()),
                                                        target: None,
                                                        data: Some(
                                                            serde_json::to_value(status)
                                                                .unwrap(),
                                                        ),
                                                    })
                                                    .unwrap();
                                                if let Some(room) = rooms_lock.get(&room_id) {
                                                    for (_, channel) in room.iter() {
                                                        for (_, (tx, _)) in channel.iter() {
                                                            let _ = tx.try_send(Ok(
                                                                Message::Text(
                                                                    notify_msg.clone().into(),
                                                                ),
                                                            ));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if let Some(status) = updated_status {
                                            distributed_broadcast(
                                                &state,
                                                &DistributedMessage {
                                                    msg_type: "user-update".into(),
                                                    room_id: room_id.clone(),
                                                    channel_id: channel_id.clone(),
                                                    user_id: target.clone(),
                                                    msg_id: Uuid::new_v4().to_string(),
                                                    status: Some(status),
                                                    data: None,
                                                    signal_msg: None,
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                }
                                "mute" | "unmute" | "deafen" | "undeafen" | "cam-off"
                                | "cam-on" | "share-off" | "share-on" => {
                                    {
                                        let mut lock = state.admin_rooms.lock().await;
                                        if let Some(a) = lock.get_mut(&room_id) {
                                            let flags =
                                                a.forced.entry(target.clone()).or_default();
                                            match action.as_str() {
                                                "mute" => flags.muted = true,
                                                "unmute" => flags.muted = false,
                                                "deafen" => flags.deafened = true,
                                                "undeafen" => flags.deafened = false,
                                                "cam-off" => flags.cam_off = true,
                                                "cam-on" => flags.cam_off = false,
                                                "share-off" => flags.share_off = true,
                                                "share-on" => flags.share_off = false,
                                                _ => {}
                                            }
                                            if !flags.any() {
                                                a.forced.remove(&target);
                                            }
                                        }
                                    }
                                    let by_name = {
                                        let rooms_lock = rooms.lock().await;
                                        rooms_lock
                                            .get(&room_id)
                                            .and_then(|room| room.get(&channel_id))
                                            .and_then(|channel| channel.get(&user_id))
                                            .map(|(_, status)| status.nickname.clone())
                                            .unwrap_or_else(|| "An admin".to_string())
                                    };
                                    let text = crate::admin::signal_text(
                                        "mod-action",
                                        Some(target.clone()),
                                        Some(serde_json::json!({
                                            "action": action,
                                            "by": by_name
                                        })),
                                    );
                                    let delivered = {
                                        let rooms_lock = rooms.lock().await;
                                        let mut delivered = false;
                                        if let Some(room) = rooms_lock.get(&room_id) {
                                            for (_, channel) in room.iter() {
                                                if let Some((target_tx, _)) =
                                                    channel.get(&target)
                                                {
                                                    let _ = target_tx.try_send(Ok(
                                                        Message::Text(text.clone().into()),
                                                    ));
                                                    delivered = true;
                                                    break;
                                                }
                                            }
                                        }
                                        delivered
                                    };
                                    if !delivered {
                                        distributed_broadcast(
                                            &state,
                                            &DistributedMessage {
                                                msg_type: "mod-action".into(),
                                                room_id: room_id.clone(),
                                                channel_id: channel_id.clone(),
                                                user_id: target.clone(),
                                                msg_id: Uuid::new_v4().to_string(),
                                                status: None,
                                                data: None,
                                                signal_msg: Some(text),
                                            },
                                        )
                                        .await;
                                    }
                                    crate::admin::broadcast_admin_update(&state, &room_id).await;
                                }
                                "ban" => {
                                    let is_creator = {
                                        let lock = state.admin_rooms.lock().await;
                                        lock.get(&room_id)
                                            .map(|a| {
                                                a.creator_id.as_deref() == Some(target.as_str())
                                            })
                                            .unwrap_or(false)
                                    };
                                    if target == user_id || is_creator {
                                        let error_msg = crate::admin::error_text(
                                            "CANNOT_BAN",
                                            "You cannot ban yourself or the room creator.",
                                        );
                                        let _ =
                                            tx.try_send(Ok(Message::Text(error_msg.into())));
                                        continue;
                                    }
                                    {
                                        let mut lock = state.admin_rooms.lock().await;
                                        if let Some(a) = lock.get_mut(&room_id) {
                                            a.banned.insert(target.clone());
                                            state
                                                .away_seats
                                                .lock()
                                                .await
                                                .remove(&(room_id.clone(), target.clone()));
                                            crate::admin::delete_resume_token(
                                                &state, &room_id, &target,
                                            )
                                            .await;
                                            a.admins.remove(&target);
                                            a.speakers.remove(&target);
                                            a.forced.remove(&target);
                                        }
                                    }
                                    let notify = crate::admin::signal_text(
                                        "user-banned",
                                        Some(target.clone()),
                                        None,
                                    );
                                    let mut victim_tx = None;
                                    let mut victim_channel = channel_id.clone();
                                    {
                                        let mut rooms_lock = rooms.lock().await;
                                        if let Some(room) = rooms_lock.get_mut(&room_id) {
                                            for (ch, channel) in room.iter_mut() {
                                                if let Some((tx, _)) =
                                                    channel.remove(&target)
                                                {
                                                    victim_tx = Some(tx);
                                                    victim_channel = ch.clone();
                                                    break;
                                                }
                                            }
                                        }
                                        if victim_tx.is_some()
                                            && let Some(room) = rooms_lock.get(&room_id)
                                            && let Some(channel) =
                                                room.get(&victim_channel)
                                        {
                                            for (_, (tx, _)) in channel.iter() {
                                                let _ = tx.try_send(Ok(Message::Text(
                                                    notify.clone().into(),
                                                )));
                                            }
                                        }
                                    }
                                    if let Some(vtx) = victim_tx {
                                        let _ = vtx
                                            .try_send(Ok(Message::Text(notify.clone().into())));
                                        let _ = vtx.try_send(Ok(Message::Close(None)));
                                    } else {
                                        let mut rl = remote_users.lock().await;
                                        remove_remote_user(
                                            &mut rl,
                                            &room_id,
                                            &victim_channel,
                                            &target,
                                        );
                                        drop(rl);
                                        remove_remote_user_owner(
                                            &state.remote_user_owners,
                                            &room_id,
                                            &victim_channel,
                                            &target,
                                        )
                                        .await;
                                    }
                                    distributed_broadcast(
                                        &state,
                                        &DistributedMessage {
                                            msg_type: "user-banned".into(),
                                            room_id: room_id.clone(),
                                            channel_id: victim_channel.clone(),
                                            user_id: target.clone(),
                                            msg_id: Uuid::new_v4().to_string(),
                                            status: None,
                                            data: None,
                                            signal_msg: None,
                                        },
                                    )
                                    .await;
                                    broadcast_channel_list(
                                        &rooms,
                                        &remote_users,
                                        &state.channel_creation_times,
                                        &room_id,
                                    )
                                    .await;
                                    schedule_empty_room_cleanup(&state, &room_id).await;
                                    crate::admin::broadcast_admin_update(&state, &room_id).await;
                                }
                                "move" => {
                                    let Some(dest) = move_channel else { continue };
                                    if dest != channel_id {
                                        // Pre-approve the landing join: the
                                        // switch closes the old connection
                                        // first, so without this a lobby room
                                        // would make the moved user knock.
                                        {
                                            let mut lock = state.admin_rooms.lock().await;
                                            if let Some(a) = lock.get_mut(&room_id) {
                                                a.approved.insert(target.clone());
                                            }
                                        }
                                        let text = crate::admin::signal_text(
                                            "channel-moved",
                                            Some(target.clone()),
                                            Some(serde_json::json!({
                                                "channelId": dest
                                            })),
                                        );
                                        let delivered = {
                                            let rooms_lock = rooms.lock().await;
                                            let mut delivered = false;
                                            if let Some(room) = rooms_lock.get(&room_id) {
                                                for (_, channel) in room.iter() {
                                                    if let Some((target_tx, _)) =
                                                        channel.get(&target)
                                                    {
                                                        let _ = target_tx.try_send(Ok(
                                                            Message::Text(text.clone().into()),
                                                        ));
                                                        delivered = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            delivered
                                        };
                                        if !delivered {
                                            distributed_broadcast(
                                                &state,
                                                &DistributedMessage {
                                                    msg_type: "mod-action".into(),
                                                    room_id: room_id.clone(),
                                                    channel_id: dest.clone(),
                                                    user_id: target.clone(),
                                                    msg_id: Uuid::new_v4().to_string(),
                                                    status: None,
                                                    data: None,
                                                    signal_msg: Some(text),
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                }
                                "knock-approve" | "knock-deny" => {
                                    let approve = action == "knock-approve";
                                    let pending_tx = {
                                        let mut lock = state.admin_rooms.lock().await;
                                        lock.get_mut(&room_id).and_then(|a| {
                                            let removed = a.pending.remove(&target);
                                            if approve && removed.is_some() {
                                                a.approved.insert(target.clone());
                                            }
                                            removed.map(|p| p.tx)
                                        })
                                    };
                                    match pending_tx {
                                        Some(ptx) => {
                                            if approve {
                                                let msg = crate::admin::signal_text(
                                                    "knock-approved",
                                                    None,
                                                    None,
                                                );
                                                let _ = ptx.try_send(Ok(Message::Text(
                                                    msg.into(),
                                                )));
                                            } else {
                                                let msg = crate::admin::signal_text(
                                                    "knock-denied",
                                                    None,
                                                    None,
                                                );
                                                let _ = ptx.try_send(Ok(Message::Text(
                                                    msg.into(),
                                                )));
                                                let _ = ptx.try_send(Ok(Message::Close(None)));
                                            }
                                        }
                                        None => {
                                            distributed_broadcast(
                                                &state,
                                                &DistributedMessage {
                                                    msg_type: "knock-response".into(),
                                                    room_id: room_id.clone(),
                                                    channel_id: String::new(),
                                                    user_id: target.clone(),
                                                    msg_id: Uuid::new_v4().to_string(),
                                                    status: None,
                                                    data: Some(serde_json::json!({
                                                        "approve": approve
                                                    })),
                                                    signal_msg: None,
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                    crate::admin::send_knock_update(&state, &room_id).await;
                                }
                                _ => {}
                            }
                        } else if parsed.msg_type == "rename-channel" {
                            let mut target_channel_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("channelId"))
                                .and_then(|v| v.as_str())
                                .unwrap_or(&channel_id)
                                .to_string();

                            if target_channel_id.eq_ignore_ascii_case("general") {
                                target_channel_id = "General".to_string();
                            }

                            if target_channel_id != "General" {
                                let new_name = parsed
                                    .data
                                    .as_ref()
                                    .and_then(|d| d.get("newName"))
                                    .and_then(|v| v.as_str())
                                    .and_then(normalize_channel_id);

                                if let Some(new_name_str) = new_name {
                                    let mut rooms_lock = rooms.lock().await;

                                    // The channel may be hosted on this node, on
                                    // another node, or both. It can be renamed
                                    // only when it is empty everywhere we can
                                    // see it and the target name is free in
                                    // both views.
                                    let can_rename = {
                                        let rl = remote_users.lock().await;
                                        let remote_room = rl.get(&room_id);
                                        let room = rooms_lock.get(&room_id);
                                        let times = state.channel_creation_times.lock().await;
                                        let metadata_room = times.get(&room_id);
                                        let local_channel =
                                            room.and_then(|r| r.get(&target_channel_id));
                                        let remote_channel =
                                            remote_room.and_then(|r| r.get(&target_channel_id));
                                        let exists_anywhere = local_channel.is_some()
                                            || remote_channel.is_some()
                                            || metadata_room.is_some_and(|channels| {
                                                channels.contains_key(&target_channel_id)
                                            });
                                        let local_ok = local_channel.is_none_or(HashMap::is_empty);
                                        let remote_ok =
                                            remote_channel.is_none_or(HashMap::is_empty);
                                        let local_collision =
                                            room.is_some_and(|r| r.contains_key(&new_name_str));
                                        let remote_collision = remote_room
                                            .is_some_and(|r| r.contains_key(&new_name_str));
                                        let metadata_collision =
                                            metadata_room.is_some_and(|channels| {
                                                channels.contains_key(&new_name_str)
                                            });
                                        exists_anywhere
                                            && local_ok
                                            && remote_ok
                                            && !local_collision
                                            && !remote_collision
                                            && !metadata_collision
                                    };

                                    if can_rename {
                                        if let Some(room) = rooms_lock.get_mut(&room_id)
                                            && let Some(channel) = room.remove(&target_channel_id)
                                        {
                                            room.insert(new_name_str.clone(), channel);
                                        }

                                        // Broadcast rename-channel to local users in this room
                                        let rename_msg = serde_json::to_string(&SignalMessage {
                                            msg_type: "rename-channel".into(),
                                            user_id: Some(user_id.clone()),
                                            target: None,
                                            data: Some(serde_json::json!({
                                                "roomId": room_id,
                                                "oldName": target_channel_id,
                                                "newName": new_name_str,
                                            })),
                                        })
                                        .unwrap();

                                        if let Some(room) = rooms_lock.get(&room_id) {
                                            for (_ch_name, channel) in room.iter() {
                                                for (_uid, (tx, _)) in channel.iter() {
                                                    let _ = tx.try_send(Ok(Message::Text(
                                                        rename_msg.clone().into(),
                                                    )));
                                                }
                                            }
                                        }

                                        drop(rooms_lock);

                                        {
                                            let mut times =
                                                state.channel_creation_times.lock().await;
                                            if let Some(room_times) = times.get_mut(&room_id)
                                                && let Some(created_at) =
                                                    room_times.remove(&target_channel_id)
                                            {
                                                room_times.insert(new_name_str.clone(), created_at);
                                            }
                                        }

                                        // Also rename in remote_users so signal routing stays consistent
                                        {
                                            let mut rl = remote_users.lock().await;
                                            if let Some(room) = rl.get_mut(&room_id)
                                                && let Some(channel_data) =
                                                    room.remove(&target_channel_id)
                                            {
                                                // Merge, don't replace: the target
                                                // name may already exist in the
                                                // remote view, and replacing it
                                                // would drop those users.
                                                let target =
                                                    room.entry(new_name_str.clone()).or_default();
                                                for (uid, status) in channel_data {
                                                    target.entry(uid).or_insert(status);
                                                }
                                            }
                                        }
                                        rename_remote_user_owners(
                                            &state.remote_user_owners,
                                            &room_id,
                                            &target_channel_id,
                                            &new_name_str,
                                        )
                                        .await;

                                        distributed_broadcast(
                                            &state,
                                            &DistributedMessage {
                                                msg_type: "rename-channel".into(),
                                                room_id: room_id.clone(),
                                                channel_id: target_channel_id.clone(),
                                                user_id: user_id.clone(),
                                                msg_id: Uuid::new_v4().to_string(),
                                                status: None,
                                                data: Some(
                                                    serde_json::json!({ "roomId": room_id, "oldName": target_channel_id, "newName": new_name_str }),
                                                ),
                                                signal_msg: None,
                                            },
                                        ).await;
                                        broadcast_channel_list(
                                            &rooms,
                                            &remote_users,
                                            &state.channel_creation_times,
                                            &room_id,
                                        )
                                        .await;
                                    }
                                }
                            }
                        } else if parsed.msg_type == "delete-channel" {
                            let mut target_channel_id = parsed
                                .data
                                .as_ref()
                                .and_then(|d| d.get("channelId"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();

                            if target_channel_id.eq_ignore_ascii_case("general") {
                                target_channel_id = "General".to_string();
                            }

                            if !target_channel_id.is_empty() && target_channel_id != "General" {
                                let mut rooms_lock = rooms.lock().await;

                                // Same semantics as rename: the channel may be
                                // hosted here, elsewhere, or both, and must be
                                // empty in every view we can see.
                                let can_delete = {
                                    let rl = remote_users.lock().await;
                                    let remote_room = rl.get(&room_id);
                                    let room = rooms_lock.get(&room_id);
                                    let times = state.channel_creation_times.lock().await;
                                    let metadata_room = times.get(&room_id);
                                    let local_channel =
                                        room.and_then(|r| r.get(&target_channel_id));
                                    let remote_channel =
                                        remote_room.and_then(|r| r.get(&target_channel_id));
                                    let exists_anywhere = local_channel.is_some()
                                        || remote_channel.is_some()
                                        || metadata_room.is_some_and(|channels| {
                                            channels.contains_key(&target_channel_id)
                                        });
                                    let local_ok = local_channel.is_none_or(HashMap::is_empty);
                                    let remote_ok = remote_channel.is_none_or(HashMap::is_empty);
                                    exists_anywhere && local_ok && remote_ok
                                };

                                if can_delete {
                                    if let Some(room) = rooms_lock.get_mut(&room_id) {
                                        room.remove(&target_channel_id);
                                    }
                                    drop(rooms_lock);

                                    {
                                        // This node won't process its own
                                        // broadcast, so clear the local remote
                                        // view of the deleted channel too.
                                        let mut rl = remote_users.lock().await;
                                        if let Some(room) = rl.get_mut(&room_id) {
                                            room.remove(&target_channel_id);
                                            if room.is_empty() {
                                                rl.remove(&room_id);
                                            }
                                        }
                                    }

                                    {
                                        let mut times = state.channel_creation_times.lock().await;
                                        if let Some(room_times) = times.get_mut(&room_id) {
                                            room_times.remove(&target_channel_id);
                                        }
                                    }
                                    remove_remote_channel_owners(
                                        &state.remote_user_owners,
                                        &room_id,
                                        &target_channel_id,
                                    )
                                    .await;

                                    distributed_broadcast(
                                        &state,
                                        &DistributedMessage {
                                            msg_type: "delete-channel".into(),
                                            room_id: room_id.clone(),
                                            channel_id: target_channel_id.clone(),
                                            user_id: user_id.clone(),
                                            msg_id: Uuid::new_v4().to_string(),
                                            status: None,
                                            data: None,
                                            signal_msg: None,
                                        },
                                    )
                                    .await;
                                    broadcast_channel_list(
                                        &rooms,
                                        &remote_users,
                                        &state.channel_creation_times,
                                        &room_id,
                                    )
                                    .await;
                                }
                            }
                        } else if let Some(ref target_id) = parsed.target
                            && matches!(parsed.msg_type.as_str(), "signal" | "identify")
                            && text.len() <= MAX_DISTRIBUTED_DATA_LEN
                        {
                            let mut found = false;
                            {
                                let rooms_lock = rooms.lock().await;
                                if let Some(room) = rooms_lock.get(&room_id)
                                    && let Some(channel) = room.get(&channel_id)
                                    && let Some((target_tx, _)) = channel.get(target_id)
                                {
                                    let mut forwarded_msg = parsed.clone();
                                    forwarded_msg.user_id = Some(user_id.clone());
                                    let forwarded_text =
                                        serde_json::to_string(&forwarded_msg).unwrap();
                                    let _ = target_tx
                                        .try_send(Ok(Message::Text(forwarded_text.into())));
                                    found = true;
                                }
                            }

                            if !found {
                                let is_remote = {
                                    let rl = remote_users.lock().await;
                                    rl.get(&room_id)
                                        .and_then(|r| r.get(&channel_id))
                                        .map(|c| c.contains_key(target_id))
                                        .unwrap_or(false)
                                };
                                if is_remote {
                                    let mut forwarded_msg = parsed.clone();
                                    forwarded_msg.user_id = Some(user_id.clone());
                                    let forwarded_text =
                                        serde_json::to_string(&forwarded_msg).unwrap();
                                    distributed_broadcast(
                                        &state,
                                        &DistributedMessage {
                                            msg_type: "signal".into(),
                                            room_id: room_id.clone(),
                                            channel_id: channel_id.clone(),
                                            user_id: user_id.clone(),
                                            msg_id: Uuid::new_v4().to_string(),
                                            status: None,
                                            data: None,
                                            signal_msg: Some(forwarded_text),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
            } else if let Message::Close(_) = msg {
                break;
            }
        } else {
            break;
        }
    }

    if is_pending_knock {
        let removed = {
            let mut lock = state.admin_rooms.lock().await;
            lock.get_mut(&room_id)
                .map(|a| {
                    a.approved.remove(&user_id);
                    a.pending.remove(&user_id).is_some()
                })
                .unwrap_or(false)
        };
        if removed {
            crate::admin::send_knock_update(&state, &room_id).await;
        }
    }

    // Stop the server-side ping task
    let _ = ping_shutdown_tx.send(());

    let mut actually_removed = false;
    let mut schedule_room_cleanup = false;
    // Unexpected drop of a joined connection: hold the seat instead
    // of announcing a departure. The participant's media is P2P and
    // keeps flowing; if they reconnect within the grace window the
    // call never notices. Explicit leaves skip the hold.
    let mut held_seat: Option<u64> = None;
    if is_joined && !intentional_leave {
        held_seat = hold_seat(&state, &room_id, &user_id, &tx).await;
    }
    if held_seat.is_none() {
    {
        let mut rooms_lock = rooms.lock().await;

        if is_joined && let Some(room) = rooms_lock.get_mut(&room_id) {
            let mut removed = false;

            if let Some(channel) = room.get_mut(&channel_id)
                && let Some((stored_tx, _)) = channel.get(&user_id)
                && stored_tx.same_channel(&tx)
            {
                channel.remove(&user_id);
                removed = true;

                if !channel.is_empty() {
                    let notify_msg = serde_json::to_string(&SignalMessage {
                        msg_type: "user-left".into(),
                        user_id: Some(user_id.clone()),
                        target: None,
                        data: None,
                    })
                    .unwrap();

                    for (_, (tx, _)) in channel.iter() {
                        let _ = tx.try_send(Ok(Message::Text(notify_msg.clone().into())));
                    }
                }
            }

            if !removed {
                for (_, channel) in room.iter_mut() {
                    if let Some((stored_tx, _)) = channel.get(&user_id)
                        && stored_tx.same_channel(&tx)
                    {
                        channel.remove(&user_id);
                        removed = true;

                        if !channel.is_empty() {
                            let notify_msg = serde_json::to_string(&SignalMessage {
                                msg_type: "user-left".into(),
                                user_id: Some(user_id.clone()),
                                target: None,
                                data: None,
                            })
                            .unwrap();

                            for (_, (tx, _)) in channel.iter() {
                                let _ = tx.try_send(Ok(Message::Text(notify_msg.clone().into())));
                            }
                        }
                        break;
                    }
                }
            }

            if removed {
                actually_removed = true;
                schedule_room_cleanup = room.values().all(|c| c.is_empty());
            }
        }
    }
    }

    if schedule_room_cleanup {
        let has_remote = remote_users
            .lock()
            .await
            .get(&room_id)
            .map(|r| r.values().any(|c| !c.is_empty()))
            .unwrap_or(false);
        if has_remote {
            schedule_room_cleanup = false;
        }
    }

    if is_joined && actually_removed {
        distributed_broadcast(
            &state,
            &DistributedMessage {
                msg_type: "user-left".into(),
                room_id: room_id.clone(),
                channel_id: channel_id.clone(),
                user_id: user_id.clone(),
                msg_id: Uuid::new_v4().to_string(),
                status: None,
                data: None,
                signal_msg: None,
            },
        )
        .await;
    }

    if schedule_room_cleanup {
        on_room_maybe_empty(&state, &room_id).await;
    }

    if let Some(generation) = held_seat {
        let state2 = state.clone();
        let room2 = room_id.clone();
        let user2 = user_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(
                crate::admin::resume_grace_secs(),
            ))
            .await;
            finalize_held_seat(&state2, &room2, &user2, Some(generation), true).await;
        });
    }
    broadcast_channel_list(
        &rooms,
        &remote_users,
        &state.channel_creation_times,
        &room_id,
    )
    .await;
}

pub(crate) async fn channel_status(
    Path((room_id, channel_id)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Some(ref allowed_url) = state.allowed_url
        && !host_is_allowed(&headers, allowed_url)
    {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let mut channel_id = channel_id;
    if channel_id.eq_ignore_ascii_case("general") {
        channel_id = "General".to_string();
    }
    if !is_valid_room_id(&room_id) {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    let Some(channel_id) = normalize_channel_id(&channel_id) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };
    let rooms_lock = state.rooms.lock().await;
    let remote_lock = state.remote_users.lock().await;
    let times_lock = state.channel_creation_times.lock().await;

    let mut users_map = HashMap::new();

    if let Some(room) = rooms_lock.get(&room_id)
        && let Some(channel) = room.get(&channel_id)
    {
        for (uid, (_, status)) in channel.iter() {
            users_map.insert(uid.clone(), status.clone());
        }
    }

    if let Some(remote_room) = remote_lock.get(&room_id)
        && let Some(remote_channel) = remote_room.get(&channel_id)
    {
        for (uid, status) in remote_channel.iter() {
            users_map.insert(uid.clone(), status.clone());
        }
    }

    let created_at = times_lock
        .get(&room_id)
        .and_then(|t| t.get(&channel_id))
        .copied()
        .unwrap_or(0);
    drop(times_lock);
    drop(remote_lock);
    drop(rooms_lock);

    axum::Json(RoomStatus {
        name: channel_id,
        users: users_map,
        created_at,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_configured_password_never_blocks_room_creation() {
        assert!(!room_creation_needs_password(None, false, false, None));
        assert!(!room_creation_needs_password(
            None,
            false,
            false,
            Some("anything")
        ));
        assert!(!room_creation_needs_password(None, true, false, None));
    }

    #[test]
    fn new_room_requires_matching_password() {
        assert!(room_creation_needs_password(
            Some("hunter2"),
            false,
            false,
            None
        ));
        assert!(room_creation_needs_password(
            Some("hunter2"),
            false,
            false,
            Some("wrong")
        ));
        assert!(!room_creation_needs_password(
            Some("hunter2"),
            false,
            false,
            Some("hunter2")
        ));
    }

    #[test]
    fn existing_local_room_skips_password_requirement() {
        assert!(!room_creation_needs_password(
            Some("hunter2"),
            true,
            false,
            None
        ));
        assert!(!room_creation_needs_password(
            Some("hunter2"),
            true,
            false,
            Some("wrong")
        ));
    }

    #[test]
    fn existing_remote_room_skips_password_requirement() {
        assert!(!room_creation_needs_password(
            Some("hunter2"),
            false,
            true,
            None
        ));
        assert!(!room_creation_needs_password(
            Some("hunter2"),
            true,
            true,
            Some("wrong")
        ));
    }
}

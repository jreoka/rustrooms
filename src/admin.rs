use crate::distributed::distributed_broadcast;
use crate::state::{
    AppState, DistributedMessage, PersistedState, ResumeRecord, RoomAdmin, SignalMessage, UserTx,
    current_unix_secs,
};
use axum::extract::ws::Message;
use hmac::{Hmac, Mac, digest::KeyInit};
use sha2::Sha256;
use std::path::PathBuf;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// A creator key is 32 random bytes, hex-encoded. The server never
// stores the key itself, only its HMAC under the boot secret bound to
// the room id, so a key cannot be replayed into a different room and
// dies with both the room state and the server process.
pub(crate) fn issue_key() -> String {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    hex(&bytes)
}

pub(crate) fn key_hash(secret: &[u8], room_id: &str, key: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(b"key:v1:");
    mac.update(room_id.as_bytes());
    mac.update(b"\0");
    mac.update(key.as_bytes());
    hex(&mac.finalize().into_bytes())
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub(crate) fn verify_key(secret: &[u8], room_id: &str, key: &str, stored_hash: &str) -> bool {
    if key.is_empty() || key.len() > 256 {
        return false;
    }
    let candidate = key_hash(secret, room_id, key);
    if constant_time_eq(candidate.as_bytes(), stored_hash.as_bytes()) {
        return true;
    }
    // Backward compatibility for persisted admin state created before v1 domain-separated hashing.
    let legacy = legacy_key_hash(secret, room_id, key);
    constant_time_eq(legacy.as_bytes(), stored_hash.as_bytes())
}

fn legacy_key_hash(secret: &[u8], room_id: &str, key: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(room_id.as_bytes());
    mac.update(b".");
    mac.update(key.as_bytes());
    hex(&mac.finalize().into_bytes())
}

// ---- Room admin messaging helpers (used by rooms.rs and distributed.rs) ----

pub(crate) fn signal_text(msg_type: &str, user_id: Option<String>, data: Option<serde_json::Value>) -> String {
    serde_json::to_string(&SignalMessage {
        msg_type: msg_type.to_string(),
        user_id,
        target: None,
        data,
    })
    .unwrap()
}

pub(crate) fn error_text(code: &str, message: &str) -> String {
    signal_text(
        "error",
        None,
        Some(serde_json::json!({ "code": code, "message": message })),
    )
}

// Sends the room's public admin state to one connection (no fan-out).
pub(crate) async fn send_admin_update_to(state: &AppState, room_id: &str, tx: &crate::state::UserTx) {
    let payload = {
        let lock = state.admin_rooms.lock().await;
        lock.get(room_id).map(|a| a.public_state())
    };
    if let Some(payload) = payload {
        let _ = tx.try_send(Ok(Message::Text(
            signal_text("admin-update", None, Some(payload)).into(),
        )));
    }
}

// Sends the room's public admin state to every local member, and
// publishes it so distributed nodes can shadow-sync and render it.
pub(crate) async fn broadcast_admin_update(state: &AppState, room_id: &str) {
    let payload = {
        let lock = state.admin_rooms.lock().await;
        lock.get(room_id).map(|a| a.public_state())
    };
    let Some(payload) = payload else { return };
    let text = signal_text("admin-update", None, Some(payload.clone()));
    {
        let rooms_lock = state.rooms.lock().await;
        if let Some(room) = rooms_lock.get(room_id) {
            for (_, channel) in room.iter() {
                for (_, (tx, _)) in channel.iter() {
                    let _ = tx.try_send(Ok(Message::Text(text.clone().into())));
                }
            }
        }
    }
    persist_state(state).await;
    let mut dist_data = payload;
    {
        let lock = state.admin_rooms.lock().await;
        if let Some(hash) = lock.get(room_id).and_then(|a| a.key_hash.clone()) {
            dist_data["keyHash"] = serde_json::json!(hash);
        }
    }
    distributed_broadcast(
        state,
        &DistributedMessage {
            msg_type: "admin-update".into(),
            room_id: room_id.to_string(),
            channel_id: String::new(),
            user_id: String::new(),
            msg_id: Uuid::new_v4().to_string(),
            status: None,
            data: Some(dist_data),
            signal_msg: None,
        },
    )
    .await;
}

pub(crate) fn merged_pending_knocks(admin: &RoomAdmin) -> Vec<serde_json::Value> {
    let mut seen = std::collections::HashSet::new();
    let mut all = Vec::new();
    for (id, p) in &admin.pending {
        if seen.insert(id.clone()) {
            all.push(serde_json::json!({
                "id": id,
                "nickname": p.nickname,
                "channel": p.channel,
            }));
        }
    }
    for remote_list in admin.remote_pending.values() {
        for item in remote_list {
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                if seen.insert(id.to_string()) {
                    all.push(item.clone());
                }
            } else {
                all.push(item.clone());
            }
        }
    }
    all
}

pub(crate) async fn notify_local_admins_knock_update(state: &AppState, room_id: &str) {
    let (merged, admin_ids) = {
        let lock = state.admin_rooms.lock().await;
        match lock.get(room_id) {
            Some(a) => (merged_pending_knocks(a), a.admins.clone()),
            None => return,
        }
    };
    if admin_ids.is_empty() {
        return;
    }
    let text = signal_text(
        "knock-update",
        None,
        Some(serde_json::json!({ "pending": merged })),
    );
    let rooms_lock = state.rooms.lock().await;
    if let Some(room) = rooms_lock.get(room_id) {
        for (_, channel) in room.iter() {
            for (uid, (tx, _)) in channel.iter() {
                if admin_ids.contains(uid) {
                    let _ = tx.try_send(Ok(Message::Text(text.clone().into())));
                }
            }
        }
    }
}

// The pending knock list goes to admins only. In distributed mode each
// node publishes its own pending list; approvals route back through
// "knock-response" distributed messages.
pub(crate) async fn send_knock_update(state: &AppState, room_id: &str) {
    let local_pending = {
        let lock = state.admin_rooms.lock().await;
        match lock.get(room_id) {
            Some(a) => a
                .pending
                .iter()
                .map(|(id, p)| {
                    serde_json::json!({ "id": id, "nickname": p.nickname, "channel": p.channel })
                })
                .collect::<Vec<_>>(),
            None => return,
        }
    };
    notify_local_admins_knock_update(state, room_id).await;
    let data = serde_json::json!({ "pending": local_pending });
    distributed_broadcast(
        state,
        &DistributedMessage {
            msg_type: "knock-update".into(),
            room_id: room_id.to_string(),
            channel_id: String::new(),
            user_id: String::new(),
            msg_id: Uuid::new_v4().to_string(),
            status: None,
            data: Some(data),
            signal_msg: None,
        },
    )
    .await;
}

/// Drain parked lobby knockers when lobby mode turns off, moving them to `approved`.
pub(crate) fn drain_pending_knocks(admin_state: &mut RoomAdmin) -> Vec<UserTx> {
    let mut approved_ptxs = Vec::with_capacity(admin_state.pending.len());
    for (target_uid, pending) in admin_state.pending.drain() {
        admin_state.approved.insert(target_uid);
        approved_ptxs.push(pending.tx);
    }
    admin_state.remote_pending.clear();
    approved_ptxs
}

/// Notify drained lobby knockers that they may now complete their join.
pub(crate) fn notify_approved_knocks(approved_ptxs: Vec<UserTx>) {
    if approved_ptxs.is_empty() {
        return;
    }
    let msg = signal_text("knock-approved", None, None);
    for ptx in approved_ptxs {
        let _ = ptx.try_send(Ok(Message::Text(msg.clone().into())));
    }
}

// ---- Persistence (restarts / rollouts keep admin state) ----

pub(crate) fn state_file_path() -> PathBuf {
    std::env::var("RUSTROOMS_STATE_FILE")
        .ok()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rustrooms_state.json"))
}

pub(crate) fn load_persisted() -> Option<PersistedState> {
    let raw = std::fs::read_to_string(state_file_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

// Atomic snapshot: write a temp file (0600, it carries the signing
// secret) and rename it over the state file.
pub(crate) async fn persist_state(state: &AppState) {
    if cfg!(test) && std::env::var_os("RUSTROOMS_STATE_FILE").is_none() {
        return;
    }
    static PERSIST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _persist_guard = PERSIST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let rooms = {
        let lock = state.admin_rooms.lock().await;
        lock.iter()
            .filter(|(_, a)| a.is_meaningful())
            .map(|(id, a)| (id.clone(), a.to_persisted()))
            .collect()
    };
    let now = current_unix_secs();
    let resume: std::collections::HashMap<String, std::collections::HashMap<String, ResumeRecord>> = {
        let lock = state.resume_records.lock().await;
        let mut out: std::collections::HashMap<
            String,
            std::collections::HashMap<String, ResumeRecord>,
        > = std::collections::HashMap::new();
        for ((room, user), rec) in lock.iter() {
            if rec.expires_at > now {
                out.entry(room.clone())
                    .or_default()
                    .insert(user.clone(), rec.clone());
            }
        }
        out
    };
    let data = PersistedState {
        secret: (*state.server_secret).clone(),
        rooms,
        resume,
    };
    let Ok(json) = serde_json::to_string(&data) else {
        return;
    };
    let path = state_file_path();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && let Err(err) = tokio::fs::create_dir_all(parent).await
    {
        eprintln!(
            "Failed to create state directory {}: {err}",
            parent.display()
        );
        return;
    }
    let tmp = path.with_extension("json.tmp");
    if let Err(err) = tokio::fs::write(&tmp, json).await {
        eprintln!("Failed to write state temp file {}: {err}", tmp.display());
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await;
    }
    if let Err(err) = tokio::fs::rename(&tmp, &path).await {
        eprintln!(
            "Failed to atomically replace state file {}: {err}",
            path.display()
        );
    }
}

// ---- Resume tokens (seamless reconnects / restarts / cross-node failover) ----
//
// Issued at every successful join and rotated each time. Presenting
// a live token for the same room+channel proves this connection is
// the same participant returning: the join bypasses the lobby and,
// if the seat is still held, announces nothing at all. Only the
// token's SHA-256 is stored in the state file, while the token itself
// carries an HMAC signature under the shared server secret so another
// node in a P2P cluster can verify it without a central database.

pub(crate) const RESUME_TOKEN_TTL_SECS: u64 = 7200;
const REVOKED_TOKEN_HASH: &str = "revoked";

pub(crate) fn resume_grace_secs() -> u64 {
    std::env::var("RUSTROOMS_RESUME_GRACE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| (1..=600).contains(v))
        .unwrap_or(15)
}

fn resume_token_hash(token: &str) -> String {
    use sha2::Digest;
    hex(&sha2::Sha256::digest(token.as_bytes()))
}

fn sign_resume_token(
    secret: &[u8],
    room_id: &str,
    user_id: &str,
    channel_id: &str,
    expires_at: u64,
    nonce: &str,
) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(b"resume:v1:");
    mac.update(room_id.as_bytes());
    mac.update(b"\0");
    mac.update(user_id.as_bytes());
    mac.update(b"\0");
    mac.update(channel_id.as_bytes());
    mac.update(b"\0");
    mac.update(expires_at.to_string().as_bytes());
    mac.update(b"\0");
    mac.update(nonce.as_bytes());
    hex(&mac.finalize().into_bytes())
}

fn verify_signed_resume_token(
    secret: &[u8],
    room_id: &str,
    user_id: &str,
    channel_id: &str,
    token: &str,
    now: u64,
) -> Option<u64> {
    let mut parts = token.splitn(3, '.');
    let expires_hex = parts.next()?;
    let nonce = parts.next()?;
    let sig = parts.next()?;
    if nonce.is_empty() || nonce.len() > 128 || sig.is_empty() {
        return None;
    }
    let expires_at = u64::from_str_radix(expires_hex, 16).ok()?;
    if expires_at <= now || expires_at > now.saturating_add(RESUME_TOKEN_TTL_SECS + 60) {
        return None;
    }
    let expected = sign_resume_token(secret, room_id, user_id, channel_id, expires_at, nonce);
    constant_time_eq(expected.as_bytes(), sig.as_bytes()).then_some(expires_at)
}

pub(crate) async fn issue_resume_token(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    channel_id: &str,
) -> String {
    let expires_at = current_unix_secs() + RESUME_TOKEN_TTL_SECS;
    let nonce = issue_key();
    let sig = sign_resume_token(
        &state.server_secret,
        room_id,
        user_id,
        channel_id,
        expires_at,
        &nonce,
    );
    let token = format!("{expires_at:x}.{nonce}.{sig}");
    let record = ResumeRecord {
        token_hash: resume_token_hash(&token),
        channel_id: channel_id.to_string(),
        expires_at,
    };
    state
        .resume_records
        .lock()
        .await
        .insert((room_id.to_string(), user_id.to_string()), record);
    persist_state(state).await;
    token
}

pub(crate) async fn validate_resume_token(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    channel_id: &str,
    token: Option<&str>,
) -> bool {
    let Some(token) = token.filter(|t| !t.is_empty() && t.len() <= 256) else {
        return false;
    };
    {
        let admins = state.admin_rooms.lock().await;
        if admins
            .get(room_id)
            .is_some_and(|a| a.banned.contains(user_id))
        {
            return false;
        }
    }
    let hash = resume_token_hash(token);
    let now = current_unix_secs();
    {
        let lock = state.resume_records.lock().await;
        if let Some(rec) = lock.get(&(room_id.to_string(), user_id.to_string())) {
            if rec.token_hash == REVOKED_TOKEN_HASH && rec.expires_at > now {
                return false;
            }
            return rec.token_hash == hash && rec.channel_id == channel_id && rec.expires_at > now;
        }
    }
    if let Some(expires_at) = verify_signed_resume_token(
        &state.server_secret,
        room_id,
        user_id,
        channel_id,
        token,
        now,
    ) {
        state.resume_records.lock().await.insert(
            (room_id.to_string(), user_id.to_string()),
            ResumeRecord {
                token_hash: hash,
                channel_id: channel_id.to_string(),
                expires_at,
            },
        );
        return true;
    }
    false
}

// Validates that `token` is a live, unrevoked resume token for `(room_id, user_id)`
// in *any* channel of `room_id` (used when switching channels inside a room so
// existing room members aren't re-parked in the lobby).
pub(crate) async fn validate_room_resume_token(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    channel_id: &str,
    token: Option<&str>,
) -> bool {
    if validate_resume_token(state, room_id, user_id, channel_id, token).await {
        return true;
    }
    let Some(token) = token.filter(|t| !t.is_empty() && t.len() <= 256) else {
        return false;
    };
    {
        let admins = state.admin_rooms.lock().await;
        if admins
            .get(room_id)
            .is_some_and(|a| a.banned.contains(user_id))
        {
            return false;
        }
    }
    let hash = resume_token_hash(token);
    let now = current_unix_secs();
    {
        let lock = state.resume_records.lock().await;
        if let Some(rec) = lock.get(&(room_id.to_string(), user_id.to_string())) {
            if rec.token_hash == REVOKED_TOKEN_HASH && rec.expires_at > now {
                return false;
            }
            return rec.token_hash == hash && rec.expires_at > now;
        }
    }
    // Check known channels in the room (for cross-node channel switches)
    let mut candidate_channels: Vec<String> = vec!["General".to_string()];
    {
        let rooms = state.rooms.lock().await;
        if let Some(room) = rooms.get(room_id) {
            for ch in room.keys() {
                if !candidate_channels.contains(ch) {
                    candidate_channels.push(ch.clone());
                }
            }
        }
    }
    {
        let remote = state.remote_users.lock().await;
        if let Some(room) = remote.get(room_id) {
            for ch in room.keys() {
                if !candidate_channels.contains(ch) {
                    candidate_channels.push(ch.clone());
                }
            }
        }
    }
    {
        let times = state.channel_creation_times.lock().await;
        if let Some(channels) = times.get(room_id) {
            for ch in channels.keys() {
                if !candidate_channels.contains(ch) {
                    candidate_channels.push(ch.clone());
                }
            }
        }
    }
    for ch in candidate_channels {
        if verify_signed_resume_token(&state.server_secret, room_id, user_id, &ch, token, now)
            .is_some()
        {
            return true;
        }
    }
    false
}

pub(crate) async fn delete_resume_token(state: &AppState, room_id: &str, user_id: &str) {
    state.resume_records.lock().await.insert(
        (room_id.to_string(), user_id.to_string()),
        ResumeRecord {
            token_hash: REVOKED_TOKEN_HASH.to_string(),
            channel_id: String::new(),
            expires_at: current_unix_secs() + RESUME_TOKEN_TTL_SECS,
        },
    );
    persist_state(state).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-secret-test-secret-32byte";

    #[test]
    fn issued_key_verifies_for_its_room() {
        let key = issue_key();
        let hash = key_hash(SECRET, "room-a", &key);
        assert!(verify_key(SECRET, "room-a", &key, &hash));
    }

    #[test]
    fn key_does_not_verify_for_another_room() {
        let key = issue_key();
        let hash = key_hash(SECRET, "room-a", &key);
        assert!(!verify_key(SECRET, "room-b", &key, &hash));
    }

    #[test]
    fn wrong_key_and_wrong_secret_fail() {
        let key = issue_key();
        let hash = key_hash(SECRET, "room-a", &key);
        assert!(!verify_key(SECRET, "room-a", &issue_key(), &hash));
        assert!(!verify_key(b"other-secret-other-secret-32by", "room-a", &key, &hash));
        assert!(!verify_key(SECRET, "room-a", "", &hash));
    }

    #[test]
    fn legacy_key_hash_verifies_for_backward_compatibility() {
        let key = issue_key();
        let legacy_hash = legacy_key_hash(SECRET, "room-a", &key);
        assert!(verify_key(SECRET, "room-a", &key, &legacy_hash));
        assert!(!verify_key(SECRET, "room-b", &key, &legacy_hash));
    }

    #[test]
    fn issued_keys_are_unique() {
        assert_ne!(issue_key(), issue_key());
    }

    #[tokio::test]
    async fn validate_room_resume_token_allows_channel_switch() {
        let state = AppState {
            rooms: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            room_cleanup_generations: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            room_creation_password: None,
            distributed_tx: tokio::sync::broadcast::channel(16).0,
            remote_users: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            remote_user_owners: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            channel_creation_times: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            allowed_url: None,
            recent_distributed_msg_ids: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
            distributed_msg_history: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
            node_id: uuid::Uuid::new_v4().to_string(),
            admin_rooms: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            cluster_key: None,
            p2p_runtime: None,
            resume_records: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            away_seats: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            departure_gen: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
            server_secret: std::sync::Arc::new(vec![7u8; 32]),
        };

        let room_id = "test-room";
        let user_id = uuid::Uuid::new_v4().to_string();
        let token = issue_resume_token(&state, room_id, &user_id, "General").await;

        // Validate directly in same channel
        assert!(validate_resume_token(&state, room_id, &user_id, "General", Some(&token)).await);

        // Validate when switching to a different channel in the same room
        assert!(validate_room_resume_token(&state, room_id, &user_id, "Lounge", Some(&token)).await);

        // Revoke token and verify both reject
        delete_resume_token(&state, room_id, &user_id).await;
        assert!(!validate_resume_token(&state, room_id, &user_id, "General", Some(&token)).await);
        assert!(!validate_room_resume_token(&state, room_id, &user_id, "Lounge", Some(&token)).await);
    }

    #[tokio::test]
    async fn toggle_lobby_updates_admin_state() {
        let mut admin = RoomAdmin::default();
        let admin_id = "admin-1".to_string();
        admin.admins.insert(admin_id.clone());
        assert!(!admin.lobby);

        admin.lobby = true;
        assert!(admin.lobby);
        assert!(admin.is_admin(&admin_id));
        assert!(!admin.is_admin("non-admin"));
    }
}

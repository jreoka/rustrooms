use hmac::{Hmac, Mac, digest::KeyInit};
use sha2::Sha256;
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
    mac.update(room_id.as_bytes());
    mac.update(b".");
    mac.update(key.as_bytes());
    hex(&mac.finalize().into_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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
    constant_time_eq(candidate.as_bytes(), stored_hash.as_bytes())
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
    fn issued_keys_are_unique() {
        assert_ne!(issue_key(), issue_key());
    }
}

// ---- Room admin messaging helpers (used by rooms.rs and distributed.rs) ----

use crate::distributed::distributed_broadcast;
use crate::state::{AppState, DistributedMessage, ResumeRecord, RoomAdmin, SignalMessage};
use axum::extract::ws::Message;

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
    store_admin_redis(state, room_id).await;
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

// The pending knock list goes to admins only. In distributed mode each
// node publishes its own pending list; approvals route back through
// "knock-response" distributed messages.
pub(crate) async fn send_knock_update(state: &AppState, room_id: &str) {
    let (pending, admin_ids) = {
        let lock = state.admin_rooms.lock().await;
        match lock.get(room_id) {
            Some(a) => (
                a.pending
                    .iter()
                    .map(|(id, p)| serde_json::json!({ "id": id, "nickname": p.nickname }))
                    .collect::<Vec<_>>(),
                a.admins.clone(),
            ),
            None => return,
        }
    };
    let data = serde_json::json!({ "pending": pending });
    let text = signal_text("knock-update", None, Some(data.clone()));
    {
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

// ---- Persistence (restarts / rollouts keep admin state) ----

use crate::state::PersistedState;
use std::path::PathBuf;

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
    if state.admin_redis.is_some() {
        return; // Redis is the shared store in distributed mode.
    }
    let rooms = {
        let lock = state.admin_rooms.lock().await;
        lock.iter()
            .filter(|(_, a)| a.is_meaningful())
            .map(|(id, a)| (id.clone(), a.to_persisted()))
            .collect()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
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
    let tmp = path.with_extension("json.tmp");
    if tokio::fs::write(&tmp, json).await.is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await;
        }
        let _ = tokio::fs::rename(&tmp, &path).await;
    }
}

// ---- Redis admin-state KV (distributed mode source of truth) ----
//
// The pub/sub bus syncs live changes, but a node that has never seen
// a room (first join there, or a fresh node mid-call) needs the
// current state on demand. It lives in one hash per room next to the
// presence data: {prefix}:admin:{room_id}. Writes happen on every
// admin change (the broadcast_admin_update choke point), reads on
// the join gate, deletes on room expiry. A long TTL refreshed on
// read/write bounds leaks from hard crashes; live nodes always hold
// the state in memory and re-store it on the next change.

use crate::state::RoomKind;
use redis::AsyncCommands;

const ADMIN_REDIS_TTL_SECS: i64 = 30 * 24 * 3600;

fn admin_redis_key(state: &AppState, room_id: &str) -> String {
    format!("{}:admin:{}", state.redis_prefix, room_id)
}

pub(crate) async fn store_admin_redis(state: &AppState, room_id: &str) {
    let Some(template) = state.admin_redis.clone() else {
        return;
    };
    let entry = {
        let lock = state.admin_rooms.lock().await;
        lock.get(room_id)
            .filter(|a| a.is_meaningful())
            .map(|a| a.to_persisted())
    };
    let mut conn = template;
    let key = admin_redis_key(state, room_id);
    match entry {
        Some(p) => {
            let mut fields: Vec<(String, String)> = vec![
                ("creator_id".into(), p.creator_id.unwrap_or_default()),
                (
                    "admins".into(),
                    serde_json::to_string(&p.admins).unwrap_or_default(),
                ),
                (
                    "banned".into(),
                    serde_json::to_string(&p.banned).unwrap_or_default(),
                ),
                ("lobby".into(), if p.lobby { "1".into() } else { "0".into() }),
                (
                    "kind".into(),
                    match p.kind {
                        RoomKind::Stage => "stage".into(),
                        RoomKind::Regular => "regular".into(),
                    },
                ),
                (
                    "speakers".into(),
                    serde_json::to_string(&p.speakers).unwrap_or_default(),
                ),
                (
                    "forced".into(),
                    serde_json::to_string(&p.forced).unwrap_or_default(),
                ),
            ];
            if let Some(hash) = p.key_hash {
                fields.push(("key_hash".into(), hash));
            }
            let _: redis::RedisResult<()> = conn.hset_multiple(&key, &fields).await;
            let _: redis::RedisResult<()> = conn.expire(&key, ADMIN_REDIS_TTL_SECS).await;
        }
        None => {
            let _: redis::RedisResult<()> = conn.del(&key).await;
        }
    }
}

// Materialize the shared state for a room into this node's memory.
// No-op when the room is already known locally or Redis has nothing;
// Redis failures degrade to today's node-local behavior, never to a
// blocked join.
pub(crate) async fn load_admin_redis(state: &AppState, room_id: &str) {
    if state.admin_rooms.lock().await.contains_key(room_id) {
        return;
    }
    let Some(template) = state.admin_redis.clone() else {
        return;
    };
    let mut conn = template;
    let key = admin_redis_key(state, room_id);
    let map: std::collections::HashMap<String, String> =
        conn.hgetall(&key).await.unwrap_or_default();
    if map.is_empty() {
        return;
    }
    let get = |k: &str| map.get(k).map(|v| v.as_str()).unwrap_or("");
    let nonempty = |v: &str| {
        if v.is_empty() {
            None
        } else {
            Some(v.to_string())
        }
    };
    let parse_set = |k: &str| serde_json::from_str(get(k)).unwrap_or_default();
    let entry = RoomAdmin {
        key_hash: nonempty(get("key_hash")),
        creator_id: nonempty(get("creator_id")),
        admins: parse_set("admins"),
        banned: parse_set("banned"),
        lobby: get("lobby") == "1",
        kind: RoomKind::from_str(get("kind")),
        speakers: parse_set("speakers"),
        forced: serde_json::from_str(get("forced")).unwrap_or_default(),
        pending: std::collections::HashMap::new(),
        approved: std::collections::HashSet::new(),
    };
    state
        .admin_rooms
        .lock()
        .await
        .insert(room_id.to_string(), entry);
    let _: redis::RedisResult<()> = conn.expire(&key, ADMIN_REDIS_TTL_SECS).await;
}

pub(crate) async fn delete_admin_redis(state: &AppState, room_id: &str) {
    let Some(template) = state.admin_redis.clone() else {
        return;
    };
    let mut conn = template;
    let _: redis::RedisResult<()> = conn.del(admin_redis_key(state, room_id)).await;
}

// ---- Resume tokens (seamless reconnects / restarts) ----
//
// Issued at every successful join and rotated each time. Presenting
// a live token for the same room+channel proves this connection is
// the same participant returning: the join bypasses the lobby and,
// if the seat is still held, announces nothing at all. Only the
// token's SHA-256 is stored (here, in Redis, and in the state
// file), so a leaked store does not hand out live sessions.

pub(crate) const RESUME_TOKEN_TTL_SECS: u64 = 300;

pub(crate) fn resume_grace_secs() -> u64 {
    std::env::var("RUSTROOMS_RESUME_GRACE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| (1..=600).contains(v))
        .unwrap_or(45)
}

pub(crate) fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn resume_token_hash(token: &str) -> String {
    use sha2::Digest;
    hex(&sha2::Sha256::digest(token.as_bytes()))
}

fn resume_redis_key(state: &AppState, room_id: &str, user_id: &str) -> String {
    format!("{}:resume:{}:{}", state.redis_prefix, room_id, user_id)
}

pub(crate) async fn issue_resume_token(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    channel_id: &str,
) -> String {
    let token = issue_key();
    let record = ResumeRecord {
        token_hash: resume_token_hash(&token),
        channel_id: channel_id.to_string(),
        expires_at: current_unix_secs() + RESUME_TOKEN_TTL_SECS,
    };
    state
        .resume_records
        .lock()
        .await
        .insert((room_id.to_string(), user_id.to_string()), record.clone());
    if let Some(template) = state.admin_redis.clone() {
        use redis::AsyncCommands;
        let mut conn = template;
        if let Ok(json) = serde_json::to_string(&record) {
            let _: redis::RedisResult<()> = conn
                .set_ex(
                    resume_redis_key(state, room_id, user_id),
                    json,
                    RESUME_TOKEN_TTL_SECS,
                )
                .await;
        }
    } else {
        persist_state(state).await;
    }
    token
}

pub(crate) async fn validate_resume_token(
    state: &AppState,
    room_id: &str,
    user_id: &str,
    channel_id: &str,
    token: Option<&str>,
) -> bool {
    let Some(token) = token.filter(|t| !t.is_empty()) else {
        return false;
    };
    let hash = resume_token_hash(token);
    let now = current_unix_secs();
    let matches = |rec: &ResumeRecord| {
        rec.token_hash == hash && rec.channel_id == channel_id && rec.expires_at > now
    };
    {
        let lock = state.resume_records.lock().await;
        if let Some(rec) = lock.get(&(room_id.to_string(), user_id.to_string())) {
            return matches(rec);
        }
    }
    if let Some(template) = state.admin_redis.clone() {
        use redis::AsyncCommands;
        let mut conn = template;
        let raw: Option<String> = conn
            .get(resume_redis_key(state, room_id, user_id))
            .await
            .ok()
            .flatten();
        if let Some(raw) = raw
            && let Ok(rec) = serde_json::from_str::<ResumeRecord>(&raw)
        {
            let ok = matches(&rec);
            if ok {
                state
                    .resume_records
                    .lock()
                    .await
                    .insert((room_id.to_string(), user_id.to_string()), rec);
            }
            return ok;
        }
    }
    false
}

pub(crate) async fn delete_resume_token(state: &AppState, room_id: &str, user_id: &str) {
    state
        .resume_records
        .lock()
        .await
        .remove(&(room_id.to_string(), user_id.to_string()));
    if let Some(template) = state.admin_redis.clone() {
        use redis::AsyncCommands;
        let mut conn = template;
        let _: redis::RedisResult<()> =
            conn.del(resume_redis_key(state, room_id, user_id)).await;
    }
}

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
use crate::state::{AppState, DistributedMessage, SignalMessage};
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
    persist_state(state).await;
    distributed_broadcast(
        state,
        &DistributedMessage {
            msg_type: "admin-update".into(),
            room_id: room_id.to_string(),
            channel_id: String::new(),
            user_id: String::new(),
            msg_id: Uuid::new_v4().to_string(),
            status: None,
            data: Some(payload),
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
    let rooms = {
        let lock = state.admin_rooms.lock().await;
        lock.iter()
            .filter(|(_, a)| a.is_meaningful())
            .map(|(id, a)| (id.clone(), a.to_persisted()))
            .collect()
    };
    let data = PersistedState {
        secret: (*state.server_secret).clone(),
        rooms,
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

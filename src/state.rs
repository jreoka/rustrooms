use axum::extract::ws::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::Mutex;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserStatus {
    pub nickname: String,
    pub avatar: Option<String>,
    pub is_gif: bool,
    pub static_frame: Option<String>,
    pub is_muted: bool,
    pub is_deafened: bool,
    pub is_screen_sharing: bool,
    #[serde(default)]
    pub is_low_bandwidth_mode: bool,
    #[serde(default)]
    pub is_mobile: bool,
    // Monotonic revision counter for the persisted profile (nickname, avatar,
    // modes). The client bumps it on every local save and sends it with join
    // and update-user; the server keeps the highest revision it has accepted
    // and echoes its own copy back, so a client whose local storage was lost
    // (e.g. iOS killing a frozen tab) can never silently clobber or drift
    // away from the server's authoritative profile.
    #[serde(default)]
    pub profile_rev: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RoomStatus {
    pub(crate) name: String,
    pub(crate) users: HashMap<String, UserStatus>,
    #[serde(default)]
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SignalMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub target: Option<String>,
    pub data: Option<serde_json::Value>,
    #[serde(rename = "userId")]
    pub user_id: Option<String>,
}

pub(crate) type UserTx = tokio::sync::mpsc::Sender<Result<Message, axum::Error>>;
pub(crate) type ChannelMap = HashMap<String, HashMap<String, (UserTx, UserStatus)>>;
pub(crate) type RoomMap = Arc<Mutex<HashMap<String, ChannelMap>>>;
pub(crate) type RoomCleanupMap = Arc<Mutex<HashMap<String, u64>>>;
pub(crate) type RemoteUsersMap =
    Arc<Mutex<HashMap<String, HashMap<String, HashMap<String, UserStatus>>>>>;
pub(crate) type RemoteUserKey = (String, String, String);
pub(crate) type RemoteUserOwnersMap = Arc<Mutex<HashMap<RemoteUserKey, String>>>;
pub(crate) type ChannelCreationTimesMap = Arc<Mutex<HashMap<String, HashMap<String, u64>>>>;
pub(crate) const ROOM_EMPTY_GRACE_SECS: u64 = 120;
pub(crate) const MAX_ROOM_ID_LEN: usize = 64;
pub(crate) const MAX_CHANNEL_ID_LEN: usize = 32;
pub(crate) const MAX_NICKNAME_LEN: usize = 32;
// Avatar data URLs are sent to every member of a channel and every distributed
// instance on join/update, so the cap must stay small to bound amplification.
// 2 MiB of base64 is roughly 1.5 MiB of raw image data.
pub(crate) const MAX_AVATAR_DATA_LEN: usize = 2 * 1024 * 1024;
pub(crate) const MAX_STATIC_FRAME_DATA_LEN: usize = 512 * 1024;
pub(crate) const CLIENT_WS_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
// Distributed join messages contain the profile in both status and signaling data.
pub(crate) const DISTRIBUTED_MAX_MESSAGE_SIZE: usize = 32 * 1024 * 1024;
pub(crate) const OUTBOUND_QUEUE_CAPACITY: usize = 32;
pub(crate) const DISTRIBUTED_BROADCAST_CAPACITY: usize = 256;
// Redis heartbeats expire hard-crashed nodes so they do not leave ghost users.
pub(crate) const REDIS_HEARTBEAT_SECS: u64 = 5;
pub(crate) const REDIS_NODE_TIMEOUT_SECS: u64 = 30;
pub(crate) const MESSAGE_RATE_WINDOW_SECS: u64 = 10;
pub(crate) const MAX_MESSAGES_PER_RATE_WINDOW: u32 = 240;
// Byte budget per rate window: bounds JSON parse work even for huge frames.
// Plenty for a join (avatar + static frame) plus profile updates and signaling.
pub(crate) const MAX_BYTES_PER_RATE_WINDOW: usize = 16 * 1024 * 1024;
// Cap for distributed payloads (signaling, cam/screen toggles, identify).
// Sized to fit a maximum-size avatar plus static frame.
pub(crate) const MAX_DISTRIBUTED_DATA_LEN: usize = 3 * 1024 * 1024;
pub(crate) const PROFILE_IMAGE_UPDATE_COOLDOWN_SECS: u64 = 5;
pub(crate) const JOIN_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DistributedMessage {
    #[serde(rename = "type")]
    pub(crate) msg_type: String,
    pub(crate) room_id: String,
    pub(crate) channel_id: String,
    pub(crate) user_id: String,
    pub(crate) msg_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<UserStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) signal_msg: Option<String>,
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) rooms: RoomMap,
    pub(crate) room_cleanup_generations: RoomCleanupMap,
    pub(crate) room_creation_password: Option<String>,
    pub(crate) distributed_tx: tokio::sync::broadcast::Sender<String>,
    pub(crate) remote_users: RemoteUsersMap,
    pub(crate) remote_user_owners: RemoteUserOwnersMap,
    pub(crate) channel_creation_times: ChannelCreationTimesMap,
    pub(crate) allowed_url: Option<String>,
    pub recent_distributed_msg_ids: Arc<Mutex<HashSet<String>>>,
    pub distributed_msg_history: Arc<Mutex<VecDeque<String>>>,
    pub(crate) node_id: String,
}

pub(crate) fn normalize_channel_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || matches!(trimmed, "." | "..")
        || trimmed.chars().count() > MAX_CHANNEL_ID_LEN
        || trimmed.chars().any(char::is_control)
        || trimmed.contains(['/', '\\'])
    {
        return None;
    }

    if trimmed.eq_ignore_ascii_case("general") {
        Some("General".to_string())
    } else {
        Some(trimmed.to_string())
    }
}

pub(crate) fn is_valid_room_id(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value.chars().count() <= MAX_ROOM_ID_LEN
        && !value.chars().any(char::is_control)
        && !value.contains(['/', '\\'])
}

pub(crate) fn normalize_user_id(value: Option<&str>) -> String {
    value
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
        .map(|id| id.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

pub(crate) fn unique_user_id(mut candidate: String, is_occupied: impl Fn(&str) -> bool) -> String {
    while is_occupied(&candidate) {
        candidate = uuid::Uuid::new_v4().to_string();
    }
    candidate
}

pub(crate) fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// Canonical nickname form: trimmed, capped, never empty. The server stores
// and broadcasts only normalized nicknames so every client renders the same
// value ("Guest" when unset) — an empty nickname can't mean different things
// to the owner and to everyone else.
pub(crate) fn normalize_nickname(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "Guest".to_string()
    } else {
        trimmed.chars().take(MAX_NICKNAME_LEN).collect()
    }
}

// Resolves the canonical status for a joining identity. If the server already
// holds a profile for it (a reconnect before the old socket's death is
// noticed), the stored copy wins unless the client provably saved something
// newer (profileRev). Otherwise a client that lost its local storage (frozen
// tab killed by iOS) would rejoin as "Guest" and clobber the profile everyone
// else still sees. Per-connection state (screen sharing) always comes from the
// new connection's payload.
pub(crate) fn reconcile_join_status(
    existing: Option<&UserStatus>,
    client_status: UserStatus,
) -> UserStatus {
    match existing {
        Some(prev) if prev.profile_rev >= client_status.profile_rev => UserStatus {
            nickname: normalize_nickname(&prev.nickname),
            avatar: prev.avatar.clone(),
            is_gif: prev.is_gif,
            static_frame: prev.static_frame.clone(),
            is_muted: prev.is_muted,
            is_deafened: prev.is_deafened,
            is_screen_sharing: client_status.is_screen_sharing,
            is_low_bandwidth_mode: prev.is_low_bandwidth_mode,
            is_mobile: client_status.is_mobile,
            profile_rev: prev.profile_rev,
        },
        _ => {
            let mut client_status = client_status;
            client_status.nickname = normalize_nickname(&client_status.nickname);
            client_status
        }
    }
}

pub(crate) fn normalize_configured_host(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let with_scheme = if value.contains("://") {
        value.to_string()
    } else {
        format!("http://{value}")
    };
    url::Url::parse(&with_scheme)
        .ok()?
        .host_str()
        .map(str::to_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_ids_are_trimmed_and_general_is_canonicalized() {
        assert_eq!(
            normalize_channel_id("  lounge  ").as_deref(),
            Some("lounge")
        );
        assert_eq!(normalize_channel_id("gEnErAl").as_deref(), Some("General"));
    }

    #[test]
    fn invalid_channel_ids_are_rejected() {
        assert!(normalize_channel_id("   ").is_none());
        assert!(normalize_channel_id("line\nbreak").is_none());
        assert!(normalize_channel_id("path/segment").is_none());
        assert!(normalize_channel_id(".").is_none());
        assert!(normalize_channel_id("..").is_none());
        assert!(normalize_channel_id(&"a".repeat(MAX_CHANNEL_ID_LEN + 1)).is_none());
    }

    #[test]
    fn dot_segment_room_ids_are_rejected() {
        assert!(!is_valid_room_id("."));
        assert!(!is_valid_room_id(".."));
    }

    #[test]
    fn invalid_user_ids_are_replaced_with_uuids() {
        let normalized = normalize_user_id(Some("not-a-uuid"));
        assert!(uuid::Uuid::parse_str(&normalized).is_ok());

        let original = uuid::Uuid::new_v4();
        assert_eq!(
            normalize_user_id(Some(&original.to_string())),
            original.to_string()
        );
    }

    #[test]
    fn occupied_user_ids_are_replaced_without_reusing_the_active_identity() {
        let occupied = uuid::Uuid::new_v4().to_string();
        let replacement = unique_user_id(occupied.clone(), |id| id == occupied);

        assert_ne!(replacement, occupied);
        assert!(uuid::Uuid::parse_str(&replacement).is_ok());
    }

    #[test]
    fn configured_hosts_are_normalized_without_losing_ipv6() {
        assert_eq!(
            normalize_configured_host("https://Example.COM:8443/path").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            normalize_configured_host("[::1]:3000").as_deref(),
            Some("[::1]")
        );
    }

    fn test_status(nickname: &str, rev: u64) -> UserStatus {
        UserStatus {
            nickname: nickname.to_string(),
            avatar: None,
            is_gif: false,
            static_frame: None,
            is_muted: false,
            is_deafened: false,
            is_screen_sharing: false,
            is_low_bandwidth_mode: false,
            is_mobile: false,
            profile_rev: rev,
        }
    }

    #[test]
    fn fresh_join_uses_client_profile() {
        let client = test_status("Lisa", 5);
        assert_eq!(reconcile_join_status(None, client.clone()), client);
    }

    #[test]
    fn stale_client_rejoin_keeps_server_profile() {
        // Local storage was lost (frozen tab killed by iOS): the client
        // rejoins as Guest with rev 0, but the server still holds "Lisa"
        // with a higher revision. The server copy must win.
        let server = test_status("Lisa", 5);
        let stale_client = test_status("Guest", 0);
        let resolved = reconcile_join_status(Some(&server), stale_client);
        assert_eq!(resolved.nickname, "Lisa");
        assert_eq!(resolved.profile_rev, 5);
    }

    #[test]
    fn equal_revision_keeps_server_profile() {
        let server = test_status("Lisa", 5);
        let client = test_status("Lisa", 5);
        let resolved = reconcile_join_status(Some(&server), client);
        assert_eq!(resolved.nickname, "Lisa");
        assert_eq!(resolved.profile_rev, 5);
    }

    #[test]
    fn newer_client_profile_wins_on_rejoin() {
        // The user edited the name in the setup overlay and reconnects
        // before the old socket's death is noticed: the bump shows this is
        // a deliberate new save, so it must win.
        let server = test_status("Lisa", 5);
        let client = test_status("Mom", 6);
        let resolved = reconcile_join_status(Some(&server), client);
        assert_eq!(resolved.nickname, "Mom");
        assert_eq!(resolved.profile_rev, 6);
    }

    #[test]
    fn stale_reconnect_adopts_join_screen_state() {
        let mut server = test_status("Lisa", 5);
        server.is_screen_sharing = true;
        let mut client = test_status("Guest", 0);
        client.is_screen_sharing = false;
        let resolved = reconcile_join_status(Some(&server), client);
        assert_eq!(resolved.nickname, "Lisa");
        // Per-connection state always reflects the new connection.
        assert!(!resolved.is_screen_sharing);
    }

    #[test]
    fn stale_reconnect_adopts_current_device_type() {
        let server = test_status("Lisa", 5);
        let mut client = test_status("Guest", 0);
        client.is_mobile = true;

        let resolved = reconcile_join_status(Some(&server), client);
        assert!(resolved.is_mobile);
    }

    #[test]
    fn empty_nickname_is_normalized_to_guest() {
        // An empty/whitespace nickname must never be stored: the owner sees
        // "Guest" locally, so everyone else must too. Otherwise the server
        // and the owner disagree forever (until reconnect).
        for raw in ["", "   ", "\t\n"] {
            let client = test_status(raw, 6);
            let resolved = reconcile_join_status(None, client);
            assert_eq!(resolved.nickname, "Guest");
            assert_eq!(resolved.profile_rev, 6);
        }
    }

    #[test]
    fn normalize_nickname_trims_and_caps() {
        assert_eq!(normalize_nickname("  Alice  "), "Alice");
        assert_eq!(normalize_nickname(""), "Guest");
        assert_eq!(normalize_nickname("   "), "Guest");
        let long = "a".repeat(MAX_NICKNAME_LEN + 10);
        assert_eq!(normalize_nickname(&long).chars().count(), MAX_NICKNAME_LEN);
    }
}

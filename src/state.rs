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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RoomKind {
    #[default]
    Regular,
    Stage,
}

impl RoomKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            RoomKind::Regular => "regular",
            RoomKind::Stage => "stage",
        }
    }

    pub(crate) fn from_str(value: &str) -> RoomKind {
        if value.eq_ignore_ascii_case("stage") {
            RoomKind::Stage
        } else {
            RoomKind::Regular
        }
    }
}

// Moderation state an admin has imposed on one user. Enforced by the
// server where it can (status clamps, toggle drops) and by the target's
// client for device-level effects (mic/cam/share stop).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ForceFlags {
    pub muted: bool,
    pub deafened: bool,
    pub cam_off: bool,
    pub share_off: bool,
}

impl ForceFlags {
    pub(crate) fn any(&self) -> bool {
        self.muted || self.deafened || self.cam_off || self.share_off
    }
}

pub(crate) struct PendingKnock {
    pub nickname: String,
    pub tx: UserTx,
}

// Per-room admin/moderation state, keyed by room id in AppState. Lives
// exactly as long as the room: every room-removal path also removes
// this entry, which is what invalidates the creator key, the ban list
// and the lobby/stage configuration when the call fully expires.
#[derive(Default)]
pub(crate) struct RoomAdmin {
    // HMAC of the creator key under the server boot secret. The key
    // itself is only ever held by the creator's browser (localStorage).
    pub key_hash: Option<String>,
    pub creator_id: Option<String>,
    pub admins: HashSet<String>,
    pub banned: HashSet<String>,
    pub lobby: bool,
    pub kind: RoomKind,
    // Stage rooms: non-admin users allowed to speak/present.
    pub speakers: HashSet<String>,
    pub forced: HashMap<String, ForceFlags>,
    // Lobby: connections parked before join, and one-shot approvals.
    pub pending: HashMap<String, PendingKnock>,
    pub approved: HashSet<String>,
}

impl RoomAdmin {
    pub(crate) fn is_admin(&self, user_id: &str) -> bool {
        self.admins.contains(user_id)
    }

    pub(crate) fn can_speak(&self, user_id: &str) -> bool {
        self.kind == RoomKind::Regular
            || self.admins.contains(user_id)
            || self.speakers.contains(user_id)
    }

    // The role state shared with every client (and, in distributed
    // mode, with the other nodes). Never includes the key hash, the
    // pending knock connections, or approvals.
    pub(crate) fn public_state(&self) -> serde_json::Value {
        serde_json::json!({
            "creatorId": self.creator_id,
            "admins": self.admins.iter().collect::<Vec<_>>(),
            "banned": self.banned.iter().collect::<Vec<_>>(),
            "lobby": self.lobby,
            "kind": self.kind.as_str(),
            "speakers": self.speakers.iter().collect::<Vec<_>>(),
            "forced": self.forced,
        })
    }

    // Distributed shadow sync: adopt role state published by the node
    // that owns the room's admin actions. Key material, pending knocks
    // and approvals are node-local and never overwritten here.
    pub(crate) fn apply_public_state(&mut self, data: &serde_json::Value) {
        if let Some(creator) = data.get("creatorId").and_then(|v| v.as_str()) {
            self.creator_id = Some(creator.to_string());
        }
        let read_set = |key: &str| -> HashSet<String> {
            data.get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        self.admins = read_set("admins");
        self.banned = read_set("banned");
        self.speakers = read_set("speakers");
        if let Some(lobby) = data.get("lobby").and_then(|v| v.as_bool()) {
            self.lobby = lobby;
        }
        if let Some(kind) = data.get("kind").and_then(|v| v.as_str()) {
            self.kind = RoomKind::from_str(kind);
        }
        if let Some(forced) = data.get("forced") {
            self.forced = serde_json::from_value(forced.clone()).unwrap_or_default();
        }
    }
}

pub(crate) type AdminMap = Arc<Mutex<HashMap<String, RoomAdmin>>>;

// On-disk form of the admin state, written by admin::persist_state so
// room creators, promoted admins, bans, lobby/stage configuration and
// force states survive server restarts and version rollouts. The
// signing secret is stored alongside (file is 0600): without it the
// stored key hashes could never verify after a restart. Entries are
// still deleted by the normal room-expiry cleanup, so a call that
// truly ends takes its admin state with it.
#[derive(Serialize, Deserialize)]
pub(crate) struct PersistedRoomAdmin {
    pub key_hash: Option<String>,
    pub creator_id: Option<String>,
    pub admins: HashSet<String>,
    pub banned: HashSet<String>,
    pub lobby: bool,
    pub kind: RoomKind,
    pub speakers: HashSet<String>,
    pub forced: HashMap<String, ForceFlags>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PersistedState {
    pub secret: Vec<u8>,
    pub rooms: HashMap<String, PersistedRoomAdmin>,
}

impl RoomAdmin {
    pub(crate) fn to_persisted(&self) -> PersistedRoomAdmin {
        PersistedRoomAdmin {
            key_hash: self.key_hash.clone(),
            creator_id: self.creator_id.clone(),
            admins: self.admins.clone(),
            banned: self.banned.clone(),
            lobby: self.lobby,
            kind: self.kind,
            speakers: self.speakers.clone(),
            forced: self.forced.clone(),
        }
    }

    // Worth writing to disk at all (join attempts against rooms owned
    // by other nodes can create empty local entries).
    pub(crate) fn is_meaningful(&self) -> bool {
        self.creator_id.is_some()
            || !self.admins.is_empty()
            || !self.banned.is_empty()
            || self.lobby
            || self.kind == RoomKind::Stage
    }
}

impl From<PersistedRoomAdmin> for RoomAdmin {
    fn from(p: PersistedRoomAdmin) -> RoomAdmin {
        RoomAdmin {
            key_hash: p.key_hash,
            creator_id: p.creator_id,
            admins: p.admins,
            banned: p.banned,
            lobby: p.lobby,
            kind: p.kind,
            speakers: p.speakers,
            forced: p.forced,
            pending: HashMap::new(),
            approved: HashSet::new(),
        }
    }
}

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
    pub(crate) admin_rooms: AdminMap,
    // Random per boot; signs creator keys. Rooms are in-memory only,
    // so a restart invalidating every key matches room lifetime.
    pub(crate) server_secret: Arc<Vec<u8>>,
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

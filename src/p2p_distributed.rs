use crate::{
    admin::constant_time_eq,
    distributed::{
        cleanup_distributed_node, distributed_user_data, is_valid_distributed_message,
        process_distributed_message, reconcile_distributed_node, schedule_empty_room_cleanup,
    },
    routes::host_is_allowed,
    state::*,
};
use axum::{
    extract::{
        State,
        ws::{Message as AxumWsMessage, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac, digest::KeyInit};
use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::UdpSocket,
    sync::{Mutex, Semaphore},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Message as TungsteniteMessage},
};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

const PROTOCOL_VERSION: u8 = 1;
const WS_CHUNK_SIZE: usize = 256 * 1024;
const UDP_CHUNK_SIZE: usize = 900;
const MAX_FRAME_SIZE: usize = DISTRIBUTED_MAX_MESSAGE_SIZE;
const MAX_WS_CHUNKS: usize = MAX_FRAME_SIZE.div_ceil(WS_CHUNK_SIZE);
const MAX_UDP_CHUNKS: usize = MAX_FRAME_SIZE.div_ceil(UDP_CHUNK_SIZE);
const RECONNECT_MAX_SECS: u64 = 30;
const CHUNK_TTL_SECS: u64 = 60;
const MAX_PENDING_CHUNK_ASSEMBLIES: usize = 256;
const MAX_PENDING_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_SNAPSHOTS: usize = 128;
const MAX_SEEN_ENVELOPES: usize = 32_768;
const MAX_UDP_CANDIDATES: usize = 256;
const MAX_HOPS: u8 = 8;
const UDP_MAGIC: &[u8; 4] = b"RRP2";
const UDP_TAG_LEN: usize = 32;
const UDP_PUNCH_INTERVAL_SECS: u64 = 3;
const UDP_WINDOW_SIZE: usize = 16;
const UDP_ACK_TIMEOUT_MS: u64 = 160;
const UDP_MAX_RETRIES: usize = 5;
const STUN_MAGIC_COOKIE: u32 = 0x2112_A442;
const BEP44_SLOTS: u8 = 8;
const DEFAULT_STUN_SERVERS: [&str; 3] = [
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
    "stun.cloudflare.com:3478",
];

fn fresh_uuid() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum EnvelopeKind {
    Event,
    Heartbeat,
    SnapshotRequest,
    SnapshotStart,
    SnapshotEnd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct P2pEnvelope {
    pub version: u8,
    #[serde(default = "fresh_uuid")]
    pub envelope_id: String,
    pub source_node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub hop_count: u8,
    pub kind: EnvelopeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<DistributedMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct P2pChunk {
    pub version: u8,
    pub sender: String,
    pub transmission_id: String,
    pub index: usize,
    pub total: usize,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "kebab-case")]
pub(crate) enum UdpWirePacket {
    Hello {
        node_id: String,
        udp_port: u16,
        http_port: u16,
        #[serde(default)]
        peers: Vec<SocketAddr>,
        #[serde(default)]
        ws_urls: Vec<String>,
    },
    Chunk {
        version: u8,
        sender: String,
        transmission_id: String,
        index: usize,
        total: usize,
        payload: String,
    },
    Ack {
        transmission_id: String,
        index: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DhtNodeRecord {
    pub node_id: String,
    pub udp_addrs: Vec<SocketAddr>,
    pub ws_urls: Vec<String>,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ForwardedEnvelope {
    pub origin_link_id: Option<String>,
    pub origin_udp_addr: Option<SocketAddr>,
    pub envelope: P2pEnvelope,
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
    removed_users: HashSet<RemoteUserKey>,
    superseded_channels: HashSet<(String, String)>,
}

struct SeenEnvelopes {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl Default for SeenEnvelopes {
    fn default() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }
}

impl SeenEnvelopes {
    fn remember(&mut self, id: &str) -> bool {
        if id.is_empty() || !self.set.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        if self.order.len() > MAX_SEEN_ENVELOPES
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        true
    }
}

type LastSeen = HashMap<String, Instant>;
type Assemblies = HashMap<(String, String), ChunkAssembly>;
type Snapshots = HashMap<(String, String), SnapshotAssembly>;

pub(crate) struct P2pRuntime {
    last_seen: Mutex<LastSeen>,
    assemblies: Mutex<Assemblies>,
    snapshots: Mutex<Snapshots>,
    snapshot_permits: Arc<Semaphore>,
    seen_envelopes: Mutex<SeenEnvelopes>,
    forward_tx: tokio::sync::broadcast::Sender<ForwardedEnvelope>,
    known_ws_peers: Mutex<HashSet<String>>,
    known_udp_candidates: Mutex<HashSet<SocketAddr>>,
    active_udp_peers: Mutex<HashMap<String, (SocketAddr, Instant)>>,
    pending_udp_acks: Mutex<HashSet<(String, usize)>>,
    reflexive_udp_addr: Mutex<Option<SocketAddr>>,
    local_udp_port: AtomicU16,
    http_port: u16,
    scheme: String,
    public_ws_url: Option<String>,
    udp_mac_key: [u8; 32],
    snapshot_udp_lock: Mutex<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct P2pConfig {
    pub cluster_key: String,
    pub peers: Vec<String>,
    pub dht_enabled: bool,
    pub scheme: String,
    pub http_port: u16,
    pub udp_port: u16,
    pub public_ws_url: Option<String>,
    pub stun_servers: Vec<String>,
}

impl P2pRuntime {
    pub(crate) fn new(config: &P2pConfig) -> Arc<Self> {
        let (forward_tx, _) = tokio::sync::broadcast::channel(DISTRIBUTED_BROADCAST_CAPACITY * 2);
        Arc::new(Self {
            last_seen: Mutex::new(HashMap::new()),
            assemblies: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(HashMap::new()),
            snapshot_permits: Arc::new(Semaphore::new(4)),
            seen_envelopes: Mutex::new(SeenEnvelopes::default()),
            forward_tx,
            known_ws_peers: Mutex::new(HashSet::new()),
            known_udp_candidates: Mutex::new(HashSet::new()),
            active_udp_peers: Mutex::new(HashMap::new()),
            pending_udp_acks: Mutex::new(HashSet::new()),
            reflexive_udp_addr: Mutex::new(None),
            local_udp_port: AtomicU16::new(config.udp_port),
            http_port: config.http_port,
            scheme: config.scheme.clone(),
            public_ws_url: config.public_ws_url.clone(),
            udp_mac_key: derive_udp_mac_key(&config.cluster_key),
            snapshot_udp_lock: Mutex::new(()),
        })
    }
}

pub(crate) fn derive_server_secret(cluster_key: &str) -> Vec<u8> {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(b"rustrooms:server-secret:v1:");
    hasher.update(cluster_key.as_bytes());
    hasher.finalize().to_vec()
}

pub(crate) fn derive_udp_mac_key(cluster_key: &str) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(b"rustrooms:udp-mac:v1:");
    hasher.update(cluster_key.as_bytes());
    hasher.finalize().into()
}

pub(crate) fn derive_cluster_auth_token(cluster_key: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(cluster_key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"rustrooms:cluster-ws-auth:v1");
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub(crate) fn verify_cluster_auth(cluster_key: &str, provided: &str) -> bool {
    if cluster_key.is_empty() || provided.is_empty() {
        return false;
    }
    let expected_token = derive_cluster_auth_token(cluster_key);
    constant_time_eq(expected_token.as_bytes(), provided.as_bytes())
        || constant_time_eq(cluster_key.as_bytes(), provided.as_bytes())
}

pub(crate) fn derive_dht_info_hash(cluster_key: &str, purpose: &str) -> mainline::Id {
    let mut hasher = Sha1::new();
    hasher.update(b"rustrooms:dht:v1:");
    hasher.update(purpose.as_bytes());
    hasher.update(b":");
    hasher.update(cluster_key.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 20];
    bytes.copy_from_slice(&digest);
    mainline::Id::from_bytes(bytes).expect("SHA1 is 20 bytes")
}

pub(crate) fn derive_dht_signing_key(cluster_key: &str) -> mainline::SigningKey {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(b"rustrooms:dht-mutable:v1:");
    hasher.update(cluster_key.as_bytes());
    let bytes: [u8; 32] = hasher.finalize().into();
    mainline::SigningKey::from_bytes(&bytes)
}

fn node_bep44_slot(node_id: &str) -> String {
    use sha2::Digest;
    let digest = Sha256::digest(node_id.as_bytes());
    let slot = digest[0] % BEP44_SLOTS;
    format!("rustrooms:slot:{slot}")
}

pub(crate) fn parse_peer_list<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut out = HashSet::new();
    for value in values {
        out.extend(
            value
                .split([',', ';', '\n'])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        );
    }
    let mut list: Vec<_> = out.into_iter().collect();
    list.sort();
    list
}

pub(crate) fn normalize_ws_peer_url(raw: &str, default_scheme: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.starts_with("udp://") {
        return None;
    }
    let with_scheme = if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_string()
    } else {
        let scheme = if default_scheme.eq_ignore_ascii_case("wss") {
            "wss"
        } else {
            "ws"
        };
        format!("{scheme}://{trimmed}")
    };

    let mut parsed = url::Url::parse(&with_scheme).ok()?;
    if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
        return None;
    }
    if parsed.path().is_empty() || parsed.path() == "/" {
        parsed.set_path("/cluster-ws");
    }
    parsed.set_fragment(None);
    Some(parsed.to_string())
}

pub(crate) fn parse_udp_peer_addr(raw: &str) -> Option<SocketAddr> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let target = trimmed.strip_prefix("udp://").unwrap_or(trimmed);
    if let Ok(addr) = target.parse::<SocketAddr>() {
        return Some(addr);
    }
    if let Ok(parsed) = url::Url::parse(trimmed)
        && let Some(host) = parsed.host_str()
        && let Some(port) = parsed.port()
        && let Ok(ip) = host.parse::<IpAddr>()
    {
        return Some(SocketAddr::new(ip, port));
    }
    None
}

pub(crate) fn resolve_udp_peer(raw: &str) -> Vec<SocketAddr> {
    use std::net::ToSocketAddrs;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let target = trimmed
        .strip_prefix("udp://")
        .unwrap_or(trimmed)
        .trim_end_matches('/');
    if let Ok(addrs) = target.to_socket_addrs() {
        let list: Vec<SocketAddr> = addrs.collect();
        if !list.is_empty() {
            return list;
        }
    }
    if let Ok(parsed) = url::Url::parse(trimmed)
        && let (Some(host), Some(port)) = (parsed.host_str(), parsed.port())
    {
        if let Ok(addrs) = (host, port).to_socket_addrs() {
            let list: Vec<SocketAddr> = addrs.collect();
            if !list.is_empty() {
                return list;
            }
        }
    }
    parse_udp_peer_addr(raw).into_iter().collect()
}

pub(crate) fn load_p2p_config_from_env(http_port: u16) -> Option<P2pConfig> {
    let peers = parse_peer_list(
        std::env::var("CLUSTER_PEERS")
            .ok()
            .iter()
            .chain(std::env::var("PEERS").ok().iter())
            .chain(std::env::var("CLUSTER_RELAY_URL").ok().iter())
            .map(String::as_str),
    );
    let cluster_key = std::env::var("KEY")
        .ok()
        .or_else(|| std::env::var("CLUSTER_KEY").ok())
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty());

    let cluster_key = match (cluster_key, peers.is_empty()) {
        (Some(key), _) => key,
        (None, false) => {
            println!(
                "P2P DISTRIBUTED: CLUSTER_PEERS set without KEY; using default cluster key (set KEY for production security)"
            );
            "rustrooms-default-cluster-key".to_string()
        }
        (None, true) => return None,
    };

    let dht_enabled = std::env::var("CLUSTER_DHT")
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true);

    let scheme = std::env::var("CLUSTER_SCHEME")
        .ok()
        .map(|s| {
            if s.trim().eq_ignore_ascii_case("wss") {
                "wss".to_string()
            } else {
                "ws".to_string()
            }
        })
        .unwrap_or_else(|| "ws".to_string());

    let udp_port = std::env::var("CLUSTER_UDP_PORT")
        .ok()
        .and_then(|p| p.trim().parse::<u16>().ok())
        .unwrap_or(http_port);

    let public_ws_url = std::env::var("CLUSTER_PUBLIC_URL")
        .ok()
        .and_then(|u| normalize_ws_peer_url(&u, &scheme));

    let mut stun_servers = parse_peer_list(std::env::var("STUN_SERVERS").ok().iter().map(String::as_str));
    if stun_servers.is_empty() {
        stun_servers = DEFAULT_STUN_SERVERS.iter().map(|s| (*s).to_string()).collect();
    }
    if let Ok(turn_url) = std::env::var("TURN_URL") {
        let cleaned = turn_url
            .trim()
            .trim_start_matches("turns:")
            .trim_start_matches("turn:")
            .trim_start_matches("stun:")
            .trim_start_matches("//");
        let host_port = cleaned.split(['?', '/']).next().unwrap_or("").trim();
        if !host_port.is_empty() && !stun_servers.contains(&host_port.to_string()) {
            stun_servers.push(host_port.to_string());
        }
    }

    Some(P2pConfig {
        cluster_key,
        peers,
        dht_enabled,
        scheme,
        http_port,
        udp_port,
        public_ws_url,
        stun_servers,
    })
}

// ---- STUN (RFC 5389) Binding Request & Response Parser ----

pub(crate) fn build_stun_binding_request(tx_id: [u8; 12]) -> [u8; 20] {
    let mut pkt = [0u8; 20];
    // Binding Request = 0x0001, Message Length = 0x0000
    pkt[0] = 0x00;
    pkt[1] = 0x01;
    pkt[2] = 0x00;
    pkt[3] = 0x00;
    pkt[4..8].copy_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    pkt[8..20].copy_from_slice(&tx_id);
    pkt
}

pub(crate) fn parse_stun_binding_response(buf: &[u8]) -> Option<SocketAddr> {
    if buf.len() < 20 {
        return None;
    }
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    if msg_type != 0x0101 {
        return None;
    }
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    if buf.len() < 20 + msg_len {
        return None;
    }
    let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if cookie != STUN_MAGIC_COOKIE {
        return None;
    }
    let tx_id = &buf[8..20];

    let mut offset = 20;
    let end = 20 + msg_len;
    let mut fallback_mapped = None;

    while offset + 4 <= end {
        let attr_type = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        let attr_len = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        offset += 4;
        if offset + attr_len > end {
            break;
        }
        let val = &buf[offset..offset + attr_len];
        // XOR-MAPPED-ADDRESS (0x0020) or XOR-MAPPED-ADDRESS legacy (0x8020)
        if (attr_type == 0x0020 || attr_type == 0x8020) && attr_len >= 8 {
            let family = val[1];
            let xport = u16::from_be_bytes([val[2], val[3]]);
            let port = xport ^ ((STUN_MAGIC_COOKIE >> 16) as u16);
            if family == 0x01 {
                let xip = u32::from_be_bytes([val[4], val[5], val[6], val[7]]);
                let ip = Ipv4Addr::from(xip ^ STUN_MAGIC_COOKIE);
                return Some(SocketAddr::V4(SocketAddrV4::new(ip, port)));
            } else if family == 0x02 && attr_len >= 20 {
                let mut ip_bytes = [0u8; 16];
                ip_bytes.copy_from_slice(&val[4..20]);
                let cookie_bytes = STUN_MAGIC_COOKIE.to_be_bytes();
                for i in 0..4 {
                    ip_bytes[i] ^= cookie_bytes[i];
                }
                for i in 0..12 {
                    ip_bytes[4 + i] ^= tx_id[i];
                }
                return Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip_bytes)), port));
            }
        } else if attr_type == 0x0001 && attr_len >= 8 {
            // MAPPED-ADDRESS (0x0001)
            let family = val[1];
            let port = u16::from_be_bytes([val[2], val[3]]);
            if family == 0x01 {
                let ip = Ipv4Addr::new(val[4], val[5], val[6], val[7]);
                fallback_mapped = Some(SocketAddr::V4(SocketAddrV4::new(ip, port)));
            }
        }
        let padded = (attr_len + 3) & !3;
        offset += padded;
    }
    fallback_mapped
}

// ---- Authenticated UDP Packet Framing ----

pub(crate) fn encode_udp_packet(mac_key: &[u8; 32], packet: &UdpWirePacket) -> Option<Vec<u8>> {
    let payload = serde_json::to_vec(packet).ok()?;
    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC accepts 32-byte key");
    mac.update(UDP_MAGIC);
    mac.update(&payload);
    let tag = mac.finalize().into_bytes();

    let mut out = Vec::with_capacity(UDP_MAGIC.len() + UDP_TAG_LEN + payload.len());
    out.extend_from_slice(UDP_MAGIC);
    out.extend_from_slice(&tag);
    out.extend_from_slice(&payload);
    Some(out)
}

pub(crate) fn decode_udp_packet(mac_key: &[u8; 32], raw: &[u8]) -> Option<UdpWirePacket> {
    let header_len = UDP_MAGIC.len() + UDP_TAG_LEN;
    if raw.len() <= header_len || &raw[..UDP_MAGIC.len()] != UDP_MAGIC {
        return None;
    }
    let tag = &raw[UDP_MAGIC.len()..header_len];
    let payload = &raw[header_len..];

    let mut mac = HmacSha256::new_from_slice(mac_key).expect("HMAC accepts 32-byte key");
    mac.update(UDP_MAGIC);
    mac.update(payload);
    let expected = mac.finalize().into_bytes();
    if !constant_time_eq(tag, &expected) {
        return None;
    }
    serde_json::from_slice(payload).ok()
}

// ---- Local LAN IP Discovery ----

fn discover_lan_ipv4() -> Option<Ipv4Addr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("8.8.8.8:80").ok()?;
    match sock.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}

// ---- Spawning the P2P Distributed Subsystem ----

pub(crate) fn spawn_p2p_distributed(
    state: AppState,
    config: P2pConfig,
    runtime: Arc<P2pRuntime>,
) {
    // 1. Bridge local state.distributed_tx events into runtime.forward_tx so all
    // active P2P transports (UDP and WebSocket) broadcast local events.
    {
        let state = state.clone();
        let runtime = runtime.clone();
        let mut distributed_rx = state.distributed_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match distributed_rx.recv().await {
                    Ok(raw) => {
                        let Ok(message) = serde_json::from_str::<DistributedMessage>(&raw) else {
                            continue;
                        };
                        let envelope = P2pEnvelope {
                            version: PROTOCOL_VERSION,
                            envelope_id: Uuid::new_v4().to_string(),
                            source_node_id: state.node_id.clone(),
                            target_node_id: None,
                            snapshot_id: None,
                            hop_count: 0,
                            kind: EnvelopeKind::Event,
                            message: Some(message),
                        };
                        runtime
                            .seen_envelopes
                            .lock()
                            .await
                            .remember(&envelope.envelope_id);
                        let _ = runtime.forward_tx.send(ForwardedEnvelope {
                            origin_link_id: None,
                            origin_udp_addr: None,
                            envelope,
                        });
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!(
                            "P2P DISTRIBUTED: Outbound queue lagged by {skipped} event(s); re-announcing local state"
                        );
                        spawn_local_snapshot(state.clone(), runtime.clone(), None);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    // 2. Periodic heartbeat & stale-node cleanup loop.
    {
        let state = state.clone();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(DISTRIBUTED_HEARTBEAT_SECS));
            interval.tick().await;
            loop {
                interval.tick().await;
                let hb = P2pEnvelope {
                    version: PROTOCOL_VERSION,
                    envelope_id: Uuid::new_v4().to_string(),
                    source_node_id: state.node_id.clone(),
                    target_node_id: None,
                    snapshot_id: None,
                    hop_count: 0,
                    kind: EnvelopeKind::Heartbeat,
                    message: None,
                };
                runtime
                    .seen_envelopes
                    .lock()
                    .await
                    .remember(&hb.envelope_id);
                let _ = runtime.forward_tx.send(ForwardedEnvelope {
                    origin_link_id: None,
                    origin_udp_addr: None,
                    envelope: hb,
                });

                cleanup_stale_nodes(&state, &runtime).await;
                runtime.assemblies.lock().await.retain(|_, assembly| {
                    assembly.last_activity.elapsed() < Duration::from_secs(CHUNK_TTL_SECS)
                });
                runtime.snapshots.lock().await.retain(|_, snapshot| {
                    snapshot.last_activity.elapsed() < Duration::from_secs(CHUNK_TTL_SECS)
                });
            }
        });
    }

    // 3. Seed static peers from CLUSTER_PEERS / PEERS.
    for peer in &config.peers {
        for udp_addr in resolve_udp_peer(peer) {
            let rt = runtime.clone();
            tokio::spawn(async move {
                add_udp_candidate(&rt, udp_addr).await;
            });
        }
        if let Some(ws_url) = normalize_ws_peer_url(peer, &config.scheme) {
            spawn_outbound_ws_peer(ws_url, state.clone(), runtime.clone(), true);
        }
    }

    // 4. Spawn UDP P2P transport (STUN + UDP Hole Punching + LAN Broadcast + Reliable Chunking).
    spawn_udp_p2p_engine(state.clone(), config.clone(), runtime.clone());

    // 5. Spawn Mainline DHT discovery (announcing & querying both UDP hole-punch endpoints,
    // WebSocket endpoints, and signed BEP 44 node records).
    if config.dht_enabled {
        spawn_dht_discovery(state, config, runtime);
    }
}

async fn add_udp_candidate(runtime: &P2pRuntime, addr: SocketAddr) {
    if addr.port() == 0 || addr.ip().is_unspecified() {
        return;
    }
    let local_port = runtime.local_udp_port.load(Ordering::Relaxed);
    if addr.ip().is_loopback() && addr.port() == local_port {
        return;
    }
    if let Some(reflexive) = *runtime.reflexive_udp_addr.lock().await
        && reflexive == addr
    {
        return;
    }
    let mut candidates = runtime.known_udp_candidates.lock().await;
    if candidates.len() < MAX_UDP_CANDIDATES {
        candidates.insert(addr);
    }
}

fn spawn_udp_p2p_engine(state: AppState, config: P2pConfig, runtime: Arc<P2pRuntime>) {
    tokio::spawn(async move {
        let bind_addr = format!("0.0.0.0:{}", config.udp_port);
        let socket = match UdpSocket::bind(&bind_addr).await {
            Ok(sock) => sock,
            Err(_) => match UdpSocket::bind("0.0.0.0:0").await {
                Ok(sock) => sock,
                Err(e) => {
                    eprintln!("P2P DISTRIBUTED: Failed to bind UDP P2P socket: {e}");
                    return;
                }
            },
        };
        let _ = socket.set_broadcast(true);
        let local_port = socket.local_addr().map(|a| a.port()).unwrap_or(config.udp_port);
        runtime.local_udp_port.store(local_port, Ordering::Relaxed);
        println!("P2P DISTRIBUTED: UDP hole-punching & mesh transport listening on port {local_port}");

        let socket = Arc::new(socket);

        // Initial STUN probe to discover reflexive NAT address before first DHT announce.
        probe_stun_servers(&socket, &config.stun_servers).await;

        // Task A: Outbound envelope sender over UDP (with per-chunk ACK and retransmission).
        {
            let socket = socket.clone();
            let runtime = runtime.clone();
            let mut forward_rx = runtime.forward_tx.subscribe();
            tokio::spawn(async move {
                loop {
                    let item = match forward_rx.recv().await {
                        Ok(item) => item,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    };
                    let targets: Vec<SocketAddr> = {
                        let peers = runtime.active_udp_peers.lock().await;
                        if let Some(ref target_node) = item.envelope.target_node_id
                            && let Some((addr, _)) = peers.get(target_node)
                        {
                            if item.origin_udp_addr == Some(*addr) {
                                Vec::new()
                            } else {
                                vec![*addr]
                            }
                        } else {
                            let mut set = HashSet::new();
                            for (addr, _) in peers.values() {
                                if item.origin_udp_addr != Some(*addr) {
                                    set.insert(*addr);
                                }
                            }
                            set.into_iter().collect()
                        }
                    };
                    if targets.is_empty() {
                        continue;
                    }
                    let socket = socket.clone();
                    let runtime = runtime.clone();
                    let envelope = item.envelope;
                    let is_snapshot = envelope.snapshot_id.is_some();
                    tokio::spawn(async move {
                        let _snapshot_guard = if is_snapshot {
                            Some(runtime.snapshot_udp_lock.lock().await)
                        } else {
                            None
                        };
                        send_envelope_udp(&socket, &runtime, &envelope, &targets).await;
                    });
                }
            });
        }

        // Task B: Periodic STUN refresh + UDP Hole-Punching Hello + LAN broadcast beacon.
        {
            let socket = socket.clone();
            let runtime = runtime.clone();
            let state = state.clone();
            let stun_servers = config.stun_servers.clone();
            tokio::spawn(async move {
                let mut punch_ticker =
                    tokio::time::interval(Duration::from_secs(UDP_PUNCH_INTERVAL_SECS));
                let mut ticks = 0u64;
                loop {
                    punch_ticker.tick().await;
                    ticks += 1;

                    // Refresh STUN mapping every ~30s or while reflexive address is unknown.
                    if ticks == 1
                        || ticks.is_multiple_of(10)
                        || runtime.reflexive_udp_addr.lock().await.is_none()
                    {
                        probe_stun_servers(&socket, &stun_servers).await;
                    }

                    let active_addrs: Vec<SocketAddr> = runtime
                        .active_udp_peers
                        .lock()
                        .await
                        .values()
                        .map(|(addr, _)| *addr)
                        .collect();
                    let mut ws_urls: Vec<String> = Vec::new();
                    if let Some(ref pub_ws) = runtime.public_ws_url {
                        ws_urls.push(pub_ws.clone());
                    }
                    if let Some(lan_ip) = discover_lan_ipv4() {
                        ws_urls.push(format!(
                            "{}://{}:{}/cluster-ws",
                            runtime.scheme, lan_ip, runtime.http_port
                        ));
                    }

                    let hello = UdpWirePacket::Hello {
                        node_id: state.node_id.clone(),
                        udp_port: local_port,
                        http_port: runtime.http_port,
                        peers: active_addrs.iter().copied().take(32).collect(),
                        ws_urls,
                    };
                    let Some(wire) = encode_udp_packet(&runtime.udp_mac_key, &hello) else {
                        continue;
                    };

                    let mut targets: HashSet<SocketAddr> =
                        runtime.known_udp_candidates.lock().await.clone();
                    targets.extend(active_addrs);

                    // Also broadcast on LAN for zero-config same-network discovery.
                    targets.insert(SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::BROADCAST,
                        config.udp_port,
                    )));
                    if config.udp_port != 3000 {
                        targets.insert(SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::BROADCAST,
                            3000,
                        )));
                    }

                    for target in targets {
                        if target.ip().is_loopback() && target.port() == local_port {
                            continue;
                        }
                        let _ = socket.send_to(&wire, target).await;
                    }
                }
            });
        }

        // Task C: UDP Receiver Loop (demultiplexing STUN responses and authenticated RRP2 frames).
        let mut buf = vec![0u8; 65_536];
        loop {
            let Ok((len, src_addr)) = socket.recv_from(&mut buf).await else {
                continue;
            };
            let packet_bytes = &buf[..len];

            // Check if this is a STUN Binding Response.
            if let Some(reflexive_addr) = parse_stun_binding_response(packet_bytes) {
                let mut stored = runtime.reflexive_udp_addr.lock().await;
                if *stored != Some(reflexive_addr) {
                    println!(
                        "P2P DISTRIBUTED: Discovered external NAT UDP endpoint via STUN: {reflexive_addr}"
                    );
                    *stored = Some(reflexive_addr);
                }
                continue;
            }

            let Some(packet) = decode_udp_packet(&runtime.udp_mac_key, packet_bytes) else {
                continue;
            };

            match packet {
                UdpWirePacket::Hello {
                    node_id,
                    udp_port: _,
                    http_port,
                    peers,
                    ws_urls,
                } => {
                    if node_id == state.node_id || Uuid::parse_str(&node_id).is_err() {
                        continue;
                    }
                    runtime
                        .last_seen
                        .lock()
                        .await
                        .insert(node_id.clone(), Instant::now());
                    add_udp_candidate(&runtime, src_addr).await;

                    let is_new_peer = {
                        let mut active = runtime.active_udp_peers.lock().await;
                        let prev = active.insert(node_id.clone(), (src_addr, Instant::now()));
                        prev.is_none()
                    };

                    // Add peer-exchanged UDP addresses.
                    for peer_addr in peers {
                        add_udp_candidate(&runtime, peer_addr).await;
                    }
                    // Also try WebSocket connection to peer's source IP + advertised http_port
                    // and any advertised ws_urls.
                    if http_port > 0 {
                        let candidate_ws = format!(
                            "{}://{}:{}/cluster-ws",
                            runtime.scheme,
                            src_addr.ip(),
                            http_port
                        );
                        spawn_outbound_ws_peer(
                            candidate_ws,
                            state.clone(),
                            runtime.clone(),
                            false,
                        );
                    }
                    for ws_url in ws_urls {
                        if let Some(normalized) = normalize_ws_peer_url(&ws_url, &runtime.scheme) {
                            spawn_outbound_ws_peer(
                                normalized,
                                state.clone(),
                                runtime.clone(),
                                false,
                            );
                        }
                    }

                    if is_new_peer {
                        println!(
                            "P2P DISTRIBUTED [UDP]: Established direct P2P session with node {node_id} at {src_addr}"
                        );
                        // Reply immediately so the peer completes its side of the hole-punch.
                        let reply = UdpWirePacket::Hello {
                            node_id: state.node_id.clone(),
                            udp_port: local_port,
                            http_port: runtime.http_port,
                            peers: vec![src_addr],
                            ws_urls: Vec::new(),
                        };
                        if let Some(wire) = encode_udp_packet(&runtime.udp_mac_key, &reply) {
                            let _ = socket.send_to(&wire, src_addr).await;
                        }
                        // Request peer's snapshot and send our local snapshot.
                        let req = P2pEnvelope {
                            version: PROTOCOL_VERSION,
                            envelope_id: Uuid::new_v4().to_string(),
                            source_node_id: state.node_id.clone(),
                            target_node_id: Some(node_id.clone()),
                            snapshot_id: None,
                            hop_count: 0,
                            kind: EnvelopeKind::SnapshotRequest,
                            message: None,
                        };
                        let _ = runtime.forward_tx.send(ForwardedEnvelope {
                            origin_link_id: None,
                            origin_udp_addr: None,
                            envelope: req,
                        });
                        spawn_local_snapshot(state.clone(), runtime.clone(), Some(node_id));
                    }
                }
                UdpWirePacket::Ack {
                    transmission_id,
                    index,
                } => {
                    runtime
                        .pending_udp_acks
                        .lock()
                        .await
                        .remove(&(transmission_id, index));
                }
                UdpWirePacket::Chunk {
                    version,
                    sender,
                    transmission_id,
                    index,
                    total,
                    payload,
                } => {
                    if sender == state.node_id {
                        continue;
                    }
                    // Immediately ACK the chunk so the sender can advance its window.
                    let ack = UdpWirePacket::Ack {
                        transmission_id: transmission_id.clone(),
                        index,
                    };
                    if let Some(wire) = encode_udp_packet(&runtime.udp_mac_key, &ack) {
                        let _ = socket.send_to(&wire, src_addr).await;
                    }

                    let chunk = P2pChunk {
                        version,
                        sender,
                        transmission_id,
                        index,
                        total,
                        payload,
                    };
                    if let Some(envelope) =
                        accept_parsed_chunk(chunk, MAX_UDP_CHUNKS, UDP_CHUNK_SIZE, &runtime.assemblies)
                            .await
                    {
                        handle_envelope(
                            envelope,
                            None,
                            Some(src_addr),
                            &state,
                            &runtime,
                        )
                        .await;
                    }
                }
            }
        }
    });
}

async fn probe_stun_servers(socket: &UdpSocket, stun_servers: &[String]) {
    for server in stun_servers {
        let Ok(addrs) = tokio::net::lookup_host(server.as_str()).await else {
            continue;
        };
        for addr in addrs.filter(|a| a.is_ipv4()).take(2) {
            let mut tx_id = [0u8; 12];
            tx_id.copy_from_slice(&Uuid::new_v4().as_bytes()[..12]);
            let req = build_stun_binding_request(tx_id);
            let _ = socket.send_to(&req, addr).await;
        }
    }
}

async fn send_envelope_udp(
    socket: &UdpSocket,
    runtime: &P2pRuntime,
    envelope: &P2pEnvelope,
    targets: &[SocketAddr],
) {
    let Ok(payload) = serde_json::to_string(envelope) else {
        return;
    };
    if payload.len() > MAX_FRAME_SIZE {
        return;
    }
    let transmission_id = Uuid::new_v4().to_string();
    let parts = split_string(&payload, UDP_CHUNK_SIZE);
    let total = parts.len();
    let is_heartbeat = envelope.kind == EnvelopeKind::Heartbeat;

    for window_start in (0..total).step_by(UDP_WINDOW_SIZE) {
        let window_end = (window_start + UDP_WINDOW_SIZE).min(total);
        let mut encoded_window = Vec::with_capacity(window_end - window_start);

        {
            let mut pending = runtime.pending_udp_acks.lock().await;
            for idx in window_start..window_end {
                if !is_heartbeat {
                    pending.insert((transmission_id.clone(), idx));
                }
                let pkt = UdpWirePacket::Chunk {
                    version: PROTOCOL_VERSION,
                    sender: envelope.source_node_id.clone(),
                    transmission_id: transmission_id.clone(),
                    index: idx,
                    total,
                    payload: parts[idx].clone(),
                };
                if let Some(wire) = encode_udp_packet(&runtime.udp_mac_key, &pkt) {
                    encoded_window.push((idx, wire));
                }
            }
        }

        let max_attempts = if is_heartbeat { 1 } else { UDP_MAX_RETRIES };
        for attempt in 0..max_attempts {
            for (idx, wire) in &encoded_window {
                let needs_send = is_heartbeat
                    || attempt == 0
                    || runtime
                        .pending_udp_acks
                        .lock()
                        .await
                        .contains(&(transmission_id.clone(), *idx));
                if needs_send {
                    for target in targets {
                        let _ = socket.send_to(wire, *target).await;
                    }
                }
            }
            if is_heartbeat {
                break;
            }
            tokio::time::sleep(Duration::from_millis(UDP_ACK_TIMEOUT_MS)).await;
            let all_acked = {
                let pending = runtime.pending_udp_acks.lock().await;
                (window_start..window_end)
                    .all(|idx| !pending.contains(&(transmission_id.clone(), idx)))
            };
            if all_acked {
                break;
            }
        }

        if !is_heartbeat {
            let mut pending = runtime.pending_udp_acks.lock().await;
            for idx in window_start..window_end {
                pending.remove(&(transmission_id.clone(), idx));
            }
        }
    }
}

// ---- Mainline DHT Discovery (Peer Announcement + BEP 44 Mutable Node Records) ----

fn spawn_dht_discovery(state: AppState, config: P2pConfig, runtime: Arc<P2pRuntime>) {
    tokio::spawn(async move {
        let udp_info_hash = derive_dht_info_hash(&config.cluster_key, "udp");
        let ws_info_hash = derive_dht_info_hash(&config.cluster_key, "ws");
        let signing_key = derive_dht_signing_key(&config.cluster_key);
        let verifying_key_bytes = signing_key.verifying_key().to_bytes();
        let my_slot = node_bep44_slot(&state.node_id);

        println!(
            "P2P DISTRIBUTED [DHT]: Starting BitTorrent Mainline DHT discovery (udp_hash={udp_info_hash:?}, ws_hash={ws_info_hash:?})"
        );

        let dht = match mainline::Dht::client() {
            Ok(d) => d.as_async(),
            Err(e) => {
                eprintln!("P2P DISTRIBUTED [DHT]: Failed to start DHT client: {e}");
                return;
            }
        };

        // Brief wait for initial STUN response and DHT bootstrap.
        tokio::time::sleep(Duration::from_secs(2)).await;
        if dht.bootstrapped().await {
            println!("P2P DISTRIBUTED [DHT]: Bootstrapped onto Mainline DHT");
        }

        let mut seq = (current_unix_secs() as i64).max(1);
        loop {
            // Check if DHT itself observed our public IPv4 address.
            let dht_public_ip = dht.info().await.public_address().map(|a| *a.ip());
            let local_udp_port = runtime.local_udp_port.load(Ordering::Relaxed);
            let reflexive_udp = *runtime.reflexive_udp_addr.lock().await;

            let announce_udp_port = reflexive_udp
                .map(|a| a.port())
                .unwrap_or(local_udp_port);

            // 1. Announce UDP hole-punch port and HTTP/WS port on the DHT.
            let _ = dht.announce_peer(udp_info_hash, Some(announce_udp_port)).await;
            if announce_udp_port != local_udp_port {
                let _ = dht.announce_peer(udp_info_hash, Some(local_udp_port)).await;
            }
            let _ = dht.announce_peer(ws_info_hash, Some(config.http_port)).await;

            // 2. Publish signed BEP 44 MutableItem containing reflexive, LAN, and loopback endpoints.
            let mut udp_addrs = Vec::new();
            if let Some(reflexive) = reflexive_udp {
                udp_addrs.push(reflexive);
            } else if let Some(pub_ip) = dht_public_ip {
                udp_addrs.push(SocketAddr::V4(SocketAddrV4::new(pub_ip, local_udp_port)));
            }
            if let Some(lan_ip) = discover_lan_ipv4() {
                let lan_addr = SocketAddr::V4(SocketAddrV4::new(lan_ip, local_udp_port));
                if !udp_addrs.contains(&lan_addr) {
                    udp_addrs.push(lan_addr);
                }
            }
            udp_addrs.push(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                local_udp_port,
            )));

            let mut ws_urls = Vec::new();
            if let Some(ref pub_url) = config.public_ws_url {
                ws_urls.push(pub_url.clone());
            }
            if let Some(lan_ip) = discover_lan_ipv4() {
                ws_urls.push(format!(
                    "{}://{}:{}/cluster-ws",
                    config.scheme, lan_ip, config.http_port
                ));
            }
            ws_urls.push(format!(
                "{}://127.0.0.1:{}/cluster-ws",
                config.scheme, config.http_port
            ));

            let record = DhtNodeRecord {
                node_id: state.node_id.clone(),
                udp_addrs,
                ws_urls,
                ts: current_unix_secs(),
            };
            if let Ok(json_bytes) = serde_json::to_vec(&record)
                && json_bytes.len() <= 950
            {
                seq += 1;
                let item = mainline::MutableItem::new(
                    signing_key.clone(),
                    &json_bytes,
                    seq,
                    Some(my_slot.as_bytes()),
                );
                let _ = dht.put_mutable(item, None).await;
            }

            // 3. Query UDP peers from DHT.
            {
                let mut stream = dht.get_peers(udp_info_hash);
                while let Some(peers) = stream.next().await {
                    for peer in peers {
                        let addr = SocketAddr::V4(peer);
                        add_udp_candidate(&runtime, addr).await;
                        // Same-machine / NAT loopback fallback:
                        if Some(*peer.ip()) == dht_public_ip
                            || reflexive_udp.is_some_and(|r| r.ip() == IpAddr::V4(*peer.ip()))
                        {
                            let loopback = SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::LOCALHOST,
                                peer.port(),
                            ));
                            add_udp_candidate(&runtime, loopback).await;
                        }
                    }
                }
            }

            // 4. Query WS peers from DHT.
            {
                let mut stream = dht.get_peers(ws_info_hash);
                while let Some(peers) = stream.next().await {
                    for peer in peers {
                        if peer.ip().is_loopback() && peer.port() == config.http_port {
                            continue;
                        }
                        let ws_url = format!("{}://{peer}/cluster-ws", config.scheme);
                        spawn_outbound_ws_peer(
                            ws_url,
                            state.clone(),
                            runtime.clone(),
                            false,
                        );
                    }
                }
            }

            // 5. Query BEP 44 MutableItem slots for full peer records (including LAN & STUN addresses).
            for slot_idx in 0..BEP44_SLOTS {
                let salt = format!("rustrooms:slot:{slot_idx}");
                let mut stream =
                    dht.get_mutable(&verifying_key_bytes, Some(salt.as_bytes()), None);
                while let Some(item) = stream.next().await {
                    if let Ok(rec) = serde_json::from_slice::<DhtNodeRecord>(item.value()) {
                        if rec.node_id == state.node_id
                            || Uuid::parse_str(&rec.node_id).is_err()
                        {
                            continue;
                        }
                        for addr in rec.udp_addrs {
                            add_udp_candidate(&runtime, addr).await;
                        }
                        for url in rec.ws_urls {
                            if let Some(normalized) = normalize_ws_peer_url(&url, &config.scheme) {
                                spawn_outbound_ws_peer(
                                    normalized,
                                    state.clone(),
                                    runtime.clone(),
                                    false,
                                );
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(20)).await;
        }
    });
}

// ---- WebSocket P2P Mesh (/cluster-ws Inbound & Outbound) ----

pub(crate) async fn cluster_ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if let Some(ref allowed_url) = state.allowed_url
        && !host_is_allowed(&headers, allowed_url)
    {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }
    let Some(ref cluster_key) = state.cluster_key else {
        return (StatusCode::NOT_FOUND, "Distributed mode not enabled").into_response();
    };
    let Some(ref runtime) = state.p2p_runtime else {
        return (StatusCode::NOT_FOUND, "Distributed mode not enabled").into_response();
    };

    let provided_key = headers
        .get("X-Cluster-Key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !verify_cluster_auth(cluster_key, provided_key) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized cluster peer").into_response();
    }

    if let Some(peer_node_id) = headers.get("X-Node-Id").and_then(|v| v.to_str().ok())
        && peer_node_id == state.node_id
    {
        return (StatusCode::BAD_REQUEST, "Self connection rejected").into_response();
    }

    let runtime = runtime.clone();
    let node_id = state.node_id.clone();
    let mut response = ws
        .max_frame_size(MAX_FRAME_SIZE)
        .max_message_size(MAX_FRAME_SIZE)
        .on_upgrade(move |socket| handle_inbound_ws_peer(socket, state, runtime));
    if let Ok(val) = axum::http::HeaderValue::from_str(&node_id) {
        response.headers_mut().insert("X-Node-Id", val);
    }
    response
}

async fn handle_inbound_ws_peer(
    socket: WebSocket,
    state: AppState,
    runtime: Arc<P2pRuntime>,
) {
    let link_id = format!("ws-in-{}", Uuid::new_v4());
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(OUTBOUND_QUEUE_CAPACITY * 4);
    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<String>(OUTBOUND_QUEUE_CAPACITY * 4);

    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if ws_tx.send(AxumWsMessage::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    let reader = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(
                Duration::from_secs(DISTRIBUTED_NODE_TIMEOUT_SECS),
                ws_rx.next(),
            )
            .await
            {
                Ok(Some(Ok(AxumWsMessage::Text(t)))) => {
                    if in_tx.send(t.to_string()).await.is_err() {
                        break;
                    }
                }
                Ok(Some(Ok(AxumWsMessage::Ping(_) | AxumWsMessage::Pong(_)))) => continue,
                _ => break,
            }
        }
    });

    run_ws_link_common(&link_id, out_tx, in_rx, &state, &runtime).await;

    reader.abort();
    writer.abort();
}

pub(crate) fn spawn_outbound_ws_peer(
    url: String,
    state: AppState,
    runtime: Arc<P2pRuntime>,
    persistent: bool,
) {
    tokio::spawn(async move {
        {
            let mut known = runtime.known_ws_peers.lock().await;
            if !known.insert(url.clone()) {
                return;
            }
        }
        let mut retry_secs = 1u64;
        let mut failures = 0u32;
        loop {
            match connect_outbound_ws_once(&url, &state, &runtime).await {
                Ok(()) => {
                    retry_secs = 1;
                    failures = 0;
                }
                Err(err) => {
                    failures += 1;
                    // NAT loopback fallback for discovered peers on the same host
                    if !persistent
                        && failures == 1
                        && let Ok(mut parsed) = url::Url::parse(&url)
                        && parsed.host_str() != Some("127.0.0.1")
                        && parsed.host_str() != Some("localhost")
                    {
                        let _ = parsed.set_host(Some("127.0.0.1"));
                        let fallback_url = parsed.to_string();
                        if connect_outbound_ws_once(&fallback_url, &state, &runtime)
                            .await
                            .is_ok()
                        {
                            retry_secs = 1;
                            failures = 0;
                        }
                    }
                    if !persistent && failures >= 3 {
                        runtime.known_ws_peers.lock().await.remove(&url);
                        return;
                    }
                    if persistent {
                        eprintln!(
                            "P2P DISTRIBUTED [WS]: Peer {url} unavailable ({err}); retrying in {retry_secs}s"
                        );
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(retry_secs)).await;
            retry_secs = (retry_secs * 2).min(RECONNECT_MAX_SECS);
        }
    });
}

async fn connect_outbound_ws_once(
    url: &str,
    state: &AppState,
    runtime: &Arc<P2pRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cluster_key = state.cluster_key.as_ref().ok_or("No cluster key")?;
    let auth_token = derive_cluster_auth_token(cluster_key);

    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("X-Cluster-Key", auth_token.parse()?);
    request
        .headers_mut()
        .insert("X-Node-Id", state.node_id.parse()?);

    let (ws_stream, response) =
        tokio::time::timeout(Duration::from_secs(10), connect_async(request)).await??;
    if let Some(peer_id) = response
        .headers()
        .get("X-Node-Id")
        .and_then(|v| v.to_str().ok())
        && peer_id == state.node_id
    {
        return Err("Peer resolved to this node".into());
    }

    let link_id = format!("ws-out-{}", Uuid::new_v4());
    let (mut ws_tx, mut ws_rx) = ws_stream.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(OUTBOUND_QUEUE_CAPACITY * 4);
    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<String>(OUTBOUND_QUEUE_CAPACITY * 4);

    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if ws_tx
                .send(TungsteniteMessage::Text(text.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let reader = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(
                Duration::from_secs(DISTRIBUTED_NODE_TIMEOUT_SECS),
                ws_rx.next(),
            )
            .await
            {
                Ok(Some(Ok(TungsteniteMessage::Text(t)))) => {
                    if in_tx.send(t.to_string()).await.is_err() {
                        break;
                    }
                }
                Ok(Some(Ok(TungsteniteMessage::Ping(_) | TungsteniteMessage::Pong(_)))) => continue,
                _ => break,
            }
        }
    });

    run_ws_link_common(&link_id, out_tx, in_rx, state, runtime).await;

    reader.abort();
    writer.abort();
    Ok(())
}

async fn run_ws_link_common(
    link_id: &str,
    out_tx: tokio::sync::mpsc::Sender<String>,
    mut in_rx: tokio::sync::mpsc::Receiver<String>,
    state: &AppState,
    runtime: &Arc<P2pRuntime>,
) {
    let mut forward_rx = runtime.forward_tx.subscribe();

    // Send initial Heartbeat + SnapshotRequest on this link, and announce our local snapshot.
    for kind in [EnvelopeKind::Heartbeat, EnvelopeKind::SnapshotRequest] {
        let env = P2pEnvelope {
            version: PROTOCOL_VERSION,
            envelope_id: Uuid::new_v4().to_string(),
            source_node_id: state.node_id.clone(),
            target_node_id: None,
            snapshot_id: None,
            hop_count: 0,
            kind,
            message: None,
        };
        if send_envelope_ws(&out_tx, &env).await.is_err() {
            return;
        }
    }
    spawn_local_snapshot(state.clone(), runtime.clone(), None);

    let link_id_owned = link_id.to_string();
    let out_tx_fwd = out_tx.clone();
    let state_fwd = state.clone();
    let runtime_fwd = runtime.clone();
    let forwarder = tokio::spawn(async move {
        loop {
            match forward_rx.recv().await {
                Ok(item) => {
                    if item.origin_link_id.as_deref() == Some(link_id_owned.as_str()) {
                        continue;
                    }
                    if send_envelope_ws(&out_tx_fwd, &item.envelope).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    eprintln!(
                        "P2P DISTRIBUTED [{link_id_owned}]: Link lagged by {skipped} event(s); re-syncing snapshot"
                    );
                    spawn_local_snapshot(state_fwd.clone(), runtime_fwd.clone(), None);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    while let Some(raw) = in_rx.recv().await {
        if raw.is_empty() {
            continue;
        }
        if let Some(envelope) = accept_ws_chunk(&raw, &runtime.assemblies).await {
            handle_envelope(
                envelope,
                Some(link_id.to_string()),
                None,
                state,
                runtime,
            )
            .await;
        }
    }

    forwarder.abort();
}

async fn send_envelope_ws(
    out_tx: &tokio::sync::mpsc::Sender<String>,
    envelope: &P2pEnvelope,
) -> Result<(), ()> {
    let payload = serde_json::to_string(envelope).map_err(|_| ())?;
    if payload.len() > MAX_FRAME_SIZE {
        return Ok(());
    }
    let transmission_id = Uuid::new_v4().to_string();
    let parts = split_string(&payload, WS_CHUNK_SIZE);
    let total = parts.len();
    for (index, part) in parts.into_iter().enumerate() {
        let wire = serde_json::to_string(&P2pChunk {
            version: PROTOCOL_VERSION,
            sender: envelope.source_node_id.clone(),
            transmission_id: transmission_id.clone(),
            index,
            total,
            payload: part,
        })
        .map_err(|_| ())?;
        out_tx.send(wire).await.map_err(|_| ())?;
    }
    Ok(())
}

// ---- Envelope & Snapshot Processing ----

pub(crate) async fn handle_envelope(
    envelope: P2pEnvelope,
    origin_link_id: Option<String>,
    origin_udp_addr: Option<SocketAddr>,
    state: &AppState,
    runtime: &Arc<P2pRuntime>,
) {
    if envelope.version != PROTOCOL_VERSION
        || envelope.source_node_id == state.node_id
        || Uuid::parse_str(&envelope.source_node_id).is_err()
        || envelope.hop_count > MAX_HOPS
    {
        return;
    }

    // Deduplicate by envelope_id so multi-path (UDP + WS + transitive hops)
    // never processes or re-forwards the same envelope twice.
    if !runtime
        .seen_envelopes
        .lock()
        .await
        .remember(&envelope.envelope_id)
    {
        return;
    }

    // Targeted snapshot traffic still proves that its source node is alive.
    runtime
        .last_seen
        .lock()
        .await
        .insert(envelope.source_node_id.clone(), Instant::now());

    // Direct UDP peer tracking: only update direct UDP peer mapping when
    // received with hop_count == 0 to avoid poisoning with intermediate relay IPs.
    if let Some(src_addr) = origin_udp_addr
        && envelope.hop_count == 0
    {
        runtime
            .active_udp_peers
            .lock()
            .await
            .insert(envelope.source_node_id.clone(), (src_addr, Instant::now()));
    }

    // Transitive mesh forwarding: if this envelope is either broadcast or
    // targeted at another node, forward it with hop_count + 1 to other links.
    let is_for_other_node = envelope
        .target_node_id
        .as_ref()
        .is_some_and(|target| target != &state.node_id);

    if envelope.hop_count < MAX_HOPS {
        let mut forwarded = envelope.clone();
        forwarded.hop_count += 1;
        let _ = runtime.forward_tx.send(ForwardedEnvelope {
            origin_link_id,
            origin_udp_addr,
            envelope: forwarded,
        });
    }

    if is_for_other_node {
        return;
    }

    match envelope.kind {
        EnvelopeKind::Heartbeat => {}
        EnvelopeKind::SnapshotRequest => {
            spawn_local_snapshot(
                state.clone(),
                runtime.clone(),
                Some(envelope.source_node_id),
            );
        }
        EnvelopeKind::SnapshotStart => {
            let Some(snapshot_id) = valid_snapshot_id(envelope.snapshot_id) else {
                return;
            };
            let mut snapshots = runtime.snapshots.lock().await;
            let key = (envelope.source_node_id, snapshot_id);
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
                return;
            };
            let mut snapshot_guard = runtime.snapshots.lock().await;
            let snapshot =
                snapshot_guard.remove(&(envelope.source_node_id.clone(), snapshot_id));
            let Some(snapshot) = snapshot else {
                return;
            };
            let affected_rooms =
                reconcile_distributed_node(state, &envelope.source_node_id, &snapshot.users)
                    .await;
            drop(snapshot_guard);
            for room_id in affected_rooms {
                schedule_empty_room_cleanup(state, &room_id).await;
            }
        }
        EnvelopeKind::Event => {
            let Some(message) = envelope.message else {
                return;
            };
            if !is_valid_distributed_message(&message) {
                return;
            }
            let user_key = (
                message.room_id.clone(),
                message.channel_id.clone(),
                message.user_id.clone(),
            );
            let mut record_snapshot_user = false;
            let mut validated_snapshot_id = None;
            if let Some(snapshot_id) = envelope.snapshot_id {
                let Some(snapshot_id) = valid_snapshot_id(Some(snapshot_id)) else {
                    return;
                };
                let mut snapshots = runtime.snapshots.lock().await;
                let Some(snapshot) =
                    snapshots.get_mut(&(envelope.source_node_id.clone(), snapshot_id.clone()))
                else {
                    return;
                };
                snapshot.last_activity = Instant::now();
                if snapshot
                    .superseded_channels
                    .contains(&(message.room_id.clone(), message.channel_id.clone()))
                {
                    return;
                }
                if message.msg_type == "user-joined" {
                    if snapshot.removed_users.contains(&user_key) {
                        return;
                    }
                    record_snapshot_user = true;
                    validated_snapshot_id = Some(snapshot_id);
                }
            } else {
                let is_presence = matches!(
                    message.msg_type.as_str(),
                    "user-joined" | "user-left" | "user-kicked" | "user-banned" | "user-resumed"
                );
                let is_channel_change = matches!(
                    message.msg_type.as_str(),
                    "rename-channel" | "delete-channel"
                );
                if is_presence || is_channel_change {
                    let mut snapshots = runtime.snapshots.lock().await;
                    for ((source, _), snapshot) in snapshots.iter_mut() {
                        if is_presence {
                            if message.msg_type == "user-resumed" {
                                snapshot.last_activity = Instant::now();
                                if source == &envelope.source_node_id {
                                    snapshot.removed_users.remove(&user_key);
                                    snapshot.users.insert(user_key.clone());
                                } else {
                                    let stale: Vec<_> = snapshot
                                        .users
                                        .iter()
                                        .filter(|(r, _, u)| {
                                            r == &message.room_id && u == &message.user_id
                                        })
                                        .cloned()
                                        .collect();
                                    for k in stale {
                                        snapshot.users.remove(&k);
                                        snapshot.removed_users.insert(k);
                                    }
                                }
                            } else {
                                let applies = matches!(
                                    message.msg_type.as_str(),
                                    "user-kicked" | "user-banned"
                                ) || source == &envelope.source_node_id;
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
                        }
                        if is_channel_change {
                            snapshot.last_activity = Instant::now();
                            let old_channel =
                                (message.room_id.clone(), message.channel_id.clone());
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
            let processed =
                process_distributed_message(message, &envelope.source_node_id, state).await;
            if processed && record_snapshot_user && let Some(snap_id) = validated_snapshot_id {
                let mut snapshots = runtime.snapshots.lock().await;
                if let Some(snapshot) =
                    snapshots.get_mut(&(envelope.source_node_id, snap_id))
                {
                    if !snapshot.removed_users.contains(&user_key) {
                        snapshot.users.insert(user_key);
                    }
                }
            }
        }
    }
}

fn valid_snapshot_id(snapshot_id: Option<String>) -> Option<String> {
    snapshot_id.filter(|id| Uuid::parse_str(id).is_ok())
}

async fn accept_ws_chunk(
    raw: &str,
    assemblies: &Mutex<Assemblies>,
) -> Option<P2pEnvelope> {
    let chunk: P2pChunk = serde_json::from_str(raw).ok()?;
    accept_parsed_chunk(chunk, MAX_WS_CHUNKS, WS_CHUNK_SIZE, assemblies).await
}

async fn accept_parsed_chunk(
    chunk: P2pChunk,
    max_chunks: usize,
    max_chunk_size: usize,
    assemblies: &Mutex<Assemblies>,
) -> Option<P2pEnvelope> {
    if chunk.version != PROTOCOL_VERSION
        || Uuid::parse_str(&chunk.sender).is_err()
        || Uuid::parse_str(&chunk.transmission_id).is_err()
        || chunk.total == 0
        || chunk.total > max_chunks
        || chunk.index >= chunk.total
        || chunk.payload.len() > max_chunk_size
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
        if assembly.bytes > MAX_FRAME_SIZE {
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
    let envelope: P2pEnvelope = serde_json::from_str(&payload).ok()?;
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
    state: AppState,
    runtime: Arc<P2pRuntime>,
    target_node_id: Option<String>,
) {
    tokio::spawn(async move {
        let Ok(_permit) = runtime.snapshot_permits.clone().acquire_owned().await else {
            return;
        };
        publish_local_snapshot(&state, &runtime, target_node_id).await;
    });
}

async fn publish_local_snapshot(
    state: &AppState,
    runtime: &P2pRuntime,
    target_node_id: Option<String>,
) {
    let snapshot_id = Uuid::new_v4().to_string();
    let start_env = P2pEnvelope {
        version: PROTOCOL_VERSION,
        envelope_id: Uuid::new_v4().to_string(),
        source_node_id: state.node_id.clone(),
        target_node_id: target_node_id.clone(),
        snapshot_id: Some(snapshot_id.clone()),
        hop_count: 0,
        kind: EnvelopeKind::SnapshotStart,
        message: None,
    };
    runtime
        .seen_envelopes
        .lock()
        .await
        .remember(&start_env.envelope_id);
    let _ = runtime.forward_tx.send(ForwardedEnvelope {
        origin_link_id: None,
        origin_udp_addr: None,
        envelope: start_env,
    });

    let mut snapshot = local_snapshot_stream(state.clone());
    while let Some(message) = snapshot.recv().await {
        let event_env = P2pEnvelope {
            version: PROTOCOL_VERSION,
            envelope_id: Uuid::new_v4().to_string(),
            source_node_id: state.node_id.clone(),
            target_node_id: target_node_id.clone(),
            snapshot_id: Some(snapshot_id.clone()),
            hop_count: 0,
            kind: EnvelopeKind::Event,
            message: Some(message),
        };
        runtime
            .seen_envelopes
            .lock()
            .await
            .remember(&event_env.envelope_id);
        let _ = runtime.forward_tx.send(ForwardedEnvelope {
            origin_link_id: None,
            origin_udp_addr: None,
            envelope: event_env,
        });
    }

    let end_env = P2pEnvelope {
        version: PROTOCOL_VERSION,
        envelope_id: Uuid::new_v4().to_string(),
        source_node_id: state.node_id.clone(),
        target_node_id,
        snapshot_id: Some(snapshot_id),
        hop_count: 0,
        kind: EnvelopeKind::SnapshotEnd,
        message: None,
    };
    runtime
        .seen_envelopes
        .lock()
        .await
        .remember(&end_env.envelope_id);
    let _ = runtime.forward_tx.send(ForwardedEnvelope {
        origin_link_id: None,
        origin_udp_addr: None,
        envelope: end_env,
    });
}

pub(crate) fn local_snapshot_stream(
    state: AppState,
) -> tokio::sync::mpsc::Receiver<DistributedMessage> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let rooms = state.rooms.lock().await.clone();
        let times = state.channel_creation_times.lock().await.clone();

        // Collect room admin snapshots & pending lobby knocks so joining nodes
        // immediately know stage/lobby settings, creator key hashes, admins,
        // bans, speakers, and pending knocks without needing a central store.
        let (admin_snapshots, knock_snapshots) = {
            let admins_lock = state.admin_rooms.lock().await;
            let mut admin_snaps = Vec::new();
            let mut knock_snaps = Vec::new();
            for (room_id, admin) in admins_lock.iter() {
                if admin.is_meaningful() {
                    let mut dist_data = admin.public_state();
                    if let Some(ref hash) = admin.key_hash {
                        dist_data["keyHash"] = serde_json::json!(hash);
                    }
                    admin_snaps.push((room_id.clone(), dist_data));
                }
                if !admin.pending.is_empty() {
                    let local_pending: Vec<_> = admin
                        .pending
                        .iter()
                        .map(|(id, p)| {
                            serde_json::json!({
                                "id": id,
                                "nickname": p.nickname,
                                "channel": p.channel,
                            })
                        })
                        .collect();
                    knock_snaps.push((room_id.clone(), local_pending));
                }
            }
            (admin_snaps, knock_snaps)
        };

        for (room_id, dist_data) in admin_snapshots {
            let msg = DistributedMessage {
                msg_type: "admin-update".into(),
                room_id,
                channel_id: String::new(),
                user_id: String::new(),
                msg_id: Uuid::new_v4().to_string(),
                status: None,
                data: Some(dist_data),
                signal_msg: None,
            };
            if tx.send(msg).await.is_err() {
                return;
            }
        }

        for (room_id, pending) in knock_snapshots {
            let msg = DistributedMessage {
                msg_type: "knock-update".into(),
                room_id,
                channel_id: String::new(),
                user_id: String::new(),
                msg_id: Uuid::new_v4().to_string(),
                status: None,
                data: Some(serde_json::json!({ "pending": pending })),
                signal_msg: None,
            };
            if tx.send(msg).await.is_err() {
                return;
            }
        }

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

async fn cleanup_stale_nodes(state: &AppState, runtime: &P2pRuntime) {
    let stale: Vec<String> = {
        let mut last_seen = runtime.last_seen.lock().await;
        let stale: Vec<_> = last_seen
            .iter()
            .filter(|(_, seen)| seen.elapsed() > Duration::from_secs(DISTRIBUTED_NODE_TIMEOUT_SECS))
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node_id in &stale {
            last_seen.remove(node_id);
        }
        stale
    };

    for node_id in stale {
        runtime
            .active_udp_peers
            .lock()
            .await
            .remove(&node_id);
        runtime
            .snapshots
            .lock()
            .await
            .retain(|(source_node_id, _), _| source_node_id != &node_id);
        let affected_rooms = cleanup_distributed_node(state, &node_id).await;
        for room_id in affected_rooms {
            schedule_empty_room_cleanup(state, &room_id).await;
        }
        eprintln!("P2P DISTRIBUTED: Node {node_id} timed out; removed its stale presence");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_lists_are_split_trimmed_and_deduplicated() {
        let peers = parse_peer_list([
            "wss://one.example:3000, wss://two.example:3000",
            "wss://one.example:3000;\nudp://192.0.2.10:3000",
        ]);
        assert_eq!(
            peers,
            vec![
                "udp://192.0.2.10:3000",
                "wss://one.example:3000",
                "wss://two.example:3000",
            ]
        );
    }

    #[test]
    fn ws_peer_urls_normalize_schemes_and_default_path() {
        assert_eq!(
            normalize_ws_peer_url("https://node.example.com:8443", "ws").as_deref(),
            Some("wss://node.example.com:8443/cluster-ws")
        );
        assert_eq!(
            normalize_ws_peer_url("192.168.1.25:3000", "ws").as_deref(),
            Some("ws://192.168.1.25:3000/cluster-ws")
        );
        assert!(normalize_ws_peer_url("udp://192.168.1.25:3000", "ws").is_none());
    }

    #[test]
    fn resolve_udp_peer_handles_schemes_and_trailing_slashes() {
        let expected: SocketAddr = "127.0.0.1:4567".parse().unwrap();
        assert_eq!(resolve_udp_peer("127.0.0.1:4567"), vec![expected]);
        assert_eq!(resolve_udp_peer("udp://127.0.0.1:4567/"), vec![expected]);
        assert_eq!(resolve_udp_peer("ws://127.0.0.1:4567/cluster-ws"), vec![expected]);
        assert!(resolve_udp_peer("").is_empty());
    }

    #[test]
    fn stun_xor_mapped_address_parses_ipv4_reflexive_endpoint() {
        let tx_id = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let req = build_stun_binding_request(tx_id);
        assert_eq!(&req[0..2], &[0x00, 0x01]);
        assert_eq!(&req[4..8], &STUN_MAGIC_COOKIE.to_be_bytes());

        // Build a synthetic STUN Binding Success Response (0x0101) with
        // XOR-MAPPED-ADDRESS (0x0020) for 203.0.113.42:54321.
        let ip = Ipv4Addr::new(203, 0, 113, 42);
        let port = 54321u16;
        let xport = port ^ ((STUN_MAGIC_COOKIE >> 16) as u16);
        let xip = u32::from(ip) ^ STUN_MAGIC_COOKIE;

        let mut resp = Vec::new();
        resp.extend_from_slice(&0x0101u16.to_be_bytes()); // Binding Response
        resp.extend_from_slice(&12u16.to_be_bytes()); // 12-byte body
        resp.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
        resp.extend_from_slice(&tx_id);
        // Attribute: XOR-MAPPED-ADDRESS (0x0020), len = 8
        resp.extend_from_slice(&0x0020u16.to_be_bytes());
        resp.extend_from_slice(&8u16.to_be_bytes());
        resp.push(0x00);
        resp.push(0x01); // IPv4
        resp.extend_from_slice(&xport.to_be_bytes());
        resp.extend_from_slice(&xip.to_be_bytes());

        let parsed = parse_stun_binding_response(&resp).expect("valid STUN response");
        assert_eq!(
            parsed,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 42), 54321))
        );
    }

    #[test]
    fn udp_packets_authenticate_and_reject_tampering_or_wrong_key() {
        let key_a = derive_udp_mac_key("secret-alpha");
        let key_b = derive_udp_mac_key("secret-beta");
        let pkt = UdpWirePacket::Hello {
            node_id: Uuid::new_v4().to_string(),
            udp_port: 3000,
            http_port: 3000,
            peers: vec![],
            ws_urls: vec![],
        };
        let mut wire = encode_udp_packet(&key_a, &pkt).expect("encodes");
        assert!(decode_udp_packet(&key_a, &wire).is_some());
        assert!(decode_udp_packet(&key_b, &wire).is_none());

        // Tamper with payload byte
        let last = wire.len() - 1;
        wire[last] ^= 0x01;
        assert!(decode_udp_packet(&key_a, &wire).is_none());
    }

    #[test]
    fn chunks_round_trip_unicode_without_splitting_code_points() {
        let input = format!("{}🙂{}", "a".repeat(10), "b".repeat(10));
        let chunks = split_string(&input, 12);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 12));
        assert_eq!(chunks.concat(), input);
    }

    fn make_test_config(key: &str, udp_port: u16, peers: Vec<String>) -> P2pConfig {
        P2pConfig {
            cluster_key: key.to_string(),
            peers,
            dht_enabled: false,
            scheme: "ws".to_string(),
            http_port: 0,
            udp_port,
            public_ws_url: None,
            stun_servers: vec![],
        }
    }

    fn make_test_state(config: &P2pConfig) -> AppState {
        let (distributed_tx, _) = tokio::sync::broadcast::channel(256);
        let runtime = P2pRuntime::new(config);
        AppState {
            rooms: Arc::new(Mutex::new(HashMap::new())),
            room_cleanup_generations: Arc::new(Mutex::new(HashMap::new())),
            room_creation_password: None,
            distributed_tx,
            remote_users: Arc::new(Mutex::new(HashMap::new())),
            remote_user_owners: Arc::new(Mutex::new(HashMap::new())),
            channel_creation_times: Arc::new(Mutex::new(HashMap::new())),
            allowed_url: None,
            recent_distributed_msg_ids: Arc::new(Mutex::new(HashSet::new())),
            distributed_msg_history: Arc::new(Mutex::new(VecDeque::new())),
            node_id: Uuid::new_v4().to_string(),
            admin_rooms: Arc::new(Mutex::new(HashMap::new())),
            cluster_key: Some(config.cluster_key.clone()),
            p2p_runtime: Some(runtime),
            resume_records: Arc::new(Mutex::new(HashMap::new())),
            away_seats: Arc::new(Mutex::new(HashMap::new())),
            departure_gen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            server_secret: Arc::new(derive_server_secret(&config.cluster_key)),
        }
    }

    #[tokio::test]
    async fn udp_p2p_synchronizes_channels_users_and_admin_state_without_redis() {
        let cluster_key = "test-p2p-secret-key";

        // Bind two ephemeral UDP sockets on 127.0.0.1 to get free ports.
        let probe_a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port_a = probe_a.local_addr().unwrap().port();
        drop(probe_a);

        let probe_b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port_b = probe_b.local_addr().unwrap().port();
        drop(probe_b);

        let cfg_a = make_test_config(cluster_key, port_a, vec![format!("udp://127.0.0.1:{port_b}")]);
        let cfg_b = make_test_config(cluster_key, port_b, vec![format!("udp://127.0.0.1:{port_a}")]);
        let state_a = make_test_state(&cfg_a);
        let state_b = make_test_state(&cfg_b);
        let alice_id = Uuid::new_v4().to_string();

        // Pre-populate state_a with a room, channel, user, and stage/lobby admin state.
        {
            let (tx, _rx) = tokio::sync::mpsc::channel(8);
            let status = crate::state::UserStatus {
                nickname: "Alice".to_string(),
                avatar: None,
                is_gif: false,
                static_frame: None,
                is_muted: false,
                is_deafened: false,
                is_screen_sharing: false,
                is_mobile: false,
                profile_rev: 1,
            };
            let mut rooms = state_a.rooms.lock().await;
            let room = rooms.entry("alpha".to_string()).or_default();
            let chan = room.entry("General".to_string()).or_default();
            chan.insert(alice_id.clone(), (tx, status));
        }
        {
            let mut admins = state_a.admin_rooms.lock().await;
            let mut admin = crate::state::RoomAdmin::default();
            admin.kind = crate::state::RoomKind::Stage;
            admin.lobby = true;
            admin.key_hash = Some("creator-hash-xyz".to_string());
            admin.speakers.insert(alice_id.clone());
            admins.insert("alpha".to_string(), admin);
        }

        let rt_a = state_a.p2p_runtime.clone().unwrap();
        let rt_b = state_b.p2p_runtime.clone().unwrap();
        spawn_p2p_distributed(state_a.clone(), cfg_a, rt_a);
        spawn_p2p_distributed(state_b.clone(), cfg_b, rt_b);

        // Wait up to 4 seconds for state_b to receive the snapshot from state_a over UDP P2P.
        let synced = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let has_user = {
                    let remote = state_b.remote_users.lock().await;
                    remote
                        .get("alpha")
                        .and_then(|r| r.get("General"))
                        .and_then(|c| c.get(&alice_id))
                        .map(|s| s.nickname == "Alice")
                        .unwrap_or(false)
                };
                let has_admin = {
                    let admins = state_b.admin_rooms.lock().await;
                    admins
                        .get("alpha")
                        .map(|a| {
                            a.kind == crate::state::RoomKind::Stage
                                && a.lobby
                                && a.key_hash.as_deref() == Some("creator-hash-xyz")
                                && a.speakers.contains(&alice_id)
                        })
                        .unwrap_or(false)
                };
                if has_user && has_admin {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            synced.is_ok(),
            "UDP P2P snapshot should synchronize channels, users, and admin state"
        );
    }

    #[tokio::test]
    async fn multi_hop_forwarding_relays_events_between_non_adjacent_peers() {
        let key = "mesh-relay-key";
        let cfg = make_test_config(key, 0, vec![]);
        let state_a = make_test_state(&cfg);
        let state_b = make_test_state(&cfg);
        let state_c = make_test_state(&cfg);

        let rt_b = state_b.p2p_runtime.clone().unwrap();
        let rt_c = state_c.p2p_runtime.clone().unwrap();

        let mut forward_rx_b = rt_b.forward_tx.subscribe();

        let env = P2pEnvelope {
            version: PROTOCOL_VERSION,
            envelope_id: Uuid::new_v4().to_string(),
            source_node_id: state_a.node_id.clone(),
            target_node_id: None,
            snapshot_id: None,
            hop_count: 0,
            kind: EnvelopeKind::Event,
            message: Some(DistributedMessage {
                msg_type: "channel-upsert".to_string(),
                room_id: "relayed-room".to_string(),
                channel_id: "Stage".to_string(),
                user_id: state_a.node_id.clone(),
                msg_id: Uuid::new_v4().to_string(),
                status: None,
                data: Some(serde_json::json!({ "createdAt": 1700000000u64 })),
                signal_msg: None,
            }),
        };

        // Process at node_b (which forwards via forward_tx)
        handle_envelope(
            env,
            Some("link-a-b".to_string()),
            None,
            &state_b,
            &rt_b,
        )
        .await;

        let forwarded = forward_rx_b.recv().await.expect("forwarded by node_b");
        assert_eq!(forwarded.envelope.hop_count, 1);
        assert_eq!(forwarded.envelope.source_node_id, state_a.node_id);

        // Deliver the forwarded envelope over a simulated WebSocket link from node_b to node_c
        let (tx_b_to_c, mut rx_b_to_c) = tokio::sync::mpsc::channel(16);
        send_envelope_ws(&tx_b_to_c, &forwarded.envelope)
            .await
            .expect("send chunks");
        let raw_chunk = rx_b_to_c.recv().await.expect("ws chunk");
        let assemblies_c = Mutex::new(HashMap::new());
        let decoded_at_c = accept_ws_chunk(&raw_chunk, &assemblies_c)
            .await
            .expect("reassembled at node_c");
        handle_envelope(
            decoded_at_c,
            Some("link-b-c".to_string()),
            None,
            &state_c,
            &rt_c,
        )
        .await;

        let times_c = state_c.channel_creation_times.lock().await;
        assert_eq!(
            times_c
                .get("relayed-room")
                .and_then(|r| r.get("Stage"))
                .copied(),
            Some(1700000000u64),
            "node_c should materialize channel-upsert relayed from node_a through node_b"
        );
    }

    #[tokio::test]
    async fn signed_resume_tokens_verify_across_nodes_without_redis() {
        let key = "shared-cluster-key";
        let cfg = make_test_config(key, 0, vec![]);
        let state_a = make_test_state(&cfg);
        let state_b = make_test_state(&cfg);

        let token =
            crate::admin::issue_resume_token(&state_a, "room-1", "user-42", "General").await;
        assert!(
            crate::admin::validate_resume_token(
                &state_b,
                "room-1",
                "user-42",
                "General",
                Some(&token)
            )
            .await,
            "node_b should validate resume token issued by node_a using shared KEY secret"
        );
        assert!(
            !crate::admin::validate_resume_token(
                &state_b,
                "room-2",
                "user-42",
                "General",
                Some(&token)
            )
            .await,
            "resume token bound to room-1 must not validate for room-2"
        );

        // Revoking on node_b should block subsequent reuse on node_b.
        crate::admin::delete_resume_token(&state_b, "room-1", "user-42").await;
        assert!(
            !crate::admin::validate_resume_token(
                &state_b,
                "room-1",
                "user-42",
                "General",
                Some(&token)
            )
            .await,
            "revoked token must be rejected"
        );
    }

    #[tokio::test]
    async fn mainline_dht_bep44_mutable_record_round_trips_on_local_testnet() {
        let testnet = mainline::Testnet::builder(10).build().unwrap();
        let dht_a = mainline::Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap()
            .as_async();
        let dht_b = mainline::Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap()
            .as_async();

        let key = "dht-test-cluster-key";
        let signer = derive_dht_signing_key(key);
        let verify_key = signer.verifying_key().to_bytes();
        let salt = b"slot-0";

        let record = DhtNodeRecord {
            node_id: Uuid::new_v4().to_string(),
            udp_addrs: vec!["198.51.100.7:4400".parse().unwrap()],
            ws_urls: vec!["wss://node.example.com:4400/cluster-ws".to_string()],
            ts: current_unix_secs(),
        };
        let payload = serde_json::to_vec(&record).unwrap();
        let item = mainline::MutableItem::new(signer, &payload, 1, Some(salt));
        dht_a.put_mutable(item, None).await.expect("put_mutable succeeds");

        let mut stream = dht_b.get_mutable(&verify_key, Some(salt), None);
        let fetched = stream.next().await.expect("get_mutable returns item");
        let decoded: DhtNodeRecord =
            serde_json::from_slice(fetched.value()).expect("valid record");
        assert_eq!(decoded.node_id, record.node_id);
        assert_eq!(decoded.udp_addrs, record.udp_addrs);
        assert_eq!(decoded.ws_urls, record.ws_urls);
    }
}

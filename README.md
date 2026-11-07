### How to use:

1. Install rust!

2.  From the repo dir just run `cargo build --release`.

3.  A standalone executable for your platform will be generated in ./target/release/

4.  Enjoy!

### Notes:

It uses port 3000 TCP (for the web interface) but can be changed by specifying the PORT env variable to a different value.

### TURN Configuration:

RustRooms requires a TURN server for WebRTC connections to work properly, especially when users are behind restrictive NATs or firewalls. You can configure a third-party TURN server using the following environment variables:

*   `TURN_URL`: The TURN server URL (e.g., `turn:your-turn-server.com:3478`)
*   `TURN_USERNAME`: The TURN server username
*   `TURN_CREDENTIAL`: The TURN server password/credential

For self-hosted TURN servers, you can use [coturn](https://github.com/coturn/coturn) or use a hosted service like [metered.ca](https://www.metered.ca).

### Security:

For a production deployment, it is **highly recommended** to set the following environment variable:

*   `ROOM_CREATION_PASSWORD`: Set this to a strong password to prevent unauthorized room creation. If this is not set, anyone can create rooms.
*   `URL`: When set, restricts access to only requests whose `Host` header matches this value. Useful for preventing access via raw IP or alternative domain names. The value is automatically normalized (scheme and path are stripped).

### Decentralized P2P Distributed Mode

Set the same `KEY` on every instance to form one logical RustRooms deployment without any centralized Redis instance or coordination server. Room/channel metadata, empty channels, presence and profile state, admin/stage/lobby state, moderation events, resume tokens, media toggles, and WebRTC signaling are synchronized across peers.

```text
# Automatic discovery via BitTorrent Mainline DHT + UDP NAT hole-punching + LAN broadcast
KEY=a-long-random-shared-secret

# Optional: add static WebSocket or UDP peers (works alongside or without DHT)
CLUSTER_PEERS=wss://peer1.example.com,udp://203.0.113.10:3000
```

How decentralized discovery and transport work:

1. **BitTorrent Mainline DHT (`mainline`)**: Nodes derive info-hashes and a BEP 44 `ed25519` signing key from `KEY` to announce and discover peers' external NAT-mapped UDP ports, LAN endpoints, and WebSocket endpoints on the public Mainline DHT.
2. **STUN + UDP Hole-Punching + Reliable Chunking**: Each node queries public STUN servers on its P2P UDP socket to learn its reflexive external `IP:port`, advertises it via the DHT, and performs simultaneous authenticated UDP hole-punching (`HMAC-SHA256` under `KEY`) so nodes behind NAT/firewalls without inbound TCP ports open can connect directly P2P. Large payloads (such as avatars and snapshots) are split into MTU-safe chunks with per-chunk ACKs and selective retransmission.
3. **Zero-Config LAN Broadcast & Peer Exchange (PEX)**: Nodes on the same LAN/subnet discover each other immediately via authenticated UDP broadcast beacons, and connected peers exchange known peer endpoints via PEX.
4. **WebSocket Mesh (`/cluster-ws`) & Transitive Multi-Hop Routing**: When nodes can reach each other over HTTP/WebSocket (via LAN, port-forwarding, or `CLUSTER_PEERS`), they also stream over `/cluster-ws`. Envelopes are forwarded transitively across up to 8 hops with `envelopeId` deduplication, so any shared peer automatically bridges nodes that cannot punch directly to each other.

Configuration:

* `KEY` (or `CLUSTER_KEY`): Shared secret enabling decentralized P2P mode, authenticating all UDP/WebSocket peer traffic, and deriving the cluster signing secret for admin/creator keys and resume tokens.
* `CLUSTER_PEERS` (or `PEERS`): Optional comma-, semicolon-, or newline-separated list of static peer URLs (`ws://`, `wss://`, `http://`, `https://`, `udp://host:port`, or `host:port`).
* `CLUSTER_DHT`: Enables BitTorrent Mainline DHT discovery (`true` by default; set to `false` for offline/static-mesh-only deployments).
* `CLUSTER_UDP_PORT`: Optional UDP port for P2P hole-punching and mesh traffic (defaults to `PORT`, falling back to an OS-assigned port if busy).
* `CLUSTER_SCHEME`: WebSocket scheme for DHT/LAN peers without an explicit scheme (`ws` by default, or `wss`).
* `CLUSTER_PUBLIC_URL`: Optional externally reachable WebSocket URL for this node to advertise in DHT/PEX records.
* `STUN_SERVERS`: Optional comma-separated STUN servers for reflexive UDP address discovery (defaults to Google and Cloudflare public STUN servers plus `TURN_URL` if configured).

### Issues & Bug Reports

If you find a bug or issue, please [open an issue on GitHub](https://github.com/dillfrescott/RustRooms/issues).

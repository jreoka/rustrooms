mod admin;
mod distributed;
mod redis_distributed;
mod rooms;
mod routes;
mod state;
mod web_assets;

use axum::{Router, routing::get};
use redis_distributed::{parse_redis_urls, spawn_redis_distributed};
use rooms::channel_status;
use routes::*;
use state::*;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::Mutex;
use uuid::Uuid;
use web_assets::*;
#[tokio::main]
async fn main() {
    // Install the provider before any rediss:// connection initializes rustls.
    // This is explicit because redis's rustls feature does not select one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let rooms: RoomMap = Arc::new(Mutex::new(HashMap::new()));
    let room_cleanup_generations: RoomCleanupMap = Arc::new(Mutex::new(HashMap::new()));
    let channel_creation_times: ChannelCreationTimesMap = Arc::new(Mutex::new(HashMap::new()));

    let room_creation_password = std::env::var("ROOM_CREATION_PASSWORD")
        .ok()
        .map(|p| p.trim().to_string())
        .filter(|s| !s.is_empty());
    let redis_urls = parse_redis_urls(std::env::var("REDIS_URL").ok().iter().map(String::as_str));
    let redis_prefix = std::env::var("REDIS_PREFIX")
        .ok()
        .map(|prefix| prefix.trim().to_string())
        .filter(|prefix| !prefix.is_empty())
        .unwrap_or_else(|| "rustrooms".to_string());
    let allowed_url = std::env::var("URL")
        .ok()
        .and_then(|url| normalize_configured_host(&url));
    let (distributed_tx, _) =
        tokio::sync::broadcast::channel::<String>(DISTRIBUTED_BROADCAST_CAPACITY);
    let remote_users: RemoteUsersMap = Arc::new(Mutex::new(HashMap::new()));
    let remote_user_owners: RemoteUserOwnersMap = Arc::new(Mutex::new(HashMap::new()));

    if !redis_urls.is_empty() {
        println!(
            "REDIS DISTRIBUTED: Enabled with {} Redis endpoint(s) (namespace: {redis_prefix})",
            redis_urls.len()
        );
    }

    if let Some(ref url) = allowed_url {
        println!(
            "URL RESTRICTION: Enabled - only allowing access from {}",
            url
        );
    }

    let node_id = Uuid::new_v4().to_string();

    let server_secret: Arc<Vec<u8>> = Arc::new(
        [
            *Uuid::new_v4().as_bytes(),
            *Uuid::new_v4().as_bytes(),
        ]
        .concat(),
    );
    let admin_rooms: AdminMap = Arc::new(Mutex::new(HashMap::new()));

    let state = AppState {
        rooms,
        room_cleanup_generations,
        room_creation_password,
        distributed_tx,
        remote_users,
        remote_user_owners,
        channel_creation_times,
        allowed_url,
        recent_distributed_msg_ids: Arc::new(Mutex::new(HashSet::new())),
        distributed_msg_history: Arc::new(Mutex::new(VecDeque::new())),
        node_id,
        admin_rooms,
        server_secret,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/new", get(new_room))
        .route("/new/", get(redirect_new_trailing_slash))
        .route("/{room_id}", get(index))
        .route("/{room_id}/", get(redirect_room_trailing_slash))
        .route("/{room_id}/{channel_id}", get(index))
        .route(
            "/{room_id}/{channel_id}/",
            get(redirect_channel_trailing_slash),
        )
        .route("/{room_id}/{channel_id}/status", get(channel_status))
        .route("/rnnoise.js", get(rnnoise_js))
        .route("/rnnoise_processor.js", get(rnnoise_processor_js))
        .route("/manifest.json", get(manifest_json))
        .route("/service-worker.js", get(service_worker_js))
        .route("/icon.svg", get(icon_svg))
        .route("/assets/tailwind.min.css", get(tailwind_css))
        .route("/assets/app.css", get(app_css))
        .route("/assets/app.js", get(app_js))
        .route("/assets/croppie.min.js", get(croppie_js))
        .route("/assets/croppie.min.css", get(croppie_css))
        .route("/assets/inter.css", get(inter_css))
        .route(
            "/fonts/inter-cyrillic-ext.woff2",
            get(inter_cyrillic_ext_woff2),
        )
        .route("/fonts/inter-cyrillic.woff2", get(inter_cyrillic_woff2))
        .route("/fonts/inter-greek-ext.woff2", get(inter_greek_ext_woff2))
        .route("/fonts/inter-greek.woff2", get(inter_greek_woff2))
        .route("/fonts/inter-vietnamese.woff2", get(inter_vietnamese_woff2))
        .route("/fonts/inter-latin-ext.woff2", get(inter_latin_ext_woff2))
        .route("/fonts/inter-latin.woff2", get(inter_latin_woff2))
        .route("/ws/{room_id}/{channel_id}", get(ws_handler))
        .route(
            "/ws/{room_id}/{channel_id}/",
            get(redirect_ws_trailing_slash),
        )
        .with_state(state.clone());

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);

    let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ERROR: Failed to bind to port {}: {}", port, e);
            eprintln!("Is the server already running? Try killing the process using this port.");
            std::process::exit(1);
        }
    };
    println!("SERVER RUNNING ON PORT {}", port);

    if !redis_urls.is_empty() {
        spawn_redis_distributed(state.clone(), redis_urls, redis_prefix);
    }

    axum::serve(listener, app).await.unwrap();
}

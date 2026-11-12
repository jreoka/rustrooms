use crate::{rooms::handle_socket, state::*, web_assets::render_html_page};
use axum::{
    extract::{Path, Query, State, ws::WebSocketUpgrade},
    http::{header, uri::Authority, Uri},
    response::{Html, IntoResponse, Redirect},
};
use std::collections::HashMap;
use uuid::Uuid;

fn request_authority(headers: &axum::http::HeaderMap) -> Option<axum::http::uri::Authority> {
    headers.get(header::HOST)?.to_str().ok()?.parse().ok()
}

fn request_host(headers: &axum::http::HeaderMap) -> Option<String> {
    request_authority(headers).map(|authority| authority.host().to_lowercase())
}

fn encode_path_segment(value: &str) -> String {
    let mut url = url::Url::parse("http://localhost/").expect("static base URL is valid");
    url.path_segments_mut()
        .expect("HTTP URLs support path segments")
        .push(value);
    url.path().trim_start_matches('/').to_string()
}

/// Origin scheme for absolute embed URLs: honor a reverse proxy's
/// `X-Forwarded-Proto`, then fall back to https (production default).
fn request_scheme(headers: &axum::http::HeaderMap) -> &'static str {
    if let Some(proto) = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        && let Some(first) = proto.split(',').next()
        && first.trim().eq_ignore_ascii_case("http")
    {
        return "http";
    }
    "https"
}

fn append_query(target: &str, uri: &Uri) -> String {
    if let Some(q) = uri.query().filter(|q| !q.is_empty())
        && !q.chars().any(char::is_control)
    {
        let candidate = format!("{target}?{q}");
        if header::HeaderValue::try_from(&candidate).is_ok() {
            return candidate;
        }
    }
    target.to_string()
}

pub(crate) fn host_is_allowed(headers: &axum::http::HeaderMap, allowed_host: &str) -> bool {
    request_host(headers).is_some_and(|host| host.eq_ignore_ascii_case(allowed_host))
}

fn origin_matches_request_host(headers: &axum::http::HeaderMap) -> bool {
    let Some(authority) = request_authority(headers) else {
        return false;
    };
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok())
        .and_then(|value| url::Url::parse(value).ok())
    else {
        return false;
    };
    let Some(origin_host) = origin.host_str() else {
        return false;
    };

    let is_loopback = authority.host().eq_ignore_ascii_case("localhost")
        || authority.host() == "127.0.0.1"
        || authority.host() == "[::1]"
        || authority.host() == "::1";

    let expected_scheme = if headers.get("x-forwarded-proto").is_none()
        && is_loopback
        && origin.scheme().eq_ignore_ascii_case("http")
    {
        "http"
    } else {
        request_scheme(headers)
    };

    if !origin.scheme().eq_ignore_ascii_case(expected_scheme) {
        return false;
    }

    let authority_port = authority.port_u16().or(match expected_scheme {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    });

    let origin_port = origin.port_or_known_default();

    let port_matches = match (authority_port, origin_port) {
        (Some(ap), Some(op)) => ap == op,
        (None, None) => true,
        _ => false,
    };
    origin_host.eq_ignore_ascii_case(authority.host()) && port_matches
}

pub(crate) async fn new_room(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Redirect, (axum::http::StatusCode, &'static str)> {
    if let Some(ref allowed_url) = state.allowed_url
        && !host_is_allowed(&headers, allowed_url)
    {
        return Err((axum::http::StatusCode::FORBIDDEN, "Forbidden"));
    }
    if let Some(ref required_pass) = state.room_creation_password {
        match params.get("password") {
            Some(p) if p == required_pass => {}
            _ => return Err((axum::http::StatusCode::UNAUTHORIZED, "Unauthorized")),
        }
    }

    let room_id = if let Some(custom_name) = params.get("name") {
        if custom_name.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            // Validate custom room name: alphanumeric, hyphens, underscores only, max length
            let trimmed = custom_name.trim();
            if trimmed.chars().count() > MAX_ROOM_ID_LEN
                || trimmed.is_empty()
                || !trimmed
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                return Err((
                    axum::http::StatusCode::BAD_REQUEST,
                    "Invalid room name: use only letters, numbers, hyphens, and underscores (max 64 characters)",
                ));
            }
            // Static routes shadow /{room_id}, so these names would redirect
            // or 404 instead of creating a room.
            if matches!(
                trimmed.to_ascii_lowercase().as_str(),
                "new" | "cluster-ws" | "assets" | "fonts" | "ws"
            ) {
                return Err((
                    axum::http::StatusCode::BAD_REQUEST,
                    "That room name is reserved. Please choose another.",
                ));
            }
            trimmed.to_string()
        }
    } else {
        Uuid::new_v4().to_string()
    };

    Ok(Redirect::to(&format!("/{}", encode_path_segment(&room_id))))
}

pub(crate) async fn redirect_room_trailing_slash(
    Path(room_id): Path<String>,
    uri: Uri,
) -> Redirect {
    // Validate before echoing into the Location header: control characters
    // (e.g. %0d%0a) would make Redirect::to panic.
    if is_valid_room_id(&room_id) {
        let path = format!("/{}", encode_path_segment(&room_id));
        Redirect::to(&append_query(&path, &uri))
    } else {
        Redirect::to("/")
    }
}

pub(crate) async fn redirect_channel_trailing_slash(
    Path((room_id, channel_id)): Path<(String, String)>,
    uri: Uri,
) -> Redirect {
    if is_valid_room_id(&room_id)
        && let Some(channel_id) = normalize_channel_id(&channel_id)
    {
        let path = format!(
            "/{}/{}",
            encode_path_segment(&room_id),
            encode_path_segment(&channel_id)
        );
        Redirect::to(&append_query(&path, &uri))
    } else {
        Redirect::to("/")
    }
}

pub(crate) async fn redirect_new_trailing_slash(uri: Uri) -> Redirect {
    Redirect::to(&append_query("/new", &uri))
}

pub(crate) async fn redirect_ws_trailing_slash(
    Path((room_id, channel_id)): Path<(String, String)>,
    uri: Uri,
) -> Redirect {
    if is_valid_room_id(&room_id)
        && let Some(channel_id) = normalize_channel_id(&channel_id)
    {
        let path = format!(
            "/ws/{}/{}",
            encode_path_segment(&room_id),
            encode_path_segment(&channel_id)
        );
        Redirect::to(&append_query(&path, &uri))
    } else {
        Redirect::to("/")
    }
}

/// Build an absolute origin (scheme + authority) for embed URLs.
fn embed_origin(headers: &axum::http::HeaderMap, authority: &Authority) -> String {
    format!("{}://{}", request_scheme(headers), authority)
}

/// Open Graph / Twitter Card tags for the landing page.
fn base_social_meta(origin: &str) -> String {
    let icon = format!("{}/icon.svg", origin);
    format!(
        r##"<meta property="og:site_name" content="RustRooms">
<meta property="og:type" content="website">
<meta property="og:title" content="RustRooms">
<meta property="og:description" content="Simple, secure, and fast video conferencing. Create a room, share the link, and you're talking in seconds — no account, no downloads.">
<meta property="og:image" content="{icon}">
<meta property="og:url" content="{origin}/">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="RustRooms">
<meta name="twitter:description" content="Simple, secure, and fast video conferencing. Create a room, share the link, and you're talking in seconds.">
<meta name="twitter:image" content="{icon}">"##
    )
}

/// Open Graph / Twitter Card tags for a call (room/channel) link.
fn call_social_meta(origin: &str, path: &str) -> String {
    let icon = format!("{}/icon.svg", origin);
    format!(
        r##"<meta property="og:site_name" content="RustRooms">
<meta property="og:type" content="website">
<meta property="og:title" content="You're invited to a RustRooms call">
<meta property="og:description" content="Tap to join the call — no sign-up, no downloads, just your browser.">
<meta property="og:image" content="{icon}">
<meta property="og:url" content="{origin}{path}">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="You're invited to a RustRooms call">
<meta name="twitter:description" content="Tap to join the call — no sign-up, no downloads, just your browser.">
<meta name="twitter:image" content="{icon}">"##
    )
}

pub(crate) async fn index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    uri: Uri,
) -> axum::response::Response {
    if let Some(ref allowed_url) = state.allowed_url
        && !host_is_allowed(&headers, allowed_url)
    {
        return (axum::http::StatusCode::FORBIDDEN, "Forbidden").into_response();
    }

    // Any path deeper than "/" is a call link (room or room/channel).
    let path = uri.path();
    let is_call_link = !path.trim_matches('/').is_empty();

    let (page_title, meta_tags) = match (is_call_link, request_authority(&headers)) {
        (false, Some(authority)) => {
            let origin = embed_origin(&headers, &authority);
            ("RustRooms", base_social_meta(&origin))
        }
        (true, Some(authority)) => {
            let origin = embed_origin(&headers, &authority);
            ("Join the call", call_social_meta(&origin, path))
        }
        _ => ("RustRooms", String::new()),
    };

    let html = render_html_page(page_title, &meta_tags);

    let csp = "default-src 'self'; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; script-src-elem 'self' 'unsafe-inline'; worker-src 'self' blob:; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' data: https: blob:; connect-src 'self' wss: ws:; media-src 'self' blob:; object-src 'none'; frame-ancestors 'none';".to_string();

    (
        [(
            header::CONTENT_SECURITY_POLICY,
            axum::http::HeaderValue::from_str(&csp).unwrap(),
        )],
        Html(html),
    )
        .into_response()
}

pub(crate) async fn ws_handler(
    Path((room_id, channel_id)): Path<(String, String)>,
    ws: WebSocketUpgrade,
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if !is_valid_room_id(&room_id) {
        return (axum::http::StatusCode::BAD_REQUEST, "Invalid room ID").into_response();
    }
    let Some(channel_id) = normalize_channel_id(&channel_id) else {
        return (axum::http::StatusCode::BAD_REQUEST, "Invalid channel ID").into_response();
    };
    if let Some(ref allowed_url) = state.allowed_url
        && !host_is_allowed(&headers, allowed_url)
    {
        return (axum::http::StatusCode::FORBIDDEN, "Forbidden").into_response();
    }
    if headers.contains_key(header::ORIGIN) && !origin_matches_request_host(&headers) {
        return (axum::http::StatusCode::FORBIDDEN, "Forbidden Origin").into_response();
    }

    ws.max_frame_size(CLIENT_WS_MAX_MESSAGE_SIZE)
        .max_message_size(CLIENT_WS_MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_socket(socket, room_id, channel_id, state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use std::collections::{HashSet, VecDeque};
    use std::sync::Arc;

    fn test_state(password: Option<&str>) -> AppState {
        AppState {
            rooms: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            room_cleanup_generations: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            room_creation_password: password.map(str::to_string),
            distributed_tx: tokio::sync::broadcast::channel(DISTRIBUTED_BROADCAST_CAPACITY).0,
            remote_users: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            remote_user_owners: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            channel_creation_times: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            allowed_url: None,
            recent_distributed_msg_ids: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            distributed_msg_history: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            node_id: Uuid::new_v4().to_string(),
            admin_rooms: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            cluster_key: None,
            p2p_runtime: None,
            resume_records: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
away_seats: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
departure_gen: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            server_secret: Arc::new(vec![7u8; 32]),
        }
    }

    #[tokio::test]
    async fn new_room_rejects_missing_password_when_configured() {
        let state = test_state(Some("hunter2"));
        let err = new_room(State(state), HeaderMap::new(), Query(HashMap::new()))
            .await
            .unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn new_room_rejects_wrong_password() {
        let state = test_state(Some("hunter2"));
        let params = HashMap::from([("password".to_string(), "wrong".to_string())]);
        let err = new_room(State(state), HeaderMap::new(), Query(params))
            .await
            .unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn new_room_redirects_with_correct_password() {
        let state = test_state(Some("hunter2"));
        let params = HashMap::from([("password".to_string(), "hunter2".to_string())]);
        let res = new_room(State(state), HeaderMap::new(), Query(params)).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn new_room_allows_anyone_without_configured_password() {
        let state = test_state(None);
        let res = new_room(State(state), HeaderMap::new(), Query(HashMap::new())).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn new_room_rejects_disallowed_hosts() {
        let state = AppState {
            allowed_url: Some("example.com".to_string()),
            ..test_state(None)
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "evil.example.net".parse().unwrap());
        let err = new_room(State(state), headers, Query(HashMap::new()))
            .await
            .unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn new_room_rejects_reserved_names() {
        let state = test_state(None);
        for name in ["new", "NEW", "cluster-ws", "CLUSTER-WS", "assets", "fonts", "ws"] {
            let params = HashMap::from([("name".to_string(), name.to_string())]);
            let err = new_room(State(state.clone()), HeaderMap::new(), Query(params))
                .await
                .unwrap_err();
            assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn new_room_percent_encodes_unicode_names() {
        let state = test_state(None);
        let params = HashMap::from([("name".to_string(), "café".to_string())]);
        let response = new_room(State(state), HeaderMap::new(), Query(params))
            .await
            .unwrap()
            .into_response();
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/caf%C3%A9"
        );
    }

    #[test]
    fn websocket_origin_must_match_the_request_host_exactly() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, "example.com".parse().unwrap());
        headers.insert(header::ORIGIN, "https://example.com".parse().unwrap());
        assert!(origin_matches_request_host(&headers));

        headers.insert(header::ORIGIN, "https://sub.example.com".parse().unwrap());
        assert!(!origin_matches_request_host(&headers));
        headers.insert(header::ORIGIN, "https://example.com:8443".parse().unwrap());
        assert!(!origin_matches_request_host(&headers));

        headers.insert(header::HOST, "example.com:8443".parse().unwrap());
        headers.insert(header::ORIGIN, "https://example.com".parse().unwrap());
        assert!(!origin_matches_request_host(&headers));
        headers.insert(header::ORIGIN, "https://example.com:8443".parse().unwrap());
        assert!(origin_matches_request_host(&headers));
    }

    #[tokio::test]
    async fn trailing_slash_redirects_percent_encode_path_segments() {
        let uri = axum::http::Uri::from_static("/room%20name/chat%20%3F%20%23/");
        let response = redirect_channel_trailing_slash(
            Path(("room name".to_string(), "chat ? #".to_string())),
            uri,
        )
        .await
        .into_response();
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/room%20name/chat%20%3F%20%23"
        );
    }

    #[tokio::test]
    async fn trailing_slash_redirects_preserve_query_parameters() {
        let uri = axum::http::Uri::from_static("/new/?name=custom&password=secret");
        let response = redirect_new_trailing_slash(uri).await.into_response();
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/new?name=custom&password=secret"
        );
    }

    #[test]
    fn append_query_handles_queries_safely() {
        let uri = axum::http::Uri::from_static("/new/?a=1&b=2");
        assert_eq!(append_query("/new", &uri), "/new?a=1&b=2");

        let uri_empty = axum::http::Uri::from_static("/new/?");
        assert_eq!(append_query("/new", &uri_empty), "/new");

        let uri_no_query = axum::http::Uri::from_static("/new/");
        assert_eq!(append_query("/new", &uri_no_query), "/new");
    }

    #[test]
    fn websocket_origin_allows_local_http_and_rejects_cross_scheme() {
        let mut headers = axum::http::HeaderMap::new();
        // Localhost HTTP on port 80 / loopback without forwarded-proto
        headers.insert(header::HOST, "localhost".parse().unwrap());
        headers.insert(header::ORIGIN, "http://localhost".parse().unwrap());
        assert!(origin_matches_request_host(&headers));

        // Reject plain HTTP origin when request is HTTPS via X-Forwarded-Proto
        headers.insert(header::HOST, "example.com".parse().unwrap());
        headers.insert(header::ORIGIN, "http://example.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        assert!(!origin_matches_request_host(&headers));

        // Multi-proxy forwarded-proto list: "http, https"
        headers.insert("x-forwarded-proto", "http, https".parse().unwrap());
        assert!(origin_matches_request_host(&headers));
    }
}

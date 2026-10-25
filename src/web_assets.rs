use axum::{http::header, response::IntoResponse};
pub(crate) async fn rnnoise_js() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "application/javascript"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("rnnoise.js"),
    )
}

pub(crate) async fn rnnoise_processor_js() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "application/javascript"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("rnnoise_processor.js"),
    )
}

pub(crate) async fn manifest_json() -> impl IntoResponse {
    let manifest = r##"{
    "name": "RustRooms",
    "short_name": "RustRooms",
    "start_url": "/",
    "scope": "/",
    "display": "standalone",
    "background_color": "#101418",
    "theme_color": "#101418",
    "description": "Simple, secure, and fast video conferencing.",
    "icons": [
        {
            "src": "/icon.svg",
            "sizes": "any",
            "type": "image/svg+xml",
            "purpose": "any maskable"
        }
    ]
}"##;
    (
        [(header::CONTENT_TYPE, "application/manifest+json")],
        manifest,
    )
}

pub(crate) async fn service_worker_js() -> impl IntoResponse {
    let sw = r##"
const CACHE_NAME = 'rustrooms-m5-v1';
const ASSETS = [
    '/icon.svg',
    '/rnnoise.js',
    '/rnnoise_processor.js',
    '/assets/tailwind.min.css',
    '/assets/app.css',
    '/assets/app.js',
    '/assets/croppie.min.js',
    '/assets/croppie.min.css',
    '/assets/inter.css',
    '/fonts/inter-cyrillic-ext.woff2',
    '/fonts/inter-cyrillic.woff2',
    '/fonts/inter-greek-ext.woff2',
    '/fonts/inter-greek.woff2',
    '/fonts/inter-vietnamese.woff2',
    '/fonts/inter-latin-ext.woff2',
    '/fonts/inter-latin.woff2'
];

self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS)).then(() => self.skipWaiting())
    );
});

self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((keys) => Promise.all(
            keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
        )).then(() => self.clients.claim())
    );
});

self.addEventListener('fetch', (event) => {
    if (event.request.method !== 'GET') return;

    event.respondWith(
        (async () => {
            try {
                const networkResponse = await fetch(event.request);
                return networkResponse;
            } catch (error) {
                const cachedResponse = await caches.match(event.request);
                if (cachedResponse) {
                    return cachedResponse;
                }
                throw error;
            }
        })()
    );
});
"##;
    (
        [
            (header::CONTENT_TYPE, "application/javascript"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        sw,
    )
}

pub(crate) async fn icon_svg() -> impl IntoResponse {
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
    <rect width="512" height="512" rx="112" fill="#101418"/>
    <circle cx="256" cy="256" r="168" fill="#575EEA"/>
</svg>"##;
    ([(header::CONTENT_TYPE, "image/svg+xml")], svg)
}

macro_rules! asset_route {
    ($func:ident, $content_type:expr, $path:expr, str) => {
        pub(crate) async fn $func() -> impl IntoResponse {
            (
                [
                    (header::CONTENT_TYPE, $content_type),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                include_str!($path),
            )
        }
    };
    ($func:ident, $content_type:expr, $path:expr, bytes) => {
        pub(crate) async fn $func() -> impl IntoResponse {
            (
                [
                    (header::CONTENT_TYPE, $content_type),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                include_bytes!($path).as_slice(),
            )
        }
    };
}

asset_route!(tailwind_css, "text/css", "assets/tailwind.min.css", str);
asset_route!(app_css, "text/css", "assets/app.css", str);
asset_route!(
    croppie_js,
    "application/javascript",
    "assets/croppie.min.js",
    str
);
asset_route!(croppie_css, "text/css", "assets/croppie.min.css", str);
asset_route!(inter_css, "text/css", "assets/inter.css", str);
asset_route!(
    inter_cyrillic_ext_woff2,
    "font/woff2",
    "assets/fonts/inter-cyrillic-ext.woff2",
    bytes
);
asset_route!(
    inter_cyrillic_woff2,
    "font/woff2",
    "assets/fonts/inter-cyrillic.woff2",
    bytes
);
asset_route!(
    inter_greek_ext_woff2,
    "font/woff2",
    "assets/fonts/inter-greek-ext.woff2",
    bytes
);
asset_route!(
    inter_greek_woff2,
    "font/woff2",
    "assets/fonts/inter-greek.woff2",
    bytes
);
asset_route!(
    inter_vietnamese_woff2,
    "font/woff2",
    "assets/fonts/inter-vietnamese.woff2",
    bytes
);
asset_route!(
    inter_latin_ext_woff2,
    "font/woff2",
    "assets/fonts/inter-latin-ext.woff2",
    bytes
);
asset_route!(
    inter_latin_woff2,
    "font/woff2",
    "assets/fonts/inter-latin.woff2",
    bytes
);

pub(crate) async fn app_js() -> impl IntoResponse {
    static JAVASCRIPT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let javascript = JAVASCRIPT.get_or_init(|| {
        let turn_url = std::env::var("TURN_URL").unwrap_or_default();
        let turn_username = std::env::var("TURN_USERNAME").unwrap_or_default();
        let turn_credential = std::env::var("TURN_CREDENTIAL").unwrap_or_default();
        let turn_url = serde_json::to_string(&turn_url).unwrap_or_else(|_| "\"\"".to_string());
        let turn_username =
            serde_json::to_string(&turn_username).unwrap_or_else(|_| "\"\"".to_string());
        let turn_credential =
            serde_json::to_string(&turn_credential).unwrap_or_else(|_| "\"\"".to_string());
        concat!(
            include_str!("assets/client/gifenc.js"),
            include_str!("assets/client/gif_resize.js"),
            include_str!("assets/client/core.js"),
            include_str!("assets/client/interface.js"),
            include_str!("assets/client/connection.js"),
            include_str!("assets/client/settings.js"),
        )
        .replace("{{TURN_URL}}", &turn_url)
        .replace("{{TURN_USERNAME}}", &turn_username)
        .replace("{{TURN_CREDENTIAL}}", &turn_credential)
    });

    (
        [
            (header::CONTENT_TYPE, "application/javascript"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        javascript.as_str(),
    )
}

/// Render the single-page app shell with a dynamic `<title>` and social
/// meta tags (Open Graph / Twitter Card) injected into the head.
pub(crate) fn render_html_page(page_title: &str, meta_tags: &str) -> String {
    include_str!("assets/index.html")
        .replace("{{PAGE_TITLE}}", page_title)
        .replace("{{META_TAGS}}", meta_tags)
}

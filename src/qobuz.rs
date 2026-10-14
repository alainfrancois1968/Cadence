use qobuz_api::api::service::QobuzApiService;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct QobuzSource {
    service: Arc<Mutex<QobuzApiService>>,
}

impl QobuzSource {
    /// Scrapes APP_ID + APP_SECRET from the current Qobuz web player,
    /// then applies any user token from the environment.
    pub async fn new() -> anyhow::Result<Self> {
        let (app_id, app_secret) = scrape_app_credentials()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to scrape Qobuz app credentials: {}", e))?;

        tracing::info!(
            "Scraped Qobuz APP_ID={} (secret length {})",
            app_id,
            app_secret.len()
        );

        let mut service = QobuzApiService::with_credentials(&app_id, &app_secret)
            .map_err(|e| anyhow::anyhow!("Failed to create Qobuz service: {}", e))?;

        if let Ok(token) = std::env::var("QOBUZ_USER_AUTH_TOKEN") {
            tracing::info!("Injecting existing user token");
            service.set_auth_token(token);
        } else {
            // Optional: still try email/password if present
            let _ = service.authenticate_with_env();
        }

        Ok(Self {
            service: Arc::new(Mutex::new(service)),
        })
    }

    pub fn service(&self) -> Arc<Mutex<QobuzApiService>> {
        self.service.clone()
    }
}

/// Fetches the current web-player bundle and reconstructs the production
/// app_id / app_secret pair (no login required).
async fn scrape_app_credentials() -> Result<(String, String), String> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .build()
        .map_err(|e| e.to_string())?;

    let login_html = client
        .get("https://play.qobuz.com/login")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;

    let bundle_re = regex::Regex::new(r#"/resources/[^"]+/bundle\.js"#).unwrap();
    let bundle_path = bundle_re
        .find(&login_html)
        .ok_or("bundle.js not found on login page")?
        .as_str();

    let bundle = client
        .get(format!("https://play.qobuz.com{}", bundle_path))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;

    let app_id_re = regex::Regex::new(r#"production:\{api:\{appId:"(\d+)""#).unwrap();
    let app_id = app_id_re.captures(&bundle).ok_or("appId not found")?[1].to_string();

    // Reconstruct the berlin (production) secret
    let seed_re =
        regex::Regex::new(r#"initialSeed\("([^"]+)",window\.utimezone\.berlin\)"#).unwrap();
    let seed = seed_re.captures(&bundle).ok_or("berlin seed not found")?[1].to_string();

    let info_re =
        regex::Regex::new(r#"name:"[^"]*/Berlin",info:"([^"]+)",extras:"([^"]+)""#).unwrap();
    let caps = info_re
        .captures(&bundle)
        .ok_or("berlin info/extras not found")?;
    let info = caps[1].to_string();
    let extras = caps[2].to_string();

    let combined = format!("{}{}{}", seed, info, extras);
    let trimmed = &combined[..combined.len().saturating_sub(44)];
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, trimmed)
        .map_err(|e| format!("base64: {}", e))?;
    let secret = String::from_utf8(decoded).map_err(|e| format!("utf8: {}", e))?;

    Ok((app_id, secret))
}

use qobuz_api::api::service::QobuzApiService;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct QobuzSource {
    service: Arc<Mutex<QobuzApiService>>,
}

impl QobuzSource {
    pub fn new() -> anyhow::Result<Self> {
        let mut service = QobuzApiService::new()
            .map_err(|e| anyhow::anyhow!("Failed to create Qobuz service: {}", e))?;

        if let Ok(token) = std::env::var("QOBUZ_USER_AUTH_TOKEN") {
            tracing::info!("Injecting existing user token (skipping login request)");
            service.set_auth_token(token);
        } else {
            service
                .authenticate_with_env()
                .map_err(|e| anyhow::anyhow!("Qobuz authentication failed: {}", e))?;
        }

        Ok(Self {
            service: Arc::new(Mutex::new(service)),
        })
    }

    pub fn service(&self) -> Arc<Mutex<QobuzApiService>> {
        self.service.clone()
    }
}

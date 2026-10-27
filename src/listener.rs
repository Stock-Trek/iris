use crate::error::ConnectionError;
use http::HeaderMap;
use std::{fmt::Debug, sync::Arc, time::Duration};

pub type SharedListener<Response> = Arc<dyn Listener<Response>>;

#[async_trait::async_trait]
pub trait Listener<Response>: Debug + Send + Sync + 'static
where
    Response: Send + 'static,
{
    async fn on_connecting(&self) {}
    async fn on_handshake_response(&self, _status: u16, _headers: &HeaderMap) {}
    async fn on_connection_backoff(&self, _delay_millis: u64) {}
    async fn on_reconnecting(&self, _attempt: u32) {}
    async fn on_connected(&self) {}
    async fn on_ping_latency(&self, _rtt: Duration) {}
    async fn on_message(&self, message: Response);
    async fn on_message_size_received(&self, _size_bytes: usize) {}
    async fn on_message_size_sent(&self, _size_bytes: usize) {}
    async fn on_error(&self, _error: &ConnectionError) {}
    async fn on_disconnecting(&self, _code: u16, _reason: &str) {}
    async fn on_disconnected(&self) {}
}

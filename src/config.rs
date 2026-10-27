use crate::{circuit_breaker::CircuitBreakerConfig, error::ConnectionError, tls::TlsConfig};
use http::{HeaderMap, HeaderName, HeaderValue, header};
use std::{fmt, time::Duration};

#[derive(Clone, Debug, Default)]
pub enum ServerCloseBehavior {
    #[default]
    Disconnect,
    Reconnect,
}

#[derive(Clone, Debug, Default)]
pub enum DisconnectedBehavior {
    DropAllQueued,
    #[default]
    SendOnReconnect,
}

#[derive(Clone)]
pub struct Config {
    pub(crate) channel_buffer_size: usize,
    pub(crate) circuit_breaker: CircuitBreakerConfig,
    pub(crate) disconnect_timeout: Duration,
    pub(crate) disconnected_behavior: DisconnectedBehavior,
    pub(crate) headers: HeaderMap,
    pub(crate) message_timeout: Duration,
    pub(crate) ping_interval: Duration,
    pub(crate) pong_timeout: Duration,
    pub(crate) server_close_behavior: ServerCloseBehavior,
    pub(crate) tls_config: TlsConfig,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("channel_buffer_size", &self.channel_buffer_size)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("disconnected_behavior", &self.disconnected_behavior)
            .field("headers", &RedactedHeaders(&self.headers))
            .field("message_timeout", &self.message_timeout)
            .field("ping_interval", &self.ping_interval)
            .field("pong_timeout", &self.pong_timeout)
            .field("server_close_behavior", &self.server_close_behavior)
            .field("tls_config", &self.tls_config)
            .finish()
    }
}

/// Wrapper that redacts the values of headers that commonly contain
/// credentials, so that [`Config`] can be logged without leaking secrets.
struct RedactedHeaders<'a>(&'a HeaderMap);

impl fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (name, value) in self.0 {
            if is_sensitive_header(name) {
                map.entry(name, &"<redacted>");
            } else {
                map.entry(name, value);
            }
        }
        map.finish()
    }
}

fn is_sensitive_header(name: &HeaderName) -> bool {
    let name = name.as_str().to_ascii_lowercase();
    matches!(name.as_str(), "cookie" | "set-cookie")
        || [
            "auth",
            "token",
            "secret",
            "password",
            "credential",
            "api-key",
            "apikey",
            "key",
        ]
        .iter()
        .any(|needle| name.contains(needle))
}

impl Default for Config {
    fn default() -> Self {
        Self {
            channel_buffer_size: 256,
            circuit_breaker: CircuitBreakerConfig::default(),
            disconnect_timeout: Duration::from_secs(10),
            disconnected_behavior: DisconnectedBehavior::default(),
            headers: HeaderMap::new(),
            message_timeout: Duration::from_secs(10),
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(30),
            server_close_behavior: ServerCloseBehavior::default(),
            tls_config: TlsConfig::default(),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_channel_buffer_size(mut self, size: usize) -> Self {
        self.channel_buffer_size = size;
        self
    }
    pub fn with_circuit_breaker_config(mut self, circuit_breaker: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }
    /// Sets the maximum time allowed for the entire graceful close handshake
    /// (sending the close frame and closing the stream). `disconnect` also
    /// falls back to forcefully aborting the connection task if it does not
    /// finish within this duration.
    pub fn with_disconnect_timeout(mut self, disconnect_timeout: Duration) -> Self {
        self.disconnect_timeout = disconnect_timeout;
        self
    }
    pub fn with_disconnected_behavior(mut self, behavior: DisconnectedBehavior) -> Self {
        self.disconnected_behavior = behavior;
        self
    }
    /// Adds a header to the WebSocket handshake request.
    ///
    /// If a header with the same name was already configured, it is replaced.
    /// Reserved handshake headers (`Host`, `Upgrade`, `Connection`,
    /// `Sec-WebSocket-Key`, and `Sec-WebSocket-Version`) are rejected when
    /// connecting and surface as [`ConnectionError::InvalidHeader`].
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }
    /// Adds all headers from `headers` to the WebSocket handshake request.
    ///
    /// Headers with names already present in the configuration are replaced.
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers.extend(headers);
        self
    }
    /// Adds a header to the WebSocket handshake request from string values.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionError::InvalidHeader`] if the name or value is not
    /// a valid HTTP header, or if the name is a reserved handshake header.
    pub fn with_header_str(self, name: &str, value: &str) -> Result<Self, ConnectionError> {
        let parsed_name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
            ConnectionError::InvalidHeader(format!("invalid header name '{name}': {e}"))
        })?;
        if tokio_websockets::client::DISALLOWED_HEADERS.contains(&parsed_name) {
            return Err(ConnectionError::InvalidHeader(format!(
                "header '{parsed_name}' is reserved for the WebSocket handshake"
            )));
        }
        let parsed_value = HeaderValue::from_str(value).map_err(|e| {
            ConnectionError::InvalidHeader(format!("invalid value for header '{parsed_name}': {e}"))
        })?;
        Ok(self.with_header(parsed_name, parsed_value))
    }
    /// Sets the `Sec-WebSocket-Protocol` handshake header, which requests a
    /// subprotocol from the server.
    pub fn with_subprotocol(self, subprotocol: &str) -> Result<Self, ConnectionError> {
        self.with_header_str(header::SEC_WEBSOCKET_PROTOCOL.as_str(), subprotocol)
    }
    pub fn with_message_timeout(mut self, message_timeout: Duration) -> Self {
        self.message_timeout = message_timeout;
        self
    }
    pub fn with_ping_interval(mut self, ping_interval: Duration) -> Self {
        self.ping_interval = ping_interval;
        self
    }
    pub fn with_pong_timeout(mut self, pong_timeout: Duration) -> Self {
        self.pong_timeout = pong_timeout;
        self
    }
    pub fn with_server_close_behavior(mut self, behavior: ServerCloseBehavior) -> Self {
        self.server_close_behavior = behavior;
        self
    }
    pub fn with_tls_config(mut self, tls_config: TlsConfig) -> Self {
        self.tls_config = tls_config;
        self
    }

    pub(crate) fn circuit_breaker(&self) -> &CircuitBreakerConfig {
        &self.circuit_breaker
    }
    pub(crate) fn disconnect_timeout(&self) -> Duration {
        self.disconnect_timeout
    }
    pub(crate) fn disconnect_queue_behavior(&self) -> &DisconnectedBehavior {
        &self.disconnected_behavior
    }
    pub(crate) fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    pub(crate) fn message_timeout(&self) -> Duration {
        self.message_timeout
    }
    pub(crate) fn ping_interval(&self) -> Duration {
        self.ping_interval
    }
    pub(crate) fn pong_timeout(&self) -> Duration {
        self.pong_timeout
    }
    pub(crate) fn server_close_behavior(&self) -> &ServerCloseBehavior {
        &self.server_close_behavior
    }
    pub(crate) fn tls_config(&self) -> &TlsConfig {
        &self.tls_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_value(value: &str) -> HeaderValue {
        HeaderValue::from_str(value).unwrap()
    }

    #[test]
    fn with_header_adds_header_to_handshake() {
        let config = Config::new().with_header(
            header::AUTHORIZATION,
            header_value("Bearer super-secret-token"),
        );
        assert_eq!(
            config.headers().get(header::AUTHORIZATION).unwrap(),
            "Bearer super-secret-token"
        );
    }

    #[test]
    fn with_headers_merges_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, header_value("iris-test"));
        headers.insert(header::COOKIE, header_value("session=123"));
        let config = Config::new()
            .with_header(header::ACCEPT, header_value("application/json"))
            .with_headers(headers);

        assert_eq!(
            config.headers().get(header::USER_AGENT).unwrap(),
            "iris-test"
        );
        assert_eq!(config.headers().get(header::COOKIE).unwrap(), "session=123");
        assert_eq!(
            config.headers().get(header::ACCEPT).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn with_header_replaces_existing_value() {
        let config = Config::new()
            .with_header(header::USER_AGENT, header_value("first"))
            .with_header(header::USER_AGENT, header_value("second"));
        assert_eq!(config.headers().get(header::USER_AGENT).unwrap(), "second");
    }

    #[test]
    fn with_header_str_parses_valid_header() {
        let config = Config::new()
            .with_header_str("x-request-id", "abc-123")
            .unwrap();
        assert_eq!(config.headers().get("x-request-id").unwrap(), "abc-123");
    }

    #[test]
    fn with_header_str_rejects_invalid_name() {
        let result = Config::new().with_header_str("invalid header", "value");
        assert!(matches!(result, Err(ConnectionError::InvalidHeader(_))));
    }

    #[test]
    fn with_header_str_rejects_invalid_value() {
        let result = Config::new().with_header_str("x-test", "bad\nvalue");
        assert!(matches!(result, Err(ConnectionError::InvalidHeader(_))));
    }

    #[test]
    fn with_header_str_rejects_reserved_headers() {
        for name in [
            "host",
            "upgrade",
            "connection",
            "sec-websocket-key",
            "sec-websocket-version",
        ] {
            let result = Config::new().with_header_str(name, "value");
            assert!(
                matches!(result, Err(ConnectionError::InvalidHeader(_))),
                "expected '{name}' to be rejected"
            );
        }
    }

    #[test]
    fn with_subprotocol_sets_sec_websocket_protocol_header() {
        let config = Config::new().with_subprotocol("graphql-ws").unwrap();
        assert_eq!(
            config
                .headers()
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .unwrap(),
            "graphql-ws"
        );
    }

    #[test]
    fn debug_redacts_sensitive_header_values() {
        let config = Config::new()
            .with_header(
                header::AUTHORIZATION,
                header_value("Bearer super-secret-token"),
            )
            .with_header(
                HeaderName::from_static("x-api-key"),
                header_value("api-secret-value"),
            )
            .with_header(header::USER_AGENT, header_value("iris-test"));

        let debug = format!("{:?}", config);

        assert!(!debug.contains("super-secret-token"));
        assert!(!debug.contains("api-secret-value"));
        assert!(debug.contains("authorization"));
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("iris-test"));
    }
}

use crate::{circuit_breaker::CircuitBreakerConfig, tls::TlsConfig};
use std::time::Duration;

/// Controls how the client reacts when the server closes the connection
/// cleanly (i.e. when the server sends a WebSocket close frame).
///
/// This setting only affects clean, server-initiated closes. Any other kind of
/// connection loss - a network error, an unexpected end of stream, a pong
/// timeout, a failed socket write and so on - goes through the reconnect and
/// back-off machinery regardless of the selected variant (unless the error is
/// permanent, see [`crate::ConnectionError::is_permanent`]).
///
/// See [`crate::Client::send`] for what happens to messages that were accepted
/// by `send` but had not been written to the socket yet when the connection
/// dropped.
#[derive(Clone, Debug, Default)]
pub enum ServerCloseBehavior {
    /// Do not reconnect after the server closes the connection.
    ///
    /// The connection task terminates as soon as the server-initiated close is
    /// processed, so the client does not attempt to reconnect on its own.
    /// Messages that were accepted by [`crate::Client::send`] but had not yet
    /// been written to the socket when the close arrived are dropped together
    /// with the connection and are never delivered; the `send` call that
    /// accepted them already returned `Ok(())`, so no error is surfaced for
    /// them.
    ///
    /// The client stays disconnected: [`crate::Client::is_connected`] returns
    /// `false` and subsequent [`crate::Client::send`] calls fail fast with
    /// [`crate::ConnectionError::ConnectionClosed`] instead of being buffered.
    #[default]
    Disconnect,
    /// Automatically reconnect after the server closes the connection.
    ///
    /// After a clean server-initiated close the client backs off (honouring
    /// the circuit breaker configuration) and attempts to reconnect. Messages
    /// that were accepted by [`crate::Client::send`] but had not yet been
    /// written to the socket when the close arrived are held in the outbound
    /// queue while the client is offline and are sent, in order, on the new
    /// connection once it is established.
    ///
    /// [`crate::Client::send`] calls made while the client is offline still
    /// fail fast with [`crate::ConnectionError::ConnectionClosed`]; only
    /// messages that were queued before the connection dropped are retained.
    /// If the connection task ends before a reconnect succeeds (for example
    /// because [`crate::Client::disconnect`] or
    /// [`crate::Client::force_disconnect`] is called), the queued messages are
    /// dropped.
    Reconnect,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) channel_buffer_size: usize,
    pub(crate) circuit_breaker: CircuitBreakerConfig,
    pub(crate) disconnect_timeout: Duration,
    pub(crate) message_timeout: Duration,
    pub(crate) ping_interval: Duration,
    pub(crate) pong_timeout: Duration,
    pub(crate) server_close_behavior: ServerCloseBehavior,
    pub(crate) tls_config: TlsConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            channel_buffer_size: 256,
            circuit_breaker: CircuitBreakerConfig::default(),
            disconnect_timeout: Duration::from_secs(10),
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
    /// Sets how the client reacts when the server closes the connection
    /// cleanly.
    ///
    /// See [`ServerCloseBehavior`] for the difference between the two variants,
    /// including what happens to messages that were queued but not yet sent
    /// when the connection dropped.
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

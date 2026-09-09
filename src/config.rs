use crate::{circuit_breaker::CircuitBreakerConfig, tls::TlsConfig};
use std::time::Duration;

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

#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) channel_buffer_size: usize,
    pub(crate) circuit_breaker: CircuitBreakerConfig,
    pub(crate) disconnect_timeout: Duration,
    pub(crate) disconnected_behavior: DisconnectedBehavior,
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
            disconnected_behavior: DisconnectedBehavior::default(),
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

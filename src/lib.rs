mod circuit_breaker;
mod client;
mod config;
mod error;
mod handler;
mod listener;
#[cfg(test)]
mod tests;
mod tls;

pub use {
    circuit_breaker::CircuitBreakerConfig,
    client::WebsocketClient,
    config::{ServerCloseBehavior, WebsocketConfig},
    error::ConnectionError,
    listener::{MessageListener, MessageListenerTrait},
    tls::TlsConfig,
};

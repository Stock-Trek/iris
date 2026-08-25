pub mod circuit_breaker;
pub mod client;
pub mod config;
pub mod error;
pub mod handler;
pub mod init_tests;
pub mod listener;
pub mod tls;

pub mod prelude {
    pub use crate::{
        circuit_breaker::CircuitBreakerConfig,
        client::WebsocketClient,
        config::{ServerCloseBehavior, WebsocketConfig},
        error::ConnectionError,
        listener::{MessageListener, MessageListenerTrait},
        tls::TlsConfig,
    };
}

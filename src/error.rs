use http::uri::InvalidUri;
use tokio::task::JoinError;
use tokio_websockets::Error as WebsocketError;

#[derive(Debug)]
pub enum ConnectionError {
    BadMessagePayload,
    ConnectionClosed,
    Deserialization(serde_json::Error),
    DisconnectRequested,
    DisconnectionError(JoinError),
    InternalState,
    InvalidUri(InvalidUri),
    MaxReconnectAttempts(u32),
    PongTimeout,
    SendMessage(String),
    Serialization(serde_json::Error),
    ServerClosed(u16, String),
    TaskPanicked(String),
    Timeout(tokio::time::error::Elapsed),
    TlsConfiguration(String),
    Websocket(WebsocketError),
}

impl ConnectionError {
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::InvalidUri(_)
                | Self::Deserialization(_)
                | Self::Serialization(_)
                | Self::BadMessagePayload
                | Self::MaxReconnectAttempts(_)
                | Self::TaskPanicked(_)
                | Self::Websocket(WebsocketError::DisallowedHeader)
        )
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMessagePayload => write!(f, "bad message payload"),
            Self::ConnectionClosed => write!(f, "connection closed"),
            Self::Deserialization(e) => write!(f, "deserialization error: {e}"),
            Self::DisconnectRequested => write!(f, "disconnect requested"),
            Self::DisconnectionError(e) => write!(f, "disconnection error: {e}"),
            Self::InternalState => write!(f, "internal state error"),
            Self::InvalidUri(e) => write!(f, "invalid URI: {e}"),
            Self::MaxReconnectAttempts(n) => {
                write!(f, "max reconnect attempts reached ({n})")
            }
            Self::PongTimeout => write!(f, "pong timeout"),
            Self::SendMessage(msg) => write!(f, "failed to send message: {msg}"),
            Self::Serialization(e) => write!(f, "serialization error: {e}"),
            Self::ServerClosed(code, reason) => {
                write!(
                    f,
                    "server closed connection (code: {code}, reason: {reason})"
                )
            }
            Self::TaskPanicked(msg) => write!(f, "connection task panicked: {msg}"),
            Self::Timeout(e) => write!(f, "timeout: {e}"),
            Self::TlsConfiguration(msg) => write!(f, "tls configuration error: {msg}"),
            Self::Websocket(e) => write!(f, "websocket error: {e}"),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Deserialization(e) => Some(e),
            Self::DisconnectionError(e) => Some(e),
            Self::InvalidUri(e) => Some(e),
            Self::Serialization(e) => Some(e),
            Self::Timeout(e) => Some(e),
            Self::Websocket(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, time::Duration};
    use tokio::time::timeout;

    fn make_json_error() -> serde_json::Error {
        serde_json::from_str::<String>("not valid json").unwrap_err()
    }

    // --- Display tests ---

    #[test]
    fn display_bad_message_payload() {
        let err = ConnectionError::BadMessagePayload;
        assert_eq!(err.to_string(), "bad message payload");
    }

    #[test]
    fn display_connection_closed() {
        let err = ConnectionError::ConnectionClosed;
        assert_eq!(err.to_string(), "connection closed");
    }

    #[test]
    fn display_deserialization() {
        let inner = make_json_error();
        let err = ConnectionError::Deserialization(inner);
        assert!(err.to_string().contains("deserialization error:"));
    }

    #[test]
    fn display_invalid_uri() {
        let inner = "http://[::1".parse::<http::Uri>().unwrap_err();
        let err = ConnectionError::InvalidUri(inner);
        assert!(err.to_string().contains("invalid URI:"));
    }

    #[test]
    fn display_max_reconnect_attempts() {
        let err = ConnectionError::MaxReconnectAttempts(5);
        assert_eq!(err.to_string(), "max reconnect attempts reached (5)");
    }

    #[test]
    fn display_pong_timeout() {
        let err = ConnectionError::PongTimeout;
        assert_eq!(err.to_string(), "pong timeout");
    }

    #[test]
    fn display_disconnect_requested() {
        let err = ConnectionError::DisconnectRequested;
        assert_eq!(err.to_string(), "disconnect requested");
    }

    #[test]
    fn display_send_message() {
        let err = ConnectionError::SendMessage("channel closed".to_string());
        assert_eq!(err.to_string(), "failed to send message: channel closed");
    }

    #[test]
    fn display_serialization() {
        let inner = make_json_error();
        let err = ConnectionError::Serialization(inner);
        assert!(err.to_string().contains("serialization error:"));
    }

    #[test]
    fn display_task_panicked() {
        let err = ConnectionError::TaskPanicked("oops".to_string());
        assert_eq!(err.to_string(), "connection task panicked: oops");
    }

    #[tokio::test]
    async fn display_timeout() {
        // Create an Elapsed error via timeout that fires
        let inner = timeout(Duration::from_nanos(1), async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        })
        .await
        .unwrap_err();
        let err = ConnectionError::Timeout(inner);
        assert!(err.to_string().contains("timeout:"));
    }

    #[test]
    fn display_tls_configuration() {
        let err = ConnectionError::TlsConfiguration("bad cert".to_string());
        assert_eq!(err.to_string(), "tls configuration error: bad cert");
    }

    #[test]
    fn display_websocket() {
        // Use an opaque error to avoid constructing a specific WebsocketError
        let inner = tokio_websockets::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        let err = ConnectionError::Websocket(inner);
        assert!(err.to_string().contains("websocket error:"));
    }

    // --- is_permanent tests ---

    #[test]
    fn is_permanent_invalid_uri() {
        let inner = "http://[::1".parse::<http::Uri>().unwrap_err();
        assert!(ConnectionError::InvalidUri(inner).is_permanent());
    }

    #[test]
    fn is_permanent_deserialization() {
        let inner = make_json_error();
        assert!(ConnectionError::Deserialization(inner).is_permanent());
    }

    #[test]
    fn is_permanent_serialization() {
        let inner = make_json_error();
        assert!(ConnectionError::Serialization(inner).is_permanent());
    }

    #[test]
    fn is_permanent_bad_message_payload() {
        assert!(ConnectionError::BadMessagePayload.is_permanent());
    }

    #[test]
    fn is_permanent_max_reconnect_attempts() {
        assert!(ConnectionError::MaxReconnectAttempts(3).is_permanent());
    }

    #[test]
    fn is_permanent_task_panicked() {
        assert!(ConnectionError::TaskPanicked("boom".into()).is_permanent());
    }

    #[test]
    fn is_not_permanent_connection_closed() {
        assert!(!ConnectionError::ConnectionClosed.is_permanent());
    }

    #[test]
    fn is_not_permanent_pong_timeout() {
        assert!(!ConnectionError::PongTimeout.is_permanent());
    }

    #[test]
    fn is_not_permanent_disconnect_requested() {
        assert!(!ConnectionError::DisconnectRequested.is_permanent());
    }

    #[tokio::test]
    async fn is_not_permanent_timeout() {
        let inner = timeout(Duration::from_nanos(1), async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        })
        .await
        .unwrap_err();
        assert!(!ConnectionError::Timeout(inner).is_permanent());
    }

    #[test]
    fn is_not_permanent_websocket() {
        let inner = tokio_websockets::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert!(!ConnectionError::Websocket(inner).is_permanent());
    }

    #[test]
    fn is_not_permanent_tls_configuration() {
        assert!(!ConnectionError::TlsConfiguration("bad".into()).is_permanent());
    }

    #[test]
    fn is_not_permanent_send_message() {
        assert!(!ConnectionError::SendMessage("fail".into()).is_permanent());
    }

    // --- source() tests ---

    #[test]
    fn source_deserialization() {
        let inner = make_json_error();
        let err = ConnectionError::Deserialization(inner);
        assert!(err.source().is_some());
    }

    #[test]
    fn source_invalid_uri() {
        let inner = "http://[::1".parse::<http::Uri>().unwrap_err();
        let err = ConnectionError::InvalidUri(inner);
        assert!(err.source().is_some());
    }

    #[test]
    fn source_serialization() {
        let inner = make_json_error();
        let err = ConnectionError::Serialization(inner);
        assert!(err.source().is_some());
    }

    #[tokio::test]
    async fn source_timeout() {
        let inner = timeout(Duration::from_nanos(1), async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        })
        .await
        .unwrap_err();
        let err = ConnectionError::Timeout(inner);
        assert!(err.source().is_some());
    }

    #[test]
    fn source_websocket() {
        let inner = tokio_websockets::Error::Io(std::io::Error::other("io error"));
        let err = ConnectionError::Websocket(inner);
        assert!(err.source().is_some());
    }

    #[test]
    fn source_none_for_bad_message_payload() {
        assert!(ConnectionError::BadMessagePayload.source().is_none());
    }

    #[test]
    fn source_none_for_disconnect_requested() {
        assert!(ConnectionError::DisconnectRequested.source().is_none());
    }

    #[test]
    fn source_none_for_connection_closed() {
        assert!(ConnectionError::ConnectionClosed.source().is_none());
    }

    #[test]
    fn source_none_for_pong_timeout() {
        assert!(ConnectionError::PongTimeout.source().is_none());
    }

    #[test]
    fn source_none_for_send_message() {
        assert!(ConnectionError::SendMessage("x".into()).source().is_none());
    }

    #[test]
    fn source_none_for_task_panicked() {
        assert!(ConnectionError::TaskPanicked("x".into()).source().is_none());
    }

    #[test]
    fn source_none_for_tls_configuration() {
        assert!(
            ConnectionError::TlsConfiguration("x".into())
                .source()
                .is_none()
        );
    }

    #[test]
    fn source_none_for_max_reconnect_attempts() {
        assert!(ConnectionError::MaxReconnectAttempts(0).source().is_none());
    }

    // --- Edge cases ---

    #[test]
    fn max_reconnect_attempts_zero() {
        let err = ConnectionError::MaxReconnectAttempts(0);
        assert!(err.is_permanent());
        assert_eq!(err.to_string(), "max reconnect attempts reached (0)");
    }

    #[test]
    fn max_reconnect_attempts_u32_max() {
        let err = ConnectionError::MaxReconnectAttempts(u32::MAX);
        assert!(err.is_permanent());
        assert_eq!(
            err.to_string(),
            format!("max reconnect attempts reached ({})", u32::MAX)
        );
    }

    #[test]
    fn send_message_empty() {
        let err = ConnectionError::SendMessage(String::new());
        assert_eq!(err.to_string(), "failed to send message: ");
    }

    #[test]
    fn task_panicked_empty() {
        let err = ConnectionError::TaskPanicked(String::new());
        assert_eq!(err.to_string(), "connection task panicked: ");
    }

    #[test]
    fn tls_configuration_empty() {
        let err = ConnectionError::TlsConfiguration(String::new());
        assert_eq!(err.to_string(), "tls configuration error: ");
    }

    #[test]
    fn debug_format() {
        let err = ConnectionError::BadMessagePayload;
        let debug = format!("{:?}", err);
        assert!(debug.contains("BadMessagePayload"));
    }

    #[test]
    fn display_disconnection_error() {
        let err = ConnectionError::ConnectionClosed;
        assert_eq!(err.to_string(), "connection closed");
    }
}

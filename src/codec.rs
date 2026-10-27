use crate::error::ConnectionError;
use serde::{Serialize, de::DeserializeOwned};
use tokio_websockets::Message;

/// Encodes outbound requests and decodes inbound responses.
///
/// Implement this trait to use a wire format other than JSON. The codec is
/// responsible for validating the incoming WebSocket message type.
pub trait Codec<Request, Response>: Send + Sync + 'static {
    /// Encodes a request into a binary WebSocket payload.
    fn encode(&self, request: &Request) -> Result<Vec<u8>, ConnectionError>;

    /// Decodes a response from an inbound WebSocket message.
    fn decode(&self, message: Message) -> Result<Response, ConnectionError>;
}

/// The default [`Codec`], using JSON for both directions.
///
/// Outbound requests are serialized to JSON and sent as binary frames. Inbound
/// text and binary frames are both decoded as JSON.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl<Request, Response> Codec<Request, Response> for JsonCodec
where
    Request: Serialize,
    Response: DeserializeOwned,
{
    fn encode(&self, request: &Request) -> Result<Vec<u8>, ConnectionError> {
        serde_json::to_vec(request).map_err(|e| ConnectionError::Codec(e.to_string()))
    }

    fn decode(&self, message: Message) -> Result<Response, ConnectionError> {
        if message.is_text() {
            if let Some(text) = message.as_text() {
                return serde_json::from_str(text)
                    .map_err(|e| ConnectionError::Codec(e.to_string()));
            }
        } else if message.is_binary() {
            let payload = message.into_payload();
            return serde_json::from_slice(&payload)
                .map_err(|e| ConnectionError::Codec(e.to_string()));
        }
        Err(ConnectionError::BadMessagePayload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Response {
        value: String,
    }

    #[test]
    fn json_codec_decodes_text() {
        let message = Message::text(r#"{"value":"hello"}"#.to_string());
        let response: Response = Codec::<(), Response>::decode(&JsonCodec, message).unwrap();
        assert_eq!(
            response,
            Response {
                value: "hello".to_string()
            }
        );
    }

    #[test]
    fn json_codec_decodes_binary() {
        let message = Message::binary(br#"{"value":"world"}"#.to_vec());
        let response: Response = Codec::<(), Response>::decode(&JsonCodec, message).unwrap();
        assert_eq!(
            response,
            Response {
                value: "world".to_string()
            }
        );
    }

    #[test]
    fn json_codec_rejects_unsupported_message_type() {
        let message = Message::ping(vec![]);
        assert!(matches!(
            Codec::<(), Response>::decode(&JsonCodec, message),
            Err(ConnectionError::BadMessagePayload)
        ));
    }
}

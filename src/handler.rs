use crate::{
    config::{Config, DisconnectedBehavior, ServerCloseBehavior},
    error::ConnectionError,
    listener::SharedListener,
    outbound_message::OutboundMessage,
};
use futures::SinkExt;
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    cmp::min,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::mpsc::Receiver, time::sleep};
use tokio_stream::StreamExt;
use tokio_websockets::{
    ClientBuilder, CloseCode, Error as WebsocketError, MaybeTlsStream, Message, Payload,
    WebSocketStream,
};
use tracing::{Span, info_span};

#[derive(Debug)]
pub(crate) struct ConnectionHandler<Request, Response>
where
    Request: Serialize + Send + 'static,
    Response: DeserializeOwned + Send + 'static,
{
    url: String,
    config: Config,
    listener: SharedListener<Response>,
    message_receiver: Receiver<OutboundMessage<Request>>,
    disconnect_receiver: Receiver<()>,
    connected: Arc<AtomicBool>,
    pong_deadline: Option<tokio::time::Instant>,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    reconnect_delay: Duration,
    consecutive_failures: u32,
    circuit_open_until: Option<Instant>,
    ping_sent_at: Option<Instant>,
    span: Span,
}

impl<Request, Response> ConnectionHandler<Request, Response>
where
    Request: Serialize + Send + 'static,
    Response: DeserializeOwned + Send + 'static,
{
    pub(crate) fn new(
        url: &str,
        config: Config,
        listener: SharedListener<Response>,
        message_receiver: Receiver<OutboundMessage<Request>>,
        disconnect_receiver: Receiver<()>,
        connected: Arc<AtomicBool>,
    ) -> Self {
        let span = info_span!("websocket", url = %url);
        let reconnect_delay = config.circuit_breaker().initial_backoff();
        Self {
            url: url.to_string(),
            config,
            listener,
            message_receiver,
            disconnect_receiver,
            connected,
            reconnect_delay,
            consecutive_failures: 0,
            circuit_open_until: None,
            ping_sent_at: None,
            pong_deadline: None,
            ws_stream: None,
            span,
        }
    }

    pub(crate) async fn run(mut self) {
        {
            let _guard = self.span.enter();
            tracing::info!("connection task started");
        }
        loop {
            if self.is_disconnect_requested() {
                break;
            }
            if let Some(open_until) = self.circuit_open_until {
                if Instant::now() < open_until {
                    let remaining = open_until.saturating_duration_since(Instant::now());
                    tracing::warn!(
                        remaining_ms = remaining.as_millis(),
                        "circuit breaker open, waiting before next reconnect attempt"
                    );
                    if self.sleep_with_disconnect(remaining).await {
                        break;
                    }
                }
                self.circuit_open_until = None;
            }
            tracing::info!("connecting...");
            self.listener.on_connecting().await;
            if self.is_disconnect_requested() {
                break;
            }
            match self.connect().await {
                Ok(()) => {
                    tracing::info!("connected");
                    self.reconnect_delay = self.config.circuit_breaker().initial_backoff();
                    self.consecutive_failures = 0;
                    self.circuit_open_until = None;
                    self.drop_queued_outbound_messages_if_configured();
                    self.connected.store(true, Ordering::Relaxed);
                    self.listener.on_connected().await;
                    let conn_result = self.handle_connection().await;
                    self.connected.store(false, Ordering::Relaxed);
                    self.listener.on_disconnected().await;
                    match conn_result {
                        Ok(()) => {
                            tracing::info!("connection ended normally");
                            break;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "connection error");
                            if matches!(e, ConnectionError::ServerClosed(..)) {
                                match self.config.server_close_behavior() {
                                    ServerCloseBehavior::Disconnect => {
                                        tracing::info!("server closed the connection cleanly");
                                        break;
                                    }
                                    ServerCloseBehavior::Reconnect => {
                                        tracing::info!(
                                            "server closed the connection, will reconnect"
                                        );
                                        self.drop_queued_outbound_messages_if_configured();
                                        self.consecutive_failures += 1;
                                        self.handle_backoff_with_circuit_breaker().await;
                                        continue;
                                    }
                                }
                            }
                            if e.is_permanent() {
                                tracing::error!("permanent error, giving up reconnection");
                                self.listener.on_error(&e).await;
                                break;
                            }
                            self.consecutive_failures += 1;
                            self.drop_queued_outbound_messages_if_configured();
                            self.handle_backoff_with_circuit_breaker().await;
                        }
                    }
                }
                Err(e) => {
                    if matches!(e, ConnectionError::DisconnectRequested) {
                        tracing::info!("user requested disconnect during connection attempt");
                        break;
                    }
                    tracing::error!(error = %e, "connection failed");
                    if e.is_permanent() {
                        tracing::error!("permanent error, giving up reconnection");
                        self.listener.on_error(&e).await;
                        break;
                    }
                    self.consecutive_failures += 1;
                    self.handle_backoff_with_circuit_breaker().await;
                }
            }
        }
        self.connected.store(false, Ordering::Relaxed);
        {
            let _guard = self.span.enter();
            tracing::info!("connection task finished");
        }
    }

    /// Check if a disconnect has been requested via the disconnect channel.
    /// Returns `true` if disconnect was requested.
    fn is_disconnect_requested(&mut self) -> bool {
        if self.disconnect_receiver.try_recv().is_ok() {
            tracing::info!("user requested disconnect during connection phase");
            true
        } else {
            false
        }
    }

    /// Sleep for the given duration, but return early if a disconnect is requested.
    /// Returns `true` if disconnect was requested.
    async fn sleep_with_disconnect(&mut self, duration: Duration) -> bool {
        tokio::select! {
            _ = sleep(duration) => false,
            _ = self.disconnect_receiver.recv() => {
                tracing::info!("user requested disconnect during sleep");
                true
            }
        }
    }

    fn should_trip_circuit_breaker(&self) -> bool {
        self.config
            .circuit_breaker()
            .max_reconnect_attempts()
            .is_some_and(|max| self.consecutive_failures >= max)
    }

    async fn handle_backoff_with_circuit_breaker(&mut self) {
        self.listener
            .on_reconnecting(self.consecutive_failures + 1)
            .await;
        if self.should_trip_circuit_breaker() {
            tracing::error!(
                consecutive_failures = self.consecutive_failures,
                "circuit breaker tripped after max reconnect attempts"
            );
            let err = ConnectionError::MaxReconnectAttempts(self.consecutive_failures);
            self.listener.on_error(&err).await;
            self.circuit_open_until =
                Some(Instant::now() + self.config.circuit_breaker().reset_timeout());
        }
        let jittered = {
            let mut rng = rand::thread_rng();
            let min_ms: u64 = self
                .config
                .circuit_breaker()
                .initial_backoff()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
            let base_ms: u64 = self
                .reconnect_delay
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
            Duration::from_millis(rng.gen_range(min_ms..=base_ms))
        };
        let delay_ms = jittered.as_millis().try_into().unwrap_or(u64::MAX);
        tracing::warn!(
            delay_ms = delay_ms,
            base_delay_ms = self.reconnect_delay.as_millis(),
            max_delay_ms = self.config.circuit_breaker().max_backoff().as_millis(),
            "backing off before reconnect (with jitter)"
        );
        self.listener.on_connection_backoff(delay_ms).await;
        self.sleep_with_disconnect(jittered).await;
        self.reconnect_delay = min(
            self.reconnect_delay * 2,
            self.config.circuit_breaker().max_backoff(),
        );
    }

    fn drop_queued_outbound_messages_if_configured(&mut self) {
        if matches!(
            self.config.disconnect_queue_behavior(),
            DisconnectedBehavior::SendOnReconnect
        ) {
            return;
        }
        let mut dropped_count = 0usize;
        while let Ok(outgoing) = self.message_receiver.try_recv() {
            let OutboundMessage { message: _, ack } = outgoing;
            let _ = ack.send(Err(ConnectionError::ConnectionClosed));
            dropped_count += 1;
        }
        if dropped_count > 0 {
            tracing::warn!(
                dropped_count = dropped_count,
                "connection lost, dropped queued outbound messages"
            );
        }
    }

    async fn connect(&mut self) -> Result<(), ConnectionError> {
        tracing::debug!("resolving URI");
        let connector = self.config.tls_config().build_connector()?;
        tracing::debug!("opening connection");
        let (ws_stream, _response) = {
            let builder = ClientBuilder::new().uri(&self.url).map_err(|e| {
                let err = ConnectionError::InvalidUri(e);
                tracing::error!(error = %err, "invalid URI");
                err
            })?;
            let builder = builder.connector(&connector);
            let connect_future = builder.connect();
            tokio::pin!(connect_future);
            tokio::select! {
                result = &mut connect_future => {
                    match result {
                        Ok(result) => result,
                        Err(e) => {
                            let err = ConnectionError::Websocket(e);
                            tracing::error!(error = %err, "connect failed");
                            self.listener.on_error(&err).await;
                            return Err(err);
                        }
                    }
                }
                _ = self.disconnect_receiver.recv() => {
                    return Err(ConnectionError::DisconnectRequested);
                }
            }
        };
        tracing::info!("connection established");
        self.ws_stream = Some(ws_stream);
        Ok(())
    }

    async fn handle_connection(&mut self) -> Result<(), ConnectionError> {
        {
            let _guard = self.span.enter();
            tracing::info!("handling connection");
        }
        let mut ping_interval = tokio::time::interval(self.config.ping_interval());
        self.pong_deadline = None;
        loop {
            let ws_stream = self.ws_stream.as_mut().unwrap();
            let deadline_delay = self.pong_deadline.map(|deadline| {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(remaining)
            });
            tokio::select! {
                biased;

                _ = async { if let Some(sleep) = deadline_delay { sleep.await } }, if self.pong_deadline.is_some() => {
                    let err = ConnectionError::PongTimeout;
                    tracing::error!(error = %err, "pong timeout");
                    self.listener.on_error(&err).await;
                    return Err(err);
                }
                _ = ping_interval.tick() => {
                    tracing::trace!("sending ping");
                    self.ping_sent_at = Some(Instant::now());
                    let ping = Message::ping(vec![]);
                    ws_stream.send(ping).await.map_err(ConnectionError::Websocket)?;
                    self.pong_deadline = Some(tokio::time::Instant::now() + self.config.pong_timeout());
                }
                next_message_result_option = Self::read_with_timeout(ws_stream, self.config.pong_timeout()) => {
                    let next_message_result_option = next_message_result_option?;
                    self.handle_message(next_message_result_option).await?
                }
                Some(outgoing) = self.message_receiver.recv() => {
                    tracing::trace!("outgoing message");
                    self.send_message(outgoing).await?
                }
                Some(_) = self.disconnect_receiver.recv() => {
                    tracing::info!("user requested disconnect");
                    // Best-effort graceful close: even if the close handshake
                    // fails (e.g. the connection is stuck), the handler must
                    // terminate instead of falling into the reconnect loop so
                    // that `disconnect()` is able to return.
                    let _ = self.close_stream(CloseCode::NORMAL_CLOSURE, "User disconnected").await;
                    return Ok(());
                }
            }
        }
    }

    async fn read_with_timeout(
        ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        pong_timeout: Duration,
    ) -> Result<Option<Result<Message, WebsocketError>>, ConnectionError> {
        match tokio::time::timeout(pong_timeout, ws_stream.next()).await {
            Ok(next) => Ok(next),
            Err(elapsed) => Err(ConnectionError::Timeout(elapsed)),
        }
    }

    async fn handle_message(
        &mut self,
        next_message_result_option: Option<Result<Message, WebsocketError>>,
    ) -> Result<(), ConnectionError> {
        match next_message_result_option {
            Some(next_message_result) => match next_message_result {
                Ok(next_message) => self.handle_valid_message(next_message).await,
                Err(e) => {
                    let err = ConnectionError::Websocket(e);
                    tracing::error!(error = %err, "read error");
                    self.listener.on_error(&err).await;
                    Err(err)
                }
            },
            None => {
                let err = ConnectionError::ConnectionClosed;
                tracing::info!("connection closed by remote");
                self.listener.on_error(&err).await;
                Err(err)
            }
        }
    }

    async fn handle_valid_message(&mut self, message: Message) -> Result<(), ConnectionError> {
        if message.is_close() {
            return self.handle_server_close(message).await;
        } else if message.is_ping() {
            tracing::trace!("received ping, sending pong");
            let payload = message.into_payload();
            return self.send_pong(payload).await;
        } else if message.is_pong() {
            tracing::trace!("received pong");
            if let Some(sent_at) = self.ping_sent_at.take() {
                let rtt = sent_at.elapsed();
                self.listener.on_ping_latency(rtt).await;
            }
            self.pong_deadline = None;
            return Ok(());
        }
        tracing::trace!("received message");
        match self.parse_message(message) {
            Ok((message, size_bytes)) => {
                self.listener.on_message(message).await;
                self.listener.on_message_size_received(size_bytes).await;
                Ok(())
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to parse message");
                self.listener.on_error(&e).await;
                Err(e)
            }
        }
    }

    async fn handle_server_close(&mut self, message: Message) -> Result<(), ConnectionError> {
        let (close_code, reason) = message
            .as_close()
            .unwrap_or((CloseCode::NO_STATUS_RECEIVED, ""));
        let code: u16 = close_code.into();
        let reason_str = reason.to_string();
        tracing::info!(
            code = code,
            reason = %reason_str,
            "received close frame from server"
        );
        self.close_stream(CloseCode::NORMAL_CLOSURE, "Server disconnected")
            .await?;
        let err = ConnectionError::ServerClosed(code, reason_str);
        Err(err)
    }

    async fn send_pong(&mut self, payload: Payload) -> Result<(), ConnectionError> {
        let pong = Message::pong(payload);
        if let Err(e) = self
            .ws_stream
            .as_mut()
            .unwrap()
            .send(pong)
            .await
            .map_err(ConnectionError::Websocket)
        {
            tracing::error!(error = %e, "failed to send pong");
            self.listener.on_error(&e).await;
            return Err(e);
        };
        tracing::trace!("pong sent");
        Ok(())
    }

    fn parse_message(&self, message: Message) -> Result<(Response, usize), ConnectionError> {
        if message.is_text() {
            if let Some(text) = message.as_text() {
                let message = serde_json::from_str(text).map_err(|e| {
                    let err = ConnectionError::Deserialization(e);
                    tracing::error!(error = %err, "failed to deserialize text message");
                    err
                })?;
                let size_bytes = text.len();
                return Ok((message, size_bytes));
            }
        } else if message.is_binary() {
            let payload = message.into_payload();
            let message = serde_json::from_slice(&payload).map_err(|e| {
                let err = ConnectionError::Deserialization(e);
                tracing::error!(error = %err, "failed to deserialize binary message");
                err
            })?;
            let size_bytes = payload.len();
            return Ok((message, size_bytes));
        }
        tracing::warn!("received unexpected message type (not text or binary)");
        Err(ConnectionError::BadMessagePayload)
    }

    /// Writes a queued outbound message to the socket and reports the outcome
    /// to the waiting [`crate::Client::send`] caller through the message's ack
    /// channel.
    ///
    /// The ack is resolved before this method returns.
    /// * `Ok(())` is sent once the message has been written to the socket;
    /// * `Err(error)` is sent when the message could not be written.
    async fn send_message(
        &mut self,
        outgoing: OutboundMessage<Request>,
    ) -> Result<(), ConnectionError> {
        let OutboundMessage { message, ack } = outgoing;
        let payload = match serde_json::to_vec(&message) {
            Ok(payload) => payload,
            Err(serialization_error) => {
                let err = ConnectionError::Serialization(serialization_error);
                tracing::error!(error = %err, "message serialization failed");
                self.listener.on_error(&err).await;
                let _ = ack.send(Err(err));
                return Ok(());
            }
        };
        let size_bytes = payload.len();
        let message = Message::binary(payload);
        match tokio::time::timeout(
            self.config.message_timeout(),
            self.ws_stream.as_mut().unwrap().send(message),
        )
        .await
        {
            Ok(result) => match result {
                Ok(()) => {
                    self.listener.on_message_size_sent(size_bytes).await;
                    let _ = ack.send(Ok(()));
                    Ok(())
                }
                Err(websocket_error) => {
                    let err = ConnectionError::Websocket(websocket_error);
                    tracing::error!(error = %err, "send failed");
                    self.listener.on_error(&err).await;
                    let _ = ack.send(Err(err));
                    Err(ConnectionError::ConnectionClosed)
                }
            },
            Err(elapsed) => {
                let err = ConnectionError::Timeout(elapsed);
                tracing::error!(error = %err, "message send timed out");
                self.listener.on_error(&err).await;
                let _ = ack.send(Err(err));
                Err(ConnectionError::ConnectionClosed)
            }
        }
    }

    async fn close_stream(&mut self, code: CloseCode, reason: &str) -> Result<(), ConnectionError> {
        tracing::info!(code = ?code, reason = %reason, "closing stream");
        self.listener.on_disconnecting(code.into(), reason).await;
        let ws_stream = self.ws_stream.as_mut().unwrap();
        let close_message = Message::close(Some(code), reason);
        let handshake = async {
            ws_stream.send(close_message).await.map_err(|e| {
                let err = ConnectionError::Websocket(e);
                tracing::error!(error = %err, "failed to send close frame");
                err
            })?;
            ws_stream.close().await.map_err(|e| {
                let err = ConnectionError::Websocket(e);
                tracing::error!(error = %err, "failed to close stream");
                err
            })?;
            Ok(())
        };
        match tokio::time::timeout(self.config.disconnect_timeout(), handshake).await {
            Ok(Ok(())) => {
                tracing::info!("stream closed");
                Ok(())
            }
            Ok(Err(e)) => {
                self.listener.on_error(&e).await;
                Err(e)
            }
            Err(elapsed) => {
                let err = ConnectionError::Timeout(elapsed);
                tracing::error!(error = %err, "close handshake timed out");
                self.listener.on_error(&err).await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreakerConfig;
    use tokio::time::{Duration, advance, pause};

    #[derive(serde::Serialize, Debug, Clone, PartialEq)]
    struct TestRequest {
        value: String,
    }

    #[derive(serde::Deserialize, Debug, Clone, PartialEq)]
    struct TestResponse {
        value: String,
    }

    #[derive(Debug)]
    struct TestListener;
    #[async_trait::async_trait]
    impl crate::listener::Listener<TestResponse> for TestListener {
        async fn on_message(&self, _message: TestResponse) {}
    }

    fn make_config() -> (Config, SharedListener<TestResponse>) {
        (
            Config::new(),
            Arc::new(TestListener) as SharedListener<TestResponse>,
        )
    }

    #[test]
    fn circuit_breaker_trips_after_max_failures() {
        let (config, listener) = make_config();
        let config = config.with_circuit_breaker_config(
            CircuitBreakerConfig::new().with_max_reconnect_attempts(5),
        );
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let mut handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        handler.consecutive_failures = 0;
        assert!(!handler.should_trip_circuit_breaker());
        handler.consecutive_failures = 5;
        assert!(
            handler.should_trip_circuit_breaker(),
            "should trip after 5 failures"
        );
    }

    #[test]
    fn circuit_breaker_never_trips_when_no_limit() {
        let (config, listener) = make_config();
        let config = config
            .with_circuit_breaker_config(CircuitBreakerConfig::new().with_no_reconnect_limit());
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let mut handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        handler.consecutive_failures = u32::MAX;
        assert!(!handler.should_trip_circuit_breaker());
    }

    #[test]
    fn parse_valid_text_message() {
        let (config, listener) = make_config();
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        let json = r#"{"value":"hello"}"#;
        let msg = Message::text(json.to_string());
        let result = handler.parse_message(msg);
        assert!(result.is_ok());
        let (parsed, size) = result.unwrap();
        assert_eq!(
            parsed,
            TestResponse {
                value: "hello".to_string()
            }
        );
        assert_eq!(size, json.len());
    }

    #[test]
    fn parse_valid_binary_message() {
        let (config, listener) = make_config();
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        let json = r#"{"value":"world"}"#;
        let msg = Message::binary(json.as_bytes().to_vec());
        let result = handler.parse_message(msg);
        assert!(result.is_ok());
        let (parsed, size) = result.unwrap();
        assert_eq!(
            parsed,
            TestResponse {
                value: "world".to_string()
            }
        );
        assert_eq!(size, json.len());
    }

    #[test]
    fn parse_invalid_json_text_returns_error() {
        let (config, listener) = make_config();
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        let msg = Message::text("not valid json".to_string());
        let result = handler.parse_message(msg);
        assert!(result.is_err());
        match result {
            Err(ConnectionError::Deserialization(_)) => {}
            _ => panic!("expected Deserialization error"),
        }
    }

    #[test]
    fn parse_invalid_json_binary_returns_error() {
        let (config, listener) = make_config();
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        let invalid_json_msg = Message::binary(vec![0, 159, 146, 150]);
        let result = handler.parse_message(invalid_json_msg);
        assert!(result.is_err());
        match result {
            Err(ConnectionError::Deserialization(_)) => {}
            _ => panic!("expected Deserialization error"),
        }
    }

    #[test]
    fn parse_unsupported_message_type_returns_bad_message_payload() {
        let (config, listener) = make_config();
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        let msg = Message::ping(vec![]);
        let result = handler.parse_message(msg);
        assert!(result.is_err());
        match result {
            Err(ConnectionError::BadMessagePayload) => {}
            _ => panic!("expected BadMessagePayload error"),
        }
    }

    #[test]
    fn parse_message_size_matches_payload() {
        let (config, listener) = make_config();
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );
        let json = r#"{"value":"hello"}"#;
        let msg = Message::binary(json.as_bytes().to_vec());
        let (_, size) = handler.parse_message(msg).unwrap();
        assert_eq!(size, json.len());
    }

    #[tokio::test]
    async fn exponential_backoff_doubles_up_to_max() {
        let (config, listener) = make_config();
        let config = config.with_circuit_breaker_config(
            CircuitBreakerConfig::new()
                .with_initial_backoff(Duration::from_secs(1))
                .with_max_backoff(Duration::from_secs(10)),
        );
        let (_msg_rx, _msg_rx_recv) =
            tokio::sync::mpsc::channel::<OutboundMessage<TestRequest>>(10);
        let (_disc_tx, _disc_rx) = tokio::sync::mpsc::channel(10);
        let connected = Arc::new(AtomicBool::new(false));
        let mut handler = ConnectionHandler::new(
            "ws://localhost:0",
            config,
            listener,
            _msg_rx_recv,
            _disc_rx,
            connected,
        );

        pause();

        assert_eq!(handler.reconnect_delay, Duration::from_secs(1));

        let future = handler.handle_backoff_with_circuit_breaker();
        advance(Duration::from_secs(1)).await;
        future.await;
        assert_eq!(handler.reconnect_delay, Duration::from_secs(2));

        let future = handler.handle_backoff_with_circuit_breaker();
        advance(Duration::from_secs(2)).await;
        future.await;
        assert_eq!(handler.reconnect_delay, Duration::from_secs(4));

        let future = handler.handle_backoff_with_circuit_breaker();
        advance(Duration::from_secs(4)).await;
        future.await;
        assert_eq!(handler.reconnect_delay, Duration::from_secs(8));

        let future = handler.handle_backoff_with_circuit_breaker();
        advance(Duration::from_secs(8)).await;
        future.await;
        assert_eq!(handler.reconnect_delay, Duration::from_secs(10));

        let future = handler.handle_backoff_with_circuit_breaker();
        advance(Duration::from_secs(10)).await;
        future.await;
        assert_eq!(handler.reconnect_delay, Duration::from_secs(10));
    }
}

use crate::{
    config::Config, error::ConnectionError, handler::ConnectionHandler, listener::SharedListener,
    outbound_message::OutboundMessage,
};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::mpsc::{Sender, channel},
    task::{JoinError, JoinHandle},
};

#[derive(Debug)]
pub struct Client<Request, Response>
where
    Request: Serialize + Send + 'static,
    Response: DeserializeOwned + Send + 'static,
{
    config: Config,
    listener: SharedListener<Response>,
    url: String,
    state: Arc<RwLock<Option<ClientState<Request>>>>,
}

#[derive(Debug)]
struct ClientState<Request>
where
    Request: Serialize + Send + 'static,
{
    message_sender: Sender<OutboundMessage<Request>>,
    disconnect_sender: Sender<()>,
    connection_task: JoinHandle<()>,
    connected: Arc<AtomicBool>,
}

impl<Request, Response> Client<Request, Response>
where
    Request: Serialize + Send + 'static,
    Response: DeserializeOwned + Send + 'static,
{
    pub fn new(config: Config, listener: SharedListener<Response>, url: &str) -> Self {
        Self {
            config,
            listener,
            url: url.to_string(),
            state: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn connect(&self) -> Result<(), ConnectionError> {
        let mut guard = self
            .state
            .write()
            .map_err(|_| ConnectionError::InternalState)?;
        if guard.is_some() {
            return Ok(());
        }
        let (message_sender, message_receiver) =
            channel::<OutboundMessage<Request>>(self.config.channel_buffer_size);
        let (disconnect_sender, disconnect_receiver) =
            channel::<()>(self.config.channel_buffer_size);
        let connected = Arc::new(AtomicBool::new(true));
        let handler = ConnectionHandler::new(
            &self.url,
            self.config.clone(),
            self.listener.clone(),
            message_receiver,
            disconnect_receiver,
            connected.clone(),
        );
        let connection_task = tokio::spawn(async move {
            handler.run().await;
        });
        *guard = Some(ClientState {
            message_sender,
            disconnect_sender,
            connection_task,
            connected,
        });
        Ok(())
    }

    /// Sends a message to the server.
    ///
    /// * `Ok(())` is returned only once the message has actually been written
    ///   to the socket by the connection task.
    /// * An error is returned whenever the message was not written, including
    ///   when the connection dropped (or the client was disconnected) while
    ///   the message was still queued or being written.
    ///
    /// Note that a successful socket write means the bytes were handed to the
    /// transport, not that the server has received or processed the message.
    pub async fn send(&self, message: Request) -> Result<(), ConnectionError> {
        let (sender, connected) = {
            let guard = self
                .state
                .read()
                .map_err(|_| ConnectionError::InternalState)?;
            match &*guard {
                None => return Err(ConnectionError::ConnectionClosed),
                Some(state) => (state.message_sender.clone(), state.connected.clone()),
            }
        };
        if !connected.load(Ordering::Relaxed) {
            return Err(ConnectionError::ConnectionClosed);
        }
        let (ack_sender, ack_receiver) = tokio::sync::oneshot::channel();
        let outgoing = OutboundMessage {
            message,
            ack: ack_sender,
        };
        sender
            .send(outgoing)
            .await
            .map_err(|e| ConnectionError::SendMessage(format!("{:?}", e)))?;
        match ack_receiver.await {
            Ok(result) => result,
            // The connection task ended (e.g. `disconnect`/`force_disconnect`,
            // a clean server close with
            // [`crate::config::ServerCloseBehavior::Disconnect`], or a
            // permanent error) before the message could be written to the
            // socket. The message was dropped without ever being sent.
            Err(_) => Err(ConnectionError::ConnectionClosed),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.state
            .read()
            .map(|opt| {
                opt.as_ref()
                    .is_some_and(|s| s.connected.load(Ordering::Relaxed))
            })
            .unwrap_or(false)
    }

    /// Gracefully disconnects from the server.
    ///
    /// A close frame is sent and the connection task is awaited. If the
    /// graceful close does not complete within the configured `disconnect_timeout`,
    /// the connection task is forcefully aborted so that this method always returns.
    pub async fn disconnect(&self) -> Result<(), ConnectionError> {
        let state_opt = {
            let mut guard = self
                .state
                .write()
                .map_err(|_| ConnectionError::InternalState)?;
            guard.take()
        };
        if let Some(state) = state_opt {
            let _ = state.disconnect_sender.send(()).await;
            let mut connection_task = state.connection_task;
            match tokio::time::timeout(self.config.disconnect_timeout(), &mut connection_task).await
            {
                Ok(join_result) => Self::handle_join_result(state.connected, join_result),
                Err(_elapsed) => {
                    tracing::warn!(
                        timeout_ms = self.config.disconnect_timeout().as_millis(),
                        "graceful disconnect timed out, forcefully aborting connection task"
                    );
                    connection_task.abort();
                    let join_result = connection_task.await;
                    Self::handle_join_result(state.connected, join_result)
                }
            }
        } else {
            Ok(())
        }
    }

    /// Immediately aborts the connection task without sending a close frame.
    ///
    /// Use this when [`Client::disconnect`] does not work (for
    /// example when the connection is stuck and the graceful close never
    /// completes). The underlying socket is dropped and the connection is torn
    /// down right away; note that listener callbacks such as `on_disconnected`
    /// will not be invoked because the task is cancelled mid-execution.
    pub async fn force_disconnect(&self) -> Result<(), ConnectionError> {
        let state_opt = {
            let mut guard = self
                .state
                .write()
                .map_err(|_| ConnectionError::InternalState)?;
            guard.take()
        };
        if let Some(state) = state_opt {
            tracing::info!("forcefully disconnecting");
            state.connection_task.abort();
            let join_result = state.connection_task.await;
            Self::handle_join_result(state.connected, join_result)
        } else {
            Ok(())
        }
    }

    /// Processes the result of awaiting the connection task and ensures the
    /// connected flag is cleared (the task may have been aborted before it
    /// could clear it itself).
    fn handle_join_result(
        connected: Arc<AtomicBool>,
        join_result: Result<(), JoinError>,
    ) -> Result<(), ConnectionError> {
        connected.store(false, Ordering::Relaxed);
        match join_result {
            Ok(()) => Ok(()),
            Err(join_err) => {
                if join_err.is_cancelled() {
                    // The task was aborted by `force_disconnect` or by the
                    // forced fallback in `disconnect`; this is expected.
                    tracing::info!("Connection task cancelled");
                    Ok(())
                } else if join_err.is_panic() {
                    let panic_payload = join_err.into_panic();
                    let panic_msg = panic_payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic_payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "Unknown panic cause".to_string());
                    tracing::error!("Connection task panicked: {}", panic_msg);
                    Err(ConnectionError::TaskPanicked(panic_msg))
                } else {
                    Err(ConnectionError::DisconnectionError(join_err))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use futures::SinkExt;
    use std::{sync::Arc, sync::atomic::Ordering, time::Duration};
    use tokio_stream::StreamExt;
    use tokio_websockets::{Message, ServerBuilder};

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
    impl Listener<TestResponse> for TestListener {
        async fn on_message(&self, _message: TestResponse) {}
    }

    #[tokio::test]
    async fn is_connected_lifecycle() {
        let mut client = client();
        assert!(!client.is_connected());
        connect(&mut client).await;
        assert!(client.is_connected());
        disconnect(&mut client).await;
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn send_message_when_connected_is_ok() {
        // `send` now only returns Ok once the message has actually been
        // written to the socket, so a live echo server is required.
        let (port, _shutdown) = run_echo_server().await;
        let mut client = client_to(&format!("ws://127.0.0.1:{}/ws", port));
        connect(&mut client).await;
        let result = client
            .send(TestRequest {
                value: "test".to_string(),
            })
            .await;
        assert!(result.is_ok());
        disconnect(&mut client).await;
    }

    #[tokio::test]
    async fn send_returns_error_when_message_is_never_written() {
        // A TCP listener that accepts the connection but never completes the
        // WebSocket handshake: the handler can never write a queued message,
        // so `send` must not report Ok. Once the connection task is torn down
        // via `disconnect`, the still-queued message is dropped and the
        // pending `send` must resolve with an error rather than a false Ok.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind");
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((_stream, _addr)) = listener.accept().await {
                // Hold the connection open without completing the handshake.
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let client = Arc::new(client_to(&format!("ws://127.0.0.1:{}/ws", port)));
        client.connect().await.unwrap();
        // Let the handler reach the (hanging) connect phase.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let sender = client.clone();
        let send_task = tokio::spawn(async move {
            sender
                .send(TestRequest {
                    value: "never sent".to_string(),
                })
                .await
        });
        // Give `send` enough time to queue the message and start waiting for
        // the write that will never happen.
        tokio::time::sleep(Duration::from_millis(100)).await;

        client.disconnect().await.unwrap();

        match send_task.await.unwrap() {
            Err(ConnectionError::ConnectionClosed) => {}
            other => panic!("expected ConnectionClosed, got: {:?}", other.map(|_| ())),
        }
    }

    #[tokio::test]
    async fn send_message_when_not_connected_is_error() {
        let client = client();
        let result = client
            .send(TestRequest {
                value: "test".to_string(),
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn send_message_when_disconnected_is_error() {
        let mut client = client();
        connect(&mut client).await;
        disconnect(&mut client).await;
        let result = client
            .send(TestRequest {
                value: "test".to_string(),
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn send_message_fails_fast_when_connected_flag_is_false() {
        let mut client = client();
        connect(&mut client).await;
        // State exists (connect was called) but the connection is down, e.g.
        // while the handler is reconnecting. `send` must fail fast and must
        // not buffer the message in the outbound channel.
        {
            let guard = client.state.read().unwrap();
            guard
                .as_ref()
                .unwrap()
                .connected
                .store(false, Ordering::Relaxed);
        }
        let result = client
            .send(TestRequest {
                value: "test".to_string(),
            })
            .await;
        match result {
            Err(ConnectionError::ConnectionClosed) => {}
            other => panic!("expected ConnectionClosed, got: {:?}", other.map(|_| ())),
        }
        disconnect(&mut client).await;
    }

    #[tokio::test]
    async fn double_dis_connect_is_ok() {
        let mut client = client();
        connect(&mut client).await;
        connect(&mut client).await;
        disconnect(&mut client).await;
        disconnect(&mut client).await;
    }

    #[tokio::test]
    async fn force_disconnect_when_not_connected_is_ok() {
        let client = client();
        let result = client.force_disconnect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn force_disconnect_after_connect_is_ok() {
        let mut client = client();
        connect(&mut client).await;
        assert!(client.is_connected());
        let result = client.force_disconnect().await;
        assert!(result.is_ok());
        assert!(!client.is_connected());
        let result = client.force_disconnect().await;
        assert!(result.is_ok());
    }

    fn client() -> Client<TestRequest, TestResponse> {
        client_to("ws://localhost:0")
    }
    fn client_to(url: &str) -> Client<TestRequest, TestResponse> {
        let config = Config::new();
        let listener: SharedListener<TestResponse> = Arc::new(TestListener);
        Client::new(config, listener, url)
    }
    async fn connect(client: &mut Client<TestRequest, TestResponse>) {
        match client.connect().await {
            Ok(_) => {}
            _ => panic!("Client did not connect"),
        }
    }
    async fn disconnect(client: &mut Client<TestRequest, TestResponse>) {
        match client.disconnect().await {
            Ok(_) => {}
            _ => panic!("Client did not disconnect"),
        }
    }

    /// Starts a minimal echo server and returns its port plus a shutdown
    /// signal.
    async fn run_echo_server() -> (u16, tokio::sync::oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind");
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let mut shutdown_rx = Some(shutdown_rx);
            loop {
                tokio::select! {
                    _ = async {
                        if let Some(rx) = &mut shutdown_rx {
                            rx.await.ok();
                        }
                    } => {
                        break;
                    }
                    accept_result = listener.accept() => {
                        let (stream, _) = accept_result.unwrap();
                        tokio::spawn(async move {
                            let builder = ServerBuilder::new();
                            if let Ok((_request, mut ws_stream)) = builder.accept(stream).await {
                                while let Some(Ok(msg)) = ws_stream.next().await {
                                    if msg.is_ping() {
                                        let payload = msg.into_payload();
                                        let _ = ws_stream.send(Message::pong(payload)).await;
                                    } else if msg.is_text() || msg.is_binary() {
                                        let _ = ws_stream.send(msg).await;
                                    } else if msg.is_close() {
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        (port, shutdown_tx)
    }
}

use crate::{
    config::WebsocketConfig, error::ConnectionError, handler::ConnectionHandler,
    listener::MessageListener,
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
pub struct WebsocketClient<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    config: WebsocketConfig,
    listener: MessageListener<T>,
    url: String,
    state: Arc<RwLock<Option<ClientState<T>>>>,
}

#[derive(Debug)]
struct ClientState<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    message_sender: Sender<T>,
    disconnect_sender: Sender<()>,
    connection_task: JoinHandle<()>,
    connected: Arc<AtomicBool>,
}

impl<T> WebsocketClient<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn new(config: WebsocketConfig, listener: MessageListener<T>, url: &str) -> Self {
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
        let (message_sender, message_receiver) = channel::<T>(self.config.channel_buffer_size);
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

    pub async fn send(&self, message: T) -> Result<(), ConnectionError> {
        let sender = {
            let guard = self
                .state
                .read()
                .map_err(|_| ConnectionError::InternalState)?;
            match &*guard {
                None => return Err(ConnectionError::ConnectionClosed),
                Some(state) => state.message_sender.clone(),
            }
        };
        sender
            .send(message)
            .await
            .map_err(|e| ConnectionError::SendMessage(format!("{:?}", e)))
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

    /// Gracefully disconnects from the websocket server.
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
    /// Use this when [`WebsocketClient::disconnect`] does not work (for
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
                    tracing::info!("websocket connection task cancelled");
                    Ok(())
                } else if join_err.is_panic() {
                    let panic_payload = join_err.into_panic();
                    let panic_msg = panic_payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic_payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "Unknown panic cause".to_string());
                    tracing::error!("Websocket connection task panicked: {}", panic_msg);
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
    use std::sync::Arc;

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
    struct TestMsg {
        value: String,
    }

    #[derive(Debug)]
    struct TestListener;
    #[async_trait::async_trait]
    impl MessageListenerTrait<TestMsg> for TestListener {
        async fn on_message(&self, _message: TestMsg) {}
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
        let mut client = client();
        connect(&mut client).await;
        let result = client
            .send(TestMsg {
                value: "test".to_string(),
            })
            .await;
        assert!(result.is_ok());
        disconnect(&mut client).await;
    }

    #[tokio::test]
    async fn send_message_when_not_connected_is_error() {
        let client = client();
        let result = client
            .send(TestMsg {
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
            .send(TestMsg {
                value: "test".to_string(),
            })
            .await;
        assert!(result.is_err());
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

    fn client() -> WebsocketClient<TestMsg> {
        let config = WebsocketConfig::new();
        let listener: MessageListener<TestMsg> = Arc::new(TestListener);
        WebsocketClient::new(config, listener, "ws://localhost:0")
    }
    async fn connect(client: &mut WebsocketClient<TestMsg>) {
        match client.connect().await {
            Ok(_) => {}
            _ => panic!("Client did not connect"),
        }
    }
    async fn disconnect(client: &mut WebsocketClient<TestMsg>) {
        match client.disconnect().await {
            Ok(_) => {}
            _ => panic!("Client did not disconnect"),
        }
    }
}

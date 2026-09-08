use crate::error::ConnectionError;
use serde::Serialize;

pub(crate) struct OutboundMessage<Request>
where
    Request: Serialize + Send + 'static,
{
    pub(crate) message: Request,
    pub(crate) ack: tokio::sync::oneshot::Sender<Result<(), ConnectionError>>,
}

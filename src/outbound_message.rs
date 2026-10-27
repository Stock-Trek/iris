use crate::error::ConnectionError;

pub(crate) struct OutboundMessage<Request>
where
    Request: Send + 'static,
{
    pub(crate) message: Request,
    pub(crate) ack: tokio::sync::oneshot::Sender<Result<(), ConnectionError>>,
}

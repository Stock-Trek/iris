use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    reset_timeout: Duration,
    max_reconnect_attempts: Option<u32>,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            reset_timeout: Duration::from_secs(60),
            max_reconnect_attempts: None,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl CircuitBreakerConfig {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_reset_timeout(mut self, timeout: Duration) -> Self {
        self.reset_timeout = timeout;
        self
    }
    pub fn with_max_reconnect_attempts(mut self, max: u32) -> Self {
        self.max_reconnect_attempts = Some(max);
        self
    }
    pub fn with_no_reconnect_limit(mut self) -> Self {
        self.max_reconnect_attempts = None;
        self
    }
    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }
    pub(crate) fn reset_timeout(&self) -> Duration {
        self.reset_timeout
    }
    pub(crate) fn max_reconnect_attempts(&self) -> Option<u32> {
        self.max_reconnect_attempts
    }
    pub(crate) fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }
    pub(crate) fn max_backoff(&self) -> Duration {
        self.max_backoff
    }
}

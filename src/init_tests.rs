#[cfg(test)]
#[ctor::ctor]
fn init_tests() {
    ensure_crypto();
}

pub fn ensure_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[ctor::ctor]
fn init() {
    ensure_crypto();
}

fn ensure_crypto() {
    println!("ensure crypto");
    let _ = rustls::crypto::ring::default_provider().install_default();
}

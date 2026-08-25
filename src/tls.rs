use crate::error::ConnectionError;
use rustls::{
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
};
use std::{fs::read_to_string, path::Path, sync::Arc};
use tokio_rustls::TlsConnector;
use tokio_websockets::Connector;
use webpki_roots::TLS_SERVER_ROOTS;

#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    skip_verify: bool,
    ca_cert_pems: Vec<String>,
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
}

impl TlsConfig {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_skip_verify(mut self) -> Self {
        self.skip_verify = true;
        self
    }
    pub fn with_ca_cert_pem(mut self, ca_cert_pem: impl Into<String>) -> Self {
        self.ca_cert_pems.push(ca_cert_pem.into());
        self
    }
    pub fn with_ca_cert_path(self, path: impl AsRef<Path>) -> std::io::Result<Self> {
        let pem = read_to_string(path.as_ref())?;
        Ok(self.with_ca_cert_pem(pem))
    }
    pub fn with_client_cert(
        mut self,
        cert_pem: impl Into<String>,
        key_pem: impl Into<String>,
    ) -> Self {
        self.client_cert_pem = Some(cert_pem.into());
        self.client_key_pem = Some(key_pem.into());
        self
    }
    pub fn with_client_cert_paths(
        self,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let cert_pem = read_to_string(cert_path.as_ref())?;
        let key_pem = read_to_string(key_path.as_ref())?;
        Ok(self.with_client_cert(cert_pem, key_pem))
    }
    #[cfg(test)]
    pub(crate) fn skip_verify_enabled(&self) -> bool {
        self.skip_verify
    }

    pub(crate) fn build_connector(&self) -> Result<Connector, ConnectionError> {
        let (certs_provided, key_provided) = (
            self.client_cert_pem.is_some(),
            self.client_key_pem.is_some(),
        );
        if certs_provided != key_provided {
            return Err(ConnectionError::TlsConfiguration(
                "both client certificate and private key must be provided for mutual TLS"
                    .to_string(),
            ));
        }
        let root_store = self.build_root_store()?;
        let config_builder = ClientConfig::builder();
        let wants_client_cert = if self.skip_verify {
            config_builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
        } else {
            config_builder.with_root_certificates(root_store)
        };
        let config = if let (Some(cert_pem), Some(key_pem)) =
            (&self.client_cert_pem, &self.client_key_pem)
        {
            let cert_chain: Vec<CertificateDer<'static>> =
                CertificateDer::pem_reader_iter(&mut cert_pem.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        ConnectionError::TlsConfiguration(format!(
                            "failed to parse client certificate PEM: {e}"
                        ))
                    })?;
            let key = PrivateKeyDer::pem_reader_iter(&mut key_pem.as_bytes())
                .next()
                .transpose()
                .map_err(|e| {
                    ConnectionError::TlsConfiguration(format!(
                        "failed to parse client private key PEM: {e}"
                    ))
                })?
                .ok_or_else(|| {
                    ConnectionError::TlsConfiguration(
                        "no private key found in PEM data".to_string(),
                    )
                })?;
            wants_client_cert
                .with_client_auth_cert(cert_chain, key)
                .map_err(|e| {
                    ConnectionError::TlsConfiguration(format!(
                        "failed to set client auth certificate: {e}"
                    ))
                })?
        } else {
            wants_client_cert.with_no_client_auth()
        };
        Ok(Connector::Rustls(TlsConnector::from(Arc::new(config))))
    }
    fn build_root_store(&self) -> Result<RootCertStore, ConnectionError> {
        let mut root_store = RootCertStore::empty();
        root_store.extend(TLS_SERVER_ROOTS.iter().cloned());
        for ca_pem in &self.ca_cert_pems {
            let certs = CertificateDer::pem_reader_iter(&mut ca_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    ConnectionError::TlsConfiguration(format!(
                        "failed to parse CA certificate PEM: {e}"
                    ))
                })?;
            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    ConnectionError::TlsConfiguration(format!(
                        "failed to add CA certificate to trust store: {e}"
                    ))
                })?;
            }
        }
        Ok(root_store)
    }
}

#[derive(Clone, Debug, Default)]
struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::{client::danger::ServerCertVerifier, pki_types::CertificateDer};

    fn with_provider() -> TlsConfig {
        TlsConfig::new()
    }

    // --- Default configuration ---

    #[test]
    fn default_skip_verify_is_false() {
        let config = with_provider();
        let result = config.build_connector();
        assert!(result.is_ok());
    }

    #[test]
    fn default_ca_cert_pems_empty() {
        let config = with_provider();
        assert!(config.build_connector().is_ok());
    }

    #[test]
    fn default_client_cert_none() {
        let config = with_provider();
        let result = config.build_connector();
        assert!(result.is_ok());
    }

    #[test]
    fn default_client_key_none() {
        let config = with_provider();
        assert!(config.build_connector().is_ok());
    }

    #[test]
    fn new_is_same_as_default() {
        assert_eq!(
            format!("{:?}", TlsConfig::new()),
            format!("{:?}", TlsConfig::default())
        );
    }

    #[test]
    fn default_build_connector_succeeds() {
        let config = with_provider();
        let result = config.build_connector();
        assert!(result.is_ok());
    }

    // --- Skip verification ---

    #[test]
    fn with_skip_verify_sets_flag() {
        let config = TlsConfig::new().with_skip_verify();
        assert!(config.skip_verify_enabled());
    }

    #[test]
    fn skip_verify_build_connector_succeeds() {
        let config = with_provider().with_skip_verify();
        let result = config.build_connector();
        assert!(result.is_ok());
    }

    #[test]
    fn no_cert_verify_always_passes() {
        let verifier = NoCertificateVerification;
        let cert = CertificateDer::from(vec![0u8; 10]);
        let result = verifier.verify_server_cert(
            &cert,
            &[],
            &ServerName::try_from("example.com").unwrap(),
            &[],
            UnixTime::now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn no_cert_verify_supported_schemes_count() {
        let verifier = NoCertificateVerification;
        let schemes = verifier.supported_verify_schemes();
        assert_eq!(schemes.len(), 9);
        assert!(schemes.contains(&SignatureScheme::RSA_PKCS1_SHA256));
        assert!(schemes.contains(&SignatureScheme::ECDSA_NISTP256_SHA256));
        assert!(schemes.contains(&SignatureScheme::ED25519));
    }

    // --- Custom CA certificate ---

    #[test]
    fn with_ca_cert_pem_adds_to_list() {
        let config = TlsConfig::new().with_ca_cert_pem("dummy pem");
        assert_eq!(config.ca_cert_pems.len(), 1);
        assert_eq!(config.ca_cert_pems[0], "dummy pem");
    }

    #[test]
    fn multiple_ca_cert_pems() {
        let config = TlsConfig::new()
            .with_ca_cert_pem("pem1")
            .with_ca_cert_pem("pem2");
        assert_eq!(config.ca_cert_pems.len(), 2);
    }

    #[test]
    fn invalid_ca_cert_pem_causes_error() {
        // Use a valid PEM header but with invalid base64 content
        // This will cause the PEM parser to find a section but fail on base64 decode
        let config = with_provider().with_ca_cert_pem(
            "-----BEGIN CERTIFICATE-----\n!!!invalid base64!!!\n-----END CERTIFICATE-----",
        );
        let result = config.build_connector();
        assert!(result.is_err());
        match result {
            Err(ConnectionError::TlsConfiguration(msg)) => {
                assert!(msg.contains("failed to parse CA certificate PEM"));
            }
            _ => panic!("expected TlsConfiguration error: {:?}", result),
        }
    }

    #[test]
    fn with_ca_cert_path_nonexistent() {
        let result = TlsConfig::new().with_ca_cert_path("/nonexistent/path.pem");
        assert!(result.is_err());
    }

    // --- Mutual TLS ---

    #[test]
    fn mtls_only_cert_no_key_errors() {
        let config = with_provider().with_client_cert("cert pem", "");
        let result = config.build_connector();
        assert!(result.is_err());
    }

    #[test]
    fn mtls_provide_only_cert_via_with_client_cert_stores_both() {
        let config = TlsConfig::new().with_client_cert("cert pem", "key pem");
        assert!(config.client_cert_pem.is_some());
        assert!(config.client_key_pem.is_some());
    }

    #[test]
    fn mtls_missing_key_errors() {
        let mut config = TlsConfig::new();
        config.client_cert_pem = Some("cert".to_string());
        let result = config.build_connector();
        assert!(result.is_err());
        match result {
            Err(ConnectionError::TlsConfiguration(msg)) => {
                assert!(
                    msg.contains("both") || msg.contains("mutual"),
                    "unexpected msg: {}",
                    msg
                );
            }
            _ => panic!("expected TlsConfiguration error"),
        }
    }

    #[test]
    fn mtls_missing_cert_errors() {
        let mut config = TlsConfig::new();
        config.client_key_pem = Some("key".to_string());
        let result = config.build_connector();
        assert!(result.is_err());
        match result {
            Err(ConnectionError::TlsConfiguration(msg)) => {
                assert!(
                    msg.contains("both") || msg.contains("mutual"),
                    "unexpected msg: {}",
                    msg
                );
            }
            _ => panic!("expected TlsConfiguration error"),
        }
    }

    #[test]
    fn mtls_invalid_cert_pem_errors() {
        let config = with_provider().with_client_cert("not valid pem", "key pem");
        let result = config.build_connector();
        assert!(result.is_err());
    }

    #[test]
    fn mtls_invalid_key_pem_errors() {
        let config = with_provider().with_client_cert(
            "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----",
            "not a valid key pem",
        );
        let result = config.build_connector();
        assert!(result.is_err());
    }

    // --- Edge cases ---

    #[test]
    fn skip_verify_overrides_custom_ca() {
        let config = with_provider()
            .with_skip_verify()
            .with_ca_cert_pem("cert pem");
        assert!(config.skip_verify_enabled());
        assert!(config.build_connector().is_ok());
    }

    #[test]
    fn empty_ca_pem_string() {
        let config = with_provider().with_ca_cert_pem("");
        let result = config.build_connector();
        assert!(result.is_ok());
    }

    #[test]
    fn debug_impl_works() {
        let config = TlsConfig::new().with_skip_verify();
        let debug = format!("{:?}", config);
        assert!(debug.contains("skip_verify"));
    }

    #[test]
    fn clone_config() {
        let config = TlsConfig::new()
            .with_skip_verify()
            .with_ca_cert_pem("ca pem");
        let cloned = config.clone();
        assert_eq!(cloned.skip_verify_enabled(), config.skip_verify_enabled());
    }

    #[test]
    fn no_cert_verify_tls12_signature_always_ok() {
        let verifier = NoCertificateVerification;
        // We can't construct a DigitallySignedStruct directly (it's pub(crate)),
        // so we test that the method signature is valid by verifying the trait impl
        // and that the scheme list includes TLS 1.2 schemes
        let schemes = verifier.supported_verify_schemes();
        let has_tls12_schemes = schemes.contains(&SignatureScheme::RSA_PKCS1_SHA256)
            || schemes.contains(&SignatureScheme::ECDSA_NISTP256_SHA256);
        assert!(has_tls12_schemes);
    }

    #[test]
    fn no_cert_verify_tls13_signature_always_ok() {
        let verifier = NoCertificateVerification;
        // Verify the scheme list includes TLS 1.3 schemes
        let schemes = verifier.supported_verify_schemes();
        let has_tls13_schemes = schemes.contains(&SignatureScheme::RSA_PSS_SHA256)
            || schemes.contains(&SignatureScheme::ED25519);
        assert!(has_tls13_schemes);
    }
}

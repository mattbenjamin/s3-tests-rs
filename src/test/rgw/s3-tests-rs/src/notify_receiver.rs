//! An HTTP endpoint for notification tests, in the test process.
//!
//! The gateway POSTs each notification to a topic's `push-endpoint`.
//! A test that starts one of these has that endpoint without a sidecar
//! (DECISIONS 2026-10-29):  it listens on the address the test reaches
//! the gateway from, on a port the system picks, so parallel tests do
//! not share one, and keeps every request it is sent.
//!
//! It plays the part of `test_bn.py`'s `HTTPServerWithEvents`:  each
//! POST is kept with its headers, an empty one is answered and dropped,
//! and `Expect: 100-continue` is honoured.  HTTP/1.1 with
//! `Content-Length` bodies and keep-alive is all it reads;  anything
//! else is recorded as an error, which `take()` reports.
//!
//! `start_tls()` serves HTTPS instead, under a CA made for the receiver,
//! and with `client_auth` asks the gateway for a certificate under the
//! same CA.  The CA, and a client certificate and key, are written to
//! files the topic names (`ca-location`, `ssl-certificate-location`,
//! `ssl-key-location`), so the gateway must share this process's
//! filesystem.  A connection whose handshake fails is counted, not
//! recorded as an error:  it is what a test of a missing CA or client
//! certificate expects.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose,
            IsCa, KeyPair, KeyUsagePurpose};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
                BufReader};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer, RootCertStore,
                           ServerConfig, server::WebPkiClientVerifier};
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;

/// One POST the gateway sent:  its headers, lower-cased, and its body.
#[derive(Debug, Clone)]
pub struct Received {
    pub headers: HashMap<String, String>,
    pub body: serde_json::Value,
}

#[derive(Default)]
struct Store {
    received: Vec<Received>,
    errors: Vec<String>,
    failed_handshakes: usize,
}

/// The files a topic names to reach an HTTPS receiver, in a directory
/// that lasts as long as the receiver.
pub struct TlsFiles {
    _dir: tempfile::TempDir,
    pub ca: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

pub struct HttpReceiver {
    addr: SocketAddr,
    store: Arc<Mutex<Store>>,
    task: JoinHandle<()>,
    tls: Option<TlsFiles>,
}

/// The local address this process reaches the gateway from, as
/// `test_bn.py`'s `get_ip()` finds one:  a UDP socket connected toward
/// the gateway sends nothing, and its local address is the one the
/// route uses.
fn local_ip_toward_gateway() -> IpAddr {
    let cfg = get_config();
    let sock = UdpSocket::bind("0.0.0.0:0").expect("bind a UDP socket");
    sock.connect((cfg.default_host.as_str(), cfg.default_port))
        .expect("route toward the gateway");
    sock.local_addr().expect("local address").ip()
}

/// A file the gateway reads, whatever user it runs as.
fn write_readable(path: &PathBuf, contents: &str) {
    std::fs::write(path, contents).expect("write a TLS file");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))
        .expect("make a TLS file readable");
}

/// A CA, a server certificate for `ip` and a client certificate, all
/// under the CA;  the server's TLS configuration, the files for the
/// gateway, and the client for the receiver's own first request.
fn make_tls(ip: IpAddr, client_auth: bool) -> (ServerConfig, TlsFiles, reqwest::Client) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name.push(DnType::CommonName, "s3-tests-rs notification CA");
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign,
                                KeyUsagePurpose::DigitalSignature];
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec![ip.to_string()]).unwrap();
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params.distinguished_name.push(DnType::CommonName, "s3-tests-rs gateway");
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();

    let dir = tempfile::Builder::new().prefix("s3tests-notify-tls").tempdir()
        .expect("a directory for the TLS files");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
        .expect("make the TLS directory readable");
    let files = TlsFiles {
        ca: dir.path().join("ca.pem"),
        client_cert: dir.path().join("client.pem"),
        client_key: dir.path().join("client-key.pem"),
        _dir: dir,
    };
    write_readable(&files.ca, &ca.pem());
    write_readable(&files.client_cert, &client.pem());
    write_readable(&files.client_key, &client_key.serialize_pem());

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions().unwrap();
    let builder = if client_auth {
        let mut roots = RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build().unwrap();
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let config = builder.with_single_cert(
            vec![server.der().clone()],
            PrivatePkcs8KeyDer::from(server_key.serialize_der()).into())
        .expect("the receiver's certificate");

    let mut http = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap());
    if client_auth {
        let identity = format!("{}{}", client.pem(), client_key.serialize_pem());
        http = http.identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap());
    }
    (config, files, http.build().unwrap())
}

impl HttpReceiver {
    pub async fn start() -> HttpReceiver {
        HttpReceiver::listen(None).await
    }

    /// An HTTPS receiver;  with `client_auth` it refuses a connection
    /// without a client certificate under its CA.
    pub async fn start_tls(client_auth: bool) -> HttpReceiver {
        HttpReceiver::listen(Some(client_auth)).await
    }

    async fn listen(tls: Option<bool>) -> HttpReceiver {
        let ip = local_ip_toward_gateway();
        let listener = TcpListener::bind((ip, 0)).await
            .expect("bind the notification receiver");
        let addr = listener.local_addr().unwrap();
        let store = Arc::new(Mutex::new(Store::default()));
        let (acceptor, files, http) = match tls {
            None => (None, None, reqwest::Client::new()),
            Some(client_auth) => {
                let (config, files, http) = make_tls(ip, client_auth);
                (Some(TlsAcceptor::from(Arc::new(config))), Some(files), http)
            }
        };
        let s = store.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((conn, _)) = listener.accept().await else { break };
                let s = s.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let served = match acceptor {
                        None => serve(conn, &s).await,
                        Some(acceptor) => match acceptor.accept(conn).await {
                            Ok(tls) => serve(tls, &s).await,
                            Err(_) => {
                                s.lock().unwrap().failed_handshakes += 1;
                                Ok(())
                            }
                        },
                    };
                    if let Err(e) = served {
                        s.lock().unwrap().errors.push(e);
                    }
                });
            }
        });
        let receiver = HttpReceiver { addr, store, task, tls: files };
        /* as HTTPServerWithEvents does:  it answers before it is used */
        let resp = http.post(receiver.endpoint())
            .body("").send().await.expect("POST to the receiver");
        assert_eq!(resp.status().as_u16(), 200, "the receiver does not answer");
        receiver
    }

    /// The `push-endpoint` to give a topic.
    pub fn endpoint(&self) -> String {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        format!("{scheme}://{}", self.addr)
    }

    /// The files an HTTPS receiver's topic names.
    pub fn tls_files(&self) -> &TlsFiles {
        self.tls.as_ref().expect("an HTTPS receiver")
    }

    /// Connections whose TLS handshake failed, since the receiver started.
    pub fn failed_handshakes(&self) -> usize {
        self.store.lock().unwrap().failed_handshakes
    }

    /// Every POST received since the last call, and none of them again
    /// (`get_and_reset_events()`).  Panics on a request it could not
    /// read, since that is the receiver's failure and not the test's.
    pub fn take(&self) -> Vec<Received> {
        let mut st = self.store.lock().unwrap();
        assert!(st.errors.is_empty(), "the receiver could not read a request: {:?}", st.errors);
        std::mem::take(&mut st.received)
    }
}

impl Drop for HttpReceiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One connection, request after request until the gateway closes it.
async fn serve<S: AsyncRead + AsyncWrite>(conn: S, store: &Mutex<Store>) -> Result<(), String> {
    let (rd, mut wr) = tokio::io::split(conn);
    let mut rd = BufReader::new(rd);
    loop {
        let mut line = String::new();
        if rd.read_line(&mut line).await.map_err(|e| e.to_string())? == 0 {
            return Ok(());
        }
        let request_line = line.trim_end().to_string();
        let mut headers = HashMap::new();
        loop {
            let mut h = String::new();
            if rd.read_line(&mut h).await.map_err(|e| e.to_string())? == 0 {
                return Err(format!("connection closed in the headers of {request_line:?}"));
            }
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            let (k, v) = h.split_once(':')
                .ok_or_else(|| format!("not a header: {h:?}"))?;
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
        if !request_line.starts_with("POST ") {
            return Err(format!("not a POST: {request_line:?}"));
        }
        if headers.contains_key("transfer-encoding") {
            return Err(format!("a body without Content-Length: {headers:?}"));
        }
        let len: usize = headers.get("content-length").map_or(Ok(0), |v| v.parse())
            .map_err(|e| format!("Content-Length: {e}"))?;
        if headers.get("expect").is_some_and(|v| v.eq_ignore_ascii_case("100-continue")) {
            wr.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await.map_err(|e| e.to_string())?;
        }
        let mut body = vec![0u8; len];
        rd.read_exact(&mut body).await.map_err(|e| e.to_string())?;
        if len > 0 {
            let body = serde_json::from_slice(&body)
                .map_err(|e| format!("a body that is not JSON ({e}): {}",
                                     String::from_utf8_lossy(&body)))?;
            store.lock().unwrap().received.push(Received { headers, body });
        }
        wr.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await
            .map_err(|e| e.to_string())?;
    }
}

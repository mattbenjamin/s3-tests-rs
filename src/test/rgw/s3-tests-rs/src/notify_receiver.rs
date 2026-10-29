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

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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
}

pub struct HttpReceiver {
    addr: SocketAddr,
    store: Arc<Mutex<Store>>,
    task: JoinHandle<()>,
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

impl HttpReceiver {
    pub async fn start() -> HttpReceiver {
        let listener = TcpListener::bind((local_ip_toward_gateway(), 0)).await
            .expect("bind the notification receiver");
        let addr = listener.local_addr().unwrap();
        let store = Arc::new(Mutex::new(Store::default()));
        let s = store.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((conn, _)) = listener.accept().await else { break };
                let s = s.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve(conn, &s).await {
                        s.lock().unwrap().errors.push(e);
                    }
                });
            }
        });
        let receiver = HttpReceiver { addr, store, task };
        /* as HTTPServerWithEvents does:  it answers before it is used */
        let resp = reqwest::Client::new().post(receiver.endpoint())
            .body("").send().await.expect("POST to the receiver");
        assert_eq!(resp.status().as_u16(), 200, "the receiver does not answer");
        receiver
    }

    /// The `push-endpoint` to give a topic.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
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
async fn serve(conn: TcpStream, store: &Mutex<Store>) -> Result<(), String> {
    let (rd, mut wr) = conn.into_split();
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

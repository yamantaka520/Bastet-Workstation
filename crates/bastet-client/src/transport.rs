use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{client::conn::http1, Request, StatusCode};
use hyper_util::rt::TokioIo;
use thiserror::Error;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const LOCAL_HTTP_ORIGIN: &str = "http://bastet.local";

#[derive(Clone)]
pub(crate) struct LocalTransport {
    endpoint: bastet_local_ipc::Endpoint,
}

#[derive(Debug)]
pub(crate) struct LocalResponse {
    pub(crate) status: StatusCode,
    pub(crate) body: Bytes,
}

#[derive(Debug, Error)]
pub enum LocalTransportError {
    #[error("local endpoint is unavailable")]
    EndpointUnavailable,
    #[error("local daemon connection timed out")]
    ConnectTimedOut,
    #[error("local daemon request timed out")]
    RequestTimedOut,
    #[error("local daemon connection failed")]
    ConnectionFailed,
    #[error("local daemon returned an invalid HTTP response")]
    InvalidHttpResponse,
    #[error("local daemon response body exceeded the limit")]
    ResponseTooLarge,
    #[error("local daemon response body could not be read")]
    ResponseBodyFailed,
    #[error("invalid local daemon request target")]
    InvalidTarget,
}

impl LocalTransport {
    pub(crate) fn new(endpoint: bastet_local_ipc::Endpoint) -> Self {
        Self { endpoint }
    }

    pub(crate) async fn send(
        &self,
        method: reqwest::Method,
        url: String,
        body: Vec<u8>,
    ) -> Result<LocalResponse, LocalTransportError> {
        let endpoint = self.endpoint.clone();
        tokio::time::timeout(REQUEST_TIMEOUT, async move {
            let stream =
                tokio::time::timeout(CONNECT_TIMEOUT, bastet_local_ipc::connect(&endpoint))
                    .await
                    .map_err(|_| LocalTransportError::ConnectTimedOut)?
                    .map_err(|_| LocalTransportError::ConnectionFailed)?;
            let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
                .await
                .map_err(|_| LocalTransportError::ConnectionFailed)?;
            let _connection = ConnectionTask::spawn(connection);

            let path = url
                .strip_prefix(LOCAL_HTTP_ORIGIN)
                .filter(|path| path.starts_with('/'))
                .ok_or(LocalTransportError::InvalidTarget)?;
            let request = Request::builder()
                .method(method.as_str())
                .uri(path)
                .header("host", "bastet.local")
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .map_err(|_| LocalTransportError::InvalidTarget)?;
            let response = sender
                .send_request(request)
                .await
                .map_err(|_| LocalTransportError::InvalidHttpResponse)?;
            let status = response.status();
            let mut body = response.into_body();
            let mut bytes = Vec::new();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|_| LocalTransportError::ResponseBodyFailed)?;
                if let Ok(data) = frame.into_data() {
                    if bytes.len().saturating_add(data.len()) > MAX_RESPONSE_BYTES {
                        return Err(LocalTransportError::ResponseTooLarge);
                    }
                    bytes.extend_from_slice(&data);
                }
            }
            Ok(LocalResponse {
                status,
                body: Bytes::from(bytes),
            })
        })
        .await
        .map_err(|_| LocalTransportError::RequestTimedOut)?
    }
}

struct ConnectionTask(tokio::task::JoinHandle<()>);

impl ConnectionTask {
    fn spawn(
        connection: hyper::client::conn::http1::Connection<
            TokioIo<bastet_local_ipc::ClientStream>,
            Full<Bytes>,
        >,
    ) -> Self {
        Self(tokio::spawn(async move {
            let _ = connection.await;
        }))
    }
}

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::io::Write;

    use axum::{response::Redirect, routing::get, Router};
    use tempfile::TempDir;

    use super::*;

    fn endpoint() -> (TempDir, bastet_local_ipc::Endpoint) {
        let directory = tempfile::tempdir().unwrap();
        let endpoint =
            bastet_local_ipc::Endpoint::for_database(&directory.path().join("bastet.db")).unwrap();
        (directory, endpoint)
    }

    async fn serve(
        router: Router,
    ) -> (
        TempDir,
        bastet_local_ipc::Endpoint,
        tokio::task::JoinHandle<()>,
    ) {
        let (directory, endpoint) = endpoint();
        let (listener, guard) = bastet_local_ipc::bind(&endpoint).unwrap();
        let server = tokio::spawn(async move {
            let _guard = guard;
            axum::serve(listener, router).await.unwrap();
        });
        (directory, endpoint, server)
    }

    #[tokio::test]
    async fn local_http_redirects_are_returned_not_followed() {
        let (_directory, endpoint, server) = serve(Router::new().route(
            "/redirect",
            get(|| async { Redirect::temporary("/target") }),
        ))
        .await;
        let response = LocalTransport::new(endpoint)
            .send(
                reqwest::Method::GET,
                format!("{LOCAL_HTTP_ORIGIN}/redirect"),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::TEMPORARY_REDIRECT);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn local_http_response_body_is_bounded() {
        let (_directory, endpoint, server) = serve(Router::new().route(
            "/large",
            get(|| async { "x".repeat(MAX_RESPONSE_BYTES + 1) }),
        ))
        .await;
        let error = LocalTransport::new(endpoint)
            .send(
                reqwest::Method::GET,
                format!("{LOCAL_HTTP_ORIGIN}/large"),
                Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, LocalTransportError::ResponseTooLarge));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn unavailable_endpoint_failure_is_sanitized() {
        let (_directory, endpoint) = endpoint();
        let error = LocalTransport::new(endpoint)
            .send(
                reqwest::Method::GET,
                format!("{LOCAL_HTTP_ORIGIN}/secret-response-body"),
                Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, LocalTransportError::ConnectionFailed));
        assert!(!error.to_string().contains("secret-response-body"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn truncated_response_is_sanitized_and_connection_driver_is_reaped() {
        use axum::serve::Listener;

        let (_directory, endpoint) = endpoint();
        let (mut listener, guard) = bastet_local_ipc::bind(&endpoint).unwrap();
        let server = tokio::spawn(async move {
            let _guard = guard;
            let (stream, ()) = listener.accept().await;
            let mut stream = stream.into_std().unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\nshort")
                .unwrap();
        });
        let error = LocalTransport::new(endpoint)
            .send(
                reqwest::Method::GET,
                format!("{LOCAL_HTTP_ORIGIN}/truncated"),
                Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &error,
                LocalTransportError::ResponseBodyFailed | LocalTransportError::InvalidHttpResponse
            ),
            "unexpected sanitized error: {error:?}"
        );
        assert!(!error.to_string().contains("short"));
        server.await.unwrap();
    }
}

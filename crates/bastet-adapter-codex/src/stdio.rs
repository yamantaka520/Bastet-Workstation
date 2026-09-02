use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde_json::{json, Value};

use crate::{AppServerTransport, TransportError};

pub struct StdioTransport {
    child: Child,
    stdin: Option<ChildStdin>,
    responses: Receiver<Result<Value, TransportError>>,
    reader: Option<JoinHandle<()>>,
    next_request_id: u64,
    timeout: Duration,
}

impl StdioTransport {
    pub fn spawn(executable: &Path, timeout: Duration) -> Result<Self, TransportError> {
        if timeout.is_zero() {
            return Err(TransportError);
        }
        let mut child = Command::new(executable)
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| TransportError)?;
        let stdin = child.stdin.take().ok_or(TransportError)?;
        let stdout = child.stdout.take().ok_or(TransportError)?;
        let (sender, responses) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let response = line
                    .map_err(|_| TransportError)
                    .and_then(|line| serde_json::from_str(&line).map_err(|_| TransportError));
                let failed = response.is_err();
                if sender.send(response).is_err() || failed {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            stdin: Some(stdin),
            responses,
            reader: Some(reader),
            next_request_id: 0,
            timeout,
        })
    }

    pub fn close(&mut self) {
        self.stdin.take();
        let deadline = Instant::now() + self.timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Ok(None) | Err(_) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break;
                }
            }
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }

    fn write_message(&mut self, message: &Value) -> Result<(), TransportError> {
        let stdin = self.stdin.as_mut().ok_or(TransportError)?;
        serde_json::to_writer(&mut *stdin, message).map_err(|_| TransportError)?;
        stdin.write_all(b"\n").map_err(|_| TransportError)?;
        stdin.flush().map_err(|_| TransportError)
    }
}

impl AppServerTransport for StdioTransport {
    fn request(&mut self, method: &str, params: Value) -> Result<Value, TransportError> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).ok_or(TransportError)?;
        self.write_message(&json!({
            "method": method,
            "id": request_id,
            "params": params
        }))?;
        let deadline = Instant::now() + self.timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(TransportError)?;
            let message = match self.responses.recv_timeout(remaining) {
                Ok(message) => message?,
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                    return Err(TransportError)
                }
            };
            match decode_response(message, request_id)? {
                ResponseDisposition::Notification => continue,
                ResponseDisposition::Result(result) => return Ok(result),
            }
        }
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError> {
        self.write_message(&json!({"method": method, "params": params}))
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        self.close();
    }
}

enum ResponseDisposition {
    Notification,
    Result(Value),
}

fn decode_response(
    message: Value,
    expected_id: u64,
) -> Result<ResponseDisposition, TransportError> {
    let Some(id) = message.get("id") else {
        return if message.get("method").and_then(Value::as_str).is_some() {
            Ok(ResponseDisposition::Notification)
        } else {
            Err(TransportError)
        };
    };
    if id.as_u64() != Some(expected_id) {
        return Err(TransportError);
    }
    if message.get("error").is_some() {
        return Err(TransportError);
    }
    message
        .get("result")
        .cloned()
        .map(ResponseDisposition::Result)
        .ok_or(TransportError)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_response_is_returned() {
        let response = decode_response(json!({"id": 7, "result": {"ok": true}}), 7).unwrap();
        match response {
            ResponseDisposition::Result(value) => assert_eq!(value["ok"], true),
            ResponseDisposition::Notification => panic!("expected response"),
        }
    }

    #[test]
    fn notifications_can_be_skipped_while_waiting() {
        assert!(matches!(
            decode_response(json!({"method": "server/notice", "params": {}}), 7).unwrap(),
            ResponseDisposition::Notification
        ));
    }

    #[test]
    fn mismatched_ids_and_remote_errors_fail_closed() {
        assert!(decode_response(json!({"id": 8, "result": {}}), 7).is_err());
        assert!(decode_response(
            json!({"id": 7, "error": {"code": 1, "message": "private"}}),
            7
        )
        .is_err());
    }
}

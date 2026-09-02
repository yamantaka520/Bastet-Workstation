use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde_json::{json, Value};

use crate::{AppServerNotification, AppServerTransport, TransportError};

pub struct StdioTransport {
    child: Child,
    stdin: Option<ChildStdin>,
    responses: Receiver<Result<Value, TransportError>>,
    pending_notifications: VecDeque<AppServerNotification>,
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
            pending_notifications: VecDeque::new(),
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

    fn receive_message(&self, deadline: Instant) -> Result<Value, TransportError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(TransportError)?;
        match self.responses.recv_timeout(remaining) {
            Ok(message) => message,
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => Err(TransportError),
        }
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
            let message = self.receive_message(deadline)?;
            if let Some(result) =
                route_request_message(message, request_id, &mut self.pending_notifications)?
            {
                return Ok(result);
            }
        }
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError> {
        self.write_message(&json!({"method": method, "params": params}))
    }

    fn next_notification(&mut self) -> Result<AppServerNotification, TransportError> {
        if let Some(notification) = self.pending_notifications.pop_front() {
            return Ok(notification);
        }
        let message = self.receive_message(Instant::now() + self.timeout)?;
        decode_notification(message)
    }
}

fn route_request_message(
    message: Value,
    expected_id: u64,
    pending_notifications: &mut VecDeque<AppServerNotification>,
) -> Result<Option<Value>, TransportError> {
    match decode_response(message, expected_id)? {
        ResponseDisposition::Notification(notification) => {
            pending_notifications.push_back(notification);
            Ok(None)
        }
        ResponseDisposition::Result(result) => Ok(Some(result)),
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        self.close();
    }
}

enum ResponseDisposition {
    Notification(AppServerNotification),
    Result(Value),
}

fn decode_response(
    message: Value,
    expected_id: u64,
) -> Result<ResponseDisposition, TransportError> {
    let Some(id) = message.get("id") else {
        return decode_notification(message).map(ResponseDisposition::Notification);
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

fn decode_notification(message: Value) -> Result<AppServerNotification, TransportError> {
    if message.get("id").is_some() {
        return Err(TransportError);
    }
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(TransportError)?;
    let params = message.get("params").cloned().ok_or(TransportError)?;
    Ok(AppServerNotification {
        method: method.into(),
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_response_is_returned() {
        let response = decode_response(json!({"id": 7, "result": {"ok": true}}), 7).unwrap();
        match response {
            ResponseDisposition::Result(value) => assert_eq!(value["ok"], true),
            ResponseDisposition::Notification(_) => panic!("expected response"),
        }
    }

    #[test]
    fn notifications_can_be_skipped_while_waiting() {
        let disposition =
            decode_response(json!({"method": "server/notice", "params": {}}), 7).unwrap();
        assert_eq!(
            match disposition {
                ResponseDisposition::Notification(notification) => notification,
                ResponseDisposition::Result(_) => panic!("expected notification"),
            },
            AppServerNotification {
                method: "server/notice".into(),
                params: json!({})
            }
        );
    }

    #[test]
    fn notifications_seen_before_a_response_are_preserved_in_order() {
        let mut pending = VecDeque::new();
        assert_eq!(
            route_request_message(
                json!({"method": "turn/started", "params": {"turn": {"id": "turn_1"}}}),
                7,
                &mut pending,
            )
            .unwrap(),
            None
        );
        assert_eq!(
            route_request_message(json!({"id": 7, "result": {"ok": true}}), 7, &mut pending)
                .unwrap(),
            Some(json!({"ok": true}))
        );
        assert_eq!(pending.pop_front().unwrap().method, "turn/started");
    }

    #[test]
    fn mismatched_ids_and_remote_errors_fail_closed() {
        assert!(decode_response(json!({"id": 8, "result": {}}), 7).is_err());
        assert!(decode_response(
            json!({"id": 7, "error": {"code": 1, "message": "private"}}),
            7
        )
        .is_err());
        assert!(decode_notification(json!({"method": "", "params": {}})).is_err());
        assert!(decode_notification(json!({"method": "turn/started"})).is_err());
        assert!(decode_notification(json!({"id": 3, "method": "approval"})).is_err());
    }
}

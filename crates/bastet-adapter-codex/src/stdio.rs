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
    notification_deadline: Option<Instant>,
    abandoned_request_ids: VecDeque<u64>,
}

impl StdioTransport {
    pub fn spawn(executable: &Path, timeout: Duration) -> Result<Self, TransportError> {
        if timeout.is_zero() {
            return Err(TransportError::ProtocolDrift);
        }
        let mut child = Command::new(executable)
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| TransportError::Unavailable)?;
        let stdin = child.stdin.take().ok_or(TransportError::Unavailable)?;
        let stdout = child.stdout.take().ok_or(TransportError::Unavailable)?;
        let (sender, responses) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let response = line
                    .map_err(|_| TransportError::Unavailable)
                    .and_then(|line| {
                        serde_json::from_str(&line).map_err(|_| TransportError::ProtocolDrift)
                    });
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
            notification_deadline: None,
            abandoned_request_ids: VecDeque::new(),
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
        let stdin = self.stdin.as_mut().ok_or(TransportError::Unavailable)?;
        serde_json::to_writer(&mut *stdin, message).map_err(|_| TransportError::ProtocolDrift)?;
        stdin
            .write_all(b"\n")
            .map_err(|_| TransportError::Unavailable)?;
        stdin.flush().map_err(|_| TransportError::Unavailable)
    }

    fn receive_message(&mut self, deadline: Instant) -> Result<Value, TransportError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(TransportError::TimedOut)?;
        match self.responses.recv_timeout(remaining) {
            Ok(message) => {
                let message = message?;
                // Activity observed while an RPC is in flight is still
                // provider transport activity. Preserve it for subsequent
                // bounded run polling rather than carrying a stale deadline.
                self.notification_deadline = Some(Instant::now() + self.timeout);
                Ok(message)
            }
            Err(RecvTimeoutError::Timeout) => Err(TransportError::TimedOut),
            Err(RecvTimeoutError::Disconnected) => Err(TransportError::Unavailable),
        }
    }
}

impl AppServerTransport for StdioTransport {
    fn request(&mut self, method: &str, params: Value) -> Result<Value, TransportError> {
        self.request_inner(method, params, self.timeout, false)
    }

    fn request_with_timeout(
        &mut self,
        method: &str,
        params: Value,
        max_wait: Duration,
    ) -> Result<Value, TransportError> {
        self.request_inner(method, params, max_wait, true)
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError> {
        self.write_message(&json!({"method": method, "params": params}))
    }

    fn next_notification(&mut self) -> Result<AppServerNotification, TransportError> {
        self.poll_notification(self.timeout)?
            .ok_or(TransportError::TimedOut)
    }

    fn poll_notification(
        &mut self,
        max_wait: Duration,
    ) -> Result<Option<AppServerNotification>, TransportError> {
        if let Some(notification) = self.pending_notifications.pop_front() {
            self.notification_deadline = Some(Instant::now() + self.timeout);
            return Ok(Some(notification));
        }
        let now = Instant::now();
        let inactivity_deadline = self.notification_deadline.unwrap_or_else(|| {
            let deadline = now + self.timeout;
            self.notification_deadline = Some(deadline);
            deadline
        });
        if now >= inactivity_deadline {
            return Err(TransportError::TimedOut);
        }
        let wait = max_wait.min(inactivity_deadline.saturating_duration_since(now));
        loop {
            match self.responses.recv_timeout(wait) {
                Ok(Ok(message)) if self.discard_abandoned_response(&message) => continue,
                Ok(Ok(message)) => {
                    self.notification_deadline = Some(Instant::now() + self.timeout);
                    return decode_notification(message).map(Some);
                }
                Ok(Err(error)) => return Err(error),
                Err(RecvTimeoutError::Disconnected) => return Err(TransportError::Unavailable),
                Err(RecvTimeoutError::Timeout) if Instant::now() >= inactivity_deadline => {
                    return Err(TransportError::TimedOut);
                }
                Err(RecvTimeoutError::Timeout) => return Ok(None),
            }
        }
    }
}

impl StdioTransport {
    fn request_inner(
        &mut self,
        method: &str,
        params: Value,
        max_wait: Duration,
        abandon_on_timeout: bool,
    ) -> Result<Value, TransportError> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(TransportError::ProtocolDrift)?;
        self.write_message(&json!({
            "method": method,
            "id": request_id,
            "params": params
        }))?;
        let deadline = Instant::now() + max_wait;
        loop {
            let message = match self.receive_message(deadline) {
                Ok(message) => message,
                Err(TransportError::TimedOut) if abandon_on_timeout => {
                    self.remember_abandoned_request(request_id);
                    return Err(TransportError::TimedOut);
                }
                Err(error) => return Err(error),
            };
            if self.discard_abandoned_response(&message) {
                continue;
            }
            if let Some(result) =
                route_request_message(message, request_id, &mut self.pending_notifications)?
            {
                return Ok(result);
            }
        }
    }

    fn remember_abandoned_request(&mut self, request_id: u64) {
        const MAX_ABANDONED_REQUESTS: usize = 64;
        if self.abandoned_request_ids.len() == MAX_ABANDONED_REQUESTS {
            self.abandoned_request_ids.pop_front();
        }
        self.abandoned_request_ids.push_back(request_id);
    }

    fn discard_abandoned_response(&mut self, message: &Value) -> bool {
        let Some(id) = message.get("id").and_then(Value::as_u64) else {
            return false;
        };
        let Some(position) = self
            .abandoned_request_ids
            .iter()
            .position(|known| *known == id)
        else {
            return false;
        };
        self.abandoned_request_ids.remove(position);
        true
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

#[derive(Debug)]
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
        return Err(TransportError::ProtocolDrift);
    }
    if let Some(error) = message.get("error") {
        let code = error
            .get("code")
            .and_then(Value::as_i64)
            .ok_or(TransportError::ProtocolDrift)?;
        if error
            .get("message")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(TransportError::ProtocolDrift);
        }
        return Err(TransportError::RemoteRejected {
            code,
            retryable: code == -32001,
        });
    }
    message
        .get("result")
        .cloned()
        .map(ResponseDisposition::Result)
        .ok_or(TransportError::ProtocolDrift)
}

fn decode_notification(message: Value) -> Result<AppServerNotification, TransportError> {
    if message.get("id").is_some() {
        return Err(TransportError::ProtocolDrift);
    }
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(TransportError::ProtocolDrift)?;
    let params = message
        .get("params")
        .cloned()
        .ok_or(TransportError::ProtocolDrift)?;
    Ok(AppServerNotification {
        method: method.into(),
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::{
        process::{Command, Stdio},
        sync::mpsc::{self, Sender},
    };

    #[cfg(unix)]
    fn clock_transport(
        timeout: Duration,
    ) -> (StdioTransport, Sender<Result<Value, TransportError>>) {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let _stdout = child.stdout.take();
        let (sender, responses) = mpsc::channel();
        (
            StdioTransport {
                child,
                stdin,
                responses,
                pending_notifications: VecDeque::new(),
                reader: None,
                next_request_id: 0,
                timeout,
                notification_deadline: None,
                abandoned_request_ids: VecDeque::new(),
            },
            sender,
        )
    }

    #[cfg(unix)]
    #[test]
    fn repeated_short_polls_do_not_extend_the_stdio_inactivity_deadline() {
        let (mut transport, _sender) = clock_transport(Duration::from_secs(10));
        assert_eq!(transport.poll_notification(Duration::ZERO).unwrap(), None);
        let deadline = transport.notification_deadline;
        for _ in 0..3 {
            assert_eq!(transport.poll_notification(Duration::ZERO).unwrap(), None);
            assert_eq!(transport.notification_deadline, deadline);
        }
        // Exercise expiry without depending on the runner's scheduling latency.
        transport.notification_deadline = Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(
            transport.poll_notification(Duration::ZERO).unwrap_err(),
            TransportError::TimedOut
        );
    }

    #[cfg(unix)]
    #[test]
    fn rpc_and_queued_notification_activity_renew_the_stdio_deadline() {
        let (mut transport, sender) = clock_transport(Duration::from_secs(10));
        transport.notification_deadline = Some(Instant::now() - Duration::from_secs(1));
        sender
            .send(Ok(json!({"method": "turn/started", "params": {}})))
            .unwrap();
        sender.send(Ok(json!({"id": 0, "result": {}}))).unwrap();
        transport.request("test/request", json!({})).unwrap();
        assert!(transport.notification_deadline.unwrap() > Instant::now());
        transport.notification_deadline = Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(
            transport.next_notification().unwrap().method,
            "turn/started"
        );
        assert!(transport.notification_deadline.unwrap() > Instant::now());
        assert_eq!(transport.poll_notification(Duration::ZERO).unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn legacy_next_notification_honors_an_elapsed_stdio_deadline() {
        let (mut transport, _sender) = clock_transport(Duration::from_secs(10));
        assert_eq!(transport.poll_notification(Duration::ZERO).unwrap(), None);
        transport.notification_deadline = Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(
            transport.next_notification().unwrap_err(),
            TransportError::TimedOut
        );
    }

    #[cfg(unix)]
    #[test]
    fn bounded_request_times_out_without_accepting_a_late_response() {
        let (mut transport, sender) = clock_transport(Duration::from_secs(10));
        assert_eq!(
            transport
                .request_with_timeout("turn/interrupt", json!({}), Duration::ZERO)
                .unwrap_err(),
            TransportError::TimedOut
        );
        assert_eq!(transport.abandoned_request_ids, VecDeque::from([0]));
        // Request 0 timed out. Its eventual response must be discarded while
        // request 1 waits, rather than being treated as request 1's success.
        sender
            .send(Ok(json!({"id": 0, "result": {"old": true}})))
            .unwrap();
        sender
            .send(Ok(json!({"id": 1, "result": {"fresh": true}})))
            .unwrap();
        assert_eq!(
            transport.request("next/request", json!({})).unwrap(),
            json!({"fresh": true})
        );
    }

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
    fn mismatched_ids_and_malformed_remote_errors_fail_closed() {
        assert_eq!(
            decode_response(json!({"id": 8, "result": {}}), 7).unwrap_err(),
            TransportError::ProtocolDrift
        );
        assert_eq!(
            decode_response(json!({"id": 7, "error": {"code": 1}}), 7).unwrap_err(),
            TransportError::ProtocolDrift
        );
        assert!(decode_notification(json!({"method": "", "params": {}})).is_err());
        assert!(decode_notification(json!({"method": "turn/started"})).is_err());
        assert!(decode_notification(json!({"id": 3, "method": "approval"})).is_err());
    }

    #[test]
    fn remote_rejections_keep_only_code_and_retryability() {
        let private = "private provider detail must not survive";
        let rejected = decode_response(
            json!({"id": 7, "error": {"code": 123, "message": private, "data": private}}),
            7,
        )
        .unwrap_err();
        assert_eq!(
            rejected,
            TransportError::RemoteRejected {
                code: 123,
                retryable: false
            }
        );
        assert!(!rejected.to_string().contains(private));

        assert_eq!(
            decode_response(
                json!({"id": 7, "error": {
                    "code": -32001,
                    "message": "Server overloaded; retry later."
                }}),
                7,
            )
            .unwrap_err(),
            TransportError::RemoteRejected {
                code: -32001,
                retryable: true
            }
        );
    }
}

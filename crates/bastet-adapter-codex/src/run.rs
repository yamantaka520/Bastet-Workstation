use bastet_core::{NormalizedRunState, RunId};
use thiserror::Error;

use crate::{
    AppServerError, AppServerTransport, CodexAppServer, CodexRunEvidence, CodexRunEvidenceUpdate,
    CodexRunStream, CodexRunUpdate, EvidenceError, LifecycleError, ThreadHandle,
    WorkspaceEvidenceError, WorkspaceSnapshot,
};

#[derive(Debug, Error)]
pub enum RunTrackerError {
    #[error(transparent)]
    AppServer(#[from] AppServerError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Evidence(#[from] EvidenceError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceEvidenceError),
}

pub struct CodexRunTracker {
    provider_thread_id: String,
    provider_turn_id: String,
    stream: CodexRunStream,
    evidence: CodexRunEvidence,
    before: Option<WorkspaceSnapshot>,
    provider_write_receipt: bool,
    pending_terminal: Option<CodexRunUpdate>,
}

impl CodexRunTracker {
    pub fn new(
        run_id: RunId,
        provider_thread_id: &str,
        provider_turn_id: &str,
        workspace_before: Option<WorkspaceSnapshot>,
    ) -> Result<Self, RunTrackerError> {
        Ok(Self {
            provider_thread_id: provider_thread_id.to_owned(),
            provider_turn_id: provider_turn_id.to_owned(),
            stream: CodexRunStream::new(run_id, provider_turn_id)?,
            evidence: CodexRunEvidence::new(provider_thread_id, provider_turn_id)?,
            before: workspace_before,
            provider_write_receipt: false,
            pending_terminal: None,
        })
    }

    pub fn request_cancellation<T: AppServerTransport>(
        &mut self,
        server: &mut CodexAppServer<T>,
        occurred_at: &str,
    ) -> Result<CodexRunUpdate, RunTrackerError> {
        server.interrupt_turn(&self.provider_thread_id, &self.provider_turn_id)?;
        Ok(CodexRunUpdate::Lifecycle(
            self.stream.cancellation_requested(occurred_at)?,
        ))
    }

    pub fn resume_thread<T: AppServerTransport>(
        &mut self,
        server: &mut CodexAppServer<T>,
        occurred_at: &str,
    ) -> Result<(ThreadHandle, CodexRunUpdate), RunTrackerError> {
        let handle = server.resume_thread(&self.provider_thread_id)?;
        let event = self
            .stream
            .recovery_started(&handle.thread_id, occurred_at)?;
        Ok((handle, CodexRunUpdate::Lifecycle(event)))
    }

    pub fn next_update<T: AppServerTransport>(
        &mut self,
        server: &mut CodexAppServer<T>,
        occurred_at: &str,
    ) -> Result<CodexRunUpdate, RunTrackerError> {
        if let Some(terminal) = self.pending_terminal.take() {
            return Ok(terminal);
        }
        let update = server.next_run_update(&mut self.stream, &self.evidence, occurred_at)?;
        self.order_update(update)
    }

    fn order_update(&mut self, update: CodexRunUpdate) -> Result<CodexRunUpdate, RunTrackerError> {
        if matches!(
            &update,
            CodexRunUpdate::Evidence(CodexRunEvidenceUpdate::WriteReceipt(_))
        ) {
            self.provider_write_receipt = true;
            return Ok(update);
        }
        let terminal = matches!(
            &update,
            CodexRunUpdate::Lifecycle(event)
                if matches!(
                    event.event.state,
                    NormalizedRunState::Cancelled
                        | NormalizedRunState::Failed
                        | NormalizedRunState::Succeeded
                        | NormalizedRunState::Uncertain
                )
        );
        if !terminal || self.provider_write_receipt {
            return Ok(update);
        }
        let Some(before) = &self.before else {
            return Ok(update);
        };
        let after = WorkspaceSnapshot::capture(before.root())?;
        let Some(receipt) = before.write_receipt(&after)? else {
            return Ok(update);
        };
        self.pending_terminal = Some(update);
        Ok(CodexRunUpdate::Evidence(
            CodexRunEvidenceUpdate::WriteReceipt(receipt),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, fs};

    use bastet_core::EvidenceClass;
    use serde_json::json;

    use super::*;
    use crate::{AppServerNotification, CodexEventNormalizer, TransportError};

    #[derive(Default)]
    struct FixtureTransport {
        responses: VecDeque<Result<serde_json::Value, TransportError>>,
        requests: Vec<(String, serde_json::Value)>,
    }

    impl AppServerTransport for FixtureTransport {
        fn request(
            &mut self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, TransportError> {
            self.requests.push((method.into(), params));
            self.responses.pop_front().unwrap()
        }

        fn notify(
            &mut self,
            _method: &str,
            _params: serde_json::Value,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        fn next_notification(&mut self) -> Result<AppServerNotification, TransportError> {
            Err(TransportError::Unavailable)
        }
    }

    fn terminal(run_id: RunId) -> CodexRunUpdate {
        let mut normalizer = CodexEventNormalizer::new(run_id);
        CodexRunUpdate::Lifecycle(
            normalizer
                .normalize_notification(
                    "turn/completed",
                    &json!({"turn": {"id": "turn_1", "status": "completed"}}),
                    "2026-09-06T00:00:00Z",
                )
                .unwrap()
                .unwrap(),
        )
    }

    #[test]
    fn local_write_receipt_is_ordered_before_terminal_state() {
        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::from_bytes([8; 16]);
        let before = WorkspaceSnapshot::capture(root.path()).unwrap();
        let mut tracker = CodexRunTracker::new(run_id, "thr_1", "turn_1", Some(before)).unwrap();
        fs::write(root.path().join("receipt.txt"), "secret").unwrap();

        let update = tracker.order_update(terminal(run_id)).unwrap();
        let CodexRunUpdate::Evidence(CodexRunEvidenceUpdate::WriteReceipt(receipt)) = update else {
            panic!("local write evidence must precede terminal state");
        };
        assert!(receipt.contains("locally_measured"));
        assert!(!receipt.contains("receipt.txt"));
        assert!(!receipt.contains("secret"));

        let CodexRunUpdate::Lifecycle(event) = tracker.pending_terminal.take().unwrap() else {
            panic!("terminal state must remain pending");
        };
        assert_eq!(event.event.state, NormalizedRunState::Succeeded);
    }

    #[test]
    fn provider_receipt_prevents_duplicate_local_receipt() {
        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::from_bytes([9; 16]);
        let before = WorkspaceSnapshot::capture(root.path()).unwrap();
        let mut tracker = CodexRunTracker::new(run_id, "thr_1", "turn_1", Some(before)).unwrap();
        fs::write(root.path().join("receipt.txt"), "secret").unwrap();
        tracker
            .order_update(CodexRunUpdate::Evidence(
                CodexRunEvidenceUpdate::WriteReceipt(
                    json!({"evidence_class": EvidenceClass::ProviderReported}).to_string(),
                ),
            ))
            .unwrap();

        let CodexRunUpdate::Lifecycle(event) = tracker.order_update(terminal(run_id)).unwrap()
        else {
            panic!("provider evidence must avoid a duplicate local receipt");
        };
        assert_eq!(event.event.state, NormalizedRunState::Succeeded);
        assert!(tracker.pending_terminal.is_none());
    }

    #[test]
    fn cancellation_is_recorded_only_after_interrupt_is_accepted() {
        let run_id = RunId::from_bytes([10; 16]);
        let mut tracker = CodexRunTracker::new(run_id, "thr_1", "turn_1", None).unwrap();
        let mut server = CodexAppServer::new(FixtureTransport {
            responses: VecDeque::from([Ok(json!({})), Ok(json!({}))]),
            ..FixtureTransport::default()
        });
        server.initialize().unwrap();
        let CodexRunUpdate::Lifecycle(update) = tracker
            .request_cancellation(&mut server, "2026-09-06T00:00:00Z")
            .unwrap()
        else {
            panic!("accepted interruption must emit lifecycle evidence");
        };
        assert_eq!(update.event.state, NormalizedRunState::Cancelling);
        let transport = server.into_transport();
        assert_eq!(transport.requests[1].0, "turn/interrupt");
        assert_eq!(
            transport.requests[1].1,
            json!({"threadId": "thr_1", "turnId": "turn_1"})
        );

        let mut rejected = CodexAppServer::new(FixtureTransport {
            responses: VecDeque::from([
                Ok(json!({})),
                Err(TransportError::RemoteRejected {
                    code: 99,
                    retryable: false,
                }),
            ]),
            ..FixtureTransport::default()
        });
        rejected.initialize().unwrap();
        let mut fresh = CodexRunTracker::new(run_id, "thr_1", "turn_1", None).unwrap();
        assert!(fresh
            .request_cancellation(&mut rejected, "2026-09-06T00:00:00Z")
            .is_err());
        assert_eq!(
            fresh
                .stream
                .cancellation_requested("later")
                .unwrap()
                .event
                .sequence,
            1
        );
    }

    #[test]
    fn recovery_is_recorded_only_after_resume_is_accepted() {
        let run_id = RunId::from_bytes([11; 16]);
        let mut tracker = CodexRunTracker::new(run_id, "thr_1", "turn_1", None).unwrap();
        let mut server = CodexAppServer::new(FixtureTransport {
            responses: VecDeque::from([
                Ok(json!({})),
                Ok(json!({"thread": {"id": "thr_1", "sessionId": "session_1"}})),
            ]),
            ..FixtureTransport::default()
        });
        server.initialize().unwrap();
        let (handle, CodexRunUpdate::Lifecycle(update)) = tracker
            .resume_thread(&mut server, "2026-09-06T00:00:00Z")
            .unwrap()
        else {
            panic!("accepted resume must emit lifecycle evidence");
        };
        assert_eq!(handle.thread_id, "thr_1");
        assert_eq!(update.event.state, NormalizedRunState::Recovering);
        assert_eq!(update.event.sequence, 1);

        let mut rejected = CodexAppServer::new(FixtureTransport {
            responses: VecDeque::from([
                Ok(json!({})),
                Err(TransportError::RemoteRejected {
                    code: 55,
                    retryable: false,
                }),
            ]),
            ..FixtureTransport::default()
        });
        rejected.initialize().unwrap();
        let mut fresh = CodexRunTracker::new(run_id, "thr_1", "turn_1", None).unwrap();
        assert!(fresh
            .resume_thread(&mut rejected, "2026-09-06T00:00:00Z")
            .is_err());
        assert_eq!(
            fresh
                .stream
                .recovery_started("thr_1", "later")
                .unwrap()
                .event
                .sequence,
            1
        );
    }
}

use bastet_core::{NormalizedRunState, RunId};
use thiserror::Error;

use crate::{
    AppServerError, AppServerTransport, CodexAppServer, CodexRunEvidence, CodexRunEvidenceUpdate,
    CodexRunStream, CodexRunUpdate, EvidenceError, LifecycleError, WorkspaceEvidenceError,
    WorkspaceSnapshot,
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
            stream: CodexRunStream::new(run_id, provider_turn_id)?,
            evidence: CodexRunEvidence::new(provider_thread_id, provider_turn_id)?,
            before: workspace_before,
            provider_write_receipt: false,
            pending_terminal: None,
        })
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
    use std::fs;

    use bastet_core::EvidenceClass;
    use serde_json::json;

    use super::*;
    use crate::CodexEventNormalizer;

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
}

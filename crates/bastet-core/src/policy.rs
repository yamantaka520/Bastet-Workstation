use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionLevel {
    Deny,
    Observe,
    Use,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyCeiling {
    pub filesystem: PermissionLevel,
    pub network: PermissionLevel,
    pub process: PermissionLevel,
    pub device: PermissionLevel,
    pub credential: PermissionLevel,
    pub persistent_approval: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyLayer {
    Workstation,
    Project,
    RoleOrAgent,
    SingleRun,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyError {
    #[error("child policy exceeds parent ceiling for {field}")]
    ExceedsParent { field: &'static str },
    #[error("policy layers must become more specific")]
    InvalidLayerOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedPolicy {
    pub layer: PolicyLayer,
    pub ceiling: PolicyCeiling,
}

impl ScopedPolicy {
    pub fn restrict(&self, child: ScopedPolicy) -> Result<ScopedPolicy, PolicyError> {
        if child.layer <= self.layer {
            return Err(PolicyError::InvalidLayerOrder);
        }
        for (field, parent, requested) in [
            (
                "filesystem",
                self.ceiling.filesystem,
                child.ceiling.filesystem,
            ),
            ("network", self.ceiling.network, child.ceiling.network),
            ("process", self.ceiling.process, child.ceiling.process),
            ("device", self.ceiling.device, child.ceiling.device),
            (
                "credential",
                self.ceiling.credential,
                child.ceiling.credential,
            ),
        ] {
            if requested > parent {
                return Err(PolicyError::ExceedsParent { field });
            }
        }
        if child.ceiling.persistent_approval && !self.ceiling.persistent_approval {
            return Err(PolicyError::ExceedsParent {
                field: "persistent_approval",
            });
        }
        Ok(child)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(layer: PolicyLayer, level: PermissionLevel) -> ScopedPolicy {
        ScopedPolicy {
            layer,
            ceiling: PolicyCeiling {
                filesystem: level,
                network: level,
                process: level,
                device: level,
                credential: level,
                persistent_approval: false,
            },
        }
    }

    #[test]
    fn child_can_reduce_parent_permissions() {
        let parent = policy(PolicyLayer::Workstation, PermissionLevel::Use);
        let child = policy(PolicyLayer::Project, PermissionLevel::Observe);
        assert_eq!(parent.restrict(child.clone()).unwrap(), child);
    }

    #[test]
    fn child_cannot_expand_parent_permissions() {
        let parent = policy(PolicyLayer::Project, PermissionLevel::Observe);
        let child = policy(PolicyLayer::RoleOrAgent, PermissionLevel::Use);
        assert_eq!(
            parent.restrict(child).unwrap_err(),
            PolicyError::ExceedsParent {
                field: "filesystem"
            }
        );
    }

    #[test]
    fn policy_layers_must_follow_the_inheritance_order() {
        let parent = policy(PolicyLayer::Project, PermissionLevel::Use);
        let child = policy(PolicyLayer::Workstation, PermissionLevel::Deny);
        assert_eq!(
            parent.restrict(child).unwrap_err(),
            PolicyError::InvalidLayerOrder
        );
    }
}

//! Read-only access to explicitly selected native credential-store entries.
//!
//! This module deliberately constructs the concrete host credential type. It
//! must not use `keyring::Entry::new`, whose process-global default can fall
//! back to the in-memory mock store when platform features are misconfigured.

// Production invocation remains closed until provider-specific secret
// injection is implemented. Keep this boundary compiled and tested meanwhile.
#![cfg_attr(not(test), allow(dead_code))]

use std::fmt;

use bastet_core::CredentialBackend;
use bastet_protocol::ProviderCredentialSelection;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const MAX_SECRET_BYTES: usize = 64 * 1024;

// Versioned Workstation-owned mapping, not an import of arbitrary CLI login
// entries. A non-default Linux target also disables keyring's legacy fallback.
#[cfg(any(test, target_os = "linux"))]
const NATIVE_NAMESPACE: &str = "bastet-workstation.v1";

#[cfg(any(test, target_os = "windows"))]
fn windows_target(service: &str, account: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    // Length-prefix both UTF-8 fields: unlike account.service, delimiters in
    // either field cannot alias a different selection.
    for field in [service, account] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    format!("bastet-workstation.v1:{:x}", digest.finalize())
}

/// Secret material whose owned storage is erased when it is dropped.
pub(super) struct SecretBytes(Zeroizing<Vec<u8>>);

impl SecretBytes {
    pub(super) fn new(bytes: Vec<u8>) -> Result<Self, CredentialReadError> {
        let bytes = Zeroizing::new(bytes);
        if bytes.is_empty() {
            return Err(CredentialReadError::EmptySecret);
        }
        if bytes.len() > MAX_SECRET_BYTES {
            return Err(CredentialReadError::SecretTooLarge);
        }
        Ok(Self(bytes))
    }

    /// Limits ordinary callers to a temporary borrow rather than exposing the
    /// owned buffer or a broadly reusable accessor.
    pub(super) fn with_bytes<T>(&self, use_secret: impl FnOnce(&[u8]) -> T) -> T {
        use_secret(self.0.as_slice())
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretBytes([REDACTED])")
    }
}

/// Payload-free failures safe to include in daemon diagnostics.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum CredentialReadError {
    #[error("credential service locator is invalid")]
    InvalidService,
    #[error("credential account locator is invalid")]
    InvalidAccount,
    #[error("credential backend is unsupported on this host")]
    UnsupportedBackend,
    #[error("credential entry was not found")]
    NotFound,
    #[error("native credential store access was denied")]
    NoStorageAccess,
    #[error("native credential store operation failed")]
    PlatformFailure,
    #[error("credential locator exceeds a native platform limit")]
    LocatorTooLong,
    #[error("native credential store rejected the locator")]
    InvalidPlatformLocator,
    #[error("credential locator matched more than one native entry")]
    Ambiguous,
    #[error("native credential store returned invalidly encoded data")]
    BadEncoding,
    #[error("native credential store returned an empty secret")]
    EmptySecret,
    #[error("native credential store returned a secret larger than 64 KiB")]
    SecretTooLarge,
}

pub(super) trait CredentialReader: Send + Sync {
    /// Reject selections that cannot be read on this host without touching the
    /// native credential store. Dispatch calls this before consuming a grant.
    fn validate(&self, selection: &ProviderCredentialSelection) -> Result<(), CredentialReadError>;

    fn read(
        &self,
        selection: &ProviderCredentialSelection,
    ) -> Result<SecretBytes, CredentialReadError>;
}

trait PlatformSecretLookup: Send + Sync {
    fn read_secret(&self, service: &str, account: &str) -> Result<Vec<u8>, CredentialReadError>;
}

struct KeyringPlatformLookup;

pub(super) struct NativeCredentialReader {
    lookup: Box<dyn PlatformSecretLookup>,
}

impl NativeCredentialReader {
    pub(super) fn new() -> Self {
        Self {
            lookup: Box::new(KeyringPlatformLookup),
        }
    }

    #[cfg(test)]
    fn with_lookup(lookup: impl PlatformSecretLookup + 'static) -> Self {
        Self {
            lookup: Box::new(lookup),
        }
    }
}

impl CredentialReader for NativeCredentialReader {
    fn validate(&self, selection: &ProviderCredentialSelection) -> Result<(), CredentialReadError> {
        validate_locator(&selection.service, CredentialReadError::InvalidService)?;
        validate_locator(
            &selection.account_label,
            CredentialReadError::InvalidAccount,
        )?;
        validate_host_backend(&selection.backend)
    }

    fn read(
        &self,
        selection: &ProviderCredentialSelection,
    ) -> Result<SecretBytes, CredentialReadError> {
        // Retain this check at the native read boundary as well, so callers
        // cannot bypass broker preflight.
        self.validate(selection)?;

        let bytes = self
            .lookup
            .read_secret(&selection.service, &selection.account_label)?;
        SecretBytes::new(bytes)
    }
}

fn validate_locator(value: &str, error: CredentialReadError) -> Result<(), CredentialReadError> {
    if value.is_empty() || value.contains('\0') {
        Err(error)
    } else {
        Ok(())
    }
}

fn validate_host_backend(backend: &CredentialBackend) -> Result<(), CredentialReadError> {
    let supported = cfg!(target_os = "macos")
        && matches!(backend, CredentialBackend::MacosKeychain)
        || cfg!(target_os = "windows")
            && matches!(backend, CredentialBackend::WindowsCredentialManager)
        || cfg!(target_os = "linux") && matches!(backend, CredentialBackend::LinuxSecretService);

    if supported {
        Ok(())
    } else {
        Err(CredentialReadError::UnsupportedBackend)
    }
}

#[cfg(target_os = "macos")]
impl PlatformSecretLookup for KeyringPlatformLookup {
    fn read_secret(&self, service: &str, account: &str) -> Result<Vec<u8>, CredentialReadError> {
        let credential = keyring::macos::MacCredential::new_with_target(None, service, account)
            .map_err(sanitize_keyring_error)?;
        keyring::Entry::new_with_credential(Box::new(credential))
            .get_secret()
            .map_err(sanitize_keyring_error)
    }
}

#[cfg(target_os = "windows")]
impl PlatformSecretLookup for KeyringPlatformLookup {
    fn read_secret(&self, service: &str, account: &str) -> Result<Vec<u8>, CredentialReadError> {
        let target = windows_target(service, account);
        let credential =
            keyring::windows::WinCredential::new_with_target(Some(&target), service, account)
                .map_err(sanitize_keyring_error)?;
        keyring::Entry::new_with_credential(Box::new(credential))
            .get_secret()
            .map_err(sanitize_keyring_error)
    }
}

#[cfg(target_os = "linux")]
impl PlatformSecretLookup for KeyringPlatformLookup {
    fn read_secret(&self, service: &str, account: &str) -> Result<Vec<u8>, CredentialReadError> {
        let credential = keyring::secret_service::SsCredential::new_with_target(
            Some(NATIVE_NAMESPACE),
            service,
            account,
        )
        .map_err(sanitize_keyring_error)?;
        keyring::Entry::new_with_credential(Box::new(credential))
            .get_secret()
            .map_err(sanitize_keyring_error)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
impl PlatformSecretLookup for KeyringPlatformLookup {
    fn read_secret(&self, _service: &str, _account: &str) -> Result<Vec<u8>, CredentialReadError> {
        Err(CredentialReadError::UnsupportedBackend)
    }
}

fn sanitize_keyring_error(error: keyring::Error) -> CredentialReadError {
    match error {
        keyring::Error::PlatformFailure(_) => CredentialReadError::PlatformFailure,
        keyring::Error::NoStorageAccess(_) => CredentialReadError::NoStorageAccess,
        keyring::Error::NoEntry => CredentialReadError::NotFound,
        keyring::Error::BadEncoding(mut bytes) => {
            // `keyring::Error::BadEncoding` carries the raw credential bytes.
            // Erase them explicitly before discarding the provider error.
            bytes.zeroize();
            CredentialReadError::BadEncoding
        }
        keyring::Error::TooLong(_, _) => CredentialReadError::LocatorTooLong,
        keyring::Error::Invalid(_, _) => CredentialReadError::InvalidPlatformLocator,
        keyring::Error::Ambiguous(_) => CredentialReadError::Ambiguous,
        _ => CredentialReadError::PlatformFailure,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use bastet_core::CredentialReferenceId;

    use super::*;

    #[test]
    fn native_namespace_avoids_legacy_fallback_and_ambiguous_windows_names() {
        assert_ne!(NATIVE_NAMESPACE, "default");
        assert_ne!(windows_target("c", "a.b"), windows_target("b.c", "a"));
        assert_ne!(
            windows_target("service", "account"),
            windows_target("account", "service")
        );
        assert_ne!(
            windows_target("service", "account"),
            windows_target("service", "account ")
        );
        assert_eq!(
            windows_target("service", "account"),
            "bastet-workstation.v1:adff881977b50c5c513322c07a5e2b1cc173820ff9598550c968c649fffa9c04"
        );
        assert_eq!(
            windows_target("service", "account").len(),
            "bastet-workstation.v1:".len() + 64
        );
    }

    #[derive(Clone, Copy)]
    enum FakeResponse {
        Secret(&'static [u8]),
        Failure(CredentialReadError),
    }

    struct FakeLookup {
        calls: Arc<AtomicUsize>,
        response: FakeResponse,
    }

    impl PlatformSecretLookup for FakeLookup {
        fn read_secret(
            &self,
            _service: &str,
            _account: &str,
        ) -> Result<Vec<u8>, CredentialReadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.response {
                FakeResponse::Secret(secret) => Ok(secret.to_vec()),
                FakeResponse::Failure(error) => Err(error),
            }
        }
    }

    struct ExactLocatorLookup;

    impl PlatformSecretLookup for ExactLocatorLookup {
        fn read_secret(
            &self,
            service: &str,
            account: &str,
        ) -> Result<Vec<u8>, CredentialReadError> {
            assert_eq!(service, "  bastet/服務  ");
            assert_eq!(account, "  account@example.test  ");
            Ok(b"fake-secret".to_vec())
        }
    }

    fn selection(backend: CredentialBackend) -> ProviderCredentialSelection {
        ProviderCredentialSelection {
            reference_id: CredentialReferenceId::from_bytes([7; 16]),
            backend,
            service: "bastet.test".to_string(),
            account_label: "test-account".to_string(),
        }
    }

    #[cfg(target_os = "macos")]
    fn host_backend() -> CredentialBackend {
        CredentialBackend::MacosKeychain
    }

    #[cfg(target_os = "windows")]
    fn host_backend() -> CredentialBackend {
        CredentialBackend::WindowsCredentialManager
    }

    #[cfg(target_os = "linux")]
    fn host_backend() -> CredentialBackend {
        CredentialBackend::LinuxSecretService
    }

    #[test]
    fn secret_bytes_are_redacted_and_available_only_during_closure_borrow() {
        let secret = SecretBytes::new(b"not-for-diagnostics".to_vec()).unwrap();

        assert_eq!(format!("{secret:?}"), "SecretBytes([REDACTED])");
        assert_eq!(secret.with_bytes(|bytes| bytes.len()), 19);
        assert_eq!(secret.with_bytes(|bytes| bytes[0]), b'n');
    }

    #[test]
    fn secret_bytes_reject_empty_and_oversized_values() {
        assert_eq!(
            SecretBytes::new(Vec::new()).unwrap_err(),
            CredentialReadError::EmptySecret
        );
        assert_eq!(
            SecretBytes::new(vec![1; MAX_SECRET_BYTES + 1]).unwrap_err(),
            CredentialReadError::SecretTooLarge
        );
        assert!(SecretBytes::new(vec![1; MAX_SECRET_BYTES]).is_ok());
    }

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    #[test]
    fn invalid_locators_are_rejected_before_the_platform_boundary() {
        for (service, account, expected) in [
            ("", "account", CredentialReadError::InvalidService),
            (
                "bad\0service",
                "account",
                CredentialReadError::InvalidService,
            ),
            ("service", "", CredentialReadError::InvalidAccount),
            (
                "service",
                "bad\0account",
                CredentialReadError::InvalidAccount,
            ),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let reader = NativeCredentialReader::with_lookup(FakeLookup {
                calls: Arc::clone(&calls),
                response: FakeResponse::Secret(b"unused"),
            });
            let mut requested = selection(host_backend());
            requested.service = service.to_string();
            requested.account_label = account.to_string();

            assert_eq!(reader.read(&requested).unwrap_err(), expected);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    #[test]
    fn mismatched_backends_are_rejected_before_the_platform_boundary() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = NativeCredentialReader::with_lookup(FakeLookup {
            calls: Arc::clone(&calls),
            response: FakeResponse::Secret(b"unused"),
        });
        let mismatched = if cfg!(target_os = "macos") {
            CredentialBackend::WindowsCredentialManager
        } else {
            CredentialBackend::MacosKeychain
        };

        assert_eq!(
            reader.read(&selection(mismatched)).unwrap_err(),
            CredentialReadError::UnsupportedBackend
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    #[test]
    fn validation_of_a_supported_selection_does_not_touch_the_platform_boundary() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = NativeCredentialReader::with_lookup(FakeLookup {
            calls: Arc::clone(&calls),
            response: FakeResponse::Secret(b"unused"),
        });

        assert!(reader.validate(&selection(host_backend())).is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    #[test]
    fn valid_fake_lookup_is_bounded_and_propagates_only_safe_errors() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = NativeCredentialReader::with_lookup(FakeLookup {
            calls: Arc::clone(&calls),
            response: FakeResponse::Failure(CredentialReadError::NoStorageAccess),
        });

        assert_eq!(
            reader.read(&selection(host_backend())).unwrap_err(),
            CredentialReadError::NoStorageAccess
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[cfg(any(target_os = "macos", target_os = "windows", target_os = "linux"))]
    #[test]
    fn service_and_account_locators_cross_the_fake_boundary_unchanged() {
        let reader = NativeCredentialReader::with_lookup(ExactLocatorLookup);
        let mut requested = selection(host_backend());
        requested.service = "  bastet/服務  ".to_string();
        requested.account_label = "  account@example.test  ".to_string();

        let secret = reader.read(&requested).unwrap();
        assert_eq!(secret.with_bytes(|bytes| bytes.len()), 11);
    }

    #[derive(Debug)]
    struct SensitivePlatformError;

    impl fmt::Display for SensitivePlatformError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("RAW_PLATFORM_PAYLOAD")
        }
    }

    impl StdError for SensitivePlatformError {}

    #[test]
    fn keyring_errors_are_mapped_without_raw_payloads() {
        let cases = [
            (
                keyring::Error::BadEncoding(b"RAW_BAD_ENCODING_PAYLOAD".to_vec()),
                CredentialReadError::BadEncoding,
            ),
            (
                keyring::Error::PlatformFailure(Box::new(SensitivePlatformError)),
                CredentialReadError::PlatformFailure,
            ),
            (
                keyring::Error::NoStorageAccess(Box::new(SensitivePlatformError)),
                CredentialReadError::NoStorageAccess,
            ),
        ];

        for (source, expected) in cases {
            let mapped = sanitize_keyring_error(source);
            assert_eq!(mapped, expected);
            let rendered = format!("{mapped:?} {mapped}");
            assert!(!rendered.contains("RAW_"));
            assert!(!rendered.contains("PAYLOAD"));
        }
    }

    #[test]
    fn native_constructor_does_not_touch_the_platform_store() {
        let _reader = NativeCredentialReader::new();
    }
}

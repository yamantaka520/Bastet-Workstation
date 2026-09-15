//! Creation-time Job/handle attributes. This is not filesystem/network isolation.
use std::{
    io,
    os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle},
    ptr::{null, null_mut, NonNull},
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::{GetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT},
    System::{
        JobObjects::*,
        Memory::{GetProcessHeap, HeapAlloc, HeapFree, HEAP_ZERO_MEMORY},
        Threading::*,
    },
};

pub struct WindowsJob {
    handle: OwnedHandle,
    cleanup_failed: bool,
}

impl WindowsJob {
    pub fn new() -> io::Result<Self> {
        // SAFETY: unnamed object, default security, non-inheritable host handle.
        let raw = unsafe { CreateJobObjectW(null(), null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transferred one owned handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // No BREAKAWAY_OK or SILENT_BREAKAWAY_OK is enabled.
        // SAFETY: exact initialized structure and valid owned Job handle.
        if unsafe {
            SetInformationJobObject(
                raw,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            handle,
            cleanup_failed: false,
        })
    }

    pub fn active_processes(&self) -> io::Result<u32> {
        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: exact writable accounting buffer, borrowed live Job handle.
        if unsafe {
            QueryInformationJobObject(
                self.handle.as_raw_handle(),
                JobObjectBasicAccountingInformation,
                (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                std::mem::size_of_val(&accounting) as u32,
                null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(accounting.ActiveProcesses)
    }

    /// Explicit termination plus observed empty Job; failure remains sticky.
    /// The owner must independently reap its process handle.
    pub fn terminate_and_wait(&mut self, timeout: Duration) -> io::Result<()> {
        if self.cleanup_failed {
            return Err(io::Error::other("Job cleanup previously failed"));
        }
        let result = self.terminate_inner(timeout);
        self.cleanup_failed = result.is_err();
        result
    }

    fn terminate_inner(&self, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid cleanup deadline")
        })?;
        // SAFETY: terminates only processes associated with this owned Job.
        if unsafe { TerminateJobObject(self.handle.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        loop {
            if self.active_processes()? == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Job cleanup not confirmed",
                ));
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// Handle arrays and their borrowed owners outlive the attribute list.
    /// Callers pass EXTENDED_STARTUPINFO_PRESENT to CreateProcessW and enable
    /// inheritance only when this list contains explicit child handles.
    pub fn attributes<'a>(
        &'a self,
        inherited: &[BorrowedHandle<'a>],
    ) -> io::Result<JobAttributes<'a>> {
        if inherited.len() > 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only explicit stdio handles are supported",
            ));
        }
        let mut handles = Vec::with_capacity(inherited.len());
        for handle in inherited {
            let raw = handle.as_raw_handle();
            let mut flags = 0;
            // SAFETY: BorrowedHandle guarantees validity through 'a.
            if unsafe { GetHandleInformation(raw, &mut flags) } == 0 {
                return Err(io::Error::last_os_error());
            }
            if flags & HANDLE_FLAG_INHERIT == 0
                || raw == self.handle.as_raw_handle()
                || handles.contains(&raw)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid child handle list",
                ));
            }
            handles.push(raw);
        }
        let count = if handles.is_empty() { 1 } else { 2 };
        let mut size = 0;
        // SAFETY: documented size-query call with a null attribute list.
        unsafe {
            InitializeProcThreadAttributeList(null_mut(), count, 0, &mut size);
        }
        if size == 0 || size > 64 * 1024 {
            return Err(io::Error::other("invalid attribute allocation size"));
        }
        // SAFETY: process heap supplies native alignment; RAII below frees it.
        let allocation =
            NonNull::new(unsafe { HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, size) })
                .ok_or_else(io::Error::last_os_error)?;
        let mut memory = AttributeMemory {
            allocation,
            initialized: false,
        };
        // SAFETY: allocation is sized/aligned according to the size query.
        if unsafe {
            InitializeProcThreadAttributeList(memory.allocation.as_ptr(), count, 0, &mut size)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        memory.initialized = true;
        let result = JobAttributes {
            memory,
            _job: self,
            _borrowed: inherited.to_vec(),
            jobs: Box::new([self.handle.as_raw_handle()]),
            handles,
        };
        result.set(
            PROC_THREAD_ATTRIBUTE_JOB_LIST,
            result.jobs.as_ptr().cast(),
            std::mem::size_of::<HANDLE>(),
        )?;
        if !result.handles.is_empty() {
            result.set(
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                result.handles.as_ptr().cast(),
                result.handles.len() * std::mem::size_of::<HANDLE>(),
            )?;
        }
        Ok(result)
    }
}

struct AttributeMemory {
    allocation: NonNull<core::ffi::c_void>,
    initialized: bool,
}
impl Drop for AttributeMemory {
    fn drop(&mut self) {
        // SAFETY: this unique allocation is deleted only after successful init
        // and is freed exactly once, including partially constructed lists.
        unsafe {
            if self.initialized {
                DeleteProcThreadAttributeList(self.allocation.as_ptr());
            }
            HeapFree(GetProcessHeap(), 0, self.allocation.as_ptr());
        }
    }
}

pub struct JobAttributes<'a> {
    // Deleted before the arrays it references (Rust drops fields in order).
    memory: AttributeMemory,
    _job: &'a WindowsJob,
    _borrowed: Vec<BorrowedHandle<'a>>,
    jobs: Box<[HANDLE; 1]>,
    handles: Vec<HANDLE>,
}

impl JobAttributes<'_> {
    fn set(&self, attribute: u32, value: *const core::ffi::c_void, size: usize) -> io::Result<()> {
        // SAFETY: backing arrays belong to self and remain stable until delete.
        if unsafe {
            UpdateProcThreadAttribute(
                self.memory.allocation.as_ptr(),
                0,
                attribute as usize,
                value,
                size,
                null_mut(),
                null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Opaque borrowed list for CreateProcessW; never delete or mutate it.
    pub fn as_ptr(&self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.memory.allocation.as_ptr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::windows::io::AsHandle;
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;

    #[test]
    fn handle_attributes_require_unique_inheritable_owned_handles() {
        let job = WindowsJob::new().unwrap();
        // SAFETY: unnamed manual-reset events, wholly owned test resources.
        let raw = unsafe { CreateEventW(null(), 1, 0, null()) };
        assert!(!raw.is_null());
        let private = unsafe { OwnedHandle::from_raw_handle(raw) };
        assert!(job.attributes(&[private.as_handle()]).is_err());
        let security = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        let raw = unsafe { CreateEventW(&security, 1, 0, null()) };
        assert!(!raw.is_null());
        let child_handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        assert!(job
            .attributes(&[child_handle.as_handle(), child_handle.as_handle()])
            .is_err());
        let attributes = job.attributes(&[child_handle.as_handle()]).unwrap();
        assert!(!attributes.as_ptr().is_null());
        drop(attributes);
        // Deleting the attribute list never closes a borrowed child handle.
        assert_ne!(unsafe { SetEvent(child_handle.as_raw_handle()) }, 0);
    }

    #[test]
    fn invalid_cleanup_deadline_is_sticky() {
        let mut job = WindowsJob::new().unwrap();
        assert!(job.terminate_and_wait(Duration::MAX).is_err());
        assert!(job.terminate_and_wait(Duration::ZERO).is_err());
    }
}

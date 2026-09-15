# Windows adapter process backend contract

This is an implementation dependency of the existing M2 process/sandbox gate,
not a new milestone or a declaration of completion. The production
`OwnedAdapterChild` still provides direct-child cleanup on Windows.

## Verified constraints

- Rust 1.88 exposes `CommandExt::creation_flags`, but its process attribute-list
  and primary-thread-handle APIs are unstable. Do not change the pinned toolchain
  to nightly or reach into the private representation of `Child`.
- `PROC_THREAD_ATTRIBUTE_JOB_LIST` lets `CreateProcessW` associate the child
  with jobs at creation. Creating a running process and assigning a Job afterward
  leaves an execution race. Creating suspended then assigning avoids execution
  before assignment but leaves a parent-crash window with an unowned suspended
  child; it is not the target implementation.
- Rust's `ChildStdin`/`ChildStdout` conversions from `OwnedHandle` require
  asynchronous handles. Plain synchronous `CreatePipe` handles must not be
  converted into these types: the existing readers/writers use asynchronous I/O.

Sources: [Rust 1.88 Windows process APIs](https://github.com/rust-lang/rust/blob/1.88.0/library/std/src/os/windows/process.rs),
[Windows process attributes](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute),
[job assignment contract](https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject).

## Required construction and ownership

1. Consume an explicit executable/argv/cwd/environment/stdio specification, not
   an attempt to reconstruct hidden `Command` options. Do not permit raw shell
   commands or `.cmd`/`.bat` fallback. Runtime selection remains separately
   authorized and bound to the immutable launch plan.
2. Use a non-null explicit application path, bounded mutable UTF-16 command line,
   and explicit Unicode environment block. Never let a null environment pointer
   re-enable ambient inheritance. Preserve Windows code units without lossy UTF-8.
3. Create an unnamed, non-inheritable kill-on-close Job with no breakaway flags.
   Keep its handle exclusively in the host. Create overlapped host pipe ends and
   only the necessary inheritable child pipe ends; stderr is explicitly discarded.
4. Build `STARTUPINFOEXW` with both JOB_LIST and HANDLE_LIST attributes. The
   attribute list, backing handle arrays, Job, environment, and mutable command
   line remain alive through `CreateProcessW`. No post-spawn unsandboxed fallback.
5. Transfer successful process/pipe handles into explicit owners; close child-side
   copies and the initial thread handle. Every partial failure closes only owned
   resources. Job assignment failure must start no executable code.
6. Cleanup terminates the owned Job, reaps the leader, and observes zero active
   Job processes before claiming tree cleanup. Errors remain sticky. Drop retains
   kill-on-close protection even if checked cleanup fails or the host crashes.

Job containment does not itself restrict filesystem, network, credentials, or
device access. AppContainer/token and destination-enforcement work remains
necessary; Job completion alone must not close M2.

## Current encoding step and native gates

The Windows Job owner and creation attribute-list primitives are now implemented
in `windows_job`. The standalone native fixture uses them with `CreateProcessW`
to test descendant accounting/termination and kill-on-close. This is component
integration only: `OwnedAdapterChild` has not switched backends, and native CI
must verify Windows-only code before any containment gate can be credited.

`windows_launch_encoding` supplies bounded CRT-style argv quoting and a sorted,
explicit environment block. It rejects embedded NUL, ambiguous executable quotes,
case-insensitive duplicate environment names, and hidden drive-directory variables.
ASCII environment keys match the existing sanitized environment contract; UTF-16
values preserve non-BMP and unpaired code units. The 32,767-unit environment cap is
an application budget, not a claim about all Windows API limits. These helpers
are not yet production process creation or authority.

Tests must include native argv round-trip (not just comparing encoded strings),
positive large stdin delivery, blocked stdin/handshake cancellation, startup Job
membership, descendant stdout closure after leader exit, denied breakaway,
unrelated-child survival, parent crash cleanup, and injected partial setup/cleanup
failures. Existing native adapter pipe controls supply only the I/O subset.

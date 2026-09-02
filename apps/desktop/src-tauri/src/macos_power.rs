use std::ptr::NonNull;

use block2::RcBlock;
use objc2_app_kit::{
    NSWorkspace, NSWorkspaceDidWakeNotification, NSWorkspaceWillSleepNotification,
};
use objc2_foundation::NSNotification;
use tauri::AppHandle;

pub fn install(app: AppHandle) {
    let center = NSWorkspace::sharedWorkspace().notificationCenter();

    let sleep_app = app.clone();
    let sleep_handler: RcBlock<dyn Fn(NonNull<NSNotification>)> =
        RcBlock::new(move |_| super::request_suspend(sleep_app.clone()));
    // SAFETY: NSWorkspace owns both notification names. The block captures an
    // AppHandle, which is Send + Sync and remains valid for the app lifetime.
    unsafe {
        center.addObserverForName_object_queue_usingBlock(
            Some(NSWorkspaceWillSleepNotification),
            None,
            None,
            &sleep_handler,
        );
    }

    let wake_handler: RcBlock<dyn Fn(NonNull<NSNotification>)> =
        RcBlock::new(move |_| super::request_resume(app.clone()));
    // SAFETY: Same lifetime and sendability guarantees as the sleep observer.
    unsafe {
        center.addObserverForName_object_queue_usingBlock(
            Some(NSWorkspaceDidWakeNotification),
            None,
            None,
            &wake_handler,
        );
    }
}

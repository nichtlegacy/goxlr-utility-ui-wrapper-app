use objc2::rc::Retained;
use objc2::AllocAnyThread;
use objc2_app_kit::{
    NSAlert, NSApplication, NSApplicationActivationPolicy, NSCriticalAlertStyle, NSImage,
    NSInformationalAlertStyle, NSWindowLevel,
};
use objc2_foundation::{NSData, NSString};

pub use dispatch2::Queue;
pub use objc2::MainThreadMarker;

const ICON: &[u8] = include_bytes!("../icons/icon.icns");

// pub trait NSAlert: Sized {
//     unsafe fn alloc(_: Self) -> id {
//         msg_send![class!(NSAlert), alloc]
//     }
// }

pub fn show_dock(mtm: MainThreadMarker) {
    // This is a little more involved, when we switch back to the regular policy, the icon will turn into a console.
    let ns_app = NSApplication::sharedApplication(mtm);
    ns_app.setActivationPolicy(NSApplicationActivationPolicy::Regular);
    unsafe {
        ns_app.setApplicationIconImage(get_icon().as_deref());
    }
}

fn get_icon() -> Option<Retained<NSImage>> {
    let data = NSData::with_bytes(ICON);
    NSImage::initWithData(NSImage::alloc(), &data)
}

// Brings the app to the front, a daemon spawned process won't be given focus by default.
#[allow(deprecated)]
pub fn activate(mtm: MainThreadMarker) {
    let ns_app = NSApplication::sharedApplication(mtm);
    ns_app.activateIgnoringOtherApps(true);
}

// Runs the closure on the main thread, immediately if we're already on it.
pub fn on_main<F: FnOnce(MainThreadMarker) + Send + 'static>(work: F) {
    match MainThreadMarker::new() {
        Some(mtm) => work(mtm),
        None => Queue::main().exec_async(move || {
            if let Some(mtm) = MainThreadMarker::new() {
                work(mtm);
            }
        }),
    }
}

// As above, but blocks the caller until the closure has completed.
pub fn on_main_sync<T: Send, F: FnOnce(MainThreadMarker) -> T + Send>(work: F) -> Option<T> {
    match MainThreadMarker::new() {
        Some(mtm) => Some(work(mtm)),
        None => {
            let mut result = None;
            Queue::main().exec_sync(|| {
                if let Some(mtm) = MainThreadMarker::new() {
                    result = Some(work(mtm));
                }
            });
            result
        }
    }
}

pub fn hide_dock(mtm: MainThreadMarker) {
    let ns_app = NSApplication::sharedApplication(mtm);
    ns_app.setActivationPolicy(NSApplicationActivationPolicy::Accessory);
}

pub fn show_messagebox(mtm: MainThreadMarker, title: String, content: String) {
    unsafe {
        let alert = NSAlert::new(mtm);
        alert.setIcon(get_icon().as_deref());
        alert.setMessageText(&NSString::from_str(&title));
        alert.setInformativeText(&NSString::from_str(&content));
        alert.setAlertStyle(NSCriticalAlertStyle);

        // Get the Window..
        let window = alert.window();
        window.setLevel(NSWindowLevel::from(10u8));

        // Send the Alert..
        alert.runModal();
    }
}

pub fn show_question(mtm: MainThreadMarker, title: String, content: String) -> Result<(), ()> {
    let result: usize = unsafe {
        let alert = NSAlert::new(mtm);
        alert.setIcon(get_icon().as_deref());
        alert.addButtonWithTitle(&NSString::from_str("No"));
        alert.addButtonWithTitle(&NSString::from_str("Yes"));
        alert.setMessageText(&NSString::from_str(&title));
        alert.setInformativeText(&NSString::from_str(&content));
        alert.setAlertStyle(NSInformationalAlertStyle);

        // Get the Window..
        let window = alert.window();
        window.setLevel(NSWindowLevel::from(10u8));

        // Send the Alert..
        alert.runModal() as usize
    };
    if result == 1001 {
        return Ok(());
    }
    Err(())
}

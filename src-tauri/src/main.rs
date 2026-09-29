// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[cfg(target_os = "macos")]
mod macos;

mod ipc;

use directories::ProjectDirs;
use interprocess::local_socket::tokio::prelude::LocalSocketStream;
use interprocess::local_socket::{GenericFilePath, GenericNamespaced, ToFsName, ToNsName};
use serde_json::{json, Value};
use std::env;
use std::fs::{create_dir_all, File};
use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use crate::ipc::Socket;
use interprocess::local_socket::traits::tokio::Stream;
use std::path::{Path, PathBuf};
use tauri::webview::PageLoadEvent;
use tauri::{AppHandle, Emitter, Listener, Manager, PhysicalPosition, Url, WebviewWindow};
use tauri_plugin_window_state::StateFlags;
use tungstenite::{client, Message, WebSocket};

static WINDOW_NAME: &str = "main";
static READY_EVENT_NAME: &str = "READY";
static SHOW_EVENT_NAME: &str = "si-event";
static HIDE_EVENT_NAME: &str = "HIDE-UI";
static STOP_EVENT_NAME: &str = "seppuku";

static SOCKET_PATH: &str = "/tmp/goxlr.socket";
static NAMED_PIPE: &str = "@goxlr.socket";

// How long we'll wait on a single IPC / Socket operation before giving up..
const IPC_TIMEOUT: Duration = Duration::from_secs(3);
const SOCKET_TIMEOUT: Duration = Duration::from_secs(3);

// How long we'll keep retrying the websocket on startup, and after the Utility goes away..
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(15);

// How long we give the Utility's page to load, and how many times we'll try..
const LOAD_TIMEOUT: Duration = Duration::from_secs(10);
const LOAD_ATTEMPTS: u32 = 3;

// Tracks whether the Utility's page has actually loaded in the webview..
#[derive(Default)]
struct UiState {
    target: Mutex<Option<Url>>,
    loaded: AtomicBool,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    // Under Wayland on Linux gtkwebkit has a bug which prevents it from launching correctly, the
    // following sets an ENV flag prior to startup that disables the DMABUF renderer to fix it.
    #[cfg(target_os = "linux")]
    env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");

    // If running the utility has an error, make sure log level is debug, and propagate the
    // error up to the user on Windows.
    if let Err(e) = run_application().await {
        show_error("GoXLR Utility UI".into(), e.to_string());
        return Err(e.to_string());
    }
    Ok(())
}

async fn run_application() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 {
        if args[1] == "--install" {
            manage(true).await?;
            return Ok(());
        }
        if args[1] == "--remove" {
            manage(false).await?;
            return Ok(());
        }
    }

    let builder = tauri::Builder::default();
    builder
        .manage(UiState::default())
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            // Register the Single Instance plugin on Windows, Linux and MacOs
            // Trigger a global event if something (eg, the util) attempts to open this again.
            let _ = app.emit(SHOW_EVENT_NAME, None::<String>);
        }))
        .plugin(
            // Don't persist visibility, we hide rather than close, so it'd always be saved as hidden.
            tauri_plugin_window_state::Builder::default()
                .with_state_flags(StateFlags::all() & !StateFlags::VISIBLE)
                .build(),
        )
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let global_window = app.handle().clone();
            app.listen_any(SHOW_EVENT_NAME, move |_| {
                focus_window(&global_window);
            });

            let hide_handle = app.handle().clone();
            app.listen_any(HIDE_EVENT_NAME, move |_| {
                if let Some(window) = hide_handle.get_webview_window(WINDOW_NAME) {
                    if let Err(e) = window.hide() {
                        println!("Unable to hide window: {}", e);
                    }
                }
                #[cfg(target_os = "macos")]
                macos::on_main(macos::hide_dock);
            });

            let ready_handle = app.handle().clone();
            app.listen_any(READY_EVENT_NAME, move |data| {
                let address = serde_json::from_str::<String>(data.payload()).ok();
                let url = address.and_then(|address| Url::parse(&address).ok());
                let window = ready_handle.get_webview_window(WINDOW_NAME);
                if let (Some(mut window), Some(url)) = (window, url) {
                    // Navigate natively, this doesn't depend on the loading page's state..
                    if let Err(e) = window.navigate(url) {
                        println!("Unable to load UI: {}", e);
                    }
                }
            });

            let shutdown_handle = app.handle().clone();
            app.listen_any(STOP_EVENT_NAME, move |_| {
                // Terminate the App..
                shutdown_handle.exit(0);
            });

            // Make sure a restored position is actually visible, then bring us to the front, we're
            // normally spawned by a background daemon, so the OS won't do this for us.
            if let Some(window) = app.get_webview_window(WINDOW_NAME) {
                ensure_on_screen(&window);
            }
            focus_window(app.handle());

            // Run the preflight here, so the single instance check has already happened and the
            // loading window is visible while we talk to the Utility.
            let handle = app.handle().clone();
            tokio::task::spawn(async move {
                match goxlr_preflight().await {
                    Ok(url) => {
                        // The monitor does blocking socket work, so give it its own thread.
                        thread::spawn(move || goxlr_utility_monitor(handle, url));
                    }
                    Err(e) => {
                        thread::spawn(move || fatal_error(&handle, "GoXLR Utility UI", &e));
                    }
                }
            });

            Ok(())
        })
        .on_page_load(|webview, payload| {
            if let PageLoadEvent::Finished = payload.event() {
                let state = webview.state::<UiState>();
                if let Ok(target) = state.target.lock() {
                    if let Some(target) = target.as_ref() {
                        if target.origin() == payload.url().origin() {
                            state.loaded.store(true, Ordering::SeqCst);
                        }
                    }
                };
            }
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                if let Err(e) = window.hide() {
                    println!("Unable to hide window: {}", e);
                }
                api.prevent_close();
                #[cfg(target_os = "macos")]
                macos::on_main(macos::hide_dock);
            }
        })
        .run(tauri::generate_context!())
        .expect("error running tauri app");

    Ok(())
}

fn focus_window(handle: &AppHandle) {
    // Do anything and everything to make sure this Window is visible and focused! On MacOS the
    // activation policy needs to be changed before we attempt to take focus.
    #[cfg(target_os = "macos")]
    {
        let handle = handle.clone();
        macos::on_main(move |mtm| {
            macos::show_dock(mtm);
            macos::activate(mtm);
            show_window(&handle);
        });
    }

    #[cfg(not(target_os = "macos"))]
    show_window(handle);
}

fn show_window(handle: &AppHandle) {
    if let Some(window) = handle.get_webview_window(WINDOW_NAME) {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn ensure_on_screen(window: &WebviewWindow) {
    let (Ok(position), Ok(size), Ok(monitors)) = (
        window.outer_position(),
        window.outer_size(),
        window.available_monitors(),
    ) else {
        return;
    };

    let (width, height) = (size.width as i32, size.height as i32);
    let visible = monitors.iter().any(|monitor| {
        let (m_pos, m_size) = (monitor.position(), monitor.size());
        position.x < m_pos.x + m_size.width as i32
            && position.x + width > m_pos.x
            && position.y < m_pos.y + m_size.height as i32
            && position.y + height > m_pos.y
    });

    if !visible {
        // Window is off every monitor, center it on the primary one..
        if let Ok(Some(monitor)) = window.primary_monitor() {
            let (m_pos, m_size) = (monitor.position(), monitor.size());
            let x = m_pos.x + (m_size.width as i32 - width) / 2;
            let y = m_pos.y + (m_size.height as i32 - height) / 2;
            let _ = window.set_position(PhysicalPosition::new(x, y.max(m_pos.y)));
        }
    }
}

async fn with_timeout<T>(future: impl Future<Output = T>) -> Result<T, String> {
    tokio::time::timeout(IPC_TIMEOUT, future)
        .await
        .map_err(|_| "The GoXLR Utility is not responding, please try again.".into())
}

async fn goxlr_preflight() -> Result<String, String> {
    // Because Windows also supports unix sockets, we need to maintain legacy behaviour..
    let path = if cfg!(windows) {
        NAMED_PIPE.to_ns_name::<GenericNamespaced>()
    } else {
        SOCKET_PATH.to_fs_name::<GenericFilePath>()
    }
    .map_err(|e| format!("Unable to get path to socket: {}", e))?;

    let connection = match with_timeout(LocalSocketStream::connect(path)).await? {
        Ok(connection) => connection,
        Err(_) => {
            let message = "The GoXLR Utility must be running before launching this app.";
            return Err(message.into());
        }
    };
    let mut socket: Socket<Value, Value> = Socket::new(connection);
    if with_timeout(socket.send(json!("GetStatus"))).await?.is_ok() {
        if let Ok(Some(result)) = with_timeout(socket.try_read()).await? {
            // Firstly, preform the general config check, and see if we need to run a command..
            // This may prompt the user, so it's not covered by the timeout.
            if let Some(command) = tokio::task::block_in_place(|| check_app_runtime(&result))? {
                // We need to send a command to the GoXLR..
                if with_timeout(socket.send(command)).await?.is_ok() {
                    // We don't actually care about the result, just clear the response..
                    let _ = with_timeout(socket.try_read()).await;
                }
            }

            // Now, grab the HTTP Address / Port for the utility..
            return get_goxlr_host(&result);
        }
    }
    Err("Unable to locate the Utility's URL".into())
}

fn check_app_runtime(status: &Value) -> Result<Option<Value>, String> {
    if let Some(status) = status.get("Status") {
        if let Some(config) = status.get("config") {
            if let Some(activation) = config.get("activation") {
                if let Some(path) = activation.get("active_path") {
                    let exe = get_current_path();

                    let path = {
                        let mut found = None;
                        if let Some(path) = path.as_str() {
                            #[cfg(not(unix))]
                            {
                                let mut command = windows_args::Args::parse_cmd(path);
                                if let Some(command) = command.next() {
                                    found.replace(command);
                                }
                            }
                            #[cfg(unix)]
                            {
                                let command = shell_words::split(path);
                                if let Ok(params) = command {
                                    if let Some(first) = params.first() {
                                        found.replace(first.clone());
                                    }
                                }
                            }
                        }
                        found
                    };
                    println!("{:#?}", path);

                    return if path.is_none() || PathBuf::from(path.unwrap()) != exe {
                        let title = String::from("GoXLR Utility UI");
                        let message = String::from("Use this app to control your GoXLR?");
                        if show_option(title, message).is_ok() {
                            let command = get_activator_command(Some(exe));
                            let json = serde_json::from_str::<Value>(&command).unwrap();
                            Ok(Some(json))
                        } else {
                            Err("Unable to obtain User Consent".into())
                        }
                    } else {
                        // We're already setup, nothing more to do here..
                        Ok(None)
                    };
                }
            } else {
                // The 'Activation' Settings aren't present, pre-1.0.6 utility..
                return Ok(None);
            }
        }
    }
    Err("Unable to locate Activation Path".into())
}

fn get_goxlr_host(status: &Value) -> Result<String, String> {
    if let Some(status) = status.get("Status") {
        if let Some(config) = status.get("config") {
            if let Some(http_settings) = config.get("http_settings") {
                if let Some(address) = http_settings.get("bind_address") {
                    if let Some(address) = address.as_str() {
                        if let Some(port) = http_settings.get("port") {
                            if let Some(port) = port.as_u64() {
                                let address = if address != "localhost" && address == "0.0.0.0" {
                                    "localhost"
                                } else {
                                    address
                                };
                                Ok(format!("{}:{}", address, port))
                            } else {
                                Err("Unable to Parse Port".into())
                            }
                        } else {
                            Err("Port Missing from http_status".into())
                        }
                    } else {
                        Err("Unable to parse bind_address as String".into())
                    }
                } else {
                    Err("bind_address Missing from http_status".into())
                }
            } else {
                Err("http_settings missing from Config response".into())
            }
        } else {
            Err("config missing from Status response".into())
        }
    } else {
        Err("Status missing from GetStatus response!".into())
    }
}

async fn supports_activation(socket: &mut Socket<Value, Value>) -> bool {
    if socket.send(json!("GetStatus")).await.is_ok() {
        if let Ok(Some(result)) = socket.try_read().await {
            if let Some(status) = result.get("Status") {
                if let Some(config) = status.get("config") {
                    if config.get("activation").is_some() {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn goxlr_utility_monitor(handle: AppHandle, host: String) {
    println!("Spawning the Monitor.. {}", host);
    let http_address = format!("http://{}/", host);

    // Attempt to connect to the websocket, the Utility may still be starting up so retry for a bit..
    let Some(mut socket) = connect_with_retry(&host, CONNECT_TIMEOUT) else {
        fatal_error(
            &handle,
            "Unable to Launch UI",
            "Unable to connect to the GoXLR Utility",
        );
        return;
    };

    loop {
        // Got a good connection, load (or reload) the UI. A window that never loads is worse
        // than an error, the user can simply open it again.
        if !load_ui(&handle, &http_address) {
            fatal_error(
                &handle,
                "Unable to Launch UI",
                "The GoXLR Utility did not serve the UI",
            );
            return;
        }

        // Anything that's not a valid message, or is a 'Close' message breaks the loop.
        while let Ok(message) = socket.read() {
            if let Message::Close(..) = message {
                break;
            }
        }

        // Socket closed, the Utility may just be restarting, so give it a chance to come back.
        println!("Lost connection to the GoXLR Utility, attempting to reconnect..");
        match connect_with_retry(&host, RECONNECT_TIMEOUT) {
            Some(new_socket) => socket = new_socket,
            None => break,
        }
    }
    // Utility has gone away, shutdown the app.
    let _ = handle.emit(STOP_EVENT_NAME, None::<String>);
}

fn fatal_error(handle: &AppHandle, title: &str, message: &str) {
    // Hide the UI itself before showing the error, and only quit once it's been dismissed..
    let _ = handle.emit(HIDE_EVENT_NAME, None::<String>);
    show_error(title.to_string(), message.to_string());
    let _ = handle.emit(STOP_EVENT_NAME, None::<String>);
}

fn load_ui(handle: &AppHandle, address: &str) -> bool {
    let state = handle.state::<UiState>();
    if let Ok(mut target) = state.target.lock() {
        *target = Url::parse(address).ok();
    }

    for attempt in 1..=LOAD_ATTEMPTS {
        state.loaded.store(false, Ordering::SeqCst);
        let _ = handle.emit(READY_EVENT_NAME, address);

        let deadline = Instant::now() + LOAD_TIMEOUT;
        while Instant::now() < deadline {
            if state.loaded.load(Ordering::SeqCst) {
                return true;
            }
            thread::sleep(Duration::from_millis(100));
        }
        println!("UI didn't load (attempt {}/{})", attempt, LOAD_ATTEMPTS);
    }
    false
}

fn connect_with_retry(host: &str, budget: Duration) -> Option<WebSocket<TcpStream>> {
    let deadline = Instant::now() + budget;
    let mut delay = Duration::from_millis(250);
    loop {
        // Only consider ourselves connected once the HTTP side is serving too..
        match connect_websocket(host).and_then(|socket| probe_http(host).map(|_| socket)) {
            Ok(socket) => return Some(socket),
            Err(e) => println!("Unable to connect to the GoXLR Utility: {}", e),
        }
        if Instant::now() + delay >= deadline {
            return None;
        }
        thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_secs(2));
    }
}

fn connect_tcp(host: &str) -> Result<TcpStream, String> {
    let mut error = format!("Unable to resolve {}", host);
    for address in host.to_socket_addrs().map_err(|e| e.to_string())? {
        match TcpStream::connect_timeout(&address, SOCKET_TIMEOUT) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(SOCKET_TIMEOUT))
                    .map_err(|e| e.to_string())?;
                stream
                    .set_write_timeout(Some(SOCKET_TIMEOUT))
                    .map_err(|e| e.to_string())?;
                return Ok(stream);
            }
            Err(e) => error = e.to_string(),
        }
    }
    Err(error)
}

fn connect_websocket(host: &str) -> Result<WebSocket<TcpStream>, String> {
    let stream = connect_tcp(host)?;
    let (socket, _) =
        client(format!("ws://{}/api/websocket", host), stream).map_err(|e| e.to_string())?;

    // Handshake done, from here we block until the Utility sends something or goes away.
    socket
        .get_ref()
        .set_read_timeout(None)
        .map_err(|e| e.to_string())?;
    Ok(socket)
}

fn probe_http(host: &str) -> Result<(), String> {
    let mut stream = connect_tcp(host)?;
    let request = format!(
        "GET / HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        host
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;

    // We only care about the status line, eg. 'HTTP/1.1 200'
    let mut status = [0u8; 12];
    stream.read_exact(&mut status).map_err(|e| e.to_string())?;
    match &status {
        [b'H', b'T', b'T', b'P', b'/', .., b' ', b'2' | b'3', _, _] => Ok(()),
        _ => Err(format!(
            "Unexpected HTTP Response: {}",
            String::from_utf8_lossy(&status)
        )),
    }
}

// Installs this app into the util..
async fn manage(install: bool) -> Result<(), String> {
    println!("Checking if Utility is Running..");
    let path = if cfg!(windows) {
        NAMED_PIPE.to_ns_name::<GenericNamespaced>()
    } else {
        SOCKET_PATH.to_fs_name::<GenericFilePath>()
    }
    .map_err(|e| format!("Unable to get path to socket: {}", e))?;

    let connection = LocalSocketStream::connect(path).await;

    if connection.is_err() {
        println!("Utility Not Running, changing config directly..");
        println!("Locating Settings File..");
        let path = get_settings_file();
        let json = if !&path.exists() {
            if !install {
                // If we're removing, and the path is missing, do nothing.
                return Ok(());
            }
            create_settings_path(&path);
            json!({ "activate": Value::Null })
        } else {
            load_settings(&path)
        };
        write_settings(&path, json, install);
    } else {
        println!("Utility Running, attempting via IPC");
        let method = if install { "Install" } else { "Remove" };

        let mut socket: Socket<Value, Value> = Socket::new(connection.unwrap());
        if supports_activation(&mut socket).await {
            let path = if install {
                Some(get_current_path())
            } else {
                None
            };

            let command = get_activator_command(path);

            // Attempt to Register ourselves as the UI App..
            println!("Executing: {}", command);

            let json = serde_json::from_str::<Value>(&command).unwrap();
            let _ = socket.send(json).await;
        } else {
            return Err(format!(
                "Unable to {}, Please stop the GoXLR Utility first.",
                method
            ));
        }
    }
    Ok(())
}

fn get_activator_command(exe: Option<PathBuf>) -> String {
    let exe = if let Some(exe) = exe {
        Value::String(get_platform_path(exe))
    } else {
        Value::Null
    };

    // Attempt to Register ourselves as the UI App..
    format!("{{ \"Daemon\": {{ \"SetActivatorPath\": {}  }} }}", exe)
}

fn get_platform_path(exe: PathBuf) -> String {
    let quoter = if cfg!(windows) { "\"" } else { "'" };
    format!("{}{}{}", quoter, exe.to_string_lossy(), quoter)
}

fn get_current_path() -> PathBuf {
    if let Ok(app_image) = env::var("APPIMAGE") {
        println!("Using AppImage at {}", &app_image);
        PathBuf::from(app_image)
    } else {
        env::current_exe().unwrap()
    }
}

fn create_settings_path(path: &Path) {
    println!("Creating path if needed..");
    if let Some(parent) = path.parent() {
        if let Err(e) = create_dir_all(parent) {
            if e.kind() != ErrorKind::AlreadyExists {
                panic!("Unable to Create Project Directory");
            }
        }
    }
}

fn load_settings(path: &PathBuf) -> Value {
    println!("Loading Existing Settings..");
    let path_str = String::from(path.to_string_lossy());
    match File::open(path) {
        Ok(reader) => serde_json::from_reader(reader)
            .unwrap_or_else(|_| panic!("Could not parse daemon settings file at {}", path_str)),
        Err(_) => panic!(
            "Could not open daemon settings file for reading at {}",
            path_str
        ),
    }
}

fn write_settings(path: &PathBuf, mut value: Value, install: bool) {
    let exe = get_current_path();

    value["activate"] = if install {
        Value::String(get_platform_path(exe))
    } else {
        Value::Null
    };

    let path_str = String::from(path.to_string_lossy());
    let writer = File::create(path).unwrap_or_else(|_| {
        panic!(
            "Could not open daemon settings file for writing at {}",
            path_str
        )
    });
    serde_json::to_writer_pretty(writer, &value)
        .unwrap_or_else(|_| panic!("Could not write to daemon settings file at {}", path_str));
}

fn get_settings_file() -> PathBuf {
    let proj_dirs = ProjectDirs::from("org", "GoXLR-on-Linux", "GoXLR-Utility")
        .expect("Couldn't find project directories");
    proj_dirs.config_dir().join("settings.json")
}

#[cfg(target_os = "linux")]
fn show_error(title: String, message: String) {
    use std::process::Command;
    // We have two choices here, kdialog, or zenity. We'll try both.
    if let Err(e) = Command::new("kdialog")
        .arg("--title")
        .arg(title.clone())
        .arg("--error")
        .arg(message.clone())
        .output()
    {
        println!("Error Running kdialog: {}, falling back to zenity..", e);
        let _ = Command::new("zenity")
            .arg("--title")
            .arg(title)
            .arg("--error")
            .arg("--text")
            .arg(message)
            .output();
    }
}

#[cfg(target_os = "linux")]
fn show_option(title: String, message: String) -> Result<(), ()> {
    use std::process::Command;
    // We need to grab the return status..
    if let Ok(status) = Command::new("kdialog")
        .arg("--title")
        .arg(title.clone())
        .arg("--yesno")
        .arg(message.clone())
        .status()
    {
        if status.success() {
            Ok(())
        } else {
            Err(())
        }
    } else if let Ok(status) = Command::new("zenity")
        .arg("--title")
        .arg(title)
        .arg("--question")
        .arg("--text")
        .arg(message)
        .status()
    {
        if status.success() {
            Ok(())
        } else {
            Err(())
        }
    } else {
        // We weren't able to trigger kdialog, or zenity, this is a failure.
        Err(())
    }
}

#[cfg(target_os = "windows")]
fn show_error(title: String, message: String) {
    use windows::core::HSTRING;
    use windows::Win32::UI::WindowsAndMessaging::{MessageBoxW, MB_ICONERROR, MB_OK};

    let title = HSTRING::from(title);
    let message = HSTRING::from(message);

    unsafe {
        MessageBoxW(None, &message, &title, MB_ICONERROR | MB_OK);
    }
}

#[cfg(target_os = "windows")]
fn show_option(title: String, message: String) -> Result<(), ()> {
    use windows::core::HSTRING;
    use windows::Win32::UI::WindowsAndMessaging::{
        MessageBoxW, MB_ICONQUESTION, MB_YESNO, MESSAGEBOX_RESULT,
    };

    let title = HSTRING::from(title);
    let message = HSTRING::from(message);

    unsafe {
        let result = MessageBoxW(None, &message, &title, MB_ICONQUESTION | MB_YESNO);

        // Either the windows crate doesn't have the enum, or I can't find it.. Either way..
        // https://learn.microsoft.com/en-us/dotnet/api/system.windows.messageboxresult
        if result == MESSAGEBOX_RESULT(6) {
            return Ok(());
        }
        Err(())
    }
}

#[cfg(target_os = "macos")]
fn show_error(title: String, message: String) {
    // Blocks until the alert is dismissed, so callers can safely exit afterwards.
    macos::on_main_sync(|mtm| macos::show_messagebox(mtm, title, message));
}

#[cfg(target_os = "macos")]
fn show_option(title: String, message: String) -> Result<(), ()> {
    macos::on_main_sync(|mtm| macos::show_question(mtm, title, message)).unwrap_or(Err(()))
}

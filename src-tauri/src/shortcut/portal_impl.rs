//! XDG Desktop Portal GlobalShortcuts implementation (Wayland-only)
//!
//! This module provides global shortcut functionality using the XDG Desktop Portal's
//! `org.freedesktop.portal.GlobalShortcuts` interface. This is the only sanctioned
//! way to capture global keyboard shortcuts on Wayland, where X11's XGrabKey mechanism
//! is blocked by the compositor's security model.
//!
//! ## How it works
//!
//! 1. A D-Bus session is created with the portal
//! 2. Shortcuts are "bound" to the session via `BindShortcuts`
//! 3. The portal may show a system dialog for the user to confirm/remap shortcuts
//! 4. `Activated`/`Deactivated` signals fire when the user presses/releases the shortcuts
//!
//! ## Requirements
//!
//! - A compositor that supports the GlobalShortcuts portal (GNOME 45+, KDE Plasma 6+,
//!   Hyprland, Sway with xdg-desktop-portal-wlr, etc.)
//! - `xdg-desktop-portal` running as a D-Bus service
//!
//! ## X11 compatibility
//!
//! This module is only compiled on Linux (`cfg(target_os = "linux")`). On X11 sessions
//! the existing Tauri/HandyKeys backends continue to work as before. The Portal backend
//! is only selected when the user explicitly chooses it or when the runtime detects a
//! Wayland session.

use ashpd::desktop::global_shortcuts::{
    BindShortcutsOptions, GlobalShortcuts, NewShortcut,
};
use ashpd::desktop::{Session, session::CreateSessionOptions};
use futures_util::StreamExt;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tauri::{AppHandle, Manager};
use tokio::sync::mpsc;

use crate::settings::{self, ShortcutBinding};

use super::handler::handle_shortcut_event;

/// Commands sent from main thread to the portal manager task
enum PortalCommand {
    /// Rebind all shortcuts (after a register/unregister)
    RebindShortcuts,
    /// Shut down the portal session
    Shutdown,
}

/// State for the portal-based shortcut manager, stored in Tauri managed state
pub struct PortalState {
    /// Channel to send commands to the portal manager task
    command_sender: mpsc::UnboundedSender<PortalCommand>,
    /// Currently registered bindings: binding_id -> ShortcutBinding
    bindings: Arc<Mutex<HashMap<String, ShortcutBinding>>>,
}

impl PortalState {
    /// Check if the GlobalShortcuts portal is available on this system.
    /// Call this before attempting to initialise the portal backend.
    pub async fn is_available() -> bool {
        match GlobalShortcuts::new().await {
            Ok(_) => {
                info!("XDG GlobalShortcuts portal is available");
                true
            }
            Err(e) => {
                debug!("XDG GlobalShortcuts portal not available: {}", e);
                false
            }
        }
    }
}

impl Drop for PortalState {
    fn drop(&mut self) {
        let _ = self.command_sender.send(PortalCommand::Shutdown);
    }
}

/// Initialise the portal-based shortcut system.
///
/// This creates a D-Bus session with the XDG GlobalShortcuts portal, registers
/// all configured shortcuts, and starts listening for activation signals.
pub fn init_shortcuts(app: &AppHandle) -> Result<(), String> {
    let default_bindings = settings::get_default_settings().bindings;
    let user_settings = settings::load_or_create_app_settings(app);

    // Collect initial bindings (same logic as tauri_impl / handy_keys)
    let mut initial_bindings = HashMap::new();
    for (id, default_binding) in default_bindings {
        if id == "cancel" {
            continue; // Cancel is registered dynamically during recording
        }
        if id == "transcribe_with_post_process" && !user_settings.post_process_enabled {
            continue;
        }
        let binding = user_settings
            .bindings
            .get(&id)
            .cloned()
            .unwrap_or(default_binding);
        initial_bindings.insert(id, binding);
    }

    let bindings = Arc::new(Mutex::new(initial_bindings));
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    let state = PortalState {
        command_sender: cmd_tx,
        bindings: bindings.clone(),
    };

    // Spawn the async portal manager task on the Tauri runtime
    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        if let Err(e) = portal_manager_task(app_clone, bindings, cmd_rx).await {
            error!("Portal shortcut manager failed: {}", e);
        }
    });

    app.manage(state);
    info!("XDG Portal shortcuts initialised");
    Ok(())
}

/// The main async task that owns the portal session and dispatches signals.
async fn portal_manager_task(
    app: AppHandle,
    bindings: Arc<Mutex<HashMap<String, ShortcutBinding>>>,
    mut cmd_rx: mpsc::UnboundedReceiver<PortalCommand>,
) -> Result<(), String> {
    let shortcuts_proxy = GlobalShortcuts::new()
        .await
        .map_err(|e| format!("Failed to connect to GlobalShortcuts portal: {}", e))?;

    let session = shortcuts_proxy
        .create_session(CreateSessionOptions::default())
        .await
        .map_err(|e| format!("Failed to create portal session: {}", e))?;

    info!("XDG GlobalShortcuts session created");

    // Initial bind
    bind_current_shortcuts(&shortcuts_proxy, &session, &bindings).await?;

    // Subscribe to Activated / Deactivated signals
    let mut activated_stream = shortcuts_proxy
        .receive_activated()
        .await
        .map_err(|e| format!("Failed to listen for Activated signals: {}", e))?;
    let mut deactivated_stream = shortcuts_proxy
        .receive_deactivated()
        .await
        .map_err(|e| format!("Failed to listen for Deactivated signals: {}", e))?;

    info!("Portal shortcut signal listeners active");

    loop {
        tokio::select! {
            Some(activated) = activated_stream.next() => {
                let shortcut_id = activated.shortcut_id();
                debug!("Portal shortcut activated: {}", shortcut_id);
                let hotkey_string = hotkey_string_for(&bindings, shortcut_id);
                handle_shortcut_event(&app, shortcut_id, &hotkey_string, true);
            }
            Some(deactivated) = deactivated_stream.next() => {
                let shortcut_id = deactivated.shortcut_id();
                debug!("Portal shortcut deactivated: {}", shortcut_id);
                let hotkey_string = hotkey_string_for(&bindings, shortcut_id);
                handle_shortcut_event(&app, shortcut_id, &hotkey_string, false);
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    PortalCommand::RebindShortcuts => {
                        if let Err(e) = bind_current_shortcuts(&shortcuts_proxy, &session, &bindings).await {
                            error!("Failed to rebind portal shortcuts: {}", e);
                        }
                    }
                    PortalCommand::Shutdown => {
                        info!("Portal shortcut manager shutting down");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Bind all current shortcuts to the portal session.
///
/// This calls `BindShortcuts` which may show a compositor dialog asking the
/// user to confirm or remap the requested key combinations.
async fn bind_current_shortcuts(
    shortcuts_proxy: &GlobalShortcuts,
    session: &Session<GlobalShortcuts>,
    bindings: &Arc<Mutex<HashMap<String, ShortcutBinding>>>,
) -> Result<(), String> {
    let bindings_snapshot = bindings
        .lock()
        .map_err(|_| "Failed to lock bindings")?
        .clone();

    if bindings_snapshot.is_empty() {
        return Ok(());
    }

    let portal_shortcuts: Vec<NewShortcut> = bindings_snapshot
        .iter()
        .map(|(id, binding)| {
            let description = match id.as_str() {
                "transcribe" => "Start/stop transcription",
                "transcribe_with_post_process" => "Start/stop transcription with post-processing",
                "cancel" => "Cancel current operation",
                _ => "Handy shortcut",
            };
            NewShortcut::new(id, description)
                .preferred_trigger(Some(binding.current_binding.as_str()))
        })
        .collect();

    let bound = shortcuts_proxy
        .bind_shortcuts(session, &portal_shortcuts, None, BindShortcutsOptions::default())
        .await
        .map_err(|e| format!("Failed to bind shortcuts via portal: {}", e))?;

    // Log what the portal actually bound (may differ from our preferred triggers)
    let response = bound.response()
        .map_err(|e| format!("Portal BindShortcuts response error: {}", e))?;
    info!(
        "Bound {} shortcuts via XDG portal",
        response.shortcuts().len()
    );
    for shortcut in response.shortcuts() {
        info!(
            "  Portal shortcut '{}': trigger_description='{}'",
            shortcut.id(),
            shortcut.trigger_description()
        );
    }

    Ok(())
}

/// Look up the configured hotkey string for a binding ID.
fn hotkey_string_for(
    bindings: &Arc<Mutex<HashMap<String, ShortcutBinding>>>,
    binding_id: &str,
) -> String {
    bindings
        .lock()
        .ok()
        .and_then(|b| b.get(binding_id).map(|sb| sb.current_binding.clone()))
        .unwrap_or_default()
}

// ============================================================================
// Public API consumed by shortcut/mod.rs
// ============================================================================

/// Register a shortcut binding. This updates the binding map and triggers a
/// rebind with the portal.
pub fn register_shortcut(app: &AppHandle, binding: ShortcutBinding) -> Result<(), String> {
    let state = app
        .try_state::<PortalState>()
        .ok_or("PortalState not initialised")?;

    {
        let mut map = state
            .bindings
            .lock()
            .map_err(|_| "Failed to lock bindings")?;
        map.insert(binding.id.clone(), binding);
    }

    state
        .command_sender
        .send(PortalCommand::RebindShortcuts)
        .map_err(|_| "Failed to send rebind command to portal task")?;

    Ok(())
}

/// Unregister a shortcut binding.
pub fn unregister_shortcut(app: &AppHandle, binding: ShortcutBinding) -> Result<(), String> {
    let state = app
        .try_state::<PortalState>()
        .ok_or("PortalState not initialised")?;

    {
        let mut map = state
            .bindings
            .lock()
            .map_err(|_| "Failed to lock bindings")?;
        map.remove(&binding.id);
    }

    state
        .command_sender
        .send(PortalCommand::RebindShortcuts)
        .map_err(|_| "Failed to send rebind command to portal task")?;

    Ok(())
}

/// Register the cancel shortcut (called when recording starts).
/// Unlike Tauri/HandyKeys, the portal supports dynamic registration on Linux.
pub fn register_cancel_shortcut(app: &AppHandle) {
    let settings = settings::get_settings(app);
    if let Some(cancel_binding) = settings.bindings.get("cancel").cloned() {
        if let Err(e) = register_shortcut(app, cancel_binding) {
            error!("Failed to register cancel shortcut via portal: {}", e);
        }
    }
}

/// Unregister the cancel shortcut (called when recording stops).
pub fn unregister_cancel_shortcut(app: &AppHandle) {
    let settings = settings::get_settings(app);
    if let Some(cancel_binding) = settings.bindings.get("cancel").cloned() {
        let _ = unregister_shortcut(app, cancel_binding);
    }
}

/// Validate a shortcut for the Portal implementation.
///
/// The portal is very flexible — it accepts most key combinations as
/// "preferred triggers". The compositor may remap them and the user confirms
/// via a system dialog, so there is almost nothing to reject here.
pub fn validate_shortcut(raw: &str) -> Result<(), String> {
    if raw.trim().is_empty() {
        return Err("Shortcut cannot be empty".into());
    }
    Ok(())
}

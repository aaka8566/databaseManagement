fn connections_file_path() -> PathBuf {
    connections_storage_dir().join(CONNECTIONS_FILE)
}

fn legacy_connections_file_path() -> PathBuf {
    connections_storage_dir().join(LEGACY_CONNECTIONS_FILE)
}

fn connections_storage_dir() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home)
                .join("Library")
                .join("Application Support")
                .join("Sharingan");
        }
    }

    #[cfg(target_os = "windows")]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            return PathBuf::from(appdata).join("Sharingan");
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".config").join("sharingan");
    }

    PathBuf::from(".")
}

fn connection_file_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![connections_file_path(), legacy_connections_file_path()];

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join(CONNECTIONS_FILE));
        candidates.push(cwd.join(LEGACY_CONNECTIONS_FILE));
    }

    if let Ok(exe) = std::env::current_exe() {
        for ancestor in exe.ancestors().take(8) {
            candidates.push(ancestor.join(CONNECTIONS_FILE));
            candidates.push(ancestor.join(LEGACY_CONNECTIONS_FILE));
        }
    }

    let mut deduped = Vec::new();
    for candidate in candidates {
        if !deduped.contains(&candidate) {
            deduped.push(candidate);
        }
    }
    deduped
}

fn load_custom_connections() -> Vec<ConnectionProfile> {
    let raw = connection_file_candidates()
        .into_iter()
        .find_map(|path| fs::read_to_string(path).ok());
    let Some(raw) = raw else {
        return Vec::new();
    };

    serde_json::from_str::<Vec<ConnectionProfile>>(&raw)
        .unwrap_or_default()
        .into_iter()
        .map(|mut connection| {
            connection.schemas.clear();
            connection
        })
        .collect()
}

fn save_custom_connections(connections: &[ConnectionProfile]) -> Result<(), String> {
    let sanitized = connections
        .iter()
        .cloned()
        .map(|mut connection| {
            connection.schemas.clear();
            connection
        })
        .collect::<Vec<_>>();

    let serialized = serde_json::to_string_pretty(&sanitized)
        .map_err(|error| format!("serialize failed: {}", error))?;
    if let Some(parent) = connections_file_path().parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create dir failed: {}", error))?;
    }
    fs::write(connections_file_path(), serialized)
        .map_err(|error| format!("write failed: {}", error))
}

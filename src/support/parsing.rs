fn fuzzy_score(query: &str, target: &str) -> Option<i32> {
    if query.trim().is_empty() {
        return Some(1);
    }

    let mut score = 0;
    let mut last_match = 0usize;
    let mut matched_any = false;

    for query_ch in query.chars() {
        let mut found = None;
        for (idx, target_ch) in target[last_match..].char_indices() {
            if target_ch.eq_ignore_ascii_case(&query_ch) {
                found = Some(last_match + idx);
                break;
            }
        }
        let index = found?;
        matched_any = true;
        score += 10;
        if index == last_match {
            score += 4;
        }
        last_match = index + 1;
    }

    if matched_any {
        score += (64 - target.len().min(64)) as i32;
        Some(score)
    } else {
        None
    }
}

fn token_at_end(sql: &str) -> TokenRange {
    let mut start = sql.len();
    let end = sql.len();

    for (index, ch) in sql.char_indices().rev() {
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            start = index;
        } else {
            break;
        }
    }

    TokenRange {
        start,
        end,
        fragment: sql[start..end].to_owned(),
    }
}

fn previous_token_before(sql: &str, current_start: usize) -> String {
    sql[..current_start]
        .trim_end()
        .rsplit(|ch: char| ch.is_whitespace() || ch == ',' || ch == '(' || ch == ')')
        .find(|token| !token.is_empty())
        .unwrap_or_default()
        .to_owned()
}

fn parse_connection_url(raw: &str) -> Result<ImportedConnectionUrl, String> {
    let url = raw.trim();
    if url.is_empty() {
        return Err("paste a connection URL first".to_owned());
    }

    let (scheme, remainder) = url
        .split_once("://")
        .ok_or_else(|| "expected a URL like mysql://user:pass@host:3306/db".to_owned())?;

    let scheme_lower = scheme.to_ascii_lowercase();
    let (base_scheme, use_ssh) = scheme_lower
        .strip_suffix("+ssh")
        .map(|base| (base, true))
        .unwrap_or((scheme_lower.as_str(), false));

    let (engine, default_port) = match base_scheme {
        "mysql" => (ConnectionEngine::MySQL, 3306),
        "mariadb" => (ConnectionEngine::MariaDB, 3306),
        "postgres" | "postgresql" => (ConnectionEngine::Postgres, 5432),
        "redshift" => (ConnectionEngine::Redshift, 5432),
        "cockroach" | "cockroachdb" => (ConnectionEngine::CockroachDB, 5432),
        "greenplum" => (ConnectionEngine::Greenplum, 5432),
        "vertica" => (ConnectionEngine::Vertica, 5432),
        "mssql" | "sqlserver" => (ConnectionEngine::MSSQL, 1433),
        "clickhouse" => (ConnectionEngine::ClickHouse, 8123),
        "duckdb" => (ConnectionEngine::DuckDB, 0),
        "sqlite" | "sqlite3" => (ConnectionEngine::SQLite, 0),
        "libsql" => (ConnectionEngine::LibSQL, 0),
        "cloudflared1" | "d1" => (ConnectionEngine::CloudflareD1, 0),
        "cassandra" => (ConnectionEngine::Cassandra, 9042),
        "redis" => (ConnectionEngine::Redis, 6379),
        "mongodb" | "mongo" => (ConnectionEngine::MongoDB, 27017),
        "oracle" => (ConnectionEngine::Oracle, 1521),
        "bigquery" => (ConnectionEngine::BigQuery, 443),
        "snowflake" => (ConnectionEngine::Snowflake, 443),
        "dynamodb" => (ConnectionEngine::DynamoDB, 443),
        other => {
            return Err(format!(
                "unsupported URL scheme '{}'. Use mysql://, postgres://, mssql://, redis://, mongodb://, etc.",
                other
            ));
        }
    };

    let (base_without_query, query_string) = remainder
        .split_once('?')
        .map(|(left, right)| (left, Some(right)))
        .unwrap_or((remainder, None));
    let without_query = base_without_query
        .split_once('#')
        .map(|(left, _)| left)
        .unwrap_or(base_without_query);

    let (authority, raw_path) = without_query
        .split_once('/')
        .map(|(left, right)| (left, right))
        .unwrap_or((without_query, ""));
    if authority.trim().is_empty() {
        return Err("missing host in connection URL".to_owned());
    }

    let (user, password, host, port, database, ssh) = if use_ssh {
        let (ssh_user, ssh_password, ssh_host, ssh_port) =
            parse_authority_credentials(authority, 22)?;
        let mut path_segments = raw_path.split('/').filter(|segment| !segment.is_empty());
        let db_authority = path_segments
            .next()
            .ok_or_else(|| "missing database host in SSH connection URL".to_owned())?;
        let database = path_segments
            .next()
            .map(decode_connection_url_component)
            .unwrap_or_default();
        let (user, password, host, port) = parse_authority_credentials(db_authority, default_port)?;
        (
            user,
            password,
            host,
            port,
            database,
            Some(ImportedSshTunnel {
                host: ssh_host,
                port: ssh_port,
                user: ssh_user,
                password: ssh_password,
                private_key_path: String::new(),
            }),
        )
    } else {
        let (user, password, host, port) = parse_authority_credentials(authority, default_port)?;
        let database = raw_path
            .split('/')
            .find(|segment| !segment.is_empty())
            .map(decode_connection_url_component)
            .unwrap_or_default();
        (user, password, host, port, database, None)
    };

    let name = if !database.is_empty() {
        format!("{}@{}", database, host)
    } else {
        host.clone()
    };

    let mut ssh = ssh;
    if let Some(query_ssh) = query_string.and_then(parse_ssh_tunnel_from_query) {
        match &mut ssh {
            Some(existing) => {
                if !query_ssh.host.is_empty() {
                    existing.host = query_ssh.host;
                }
                if query_ssh.port != 22 || existing.port == 22 {
                    existing.port = query_ssh.port;
                }
                if !query_ssh.user.is_empty() {
                    existing.user = query_ssh.user;
                }
                if !query_ssh.password.is_empty() {
                    existing.password = query_ssh.password;
                }
                if !query_ssh.private_key_path.is_empty() {
                    existing.private_key_path = query_ssh.private_key_path;
                }
            }
            None => ssh = Some(query_ssh),
        }
    }

    Ok(ImportedConnectionUrl {
        engine,
        host,
        port,
        database,
        user,
        password,
        name,
        use_ssh,
        ssh,
    })
}

fn parse_authority_credentials(
    authority: &str,
    default_port: u16,
) -> Result<(String, String, String, u16), String> {
    if authority.trim().is_empty() {
        return Err("missing host in connection URL".to_owned());
    }

    let (auth_part, host_part) = authority
        .rsplit_once('@')
        .map(|(auth, host)| (Some(auth), host))
        .unwrap_or((None, authority));

    let (user, password) = match auth_part {
        Some(auth) => {
            let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
            (
                decode_connection_url_component(user),
                decode_connection_url_component(password),
            )
        }
        None => (String::new(), String::new()),
    };

    let (host, port) = parse_host_port(host_part, default_port)?;
    Ok((user, password, host, port))
}

fn parse_ssh_tunnel_from_query(query: &str) -> Option<ImportedSshTunnel> {
    let query = query.split('#').next().unwrap_or(query);
    let mut ssh = ImportedSshTunnel {
        port: 22,
        ..ImportedSshTunnel::default()
    };
    let mut enabled = false;

    for pair in query.split('&').filter(|part| !part.trim().is_empty()) {
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_connection_url_component(raw_key).to_ascii_lowercase();
        let value = decode_connection_url_component(raw_value);

        match key.as_str() {
            "ssh" | "use_ssh" | "ssh_tunnel" => {
                enabled = matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "" | "1" | "true" | "yes" | "on"
                );
            }
            "ssh_host" | "sshhost" | "tunnel_host" | "ssh_hostname" => {
                ssh.host = value;
                enabled = true;
            }
            "ssh_port" | "sshport" | "tunnel_port" => {
                if let Ok(port) = value.trim().parse::<u16>() {
                    ssh.port = port;
                }
                enabled = true;
            }
            "ssh_user" | "ssh_username" | "sshuser" | "tunnel_user" => {
                ssh.user = value;
                enabled = true;
            }
            "ssh_password" | "sshpassword" | "tunnel_password" => {
                ssh.password = value;
                enabled = true;
            }
            "ssh_key" | "ssh_private_key" | "ssh_private_key_path" | "ssh_key_path" => {
                ssh.private_key_path = value;
                enabled = true;
            }
            _ => {}
        }
    }

    if enabled { Some(ssh) } else { None }
}

fn parse_host_port(authority: &str, default_port: u16) -> Result<(String, u16), String> {
    if authority.is_empty() {
        return Err("missing host in connection URL".to_owned());
    }

    if let Some(rest) = authority.strip_prefix('[') {
        let (host, tail) = rest
            .split_once(']')
            .ok_or_else(|| "invalid IPv6 host in connection URL".to_owned())?;
        let port = tail
            .strip_prefix(':')
            .filter(|value| !value.is_empty())
            .map(|value| {
                value
                    .parse::<u16>()
                    .map_err(|_| "invalid port in connection URL".to_owned())
            })
            .transpose()?
            .unwrap_or(default_port);
        return Ok((host.to_owned(), port));
    }

    let (host, port) = authority
        .rsplit_once(':')
        .filter(|(_, port)| !port.is_empty() && port.chars().all(|ch| ch.is_ascii_digit()))
        .map(|(host, port)| {
            port.parse::<u16>()
                .map(|parsed| (host.to_owned(), parsed))
                .map_err(|_| "invalid port in connection URL".to_owned())
        })
        .transpose()?
        .unwrap_or((authority.to_owned(), default_port));

    Ok((host, port))
}

fn decode_connection_url_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = String::with_capacity(value.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = &value[index + 1..index + 3];
                if let Ok(parsed) = u8::from_str_radix(hex, 16) {
                    decoded.push(parsed as char);
                    index += 3;
                    continue;
                }
                decoded.push('%');
            }
            b'+' => {
                decoded.push(' ');
                index += 1;
                continue;
            }
            other => decoded.push(other as char),
        }
        index += 1;
    }

    decoded
}

fn autocomplete_matches(entry: &AutocompleteRecord, prefix_lower: &str) -> bool {
    if prefix_lower.is_empty() {
        return true;
    }

    entry.label_lower.starts_with(prefix_lower)
        || entry.insert_lower.starts_with(prefix_lower)
        || entry
            .label_lower
            .rsplit('.')
            .next()
            .map(|segment| segment.starts_with(prefix_lower))
            .unwrap_or(false)
        || entry
            .insert_lower
            .rsplit('.')
            .next()
            .map(|segment| segment.starts_with(prefix_lower))
            .unwrap_or(false)
}

fn autocomplete_popup_position(
    cursor_pos: egui::Pos2,
    _editor_rect: egui::Rect,
    viewport_rect: egui::Rect,
    popup_width: f32,
    popup_height: f32,
) -> egui::Pos2 {
    let min_x = viewport_rect.left() + 8.0;
    let max_x = (viewport_rect.right() - popup_width - 8.0).max(min_x);
    let x = (cursor_pos.x + 4.0).clamp(min_x, max_x);

    let below_y = cursor_pos.y + 20.0;
    let above_y = cursor_pos.y - popup_height - 6.0;
    let min_y = viewport_rect.top() + 8.0;
    let max_y = (viewport_rect.bottom() - popup_height - 8.0).max(min_y);
    let y = if below_y + popup_height <= viewport_rect.bottom() - 8.0 {
        below_y
    } else if above_y >= min_y {
        above_y
    } else {
        below_y.clamp(min_y, max_y)
    };

    egui::Pos2::new(x, y)
}

fn connection_database_label(connection: &ConnectionProfile) -> String {
    let database = connection.database.trim();
    if database.is_empty() {
        "all schemas".to_owned()
    } else {
        database.to_owned()
    }
}


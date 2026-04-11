fn spawn_background_worker(
    ctx: egui::Context,
) -> (Sender<BackgroundCommand>, Receiver<BackgroundEvent>) {
    let (command_tx, command_rx) = mpsc::channel::<BackgroundCommand>();
    let (event_tx, event_rx) = mpsc::channel::<BackgroundEvent>();

    thread::spawn(move || {
        let mut resources = WorkerResources::default();
        while let Ok(command) = command_rx.recv() {
            let event = match command {
                BackgroundCommand::LoadSchemas {
                    job_id,
                    connection_index,
                    profile,
                } => BackgroundEvent::SchemasLoaded {
                    job_id,
                    connection_index,
                    connection_name: profile.name.clone(),
                    result: load_live_schemas(&profile, &mut resources),
                },
                BackgroundCommand::ExecuteQuery {
                    job_id,
                    tab_id,
                    connection_name,
                    profile,
                    sql,
                } => BackgroundEvent::QueryFinished {
                    job_id,
                    tab_id,
                    connection_name,
                    result: execute_live_query(&profile, &sql, &mut resources),
                    sql,
                },
                BackgroundCommand::PreviewTable {
                    job_id,
                    connection_index,
                    tab_id,
                    table_selection,
                    row_limit,
                    profile,
                    table,
                } => {
                    let (table, result) =
                        match preview_live_table(&profile, &table, row_limit, &mut resources) {
                            Ok((updated_table, result)) => (updated_table, Ok(result)),
                            Err(error) => (table, Err(error)),
                        };
                    BackgroundEvent::TablePreviewLoaded {
                        job_id,
                        connection_index,
                        tab_id,
                        table_selection,
                        table,
                        result,
                    }
                }
                BackgroundCommand::SaveRow {
                    job_id,
                    profile,
                    source,
                    columns,
                    original_row,
                    updated_row,
                    row_index,
                } => BackgroundEvent::RowSaved {
                    job_id,
                    source: source.clone(),
                    row_index,
                    result: update_live_row(
                        &profile,
                        &source,
                        &columns,
                        &original_row,
                        &updated_row,
                        &mut resources,
                    ),
                    updated_row,
                },
                BackgroundCommand::LoadDatabases {
                    job_id,
                    connection_index,
                    profile,
                } => BackgroundEvent::DatabasesLoaded {
                    job_id,
                    connection_index,
                    result: load_live_databases(&profile, &mut resources),
                },
                BackgroundCommand::Disconnect {
                    connection_index,
                    profile,
                } => {
                    resources.disconnect(&profile);
                    BackgroundEvent::Disconnected { connection_index }
                }
                BackgroundCommand::LoadTableColumns {
                    job_id,
                    connection_index,
                    schema_name,
                    table_name,
                    profile,
                } => BackgroundEvent::TableColumnsLoaded {
                    job_id,
                    connection_index,
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    result: load_live_table_columns(
                        &profile,
                        &schema_name,
                        &table_name,
                        &mut resources,
                    ),
                },
            };

            if event_tx.send(event).is_err() {
                break;
            }
            ctx.request_repaint();
        }
    });

    (command_tx, event_rx)
}

fn load_live_table_columns(
    profile: &ConnectionProfile,
    schema_name: &str,
    table_name: &str,
    resources: &mut WorkerResources,
) -> Result<Vec<TableColumn>, String> {
    let (host, port) = live_endpoint(profile, resources)?;

    match profile.engine {
        ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
            load_mysql_table_columns(profile, &host, port, schema_name, table_name, resources)
        }
        ConnectionEngine::Postgres
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => {
            load_postgres_table_columns(profile, &host, port, schema_name, table_name, resources)
        }
        ConnectionEngine::ClickHouse => {
            load_clickhouse_table_columns(profile, &host, port, schema_name, table_name, resources)
        }
        ConnectionEngine::SQLite | ConnectionEngine::LibSQL => {
            load_sqlite_table_columns(profile, table_name)
        }
        ConnectionEngine::DuckDB => load_duckdb_table_columns(profile, schema_name, table_name),
        _ => Err(format!(
            "On-demand column loading not yet implemented for {:?}",
            profile.engine
        )),
    }
}

fn load_clickhouse_table_columns(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    schema_name: &str,
    table_name: &str,
    resources: &mut WorkerResources,
) -> Result<Vec<TableColumn>, String> {
    let client = worker_clickhouse_client(resources, profile, host, port)?;
    let sql = format!(
        "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}'",
        schema_name, table_name
    );

    let rows: Vec<(String, String)> = resources.runtime.block_on(async {
        client
            .query(&sql)
            .fetch_all()
            .await
            .map_err(|error| format!("clickhouse query failed: {}", error))
    })?;

    Ok(rows
        .into_iter()
        .map(|(name, kind)| TableColumn {
            name,
            kind,
            nullable: false,
            primary: false,
            character_set: String::new(),
            collation: String::new(),
            default_value: String::new(),
            extra: String::new(),
            comment: String::new(),
        })
        .collect())
}

fn load_mysql_table_columns(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    schema_name: &str,
    table_name: &str,
    resources: &mut WorkerResources,
) -> Result<Vec<TableColumn>, String> {
    let pool = worker_mysql_pool(resources, profile, host, port)?;
    let mut conn = pool
        .get_conn()
        .map_err(|error| format!("mysql connection failed: {}", error))?;

    let rows: Vec<(
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = conn
        .exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT \
             FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ORDINAL_POSITION",
            (schema_name.to_owned(), table_name.to_owned()),
        )
        .map_err(|error| format!("failed to load mysql columns: {}", error))?;

    Ok(rows
        .into_iter()
        .map(
            |(
                name,
                kind,
                nullable,
                column_key,
                character_set,
                collation,
                default_value,
                extra,
                comment,
            )| TableColumn {
                name,
                kind,
                nullable: nullable == "YES",
                primary: column_key == "PRI",
                character_set: character_set.unwrap_or_else(|| "NULL".to_owned()),
                collation: collation.unwrap_or_else(|| "NULL".to_owned()),
                default_value: default_value.unwrap_or_else(|| "NULL".to_owned()),
                extra: extra.unwrap_or_default(),
                comment: comment.unwrap_or_default(),
            },
        )
        .collect())
}

fn load_sqlite_table_columns(
    profile: &ConnectionProfile,
    table_name: &str,
) -> Result<Vec<TableColumn>, String> {
    let path = profile.path.as_deref().unwrap_or(":memory:");
    let conn = SqliteConnection::open(path)
        .map_err(|error| format!("sqlite connection failed: {}", error))?;

    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({})", table_name))
        .map_err(|error| format!("sqlite pragma failed: {}", error))?;

    let rows = stmt
        .query_map([], |row| {
            Ok(TableColumn {
                name: row.get(1)?,
                kind: row.get(2)?,
                nullable: row.get::<_, i32>(3)? == 0,
                primary: row.get::<_, i32>(5)? == 1,
                character_set: String::new(),
                collation: String::new(),
                default_value: "NULL".to_owned(),
                extra: String::new(),
                comment: String::new(),
            })
        })
        .map_err(|error| format!("sqlite query failed: {}", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("sqlite collect failed: {}", error))?;

    Ok(rows)
}

fn load_duckdb_table_columns(
    profile: &ConnectionProfile,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<TableColumn>, String> {
    let path = profile.path.as_deref().unwrap_or(":memory:");
    let conn = DuckDbConnection::open(path)
        .map_err(|error| format!("duckdb connection failed: {}", error))?;

    let mut stmt = conn
        .prepare("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")
        .map_err(|error| format!("duckdb prepare failed: {}", error))?;

    let mut rows = stmt
        .query(params![schema_name, table_name])
        .map_err(|error| format!("duckdb query failed: {}", error))?;

    let mut columns = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("duckdb row failed: {}", error))?
    {
        columns.push(TableColumn {
            name: row.get(0).unwrap_or_default(),
            kind: row.get(1).unwrap_or_default(),
            nullable: row.get::<_, String>(2).unwrap_or_default() == "YES",
            primary: false,
            character_set: String::new(),
            collation: String::new(),
            default_value: String::new(),
            extra: String::new(),
            comment: String::new(),
        });
    }

    Ok(columns)
}

fn load_mysql_table_indexes(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    schema_name: &str,
    table_name: &str,
    resources: &mut WorkerResources,
) -> Result<Vec<TableIndexEntry>, String> {
    let pool = worker_mysql_pool(resources, profile, host, port)?;
    let mut conn = pool
        .get_conn()
        .map_err(|error| format!("mysql connection failed: {}", error))?;

    let rows: Vec<(String, String, u8, String)> = conn
        .exec(
            "SELECT INDEX_NAME, INDEX_TYPE, NON_UNIQUE, COLUMN_NAME \
             FROM information_schema.statistics \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (schema_name.to_owned(), table_name.to_owned()),
        )
        .map_err(|error| format!("failed to load mysql indexes: {}", error))?;

    Ok(rows
        .into_iter()
        .map(
            |(index_name, index_algorithm, non_unique, column_name)| TableIndexEntry {
                index_name,
                index_algorithm,
                is_unique: non_unique == 0,
                column_name,
            },
        )
        .collect())
}

fn load_postgres_table_indexes(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    schema_name: &str,
    table_name: &str,
    resources: &mut WorkerResources,
) -> Result<Vec<TableIndexEntry>, String> {
    let client = worker_postgres_client(resources, profile, host, port)?;
    let rows = client
        .query(
            "SELECT i.relname AS index_name,
                    am.amname AS index_algorithm,
                    ix.indisunique AS is_unique,
                    COALESCE(a.attname, '<expression>') AS column_name
             FROM pg_class t
             JOIN pg_namespace n ON n.oid = t.relnamespace
             JOIN pg_index ix ON t.oid = ix.indrelid
             JOIN pg_class i ON i.oid = ix.indexrelid
             JOIN pg_am am ON am.oid = i.relam
             JOIN unnest(ix.indkey) WITH ORDINALITY AS cols(attnum, ord) ON true
             LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
             WHERE n.nspname = $1 AND t.relname = $2
             ORDER BY i.relname, cols.ord",
            &[&schema_name, &table_name],
        )
        .map_err(|error| format!("failed to load postgres indexes: {}", error))?;

    Ok(rows
        .into_iter()
        .map(|row| TableIndexEntry {
            index_name: row.get(0),
            index_algorithm: row.get(1),
            is_unique: row.get(2),
            column_name: row.get(3),
        })
        .collect())
}


struct TemporaryTunnel {
    local_port: u16,
    stop_signal: Sender<()>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

struct WorkerResources {
    tunnels: BTreeMap<String, TemporaryTunnel>,
    mysql_pools: BTreeMap<String, Pool>,
    postgres_clients: BTreeMap<String, PostgresClient>,
    clickhouse_clients: BTreeMap<String, ClickHouseClient>,
    runtime: tokio::runtime::Runtime,
}

impl Default for WorkerResources {
    fn default() -> Self {
        Self {
            tunnels: BTreeMap::new(),
            mysql_pools: BTreeMap::new(),
            postgres_clients: BTreeMap::new(),
            clickhouse_clients: BTreeMap::new(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime"),
        }
    }
}

impl WorkerResources {
    fn disconnect(&mut self, profile: &ConnectionProfile) {
        let (host, port) = if let Some(ssh) = &profile.ssh_tunnel {
            let tunnel_key = format!(
                "{}@{}:{}|{}:{}|{}|{}",
                ssh.user,
                ssh.host,
                ssh.port,
                profile.host,
                profile.port,
                ssh.private_key_path,
                if ssh.password.is_empty() {
                    "key"
                } else {
                    "pwd"
                }
            );
            if let Some(tunnel) = self.tunnels.remove(&tunnel_key) {
                ("127.0.0.1".to_owned(), tunnel.local_port)
            } else {
                (profile.host.clone(), profile.port)
            }
        } else {
            (profile.host.clone(), profile.port)
        };

        let key = live_resource_key(profile, &host, port);
        self.mysql_pools.remove(&key);
        self.postgres_clients.remove(&key);
        self.clickhouse_clients.remove(&key);
    }
}

impl Drop for TemporaryTunnel {
    fn drop(&mut self) {
        let _ = self.stop_signal.send(());
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

fn mysql_pool(profile: &ConnectionProfile, host: &str, port: u16) -> Result<Pool, String> {
    let tunneled = host == "127.0.0.1" && profile.ssh_tunnel.is_some();
    let connect_timeout = Some(Duration::from_secs(5));
    let io_timeout = Some(Duration::from_secs(12));

    let mut builder = OptsBuilder::new()
        .ip_or_hostname(Some(host.to_owned()))
        .tcp_port(port)
        .user(Some(profile.user.clone()))
        .tcp_connect_timeout(connect_timeout)
        .read_timeout(io_timeout)
        .write_timeout(io_timeout);

    if !profile.database.trim().is_empty() {
        builder = builder.db_name(Some(profile.database.clone()));
    }

    if !profile.password.is_empty() {
        builder = builder.pass(Some(profile.password.clone()));
    }

    if tunneled {
        // The SSH tunnel is already encrypted end-to-end. Attempting MySQL TLS
        // on top fails because the server's certificate is for the real RDS
        // hostname, not 127.0.0.1. Disabling SSL here causes caching_sha2_password
        // to fall back to RSA public-key exchange, which works over the tunnel.
        builder = builder.ssl_opts(None);
        builder = builder.prefer_socket(false);
    }

    let constraints = mysql::PoolConstraints::new(1, 1).unwrap();
    let pool_opts = mysql::PoolOpts::default().with_constraints(constraints);
    builder = builder.pool_opts(pool_opts);

    Pool::new(builder).map_err(|error| format!("mysql connection failed: {}", error))
}

fn live_resource_key(profile: &ConnectionProfile, host: &str, port: u16) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        profile.engine, host, port, profile.user, profile.database
    )
}

fn worker_mysql_pool(
    resources: &mut WorkerResources,
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
) -> Result<Pool, String> {
    let key = live_resource_key(profile, host, port);
    if let Some(pool) = resources.mysql_pools.get(&key) {
        return Ok(pool.clone());
    }
    let pool = mysql_pool(profile, host, port)?;
    resources.mysql_pools.insert(key, pool.clone());
    Ok(pool)
}

fn worker_clickhouse_client(
    resources: &mut WorkerResources,
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
) -> Result<ClickHouseClient, String> {
    let key = live_resource_key(profile, host, port);
    if let Some(client) = resources.clickhouse_clients.get(&key) {
        return Ok(client.clone());
    }
    let client = clickhouse_client(profile, host, port)?;
    resources.clickhouse_clients.insert(key, client.clone());
    Ok(client)
}

fn worker_postgres_client<'a>(
    resources: &'a mut WorkerResources,
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
) -> Result<&'a mut PostgresClient, String> {
    let key = live_resource_key(profile, host, port);
    if !resources.postgres_clients.contains_key(&key) {
        let client = postgres_client(profile, host, port)?;
        resources.postgres_clients.insert(key.clone(), client);
    }
    resources
        .postgres_clients
        .get_mut(&key)
        .ok_or_else(|| "postgres client cache unavailable".to_owned())
}


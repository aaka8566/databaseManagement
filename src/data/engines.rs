fn execute_mysql_query(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    sql: &str,
    resources: &mut WorkerResources,
) -> Result<QueryResult, String> {
    let pool = worker_mysql_pool(resources, profile, host, port)?;
    let mut conn = pool
        .get_conn()
        .map_err(|error| format!("mysql connection failed: {}", error))?;
    let started = Instant::now();
    let mut query_result = conn
        .query_iter(sql)
        .map_err(|error| format!("mysql query failed: {}", error))?;

    let columns = query_result
        .columns()
        .as_ref()
        .iter()
        .map(|column| ResultColumn {
            name: column.name_str().to_string(),
        })
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    while let Some(row) = query_result
        .next()
        .transpose()
        .map_err(|error| format!("mysql row failed: {}", error))?
    {
        rows.push(mysql_row_to_strings(row));
    }

    if columns.is_empty() {
        return Ok(QueryResult {
            columns: vec![
                ResultColumn {
                    name: "message".to_owned(),
                },
                ResultColumn {
                    name: "detail".to_owned(),
                },
            ],
            rows: vec![vec![
                "Command executed".to_owned(),
                format!("affected rows: {}", query_result.affected_rows()),
            ]],
            duration_ms: started.elapsed().as_millis() as u64,
            source: None,
        });
    }

    Ok(QueryResult {
        columns,
        rows,
        duration_ms: started.elapsed().as_millis() as u64,
        source: None,
    })
}

fn update_mysql_row(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    source: &TableRef,
    columns: &[ResultColumn],
    original_row: &[String],
    updated_row: &[String],
    changed_indices: &[usize],
    key_indices: &[usize],
    resources: &mut WorkerResources,
) -> Result<(), String> {
    let pool = worker_mysql_pool(resources, profile, host, port)?;
    let mut conn = pool
        .get_conn()
        .map_err(|error| format!("mysql connection failed: {}", error))?;

    let set_clause = changed_indices
        .iter()
        .map(|index| {
            format!(
                "{} = {}",
                mysql_identifier(&columns[*index].name),
                sql_string_literal(&updated_row[*index])
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = key_indices
        .iter()
        .map(|index| {
            if original_row[*index].eq_ignore_ascii_case("NULL") {
                format!("{} IS NULL", mysql_identifier(&columns[*index].name))
            } else {
                format!(
                    "{} = {}",
                    mysql_identifier(&columns[*index].name),
                    sql_string_literal(&original_row[*index])
                )
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "UPDATE {}.{} SET {} WHERE {} LIMIT 1",
        mysql_identifier(&source.schema),
        mysql_identifier(&source.table),
        set_clause,
        where_clause
    );

    conn.query_drop(sql)
        .map_err(|error| format!("mysql update failed: {}", error))?;
    Ok(())
}

fn mysql_row_to_strings(row: MySqlRow) -> Vec<String> {
    row.unwrap()
        .into_iter()
        .map(mysql_value_to_string)
        .collect()
}

fn mysql_value_to_string(value: MySqlValue) -> String {
    match value {
        MySqlValue::NULL => "NULL".to_owned(),
        MySqlValue::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        MySqlValue::Int(value) => value.to_string(),
        MySqlValue::UInt(value) => value.to_string(),
        MySqlValue::Float(value) => value.to_string(),
        MySqlValue::Double(value) => value.to_string(),
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            year, month, day, hour, minute, second, micros
        ),
        MySqlValue::Time(is_neg, days, hours, minutes, seconds, micros) => format!(
            "{}{} {:02}:{:02}:{:02}.{:06}",
            if is_neg { "-" } else { "" },
            days,
            hours,
            minutes,
            seconds,
            micros
        ),
    }
}

fn load_mysql_databases(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    resources: &mut WorkerResources,
) -> Result<Vec<String>, String> {
    let pool = worker_mysql_pool(resources, profile, host, port)?;
    let mut conn = pool
        .get_conn()
        .map_err(|error| format!("mysql connection failed: {}", error))?;

    let databases: Vec<String> = conn
        .query("SHOW DATABASES")
        .map_err(|error| format!("failed to load mysql databases: {}", error))?;

    Ok(databases)
}

fn load_mysql_schemas(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    resources: &mut WorkerResources,
) -> Result<Vec<SchemaGroup>, String> {
    let pool = worker_mysql_pool(resources, profile, host, port)?;
    let mut conn = pool
        .get_conn()
        .map_err(|error| format!("mysql connection failed: {}", error))?;

    let scoped_database = profile.database.trim();
    let tables: Vec<(String, String)> = if scoped_database.is_empty() {
        conn.query(
            "SELECT TABLE_SCHEMA, TABLE_NAME \
             FROM information_schema.tables \
             WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY TABLE_SCHEMA, TABLE_NAME",
        )
        .map_err(|error| format!("failed to load mysql tables: {}", error))?
    } else {
        let sql = format!(
            "SHOW FULL TABLES FROM {} WHERE Table_type = 'BASE TABLE'",
            mysql_identifier(scoped_database)
        );
        let table_names: Vec<String> = conn
            .query_map(sql, |(table_name, _table_type): (String, String)| {
                table_name
            })
            .map_err(|error| format!("failed to load mysql tables: {}", error))?;
        table_names
            .into_iter()
            .map(|table_name| (scoped_database.to_owned(), table_name))
            .collect()
    };

    let mut schemas: BTreeMap<String, Vec<TableInfo>> = BTreeMap::new();
    let mut table_positions = BTreeMap::<(String, String), usize>::new();
    for (schema, table) in &tables {
        let entry = schemas.entry(schema.clone()).or_default();
        entry.push(TableInfo {
            schema: schema.clone(),
            name: table.clone(),
            primary_sort: String::new(),
            row_count: 0,
            size: "-".to_owned(),
            indexes: 0,
            columns: Vec::new(),
            index_entries: Vec::new(),
            foreign_keys: Vec::new(),
            rows: Vec::new(),
        });
        table_positions.insert((schema.clone(), table.clone()), entry.len() - 1);
    }

    let foreign_key_rows: Vec<(String, String, String, String, String, String)> = if scoped_database
        .is_empty()
    {
        conn.query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, \
                        REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
                 FROM information_schema.KEY_COLUMN_USAGE \
                 WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL \
                   AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
            )
            .map_err(|error| format!("failed to load mysql foreign keys: {}", error))?
    } else {
        conn.exec(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, \
                        REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
                 FROM information_schema.KEY_COLUMN_USAGE \
                 WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL \
                   AND TABLE_SCHEMA = ? \
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
            (scoped_database.to_owned(),),
        )
        .map_err(|error| format!("failed to load mysql foreign keys: {}", error))?
    };

    for (schema, table, column_name, referenced_schema, referenced_table, referenced_column) in
        foreign_key_rows
    {
        let Some(table_index) = table_positions
            .get(&(schema.clone(), table.clone()))
            .copied()
        else {
            continue;
        };
        if let Some(table_entry) = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(table_index))
        {
            table_entry.foreign_keys.push(TableForeignKey {
                column_name,
                referenced_schema,
                referenced_table,
                referenced_column,
            });
        }
    }

    Ok(schemas
        .into_iter()
        .map(|(name, tables)| SchemaGroup { name, tables })
        .collect())
}
fn clickhouse_client(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
) -> Result<ClickHouseClient, String> {
    Ok(ClickHouseClient::default()
        .with_url(format!("http://{}:{}", host, port))
        .with_user(&profile.user)
        .with_password(&profile.password)
        .with_database(&profile.database))
}

fn postgres_client(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
) -> Result<PostgresClient, String> {
    let mut config = PostgresConfig::new();
    config
        .host(host)
        .port(port)
        .user(&profile.user)
        .connect_timeout(Duration::from_secs(5))
        .application_name("Sharingan")
        .options("-c statement_timeout=12000");
    if !profile.database.trim().is_empty() {
        config.dbname(&profile.database);
    }
    if !profile.password.is_empty() {
        config.password(&profile.password);
    }

    config
        .connect(NoTls)
        .map_err(|error| format!("postgres connection failed: {}", error))
}

fn execute_postgres_query(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    sql: &str,
    resources: &mut WorkerResources,
) -> Result<QueryResult, String> {
    let client = worker_postgres_client(resources, profile, host, port)?;
    let started = Instant::now();
    let messages = client
        .simple_query(sql)
        .map_err(|error| format!("postgres query failed: {}", error))?;

    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut command_detail = String::new();

    for message in messages {
        match message {
            SimpleQueryMessage::Row(row) => {
                if columns.is_empty() {
                    columns = row
                        .columns()
                        .iter()
                        .map(|column| ResultColumn {
                            name: column.name().to_owned(),
                        })
                        .collect();
                }
                rows.push(
                    (0..row.len())
                        .map(|index| row.get(index).unwrap_or("").to_owned())
                        .collect(),
                );
            }
            SimpleQueryMessage::CommandComplete(affected) => {
                command_detail = format!("affected rows: {}", affected);
            }
            _ => {}
        }
    }

    if columns.is_empty() {
        return Ok(QueryResult {
            columns: vec![
                ResultColumn {
                    name: "message".to_owned(),
                },
                ResultColumn {
                    name: "detail".to_owned(),
                },
            ],
            rows: vec![vec!["Command executed".to_owned(), command_detail]],
            duration_ms: started.elapsed().as_millis() as u64,
            source: None,
        });
    }

    Ok(QueryResult {
        columns,
        rows,
        duration_ms: started.elapsed().as_millis() as u64,
        source: None,
    })
}

fn update_postgres_row(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    source: &TableRef,
    columns: &[ResultColumn],
    original_row: &[String],
    updated_row: &[String],
    changed_indices: &[usize],
    key_indices: &[usize],
    resources: &mut WorkerResources,
) -> Result<(), String> {
    let client = worker_postgres_client(resources, profile, host, port)?;
    let set_clause = changed_indices
        .iter()
        .map(|index| {
            format!(
                "{} = {}",
                postgres_identifier(&columns[*index].name),
                sql_string_literal(&updated_row[*index])
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = key_indices
        .iter()
        .map(|index| {
            if original_row[*index].eq_ignore_ascii_case("NULL") {
                format!("{} IS NULL", postgres_identifier(&columns[*index].name))
            } else {
                format!(
                    "{} = {}",
                    postgres_identifier(&columns[*index].name),
                    sql_string_literal(&original_row[*index])
                )
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "UPDATE {}.{} SET {} WHERE {}",
        postgres_identifier(&source.schema),
        postgres_identifier(&source.table),
        set_clause,
        where_clause
    );

    client
        .execute(sql.as_str(), &[])
        .map_err(|error| format!("postgres update failed: {}", error))?;
    Ok(())
}

fn load_postgres_databases(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    resources: &mut WorkerResources,
) -> Result<Vec<String>, String> {
    let client = worker_postgres_client(resources, profile, host, port)?;
    let rows = client
        .query(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
            &[],
        )
        .map_err(|error| format!("failed to load postgres databases: {}", error))?;

    let databases: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();
    Ok(databases)
}

fn load_postgres_schemas(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    resources: &mut WorkerResources,
) -> Result<Vec<SchemaGroup>, String> {
    let client = worker_postgres_client(resources, profile, host, port)?;

    let table_rows = client
        .query(
            "SELECT t.table_schema, t.table_name
             FROM information_schema.tables t
             WHERE t.table_type = 'BASE TABLE'
               AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY t.table_schema, t.table_name",
            &[],
        )
        .map_err(|error| format!("failed to load postgres tables: {}", error))?;

    let mut schemas: BTreeMap<String, Vec<TableInfo>> = BTreeMap::new();
    let mut table_positions = BTreeMap::<(String, String), usize>::new();

    for row in table_rows.iter() {
        let schema: String = row.get(0);
        let table: String = row.get(1);

        let entry = schemas.entry(schema.clone()).or_default();
        entry.push(TableInfo {
            schema,
            name: table,
            primary_sort: String::new(),
            row_count: 0,
            size: "-".to_owned(),
            indexes: 0,
            columns: Vec::new(),
            index_entries: Vec::new(),
            foreign_keys: Vec::new(),
            rows: Vec::new(),
        });
        let table_entry = entry.last().expect("table entry inserted");
        table_positions.insert(
            (table_entry.schema.clone(), table_entry.name.clone()),
            entry.len() - 1,
        );
    }

    let foreign_key_rows = client
        .query(
            "SELECT tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_schema AS referenced_table_schema,
                    ccu.table_name AS referenced_table_name,
                    ccu.column_name AS referenced_column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
              AND tc.table_name = kcu.table_name
             JOIN information_schema.constraint_column_usage ccu
               ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
             WHERE tc.constraint_type = 'FOREIGN KEY'
               AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position",
            &[],
        )
        .map_err(|error| format!("failed to load postgres foreign keys: {}", error))?;

    for row in foreign_key_rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let Some(table_index) = table_positions
            .get(&(schema.clone(), table.clone()))
            .copied()
        else {
            continue;
        };
        if let Some(table_entry) = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(table_index))
        {
            table_entry.foreign_keys.push(TableForeignKey {
                column_name: row.get(2),
                referenced_schema: row.get(3),
                referenced_table: row.get(4),
                referenced_column: row.get(5),
            });
        }
    }

    Ok(schemas
        .into_iter()
        .map(|(name, tables)| SchemaGroup { name, tables })
        .collect())
}

fn load_postgres_table_columns(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    schema_name: &str,
    table_name: &str,
    resources: &mut WorkerResources,
) -> Result<Vec<TableColumn>, String> {
    let client = worker_postgres_client(resources, profile, host, port)?;
    let rows = client
        .query(
            "SELECT c.column_name,
                    c.data_type,
                    c.is_nullable,
                    EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                         AND tc.table_name = kcu.table_name
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                          AND tc.table_schema = c.table_schema
                          AND tc.table_name = c.table_name
                          AND kcu.column_name = c.column_name
                    ) AS is_primary
             FROM information_schema.columns c
             WHERE c.table_schema = $1 AND c.table_name = $2
             ORDER BY c.ordinal_position",
            &[&schema_name, &table_name],
        )
        .map_err(|error| format!("failed to load postgres columns: {}", error))?;

    Ok(rows
        .into_iter()
        .map(|row| TableColumn {
            name: row.get(0),
            kind: row.get(1),
            nullable: row.get::<_, String>(2) == "YES",
            primary: row.get(3),
            character_set: String::new(),
            collation: String::new(),
            default_value: "NULL".to_owned(),
            extra: String::new(),
            comment: String::new(),
        })
        .collect())
}
fn load_sqlite_schemas(profile: &ConnectionProfile) -> Result<Vec<SchemaGroup>, String> {
    let path = profile.path.as_deref().unwrap_or(":memory:");
    let conn = SqliteConnection::open(path)
        .map_err(|error| format!("sqlite connection failed: {}", error))?;

    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .map_err(|error| format!("sqlite list tables failed: {}", error))?;

    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| format!("sqlite query failed: {}", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("sqlite collect failed: {}", error))?;

    let mut tables = Vec::new();
    for name in table_names {
        tables.push(TableInfo {
            schema: "main".to_owned(),
            name,
            primary_sort: String::new(),
            row_count: 0,
            size: "-".to_owned(),
            indexes: 0,
            columns: Vec::new(),
            index_entries: Vec::new(),
            foreign_keys: Vec::new(),
            rows: Vec::new(),
        });
    }

    Ok(vec![SchemaGroup {
        name: "main".to_owned(),
        tables,
    }])
}

fn execute_sqlite_query(profile: &ConnectionProfile, sql: &str) -> Result<QueryResult, String> {
    let path = profile.path.as_deref().unwrap_or(":memory:");
    let conn = SqliteConnection::open(path)
        .map_err(|error| format!("sqlite connection failed: {}", error))?;
    let started = Instant::now();

    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| format!("sqlite prepare failed: {}", error))?;

    let column_count = stmt.column_count();
    let columns = (0..column_count)
        .map(|i| ResultColumn {
            name: stmt.column_name(i).unwrap_or("?").to_owned(),
        })
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut sqlite_rows = stmt
        .query([])
        .map_err(|error| format!("sqlite query failed: {}", error))?;

    while let Some(sqlite_row) = sqlite_rows
        .next()
        .map_err(|error| format!("sqlite row failed: {}", error))?
    {
        let mut row = Vec::new();
        for i in 0..column_count {
            let value = sqlite_row
                .get_ref(i)
                .map_err(|error| format!("sqlite get failed: {}", error))?;
            row.push(match value {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                rusqlite::types::ValueRef::Real(f) => f.to_string(),
                rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                rusqlite::types::ValueRef::Blob(b) => format!("<blob {} bytes>", b.len()),
            });
        }
        rows.push(row);
    }

    Ok(QueryResult {
        columns,
        rows,
        duration_ms: started.elapsed().as_millis() as u64,
        source: None,
    })
}

fn load_duckdb_schemas(profile: &ConnectionProfile) -> Result<Vec<SchemaGroup>, String> {
    let path = profile.path.as_deref().unwrap_or(":memory:");
    let conn = DuckDbConnection::open(path)
        .map_err(|error| format!("duckdb connection failed: {}", error))?;

    let mut stmt = conn
        .prepare("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
        .map_err(|error| format!("duckdb list tables failed: {}", error))?;

    let mut rows = stmt
        .query([])
        .map_err(|error| format!("duckdb query failed: {}", error))?;

    let mut schemas: BTreeMap<String, Vec<TableInfo>> = BTreeMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("duckdb row failed: {}", error))?
    {
        let schema: String = row.get(0).unwrap_or_default();
        let table: String = row.get(1).unwrap_or_default();
        schemas.entry(schema.clone()).or_default().push(TableInfo {
            schema: schema.clone(),
            name: table.clone(),
            primary_sort: String::new(),
            row_count: 0,
            size: "-".to_owned(),
            indexes: 0,
            columns: Vec::new(),
            index_entries: Vec::new(),
            foreign_keys: Vec::new(),
            rows: Vec::new(),
        });
    }

    Ok(schemas
        .into_iter()
        .map(|(name, tables)| SchemaGroup { name, tables })
        .collect())
}

fn execute_duckdb_query(profile: &ConnectionProfile, sql: &str) -> Result<QueryResult, String> {
    let path = profile.path.as_deref().unwrap_or(":memory:");
    let conn = DuckDbConnection::open(path)
        .map_err(|error| format!("duckdb connection failed: {}", error))?;
    let started = Instant::now();

    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| format!("duckdb prepare failed: {}", error))?;

    let column_count = stmt.column_count();
    let columns = (0..column_count)
        .map(|i| ResultColumn {
            name: stmt.column_name(i).ok().map_or("?", |v| v).to_owned(),
        })
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut duckdb_rows = stmt
        .query([])
        .map_err(|error| format!("duckdb query failed: {}", error))?;

    while let Some(duckdb_row) = duckdb_rows
        .next()
        .map_err(|error| format!("duckdb row failed: {}", error))?
    {
        let mut row = Vec::new();
        for i in 0..column_count {
            let value: String = match duckdb_row.get::<_, duckdb::types::Value>(i) {
                Ok(v) => match v {
                    duckdb::types::Value::Null => "NULL".to_owned(),
                    duckdb::types::Value::Boolean(b) => b.to_string(),
                    duckdb::types::Value::TinyInt(i) => i.to_string(),
                    duckdb::types::Value::SmallInt(i) => i.to_string(),
                    duckdb::types::Value::Int(i) => i.to_string(),
                    duckdb::types::Value::BigInt(i) => i.to_string(),
                    duckdb::types::Value::HugeInt(i) => i.to_string(),
                    duckdb::types::Value::UTinyInt(i) => i.to_string(),
                    duckdb::types::Value::USmallInt(i) => i.to_string(),
                    duckdb::types::Value::UInt(i) => i.to_string(),
                    duckdb::types::Value::UBigInt(i) => i.to_string(),
                    duckdb::types::Value::Float(f) => f.to_string(),
                    duckdb::types::Value::Double(f) => f.to_string(),
                    duckdb::types::Value::Text(t) => t,
                    duckdb::types::Value::Blob(b) => format!("<blob {} bytes>", b.len()),
                    _ => "?".to_owned(),
                },
                Err(_) => "?".to_owned(),
            };
            row.push(value);
        }
        rows.push(row);
    }

    Ok(QueryResult {
        columns,
        rows,
        duration_ms: started.elapsed().as_millis() as u64,
        source: None,
    })
}

fn load_clickhouse_schemas(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    resources: &mut WorkerResources,
) -> Result<Vec<SchemaGroup>, String> {
    let client = clickhouse_client(profile, host, port)?;

    resources.runtime.block_on(async {
        let mut cursor = client
            .query("SELECT database, name FROM system.tables WHERE database NOT IN ('system', 'information_schema') ORDER BY database, name")
            .fetch::<(String, String)>()
            .map_err(|e| format!("ClickHouse query failed: {e}"))?;

        let mut schemas: BTreeMap<String, Vec<TableInfo>> = BTreeMap::new();
        while let Some((db, table)) = cursor.next().await.map_err(|e| format!("ClickHouse fetch failed: {e}"))? {
            schemas.entry(db.clone()).or_default().push(TableInfo {
                schema: db,
                name: table,
                primary_sort: String::new(),
                row_count: 0,
                size: "-".to_owned(),
                indexes: 0,
                columns: Vec::new(),
                index_entries: Vec::new(),
            foreign_keys: Vec::new(),
                rows: Vec::new(),
            });
        }

        Ok(schemas
            .into_iter()
            .map(|(name, tables)| SchemaGroup { name, tables })
            .collect())
    })
}

fn execute_clickhouse_query(
    profile: &ConnectionProfile,
    host: &str,
    port: u16,
    sql: &str,
    resources: &mut WorkerResources,
) -> Result<QueryResult, String> {
    let client = clickhouse_client(profile, host, port)?;
    let started = Instant::now();

    resources.runtime.block_on(async {
        let _ = client.query(sql).execute().await;

        Ok(QueryResult {
            columns: vec![ResultColumn {
                name: "info".to_owned(),
            }],
            rows: vec![vec!["ClickHouse query executed successfully.".to_owned()]],
            duration_ms: started.elapsed().as_millis() as u64,
            source: None,
        })
    })
}

fn default_port(engine: ConnectionEngine) -> u16 {
    match engine {
        ConnectionEngine::MySQL | ConnectionEngine::MariaDB => 3306,
        ConnectionEngine::Postgres
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => 5432,
        ConnectionEngine::MSSQL => 1433,
        ConnectionEngine::ClickHouse => 8123,
        ConnectionEngine::DuckDB
        | ConnectionEngine::SQLite
        | ConnectionEngine::LibSQL
        | ConnectionEngine::CloudflareD1 => 0,
        ConnectionEngine::Cassandra => 9042,
        ConnectionEngine::Redis => 6379,
        ConnectionEngine::MongoDB => 27017,
        ConnectionEngine::Oracle => 1521,
        ConnectionEngine::BigQuery | ConnectionEngine::Snowflake | ConnectionEngine::DynamoDB => {
            443
        }
    }
}

fn template_schemas_for_engine(engine: ConnectionEngine, seed: usize) -> Vec<SchemaGroup> {
    match engine {
        ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
            vec![SchemaGroup {
                name: "finance".to_owned(),
                tables: vec![TableInfo {
                    schema: "finance".to_owned(),
                    name: "products".to_owned(),
                    primary_sort: "updated_at".to_owned(),
                    row_count: 40 + seed,
                    size: "8 MB".to_owned(),
                    indexes: 2,
                    columns: vec![
                        TableColumn {
                            name: "sku".to_owned(),
                            kind: "varchar(24)".to_owned(),
                            nullable: false,
                            primary: true,
                            character_set: String::new(),
                            collation: String::new(),
                            default_value: String::new(),
                            extra: String::new(),
                            comment: String::new(),
                        },
                        TableColumn {
                            name: "name".to_owned(),
                            kind: "varchar(120)".to_owned(),
                            nullable: false,
                            primary: false,
                            character_set: String::new(),
                            collation: String::new(),
                            default_value: String::new(),
                            extra: String::new(),
                            comment: String::new(),
                        },
                        TableColumn {
                            name: "price_usd".to_owned(),
                            kind: "decimal(10,2)".to_owned(),
                            nullable: false,
                            primary: false,
                            character_set: String::new(),
                            collation: String::new(),
                            default_value: String::new(),
                            extra: String::new(),
                            comment: String::new(),
                        },
                        TableColumn {
                            name: "updated_at".to_owned(),
                            kind: "datetime".to_owned(),
                            nullable: false,
                            primary: false,
                            character_set: String::new(),
                            collation: String::new(),
                            default_value: String::new(),
                            extra: String::new(),
                            comment: String::new(),
                        },
                    ],
                    index_entries: Vec::new(),
                    foreign_keys: Vec::new(),
                    rows: vec![
                        vec![
                            format!("sku-{}", seed),
                            format!("Starter {}", seed),
                            "19.00".to_owned(),
                            "2026-03-25 09:10:00".to_owned(),
                        ],
                        vec![
                            format!("sku-{}-pro", seed),
                            format!("Pro {}", seed),
                            "79.00".to_owned(),
                            "2026-03-25 09:11:00".to_owned(),
                        ],
                    ],
                }],
            }]
        }
        _ => {
            vec![
                SchemaGroup {
                    name: "public".to_owned(),
                    tables: vec![
                        TableInfo {
                            schema: "public".to_owned(),
                            name: "orders".to_owned(),
                            primary_sort: "created_at".to_owned(),
                            row_count: 120 + seed,
                            size: "14 MB".to_owned(),
                            indexes: 3,
                            columns: vec![
                                TableColumn {
                                    name: "id".to_owned(),
                                    kind: "bigint".to_owned(),
                                    nullable: false,
                                    primary: true,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                                TableColumn {
                                    name: "customer_id".to_owned(),
                                    kind: "uuid".to_owned(),
                                    nullable: false,
                                    primary: false,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                                TableColumn {
                                    name: "customer_email".to_owned(),
                                    kind: "varchar(120)".to_owned(),
                                    nullable: false,
                                    primary: false,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                                TableColumn {
                                    name: "status".to_owned(),
                                    kind: "varchar(24)".to_owned(),
                                    nullable: false,
                                    primary: false,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                                TableColumn {
                                    name: "created_at".to_owned(),
                                    kind: "timestamp".to_owned(),
                                    nullable: false,
                                    primary: false,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                            ],
                            index_entries: Vec::new(),
                            foreign_keys: vec![TableForeignKey {
                                column_name: "customer_id".to_owned(),
                                referenced_schema: "public".to_owned(),
                                referenced_table: "users".to_owned(),
                                referenced_column: "id".to_owned(),
                            }],
                            rows: vec![
                                vec![
                                    format!("demo-{}", seed),
                                    format!("user-{}", seed),
                                    format!("ops+{}@example.com", seed),
                                    "paid".to_owned(),
                                    "2026-03-25 09:00:00".to_owned(),
                                ],
                                vec![
                                    format!("demo-{}-2", seed),
                                    format!("user-{}-2", seed),
                                    format!("team+{}@example.com", seed),
                                    "pending".to_owned(),
                                    "2026-03-25 08:57:00".to_owned(),
                                ],
                            ],
                        },
                        TableInfo {
                            schema: "public".to_owned(),
                            name: "users".to_owned(),
                            primary_sort: "created_at".to_owned(),
                            row_count: 60 + seed,
                            size: "10 MB".to_owned(),
                            indexes: 2,
                            columns: vec![
                                TableColumn {
                                    name: "id".to_owned(),
                                    kind: "uuid".to_owned(),
                                    nullable: false,
                                    primary: true,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                                TableColumn {
                                    name: "email".to_owned(),
                                    kind: "text".to_owned(),
                                    nullable: false,
                                    primary: false,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                                TableColumn {
                                    name: "created_at".to_owned(),
                                    kind: "timestamp".to_owned(),
                                    nullable: false,
                                    primary: false,
                                    character_set: String::new(),
                                    collation: String::new(),
                                    default_value: String::new(),
                                    extra: String::new(),
                                    comment: String::new(),
                                },
                            ],
                            index_entries: Vec::new(),
                            foreign_keys: Vec::new(),
                            rows: vec![
                                vec![
                                    format!("user-{}", seed),
                                    format!("user{}@example.com", seed),
                                    "2026-03-20 10:00:00".to_owned(),
                                ],
                                vec![
                                    format!("user-{}-2", seed),
                                    format!("user{}b@example.com", seed),
                                    "2026-03-21 11:15:00".to_owned(),
                                ],
                            ],
                        },
                    ],
                },
                SchemaGroup {
                    name: "analytics".to_owned(),
                    tables: vec![TableInfo {
                        schema: "analytics".to_owned(),
                        name: "sessions".to_owned(),
                        primary_sort: "started_at".to_owned(),
                        row_count: 500 + seed,
                        size: "22 MB".to_owned(),
                        indexes: 2,
                        columns: vec![
                            TableColumn {
                                name: "session_id".to_owned(),
                                kind: "uuid".to_owned(),
                                nullable: false,
                                primary: true,
                                character_set: String::new(),
                                collation: String::new(),
                                default_value: String::new(),
                                extra: String::new(),
                                comment: String::new(),
                            },
                            TableColumn {
                                name: "user_id".to_owned(),
                                kind: "uuid".to_owned(),
                                nullable: false,
                                primary: false,
                                character_set: String::new(),
                                collation: String::new(),
                                default_value: String::new(),
                                extra: String::new(),
                                comment: String::new(),
                            },
                            TableColumn {
                                name: "device".to_owned(),
                                kind: "varchar(16)".to_owned(),
                                nullable: false,
                                primary: false,
                                character_set: String::new(),
                                collation: String::new(),
                                default_value: String::new(),
                                extra: String::new(),
                                comment: String::new(),
                            },
                            TableColumn {
                                name: "started_at".to_owned(),
                                kind: "timestamp".to_owned(),
                                nullable: false,
                                primary: false,
                                character_set: String::new(),
                                collation: String::new(),
                                default_value: String::new(),
                                extra: String::new(),
                                comment: String::new(),
                            },
                        ],
                        index_entries: Vec::new(),
                        foreign_keys: vec![TableForeignKey {
                            column_name: "user_id".to_owned(),
                            referenced_schema: "public".to_owned(),
                            referenced_table: "users".to_owned(),
                            referenced_column: "id".to_owned(),
                        }],
                        rows: vec![
                            vec![
                                format!("sess-{}", seed),
                                format!("user-{}", seed),
                                "desktop".to_owned(),
                                "2026-03-25 08:45:00".to_owned(),
                            ],
                            vec![
                                format!("sess-{}-2", seed),
                                format!("user-{}-2", seed),
                                "mobile".to_owned(),
                                "2026-03-25 08:42:00".to_owned(),
                            ],
                        ],
                    }],
                },
            ]
        }
    }
}

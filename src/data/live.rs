fn load_live_schemas(
    profile: &ConnectionProfile,
    resources: &mut WorkerResources,
) -> Result<Vec<SchemaGroup>, String> {
    match profile.engine {
        ConnectionEngine::MySQL
        | ConnectionEngine::MariaDB
        | ConnectionEngine::Postgres
        | ConnectionEngine::ClickHouse
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => {
            with_live_endpoint(profile, resources, |host, port, resources| {
                match profile.engine {
                    ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
                        load_mysql_schemas(profile, host, port, resources)
                    }
                    ConnectionEngine::Postgres
                    | ConnectionEngine::Redshift
                    | ConnectionEngine::CockroachDB
                    | ConnectionEngine::Greenplum
                    | ConnectionEngine::Vertica => {
                        load_postgres_schemas(profile, host, port, resources)
                    }
                    ConnectionEngine::ClickHouse => {
                        load_clickhouse_schemas(profile, host, port, resources)
                    }
                    _ => unreachable!(),
                }
            })
        }
        ConnectionEngine::DuckDB => load_duckdb_schemas(profile),
        ConnectionEngine::SQLite | ConnectionEngine::LibSQL | ConnectionEngine::CloudflareD1 => {
            load_sqlite_schemas(profile)
        }
        _ => Err(format!(
            "Schema loading for {} is not yet implemented.",
            profile.engine
        )),
    }
}

fn load_live_databases(
    profile: &ConnectionProfile,
    resources: &mut WorkerResources,
) -> Result<Vec<String>, String> {
    match profile.engine {
        ConnectionEngine::MySQL
        | ConnectionEngine::MariaDB
        | ConnectionEngine::Postgres
        | ConnectionEngine::ClickHouse
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => {
            with_live_endpoint(profile, resources, |host, port, resources| {
                match profile.engine {
                    ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
                        load_mysql_databases(profile, host, port, resources)
                    }
                    ConnectionEngine::Postgres
                    | ConnectionEngine::Redshift
                    | ConnectionEngine::CockroachDB
                    | ConnectionEngine::Greenplum
                    | ConnectionEngine::Vertica => {
                        load_postgres_databases(profile, host, port, resources)
                    }
                    ConnectionEngine::ClickHouse => Ok(vec!["default".to_owned()]),
                    _ => unreachable!(),
                }
            })
        }
        ConnectionEngine::DuckDB
        | ConnectionEngine::SQLite
        | ConnectionEngine::LibSQL
        | ConnectionEngine::CloudflareD1 => Ok(vec!["main".to_owned()]),
        _ => Ok(vec!["default".to_owned()]),
    }
}

fn execute_live_query(
    profile: &ConnectionProfile,
    sql: &str,
    resources: &mut WorkerResources,
) -> Result<QueryResult, String> {
    let mut result = match profile.engine {
        ConnectionEngine::MySQL
        | ConnectionEngine::MariaDB
        | ConnectionEngine::Postgres
        | ConnectionEngine::ClickHouse
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => {
            with_live_endpoint(profile, resources, |host, port, resources| {
                match profile.engine {
                    ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
                        execute_mysql_query(profile, host, port, sql, resources)
                    }
                    ConnectionEngine::Postgres
                    | ConnectionEngine::Redshift
                    | ConnectionEngine::CockroachDB
                    | ConnectionEngine::Greenplum
                    | ConnectionEngine::Vertica => {
                        execute_postgres_query(profile, host, port, sql, resources)
                    }
                    ConnectionEngine::ClickHouse => {
                        execute_clickhouse_query(profile, host, port, sql, resources)
                    }
                    _ => unreachable!(),
                }
            })
        }
        ConnectionEngine::DuckDB => execute_duckdb_query(profile, sql),
        ConnectionEngine::SQLite | ConnectionEngine::LibSQL | ConnectionEngine::CloudflareD1 => {
            execute_sqlite_query(profile, sql)
        }
        _ => Err(format!(
            "Query execution for {} is not yet implemented.",
            profile.engine
        )),
    }?;

    if result.source.is_none() {
        if let Some(table_ref) = infer_single_table_ref(profile, sql) {
            result.source = Some(table_ref);
        }
    }

    Ok(result)
}

fn infer_single_table_ref(profile: &ConnectionProfile, sql: &str) -> Option<TableRef> {
    if profile.schemas.is_empty() {
        return None;
    }

    let normalized = sql
        .replace('\n', " ")
        .replace('\r', " ")
        .replace('\t', " ")
        .trim()
        .to_owned();
    if normalized.is_empty() {
        return None;
    }

    let lower_normalized = normalized.to_lowercase();
    let trimmed_lower = lower_normalized.trim_start();
    if !trimmed_lower.starts_with("select ") {
        return None;
    }
    if trimmed_lower.starts_with("with ") {
        return None;
    }

    if lower_normalized.contains(" join ")
        || lower_normalized.contains(" join\t")
        || lower_normalized.contains(" join\n")
        || lower_normalized.contains(" join\r")
    {
        return None;
    }

    let (fragment, fragment_lower) = extract_table_fragment(&normalized)?;
    let fragment_lower = fragment_lower.to_lowercase();
    if fragment_lower.contains(',') || fragment_lower.contains("select") {
        return None;
    }
    if fragment_lower
        .split_whitespace()
        .any(|token| token == "join")
    {
        return None;
    }

    let first_token = fragment
        .split_whitespace()
        .next()?
        .trim_end_matches(|c: char| c == ',' || c == ';')
        .trim();
    if first_token.is_empty() || first_token.starts_with('(') {
        return None;
    }

    let segments = first_token
        .split('.')
        .map(|segment| segment.trim_matches(|c: char| matches!(c, '\"' | '\'' | '`' | '[' | ']')))
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return None;
    }

    let table_hint = segments.last().unwrap().trim();
    let schema_hint = if segments.len() >= 2 {
        Some(segments[segments.len() - 2].trim())
    } else {
        None
    };

    find_matching_table_ref(profile, schema_hint, table_hint)
}

fn extract_table_fragment(sql: &str) -> Option<(&str, String)> {
    let lower = sql.to_lowercase();
    let from_marker = lower.find(" from ")?;
    let after = &sql[from_marker + 6..];
    let after_lower = &lower[from_marker + 6..];

    let mut end = after.len();
    for term in [
        " where ",
        " group by ",
        " order by ",
        " limit ",
        " offset ",
        " having ",
        " fetch ",
        " union ",
        " intersect ",
        " except ",
        " returning ",
    ] {
        if let Some(pos) = after_lower.find(term) {
            end = end.min(pos);
        }
    }
    if let Some(pos) = after_lower.find(';') {
        end = end.min(pos);
    }

    if end == 0 {
        return None;
    }

    let fragment = after[..end].trim();
    Some((fragment, after_lower[..end].to_string()))
}

fn find_matching_table_ref(
    profile: &ConnectionProfile,
    schema_hint: Option<&str>,
    table_hint: &str,
) -> Option<TableRef> {
    if table_hint.trim().is_empty() {
        return None;
    }

    if let Some(schema_hint) = schema_hint {
        if let Some(schema) = profile
            .schemas
            .iter()
            .find(|schema| schema.name.eq_ignore_ascii_case(schema_hint))
        {
            if let Some(table) = schema
                .tables
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case(table_hint))
            {
                return Some(TableRef {
                    schema: schema.name.clone(),
                    table: table.name.clone(),
                });
            }
        }
    }

    for schema in &profile.schemas {
        if let Some(table) = schema
            .tables
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case(table_hint))
        {
            return Some(TableRef {
                schema: schema.name.clone(),
                table: table.name.clone(),
            });
        }
    }

    None
}

fn preview_live_table(
    profile: &ConnectionProfile,
    table: &TableInfo,
    row_limit: Option<usize>,
    resources: &mut WorkerResources,
) -> Result<(TableInfo, QueryResult), String> {
    let columns = match profile.engine {
        ConnectionEngine::MySQL
        | ConnectionEngine::MariaDB
        | ConnectionEngine::Postgres
        | ConnectionEngine::ClickHouse
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => {
            with_live_endpoint(profile, resources, |host, port, resources| {
                match profile.engine {
                    ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
                        load_mysql_table_columns(
                            profile,
                            host,
                            port,
                            &table.schema,
                            &table.name,
                            resources,
                        )
                    }
                    ConnectionEngine::Postgres
                    | ConnectionEngine::Redshift
                    | ConnectionEngine::CockroachDB
                    | ConnectionEngine::Greenplum
                    | ConnectionEngine::Vertica => load_postgres_table_columns(
                        profile,
                        host,
                        port,
                        &table.schema,
                        &table.name,
                        resources,
                    ),
                    ConnectionEngine::ClickHouse => load_clickhouse_table_columns(
                        profile,
                        host,
                        port,
                        &table.schema,
                        &table.name,
                        resources,
                    ),
                    _ => unreachable!(),
                }
            })?
        }
        ConnectionEngine::DuckDB => load_duckdb_table_columns(profile, &table.schema, &table.name)?,
        ConnectionEngine::SQLite | ConnectionEngine::LibSQL | ConnectionEngine::CloudflareD1 => {
            load_sqlite_table_columns(profile, &table.name)?
        }
        _ => {
            return Err(format!(
                "Table preview for {} is not yet implemented.",
                profile.engine
            ));
        }
    };

    let index_entries = match profile.engine {
        ConnectionEngine::MySQL | ConnectionEngine::MariaDB => {
            with_live_endpoint(profile, resources, |host, port, resources| {
                load_mysql_table_indexes(profile, host, port, &table.schema, &table.name, resources)
            })?
        }
        ConnectionEngine::Postgres
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => {
            with_live_endpoint(profile, resources, |host, port, resources| {
                load_postgres_table_indexes(
                    profile,
                    host,
                    port,
                    &table.schema,
                    &table.name,
                    resources,
                )
            })?
        }
        _ => Vec::new(),
    };

    let mut detailed_table = table.clone();
    detailed_table.columns = columns;
    detailed_table.indexes = index_entries
        .iter()
        .map(|entry| entry.index_name.as_str())
        .collect::<HashSet<_>>()
        .len();
    detailed_table.index_entries = index_entries;
    detailed_table.primary_sort = primary_sort_column(&detailed_table.columns);

    let sql = preview_table_sql(profile, &detailed_table, row_limit);
    let mut result = execute_live_query(profile, &sql, resources)?;
    result.source = Some(TableRef {
        schema: detailed_table.schema.clone(),
        table: detailed_table.name.clone(),
    });
    Ok((detailed_table, result))
}

fn update_live_row(
    profile: &ConnectionProfile,
    source: &TableRef,
    columns: &[ResultColumn],
    original_row: &[String],
    updated_row: &[String],
    resources: &mut WorkerResources,
) -> Result<(), String> {
    let changed_indices = changed_column_indices(original_row, updated_row);
    if changed_indices.is_empty() {
        return Ok(());
    }

    let key_indices = row_identity_indices(columns, original_row);
    if key_indices.is_empty() {
        return Err("could not determine a stable key column for this row".to_owned());
    }

    with_live_endpoint(profile, resources, |host, port, resources| {
        if profile.engine == ConnectionEngine::MySQL {
            update_mysql_row(
                profile,
                host,
                port,
                source,
                columns,
                original_row,
                updated_row,
                &changed_indices,
                &key_indices,
                resources,
            )
        } else if profile.engine == ConnectionEngine::Postgres {
            update_postgres_row(
                profile,
                host,
                port,
                source,
                columns,
                original_row,
                updated_row,
                &changed_indices,
                &key_indices,
                resources,
            )
        } else {
            Err(format!("Unsupported engine {}", profile.engine))
        }
    })
}

fn changed_column_indices(original_row: &[String], updated_row: &[String]) -> Vec<usize> {
    original_row
        .iter()
        .zip(updated_row.iter())
        .enumerate()
        .filter_map(|(index, (left, right))| (left != right).then_some(index))
        .collect()
}

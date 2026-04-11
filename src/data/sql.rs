fn row_identity_indices(columns: &[ResultColumn], row: &[String]) -> Vec<usize> {
    let preferred = ["id"];
    for name in preferred {
        if let Some(index) = columns.iter().enumerate().find_map(|(index, column)| {
            (column.name.eq_ignore_ascii_case(name)
                && !row
                    .get(index)
                    .map(|value| value.eq_ignore_ascii_case("NULL"))
                    .unwrap_or(true))
            .then_some(index)
        }) {
            return vec![index];
        }
    }

    let foreign_keys = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            (column.name.ends_with("_id")
                && !row
                    .get(index)
                    .map(|value| value.eq_ignore_ascii_case("NULL"))
                    .unwrap_or(true))
            .then_some(index)
        })
        .collect::<Vec<_>>();
    if !foreign_keys.is_empty() {
        return foreign_keys;
    }

    columns
        .iter()
        .enumerate()
        .find_map(|(index, _)| {
            (!row
                .get(index)
                .map(|value| value.eq_ignore_ascii_case("NULL"))
                .unwrap_or(true))
            .then_some(vec![index])
        })
        .unwrap_or_default()
}

fn format_update_sql(
    profile: &ConnectionProfile,
    source: &TableRef,
    columns: &[ResultColumn],
    original_row: &[String],
    updated_row: &[String],
    changed_indices: &[usize],
    key_indices: &[usize],
) -> Option<String> {
    if changed_indices.is_empty() || key_indices.is_empty() {
        return None;
    }

    let identifier = identifier_for_engine(profile.engine);
    let target = if source.schema.trim().is_empty() {
        identifier(&source.table)
    } else {
        format!(
            "{}.{}",
            identifier(&source.schema),
            identifier(&source.table)
        )
    };

    let set_clause = changed_indices
        .iter()
        .map(|index| {
            format!(
                "{} = {}",
                identifier(&columns[*index].name),
                sql_string_literal(&updated_row[*index])
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = key_indices
        .iter()
        .map(|index| {
            let column = identifier(&columns[*index].name);
            let value = &original_row[*index];
            if value.eq_ignore_ascii_case("NULL") {
                format!("{} IS NULL", column)
            } else {
                format!("{} = {}", column, sql_string_literal(value))
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    let limit_clause = if matches!(
        profile.engine,
        ConnectionEngine::MySQL | ConnectionEngine::MariaDB
    ) {
        " LIMIT 1"
    } else {
        ""
    };

    Some(format!(
        "UPDATE {target} SET {set_clause} WHERE {where_clause}{limit_clause};"
    ))
}

fn identifier_for_engine(engine: ConnectionEngine) -> fn(&str) -> String {
    match engine {
        ConnectionEngine::MySQL | ConnectionEngine::MariaDB => mysql_identifier,
        ConnectionEngine::Postgres
        | ConnectionEngine::Redshift
        | ConnectionEngine::CockroachDB
        | ConnectionEngine::Greenplum
        | ConnectionEngine::Vertica => postgres_identifier,
        _ => identity_identifier,
    }
}

fn identity_identifier(value: &str) -> String {
    value.to_owned()
}

fn primary_sort_column(columns: &[TableColumn]) -> String {
    columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case("id"))
        .or_else(|| {
            columns
                .iter()
                .find(|column| column.name.contains("updated") || column.name.contains("created"))
        })
        .or_else(|| columns.iter().find(|column| column.name.ends_with("_id")))
        .or_else(|| columns.first())
        .map(|column| column.name.clone())
        .unwrap_or_default()
}

fn preview_table_sql(
    profile: &ConnectionProfile,
    table: &TableInfo,
    row_limit: Option<usize>,
) -> String {
    let (schema, table_name, sort_column) = match profile.engine {
        ConnectionEngine::MySQL
        | ConnectionEngine::MariaDB
        | ConnectionEngine::ClickHouse
        | ConnectionEngine::BigQuery
        | ConnectionEngine::Snowflake => (
            mysql_identifier(&table.schema),
            mysql_identifier(&table.name),
            mysql_identifier(&table.primary_sort),
        ),
        _ => (
            postgres_identifier(&table.schema),
            postgres_identifier(&table.name),
            postgres_identifier(&table.primary_sort),
        ),
    };

    if table.primary_sort.is_empty() {
        format!(
            "SELECT * FROM {}.{}{}",
            schema,
            table_name,
            limit_clause_inline(row_limit)
        )
    } else {
        format!(
            "SELECT * FROM {}.{} ORDER BY {} ASC{}",
            schema,
            table_name,
            sort_column,
            limit_clause_inline(row_limit)
        )
    }
}

fn sql_string_literal(value: &str) -> String {
    if value.eq_ignore_ascii_case("NULL") {
        "NULL".to_owned()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

fn mysql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn postgres_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

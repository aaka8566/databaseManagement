#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
enum ConnectionEngine {
    #[default]
    MySQL,
    Postgres,
    ClickHouse,
    DuckDB,
    SQLite,
    MariaDB,
    MSSQL,
    Redshift,
    BigQuery,
    Cassandra,
    DynamoDB,
    LibSQL,
    CloudflareD1,
    MongoDB,
    Snowflake,
    Redis,
    Oracle,
    CockroachDB,
    Greenplum,
    Vertica,
}

impl std::fmt::Display for ConnectionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MySQL => write!(f, "MySQL"),
            Self::Postgres => write!(f, "PostgreSQL"),
            Self::ClickHouse => write!(f, "ClickHouse"),
            Self::DuckDB => write!(f, "DuckDB"),
            Self::SQLite => write!(f, "SQLite"),
            Self::MariaDB => write!(f, "MariaDB"),
            Self::MSSQL => write!(f, "SQL Server"),
            Self::Redshift => write!(f, "Redshift"),
            Self::BigQuery => write!(f, "BigQuery"),
            Self::Cassandra => write!(f, "Cassandra"),
            Self::DynamoDB => write!(f, "DynamoDB"),
            Self::LibSQL => write!(f, "LibSQL"),
            Self::CloudflareD1 => write!(f, "Cloudflare D1"),
            Self::MongoDB => write!(f, "MongoDB"),
            Self::Snowflake => write!(f, "Snowflake"),
            Self::Redis => write!(f, "Redis"),
            Self::Oracle => write!(f, "Oracle"),
            Self::CockroachDB => write!(f, "CockroachDB"),
            Self::Greenplum => write!(f, "Greenplum"),
            Self::Vertica => write!(f, "Vertica"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ConnectionProfile {
    name: String,
    #[serde(default = "default_connection_source")]
    source: ConnectionSource,
    engine: ConnectionEngine,
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    path: Option<String>,
    ssh_tunnel: Option<SshTunnelProfile>,
    schemas: Vec<SchemaGroup>,
    #[serde(skip, default = "default_disconnected")]
    is_disconnected: bool,
}

fn default_disconnected() -> bool {
    true
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum ConnectionSource {
    Demo,
    Live,
}

fn default_connection_source() -> ConnectionSource {
    ConnectionSource::Live
}

impl ConnectionProfile {
    fn execute_mock_query(&self, sql: &str) -> QueryResult {
        let sql_lower = sql.to_lowercase();
        let mut matched: Option<&TableInfo> = None;

        for schema in &self.schemas {
            for table in &schema.tables {
                let bare = table.name.to_lowercase();
                let full = format!(
                    "{}.{}",
                    table.schema.to_lowercase(),
                    table.name.to_lowercase()
                );
                if sql_lower.contains(&full) || sql_lower.contains(&bare) {
                    matched = Some(table);
                    break;
                }
            }
        }

        if let Some(table) = matched {
            let mut result = table.preview_result_with_limit(Some(DEFAULT_TABLE_PREVIEW_LIMIT));
            result.duration_ms = 11;
            return result;
        }

        QueryResult {
            columns: vec![
                ResultColumn {
                    name: "message".to_owned(),
                },
                ResultColumn {
                    name: "hint".to_owned(),
                },
            ],
            rows: vec![vec![
                "No table matched that query.".to_owned(),
                "Try orders, users, sessions, invoices, or products.".to_owned(),
            ]],
            duration_ms: 7,
            source: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct SshTunnelProfile {
    host: String,
    port: u16,
    user: String,
    password: String,
    private_key_path: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct SchemaGroup {
    name: String,
    tables: Vec<TableInfo>,
}

#[derive(Clone, Serialize, Deserialize)]
struct TableInfo {
    schema: String,
    name: String,
    primary_sort: String,
    row_count: usize,
    size: String,
    indexes: usize,
    columns: Vec<TableColumn>,
    #[serde(default)]
    index_entries: Vec<TableIndexEntry>,
    #[serde(default)]
    foreign_keys: Vec<TableForeignKey>,
    rows: Vec<Vec<String>>,
}

impl TableInfo {
    fn preview_result_with_limit(&self, row_limit: Option<usize>) -> QueryResult {
        let rows = match row_limit {
            Some(limit) => self.rows.iter().take(limit).cloned().collect(),
            None => self.rows.clone(),
        };
        QueryResult {
            columns: self
                .columns
                .iter()
                .map(|column| ResultColumn {
                    name: column.name.clone(),
                })
                .collect(),
            rows,
            duration_ms: 12,
            source: Some(TableRef {
                schema: self.schema.clone(),
                table: self.name.clone(),
            }),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TableColumn {
    name: String,
    kind: String,
    nullable: bool,
    primary: bool,
    character_set: String,
    collation: String,
    default_value: String,
    extra: String,
    comment: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct TableForeignKey {
    column_name: String,
    referenced_schema: String,
    referenced_table: String,
    referenced_column: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct TableIndexEntry {
    index_name: String,
    index_algorithm: String,
    is_unique: bool,
    column_name: String,
}

const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "AS",
    "ORDER BY",
    "GROUP BY",
    "LIMIT",
    "JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "INSERT INTO",
    "UPDATE",
    "DELETE",
    "VALUES",
    "SET",
    "LIKE",
    "IN",
    "IS NULL",
    "IS NOT NULL",
    "COUNT",
    "SUM",
    "AVG",
];

const ENGINE_OPTIONS: &[ConnectionEngine] = &[
    ConnectionEngine::Postgres,
    ConnectionEngine::MySQL,
    ConnectionEngine::MariaDB,
    ConnectionEngine::MSSQL,
    ConnectionEngine::ClickHouse,
    ConnectionEngine::DuckDB,
    ConnectionEngine::SQLite,
    ConnectionEngine::Redshift,
    ConnectionEngine::BigQuery,
    ConnectionEngine::Cassandra,
    ConnectionEngine::DynamoDB,
    ConnectionEngine::LibSQL,
    ConnectionEngine::CloudflareD1,
    ConnectionEngine::MongoDB,
    ConnectionEngine::Snowflake,
    ConnectionEngine::Redis,
    ConnectionEngine::Oracle,
    ConnectionEngine::CockroachDB,
    ConnectionEngine::Greenplum,
    ConnectionEngine::Vertica,
];
const CONNECTIONS_FILE: &str = "sharingan_connections.json";
const LEGACY_CONNECTIONS_FILE: &str = "mangabase_connections.json";
const RESULT_COLUMNS_PER_PAGE: usize = 10;
const RESULT_COLUMN_SCROLL_STEP: usize = 4;
const MAX_SQL_AUTOCOMPLETE_BYTES: usize = 16_000;
const ROW_INSPECTOR_CARD_LIMIT: usize = 20;
const DEFAULT_TABLE_PREVIEW_LIMIT: usize = 500;
const SCHEMA_DIAGRAM_COLUMN_PREVIEW: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
struct TableSelection {
    schema_index: usize,
    table_index: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TableDetailView {
    Data,
    Structure,
}

#[derive(Clone)]
struct QueryTab {
    id: usize,
    title: String,
    sql: String,
    result: QueryResult,
    autocomplete_index: usize,
    column_page: usize,
    last_access_seq: u64,
    filter_mode: ResultFilterMode,
    draft_filter_rules: Vec<ResultFilterRule>,
    applied_filter_rules: Vec<ResultFilterRule>,
    filter_raw_sql: String,
    table_detail_view: TableDetailView,
    structure_filter: String,
    structure_selected_row: Option<usize>,
    filtered_row_cache_key: Option<FilteredRowCacheKey>,
    filtered_row_indices: Vec<usize>,
    kind: TabKind,
}

#[derive(Clone)]
struct ConnectionWorkspace {
    connection_index: usize,
    selected_table: TableSelection,
    query_tabs: Vec<QueryTab>,
    active_tab: usize,
    selected_result_cell: Option<CellSelection>,
    editing_cell: Option<CellEditState>,
    result_grid_has_focus: bool,
    row_inspector_filter: String,
    row_inspector: Option<RowInspectorState>,
    row_inspector_expanded: bool,
    schema_filter: String,
    table_preview_limit_input: String,
}

#[derive(Clone)]
struct SchemaDiagramNode {
    schema_index: usize,
    table_index: usize,
    schema: String,
    name: String,
    columns: Vec<TableColumn>,
    foreign_keys: Vec<TableForeignKey>,
}

impl SchemaDiagramNode {
    fn from_table(schema_index: usize, table_index: usize, table: &TableInfo) -> Self {
        Self {
            schema_index,
            table_index,
            schema: table.schema.clone(),
            name: table.name.clone(),
            columns: table.columns.clone(),
            foreign_keys: table.foreign_keys.clone(),
        }
    }
}

struct SchemaDiagramEdge {
    from_index: usize,
    to_index: usize,
    label: String,
}

struct SchemaDiagramLayout {
    canvas_size: Vec2,
    node_rects: Vec<egui::Rect>,
    schema_groups: Vec<SchemaDiagramSchemaGroupLayout>,
}

struct SchemaDiagramSchemaGroupLayout {
    name: String,
    rect: egui::Rect,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FilteredRowCacheKey {
    rows_ptr: usize,
    row_count: usize,
    column_count: usize,
    filter_hash: u64,
}

#[derive(Clone, Hash)]
struct ResultFilterRule {
    column_search: String,
    column: Option<usize>,
    column_picker_highlight: usize,
    operator: ResultFilterOperator,
    operator_picker_highlight: usize,
    value: String,
}

impl Default for ResultFilterRule {
    fn default() -> Self {
        Self {
            column_search: String::new(),
            column: None,
            column_picker_highlight: 0,
            operator: ResultFilterOperator::Equals,
            operator_picker_highlight: 0,
            value: String::new(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ResultFilterMode {
    Column,
    RawSql,
}

impl ResultFilterMode {
    fn label(self) -> &'static str {
        match self {
            Self::Column => "Column Filter",
            Self::RawSql => "Raw SQL",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ResultFilterOperator {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    In,
    NotIn,
    IsNull,
    IsNotNull,
    Between,
    NotBetween,
    Like,
    Contains,
    NotContains,
    ContainsCaseSensitive,
    NotContainsCaseSensitive,
    HasPrefix,
    HasSuffix,
    HasPrefixCaseSensitive,
    HasSuffixCaseSensitive,
}

impl ResultFilterOperator {
    fn label(self) -> &'static str {
        match self {
            Self::Equals => "=",
            Self::NotEquals => "<>",
            Self::LessThan => "<",
            Self::GreaterThan => ">",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThanOrEqual => ">=",
            Self::In => "IN",
            Self::NotIn => "NOT IN",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
            Self::Between => "BETWEEN",
            Self::NotBetween => "NOT BETWEEN",
            Self::Like => "LIKE",
            Self::Contains => "contains",
            Self::NotContains => "not contains",
            Self::ContainsCaseSensitive => "contains - case sensitive",
            Self::NotContainsCaseSensitive => "not contains - case sensitive",
            Self::HasPrefix => "has prefix",
            Self::HasSuffix => "has suffix",
            Self::HasPrefixCaseSensitive => "has prefix - case sensitive",
            Self::HasSuffixCaseSensitive => "has suffix - case sensitive",
        }
    }

    fn requires_value(self) -> bool {
        match self {
            Self::IsNull | Self::IsNotNull => false,
            _ => true,
        }
    }

    fn matches(self, value: &str, needle: &str) -> bool {
        let normalized_value = normalized_filter_value(value);
        let normalized_needle = normalized_filter_value(needle);
        let lower_value = normalized_value.to_lowercase();
        let lower_needle = normalized_needle.to_lowercase();

        match self {
            Self::Equals => lower_value == lower_needle,
            Self::NotEquals => lower_value != lower_needle,
            Self::LessThan => compare_filter_values(normalized_value, normalized_needle).is_lt(),
            Self::GreaterThan => compare_filter_values(normalized_value, normalized_needle).is_gt(),
            Self::LessThanOrEqual => {
                compare_filter_values(normalized_value, normalized_needle).is_le()
            }
            Self::GreaterThanOrEqual => {
                compare_filter_values(normalized_value, normalized_needle).is_ge()
            }
            Self::In => split_filter_list(needle)
                .into_iter()
                .any(|entry| normalized_value.eq_ignore_ascii_case(&entry)),
            Self::NotIn => split_filter_list(needle)
                .into_iter()
                .all(|entry| !normalized_value.eq_ignore_ascii_case(&entry)),
            Self::IsNull => looks_like_null(value),
            Self::IsNotNull => !looks_like_null(value),
            Self::Between => parse_filter_between(needle)
                .map(|(start, end)| {
                    compare_filter_values(normalized_value, &start).is_ge()
                        && compare_filter_values(normalized_value, &end).is_le()
                })
                .unwrap_or(false),
            Self::NotBetween => parse_filter_between(needle)
                .map(|(start, end)| {
                    compare_filter_values(normalized_value, &start).is_lt()
                        || compare_filter_values(normalized_value, &end).is_gt()
                })
                .unwrap_or(false),
            Self::Like => sql_like_matches(normalized_value, normalized_needle),
            Self::Contains => lower_value.contains(&lower_needle),
            Self::NotContains => !lower_value.contains(&lower_needle),
            Self::ContainsCaseSensitive => normalized_value.contains(normalized_needle),
            Self::NotContainsCaseSensitive => !normalized_value.contains(normalized_needle),
            Self::HasPrefix => lower_value.starts_with(&lower_needle),
            Self::HasSuffix => lower_value.ends_with(&lower_needle),
            Self::HasPrefixCaseSensitive => normalized_value.starts_with(normalized_needle),
            Self::HasSuffixCaseSensitive => normalized_value.ends_with(normalized_needle),
        }
    }
}

#[derive(Clone)]
enum TabKind {
    Query,
    Table {
        connection_index: usize,
        table_selection: TableSelection,
        table_ref: TableRef,
    },
}

impl QueryTab {
    fn new(id: usize, title: &str, sql: &str) -> Self {
        Self {
            id,
            title: title.to_owned(),
            sql: sql.to_owned(),
            result: QueryResult::empty(),
            autocomplete_index: 0,
            column_page: 0,
            last_access_seq: id as u64,
            filter_mode: ResultFilterMode::Column,
            draft_filter_rules: vec![ResultFilterRule::default()],
            applied_filter_rules: vec![ResultFilterRule::default()],
            filter_raw_sql: String::new(),
            table_detail_view: TableDetailView::Data,
            structure_filter: String::new(),
            structure_selected_row: None,
            filtered_row_cache_key: None,
            filtered_row_indices: Vec::new(),
            kind: TabKind::Query,
        }
    }

    fn new_table(
        id: usize,
        title: &str,
        connection_index: usize,
        table_selection: TableSelection,
        table_ref: TableRef,
    ) -> Self {
        Self {
            id,
            title: title.to_owned(),
            sql: String::new(),
            result: QueryResult::empty(),
            autocomplete_index: 0,
            column_page: 0,
            last_access_seq: id as u64,
            filter_mode: ResultFilterMode::Column,
            draft_filter_rules: vec![ResultFilterRule::default()],
            applied_filter_rules: vec![ResultFilterRule::default()],
            filter_raw_sql: String::new(),
            table_detail_view: TableDetailView::Data,
            structure_filter: String::new(),
            structure_selected_row: None,
            filtered_row_cache_key: None,
            filtered_row_indices: Vec::new(),
            kind: TabKind::Table {
                connection_index,
                table_selection,
                table_ref,
            },
        }
    }

    fn ensure_filtered_row_cache(&mut self) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};

        self.filter_mode.hash(&mut hasher);
        self.applied_filter_rules.hash(&mut hasher);
        let key = FilteredRowCacheKey {
            rows_ptr: self.result.rows.as_ptr() as usize,
            row_count: self.result.rows.len(),
            column_count: self.result.columns.len(),
            filter_hash: hasher.finish(),
        };

        if self.filtered_row_cache_key != Some(key) {
            self.filtered_row_indices =
                filtered_row_indices(&self.result, self.filter_mode, &self.applied_filter_rules);
            self.filtered_row_cache_key = Some(key);
        }
    }

    fn invalidate_filtered_row_cache(&mut self) {
        self.filtered_row_cache_key = None;
    }

    fn filtered_rows(&self) -> &[usize] {
        &self.filtered_row_indices
    }
}

#[derive(Clone, Default)]
struct CommandPalette {
    open: bool,
    query: String,
    selection: usize,
    focus_requested: bool,
    mode: PaletteMode,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum PaletteMode {
    #[default]
    All,
    Connections,
    Databases,
}

#[derive(Clone)]
struct QueryResult {
    columns: Vec<ResultColumn>,
    rows: Vec<Vec<String>>,
    duration_ms: u64,
    source: Option<TableRef>,
}

impl QueryResult {
    fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            duration_ms: 0,
            source: None,
        }
    }

    fn message(title: &str, detail: &str) -> Self {
        Self {
            columns: vec![
                ResultColumn {
                    name: "message".to_owned(),
                },
                ResultColumn {
                    name: "detail".to_owned(),
                },
            ],
            rows: vec![vec![title.to_owned(), detail.to_owned()]],
            duration_ms: 0,
            source: None,
        }
    }
}

#[derive(Clone)]
struct ResultColumn {
    name: String,
}

#[derive(Clone)]
struct TableRef {
    schema: String,
    table: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct CellSelection {
    row: usize,
    col: usize,
}

#[derive(Clone)]
struct CellEditState {
    row: usize,
    col: usize,
    value: String,
}

#[derive(Clone)]
struct RowInspectorState {
    row: usize,
    original_values: Vec<String>,
    values: Vec<String>,
}

impl RowInspectorState {
    fn is_dirty(&self) -> bool {
        self.values != self.original_values
    }
}

struct PendingRowUpdate {
    source: TableRef,
    columns: Vec<ResultColumn>,
    original_row: Vec<String>,
    updated_row: Vec<String>,
    row_index: usize,
    sql: String,
}

#[derive(Clone)]
struct StructureClipboardRow {
    column: TableColumn,
    foreign_key_value: String,
}

enum BackgroundCommand {
    LoadSchemas {
        job_id: u64,
        connection_index: usize,
        profile: ConnectionProfile,
    },
    ExecuteQuery {
        job_id: u64,
        tab_id: usize,
        connection_name: String,
        profile: ConnectionProfile,
        sql: String,
    },
    PreviewTable {
        job_id: u64,
        connection_index: usize,
        tab_id: usize,
        table_selection: TableSelection,
        row_limit: Option<usize>,
        profile: ConnectionProfile,
        table: TableInfo,
    },
    SaveRow {
        job_id: u64,
        profile: ConnectionProfile,
        source: TableRef,
        columns: Vec<ResultColumn>,
        original_row: Vec<String>,
        updated_row: Vec<String>,
        row_index: usize,
    },
    LoadDatabases {
        job_id: u64,
        connection_index: usize,
        profile: ConnectionProfile,
    },
    Disconnect {
        connection_index: usize,
        profile: ConnectionProfile,
    },
    LoadTableColumns {
        job_id: u64,
        connection_index: usize,
        schema_name: String,
        table_name: String,
        profile: ConnectionProfile,
    },
}

enum BackgroundEvent {
    SchemasLoaded {
        job_id: u64,
        connection_index: usize,
        connection_name: String,
        result: Result<Vec<SchemaGroup>, String>,
    },
    QueryFinished {
        job_id: u64,
        tab_id: usize,
        connection_name: String,
        sql: String,
        result: Result<QueryResult, String>,
    },
    TableColumnsLoaded {
        job_id: u64,
        connection_index: usize,
        schema_name: String,
        table_name: String,
        result: Result<Vec<TableColumn>, String>,
    },
    TablePreviewLoaded {
        job_id: u64,
        connection_index: usize,
        tab_id: usize,
        table_selection: TableSelection,
        table: TableInfo,
        result: Result<QueryResult, String>,
    },
    Disconnected {
        connection_index: usize,
    },
    RowSaved {
        job_id: u64,
        source: TableRef,
        row_index: usize,
        updated_row: Vec<String>,
        result: Result<(), String>,
    },
    DatabasesLoaded {
        job_id: u64,
        connection_index: usize,
        result: Result<Vec<String>, String>,
    },
}

#[derive(Clone)]
struct HistoryEntry {
    title: String,
    sql: String,
    summary: String,
}

#[derive(Clone)]
struct SavedQuery {
    name: String,
    description: String,
    sql: String,
}

#[derive(Clone)]
struct QuerySnippet {
    name: String,
    description: String,
    body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AutocompleteKind {
    ColumnSelected = 0,
    Column = 1,
    Table = 2,
    Keyword = 3,
}

#[derive(Clone)]
struct AutocompleteItem {
    label: String,
    insert_text: String,
    kind: AutocompleteKind,
}

#[derive(Clone)]
struct AutocompleteRecord {
    item: AutocompleteItem,
    label_lower: String,
    insert_lower: String,
}

impl AutocompleteRecord {
    fn new(item: AutocompleteItem) -> Self {
        Self {
            label_lower: item.label.to_lowercase(),
            insert_lower: item.insert_text.to_lowercase(),
            item,
        }
    }
}

struct AutocompleteCatalog {
    table_entries: Vec<AutocompleteRecord>,
    columns_by_qualifier: BTreeMap<String, Vec<AutocompleteRecord>>,
    all_column_entries: Vec<AutocompleteRecord>,
}

struct ImportedConnectionUrl {
    engine: ConnectionEngine,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    name: String,
    use_ssh: bool,
    ssh: Option<ImportedSshTunnel>,
}

#[derive(Clone, Default)]
struct ImportedSshTunnel {
    host: String,
    port: u16,
    user: String,
    password: String,
    private_key_path: String,
}

#[derive(Clone)]
pub struct TokenRange {
    pub start: usize,
    pub end: usize,
    pub fragment: String,
}

#[derive(Clone)]
struct PaletteItem {
    title: String,
    subtitle: String,
    action: PaletteAction,
}

#[derive(Clone)]
struct ConnectionForm {
    connection_url: String,
    name: String,
    engine: ConnectionEngine,
    host: String,
    port: String,
    database: String,
    user: String,
    path: String,
    password: String,
    use_ssh: bool,
    ssh_host: String,
    ssh_port: String,
    ssh_user: String,
    ssh_password: String,
    ssh_private_key_path: String,
}

impl Default for ConnectionForm {
    fn default() -> Self {
        Self {
            connection_url: String::new(),
            name: String::new(),
            engine: ConnectionEngine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: "5432".to_owned(),
            database: String::new(),
            user: "app".to_owned(),
            path: String::new(),
            password: String::new(),
            use_ssh: false,
            ssh_host: "bastion.internal".to_owned(),
            ssh_port: "22".to_owned(),
            ssh_user: "ec2-user".to_owned(),
            ssh_password: String::new(),
            ssh_private_key_path: "~/.ssh/id_ed25519".to_owned(),
        }
    }
}

impl ConnectionForm {
    fn import_connection_url(&mut self) -> Result<(), String> {
        let imported = parse_connection_url(&self.connection_url)?;
        self.engine = imported.engine;
        self.host = imported.host;
        self.port = imported.port.to_string();
        self.database = imported.database;
        self.user = imported.user;
        self.password = imported.password;
        if let Some(ssh) = imported.ssh {
            self.use_ssh = true;
            self.ssh_host = ssh.host;
            self.ssh_port = ssh.port.to_string();
            self.ssh_user = ssh.user;
            self.ssh_password = ssh.password;
            self.ssh_private_key_path = ssh.private_key_path;
        } else if imported.use_ssh {
            self.use_ssh = true;
        } else {
            self.use_ssh = false;
        }
        if self.name.trim().is_empty() {
            self.name = imported.name;
        }
        Ok(())
    }

    fn build_profile(&self, seed: usize) -> ConnectionProfile {
        let engine = self.engine;
        let port = self.port.parse::<u16>().unwrap_or(default_port(engine));
        let ssh_tunnel = if self.use_ssh {
            Some(SshTunnelProfile {
                host: self.ssh_host.trim().to_owned(),
                port: self.ssh_port.parse::<u16>().unwrap_or(22),
                user: self.ssh_user.trim().to_owned(),
                password: self.ssh_password.clone(),
                private_key_path: self.ssh_private_key_path.trim().to_owned(),
            })
        } else {
            None
        };

        ConnectionProfile {
            name: self.name.trim().to_owned(),
            source: ConnectionSource::Live,
            engine,
            host: self.host.trim().to_owned(),
            port,
            user: self.user.trim().to_owned(),
            password: self.password.clone(),
            database: self.database.trim().to_owned(),
            path: if self.path.trim().is_empty() {
                None
            } else {
                Some(self.path.trim().to_owned())
            },
            ssh_tunnel,
            schemas: template_schemas_for_engine(engine, seed),
            is_disconnected: false,
        }
    }
}

#[derive(Clone)]
enum PaletteAction {
    SelectConnection(usize),
    OpenTable {
        connection_index: usize,
        schema_index: usize,
        table_index: usize,
    },
    LoadSql {
        title: String,
        sql: String,
    },
    SwitchDatabase(String),
}

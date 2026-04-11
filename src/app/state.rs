struct MangabaseApp {
    connections: Vec<ConnectionProfile>,
    workspaces: Vec<ConnectionWorkspace>,
    active_workspace: Option<usize>,
    selected_connection: usize,
    selected_table: TableSelection,
    query_tabs: Vec<QueryTab>,
    active_tab: usize,
    next_tab_id: usize,
    command_palette: CommandPalette,
    query_history: Vec<HistoryEntry>,
    bookmarks: Vec<SavedQuery>,
    snippets: Vec<QuerySnippet>,
    selected_result_cell: Option<CellSelection>,
    editing_cell: Option<CellEditState>,
    result_grid_has_focus: bool,
    row_inspector_filter: String,
    row_inspector: Option<RowInspectorState>,
    row_inspector_expanded: bool,
    activity_log: Vec<String>,
    schema_filter: String,
    table_preview_limit_input: String,
    show_schema_diagram: bool,
    schema_diagram_filter: String,
    schema_diagram_zoom: f32,
    schema_diagram_current_schema_only: bool,
    connection_manager_open: bool,
    connection_form: ConnectionForm,
    editing_connection_index: Option<usize>,
    left_sidebar_open: bool,
    right_sidebar_open: bool,
    worker_tx: Sender<BackgroundCommand>,
    worker_rx: Receiver<BackgroundEvent>,
    active_jobs: HashSet<u64>,
    busy_message: Option<String>,
    last_results_page_change: Instant,
    next_tab_access_seq: u64,
    next_job_id: u64,
    latest_schema_jobs: BTreeMap<usize, u64>,
    latest_query_jobs: BTreeMap<usize, u64>,
    latest_preview_jobs: BTreeMap<usize, u64>,
    jit_column_loading: HashSet<(usize, String, String)>,
    autocomplete_cache: BTreeMap<usize, AutocompleteCatalog>,
    autocomplete_open: bool,
    schema_loading: HashSet<usize>,
    database_list: Vec<String>,
    database_list_connection: Option<usize>,
    database_list_loading: HashSet<usize>,
    database_list_filter: String,
    latest_database_jobs: BTreeMap<usize, u64>,
    last_disconnect_time: Option<std::time::Instant>,
    show_shortcuts_help: bool,
    ctx: egui::Context,
    pending_row_update: Option<PendingRowUpdate>,
    copied_structure_row: Option<StructureClipboardRow>,
}

impl MangabaseApp {
    fn new(cc: &CreationContext<'_>) -> Self {
        let ctx = cc.egui_ctx.clone();
        configure_theme(&ctx);
        let (worker_tx, worker_rx) = spawn_background_worker(ctx.clone());

        let mut connections = demo_connections();
        connections.extend(load_custom_connections());
        let mut app = Self {
            connections,
            workspaces: Vec::new(),
            active_workspace: None,
            selected_connection: 0,
            selected_table: TableSelection {
                schema_index: 0,
                table_index: 0,
            },
            query_tabs: vec![QueryTab::new(
                1,
                "orders.sql",
                "SELECT id, customer_email, status, total_cents, created_at\nFROM public.orders\nORDER BY created_at DESC\nLIMIT 100;",
            )],
            active_tab: 0,
            next_tab_id: 2,
            command_palette: CommandPalette::default(),
            query_history: demo_history(),
            bookmarks: demo_bookmarks(),
            snippets: demo_snippets(),
            selected_result_cell: None,
            editing_cell: None,
            result_grid_has_focus: false,
            row_inspector_filter: String::new(),
            row_inspector: None,
            row_inspector_expanded: false,
            activity_log: vec!["Workspace ready.".to_owned()],
            schema_filter: String::new(),
            table_preview_limit_input: DEFAULT_TABLE_PREVIEW_LIMIT.to_string(),
            show_schema_diagram: false,
            schema_diagram_filter: String::new(),
            schema_diagram_zoom: 1.0,
            schema_diagram_current_schema_only: false,
            connection_manager_open: false,
            connection_form: ConnectionForm::default(),
            editing_connection_index: None,
            left_sidebar_open: true,
            right_sidebar_open: true,
            worker_tx,
            worker_rx,
            active_jobs: HashSet::new(),
            busy_message: None,
            last_results_page_change: Instant::now(),
            next_tab_access_seq: 1,
            next_job_id: 1,
            latest_schema_jobs: BTreeMap::new(),
            latest_query_jobs: BTreeMap::new(),
            latest_preview_jobs: BTreeMap::new(),
            jit_column_loading: HashSet::new(),
            autocomplete_cache: BTreeMap::new(),
            autocomplete_open: false,
            schema_loading: HashSet::new(),
            database_list: Vec::new(),
            database_list_connection: None,
            database_list_loading: HashSet::new(),
            database_list_filter: String::new(),
            latest_database_jobs: BTreeMap::new(),
            last_disconnect_time: None,
            show_shortcuts_help: false,
            ctx,
            pending_row_update: None,
            copied_structure_row: None,
        };

        if !app.connections.is_empty() {
            app.open_workspace_for_connection(0);
        } else {
            app.push_activity("No connections configured.");
        }
        app
    }

    fn connection(&self) -> &ConnectionProfile {
        &self.connections[self.selected_connection]
    }

    fn connection_opt(&self) -> Option<&ConnectionProfile> {
        self.connections.get(self.selected_connection)
    }

    fn has_connections(&self) -> bool {
        !self.connections.is_empty()
    }

    fn selected_table(&self) -> Option<&TableInfo> {
        self.connection_opt()?
            .schemas
            .get(self.selected_table.schema_index)
            .and_then(|schema| schema.tables.get(self.selected_table.table_index))
    }

    fn active_table_info(&self) -> Option<&TableInfo> {
        let TabKind::Table {
            connection_index,
            table_selection,
            ..
        } = self.active_tab().kind
        else {
            return None;
        };
        self.connections
            .get(connection_index)?
            .schemas
            .get(table_selection.schema_index)
            .and_then(|schema| schema.tables.get(table_selection.table_index))
    }

    fn active_table_info_mut(&mut self) -> Option<&mut TableInfo> {
        let TabKind::Table {
            connection_index,
            table_selection,
            ..
        } = self.active_tab().kind
        else {
            return None;
        };
        self.connections
            .get_mut(connection_index)?
            .schemas
            .get_mut(table_selection.schema_index)
            .and_then(|schema| schema.tables.get_mut(table_selection.table_index))
    }

    fn copy_selected_structure_row(&mut self) {
        let Some(row_index) = self.active_tab().structure_selected_row else {
            self.push_activity("Select a structure row first.");
            return;
        };
        let Some(table) = self.active_table_info() else {
            return;
        };
        let Some(column) = table.columns.get(row_index).cloned() else {
            self.push_activity("Selected structure row is no longer available.");
            return;
        };
        let foreign_key_value = table
            .foreign_keys
            .iter()
            .find(|fk| fk.column_name.eq_ignore_ascii_case(&column.name))
            .map(|fk| {
                format!(
                    "{}.{}({})",
                    fk.referenced_schema, fk.referenced_table, fk.referenced_column
                )
            })
            .unwrap_or_default();
        self.ctx.copy_text(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            column.name,
            column.kind,
            column.character_set,
            column.collation,
            if column.nullable { "YES" } else { "NO" },
            column.default_value,
            column.extra,
            foreign_key_value,
            column.comment,
            if column.primary { "PK" } else { "" }
        ));
        self.copied_structure_row = Some(StructureClipboardRow {
            column,
            foreign_key_value,
        });
        self.push_activity("Copied structure row.");
    }

    fn paste_structure_row(&mut self) {
        let Some(copied) = self.copied_structure_row.clone() else {
            self.push_activity("Copy a structure row first.");
            return;
        };
        let insert_index = self
            .active_tab()
            .structure_selected_row
            .map(|index| index + 1)
            .unwrap_or_else(|| {
                self.active_table_info()
                    .map(|table| table.columns.len())
                    .unwrap_or(0)
            });
        let Some(table) = self.active_table_info_mut() else {
            return;
        };
        let safe_index = insert_index.min(table.columns.len());
        table.columns.insert(safe_index, copied.column.clone());
        if let Some(foreign_key) = parse_structure_foreign_key(
            &copied.foreign_key_value,
            &table.schema,
            &copied.column.name,
        ) {
            table.foreign_keys.push(foreign_key);
        }
        self.active_tab_mut().structure_selected_row = Some(safe_index);
        self.push_activity("Duplicated structure row.");
    }

    fn is_live_selected_connection(&self) -> bool {
        self.connection_opt()
            .map(|connection| connection.source == ConnectionSource::Live)
            .unwrap_or(false)
    }

    fn active_tab(&self) -> &QueryTab {
        &self.query_tabs[self.active_tab]
    }

    fn active_tab_mut(&mut self) -> &mut QueryTab {
        &mut self.query_tabs[self.active_tab]
    }

    fn workspace_index_for_connection(&self, connection_index: usize) -> Option<usize> {
        self.workspaces
            .iter()
            .position(|workspace| workspace.connection_index == connection_index)
    }

    fn workspace_seed(&mut self, connection_index: usize) -> ConnectionWorkspace {
        let id = self.next_tab_id;
        self.next_tab_id += 1;
        let (title, sql) = self.workspace_seed_tab(connection_index);
        ConnectionWorkspace {
            connection_index,
            selected_table: TableSelection {
                schema_index: 0,
                table_index: 0,
            },
            query_tabs: vec![QueryTab::new(id, &title, &sql)],
            active_tab: 0,
            selected_result_cell: None,
            editing_cell: None,
            result_grid_has_focus: false,
            row_inspector_filter: String::new(),
            row_inspector: None,
            row_inspector_expanded: false,
            schema_filter: String::new(),
            table_preview_limit_input: DEFAULT_TABLE_PREVIEW_LIMIT.to_string(),
        }
    }

    fn workspace_seed_tab(&self, connection_index: usize) -> (String, String) {
        let Some(connection) = self.connections.get(connection_index) else {
            return (
                "scratch.sql".to_owned(),
                "-- Add a connection to get started.\nSELECT 1;".to_owned(),
            );
        };
        let title = format!("{}.sql", connection.name.to_lowercase());
        let sql = if connection.is_disconnected {
            "-- Connection is disconnected. Click Connect to load schema and run queries.\nSELECT 1;"
                .to_owned()
        } else if !connection.schemas.is_empty() {
            "-- Double-click a table from the schema list to load it.\nSELECT 1;".to_owned()
        } else {
            "-- Click Connect to load schema, then double-click a table to preview it.\nSELECT 1;"
                .to_owned()
        };
        (title, sql)
    }

    fn save_current_workspace(&mut self) {
        let Some(workspace_index) = self.active_workspace else {
            return;
        };
        if let Some(workspace) = self.workspaces.get_mut(workspace_index) {
            workspace.connection_index = self.selected_connection;
            workspace.selected_table = self.selected_table;
            workspace.query_tabs = self.query_tabs.clone();
            workspace.active_tab = self.active_tab;
            workspace.selected_result_cell = self.selected_result_cell;
            workspace.editing_cell = self.editing_cell.clone();
            workspace.result_grid_has_focus = self.result_grid_has_focus;
            workspace.row_inspector_filter = self.row_inspector_filter.clone();
            workspace.row_inspector = self.row_inspector.clone();
            workspace.row_inspector_expanded = self.row_inspector_expanded;
            workspace.schema_filter = self.schema_filter.clone();
            workspace.table_preview_limit_input = self.table_preview_limit_input.clone();
        }
    }

    fn restore_workspace(&mut self, workspace_index: usize) {
        let Some(workspace) = self.workspaces.get(workspace_index).cloned() else {
            return;
        };
        self.active_workspace = Some(workspace_index);
        self.selected_connection = workspace
            .connection_index
            .min(self.connections.len().saturating_sub(1));
        self.selected_table = workspace.selected_table;
        self.query_tabs = if workspace.query_tabs.is_empty() {
            let id = self.next_tab_id;
            self.next_tab_id += 1;
            let (title, sql) = self.workspace_seed_tab(self.selected_connection);
            vec![QueryTab::new(id, &title, &sql)]
        } else {
            workspace.query_tabs
        };
        self.active_tab = workspace
            .active_tab
            .min(self.query_tabs.len().saturating_sub(1));
        self.selected_result_cell = workspace.selected_result_cell;
        self.editing_cell = workspace.editing_cell;
        self.result_grid_has_focus = workspace.result_grid_has_focus;
        self.row_inspector_filter = workspace.row_inspector_filter;
        self.row_inspector = workspace.row_inspector;
        self.row_inspector_expanded = workspace.row_inspector_expanded;
        self.schema_filter = workspace.schema_filter;
        self.table_preview_limit_input = workspace.table_preview_limit_input;
        self.pending_row_update = None;
    }

    fn parsed_table_preview_limit(&self) -> Result<Option<usize>, String> {
        parse_table_preview_limit_input(&self.table_preview_limit_input)
    }

    fn open_workspace_for_connection(&mut self, connection_index: usize) {
        if connection_index >= self.connections.len() {
            return;
        }

        if self.active_workspace.is_some() {
            self.save_current_workspace();
        }

        let workspace_index =
            if let Some(existing) = self.workspace_index_for_connection(connection_index) {
                existing
            } else {
                let workspace = self.workspace_seed(connection_index);
                self.workspaces.push(workspace);
                self.workspaces.len() - 1
            };

        self.restore_workspace(workspace_index);

        if self
            .connections
            .get(connection_index)
            .map(|connection| !connection.schemas.is_empty())
            .unwrap_or(false)
            && !self.autocomplete_cache.contains_key(&connection_index)
        {
            self.rebuild_autocomplete_cache_for(connection_index);
        }

        if self.database_list_connection != Some(connection_index) {
            self.database_list.clear();
        }

        if self.connections[connection_index].source == ConnectionSource::Live
            && !self.connections[connection_index].is_disconnected
            && !self.connections[connection_index].schemas.is_empty()
            && self.database_list_connection != Some(connection_index)
        {
            self.queue_database_list_refresh(connection_index);
        }
    }

    fn tab_index_by_id(&self, tab_id: usize) -> Option<usize> {
        self.query_tabs.iter().position(|tab| tab.id == tab_id)
    }

    fn workspace_tab_location_by_id(&self, tab_id: usize) -> Option<(usize, usize)> {
        self.workspaces
            .iter()
            .enumerate()
            .find_map(|(workspace_index, workspace)| {
                workspace
                    .query_tabs
                    .iter()
                    .position(|tab| tab.id == tab_id)
                    .map(|tab_index| (workspace_index, tab_index))
            })
    }

    fn sync_context_from_active_tab(&mut self) {
        if let Some(tab) = self.query_tabs.get(self.active_tab).cloned() {
            if let TabKind::Table {
                connection_index,
                table_selection,
                ..
            } = tab.kind
            {
                if connection_index < self.connections.len() {
                    self.selected_connection = connection_index;
                    self.selected_table = table_selection;
                }
            }
        }
    }

    fn ensure_table_tab(
        &mut self,
        connection_index: usize,
        table_selection: TableSelection,
        table: &TableInfo,
    ) -> usize {
        if let Some(index) = self.query_tabs.iter().position(|tab| {
            matches!(
                &tab.kind,
                TabKind::Table {
                    connection_index: existing_connection,
                    table_ref,
                    ..
                } if *existing_connection == connection_index
                    && table_ref.schema == table.schema
                    && table_ref.table == table.name
            )
        }) {
            self.active_tab = index;
            self.sync_context_from_active_tab();
            return index;
        }

        let id = self.next_tab_id;
        self.next_tab_id += 1;
        self.query_tabs.push(QueryTab::new_table(
            id,
            &table.name,
            connection_index,
            table_selection,
            TableRef {
                schema: table.schema.clone(),
                table: table.name.clone(),
            },
        ));
        self.active_tab = self.query_tabs.len() - 1;
        self.touch_tab_index(self.active_tab);
        self.sync_context_from_active_tab();
        self.active_tab
    }

    fn next_background_job_id(&mut self) -> u64 {
        let id = self.next_job_id;
        self.next_job_id += 1;
        id
    }

    fn open_command_palette(&mut self, mode: PaletteMode) {
        self.command_palette.open = true;
        self.command_palette.focus_requested = true;
        self.command_palette.selection = 0;
        self.command_palette.query.clear();
        self.command_palette.mode = mode;
    }

    fn next_tab_access_seq(&mut self) -> u64 {
        let id = self.next_tab_access_seq;
        self.next_tab_access_seq += 1;
        id
    }

    fn touch_tab_index(&mut self, index: usize) {
        let seq = self.next_tab_access_seq();
        if let Some(tab) = self.query_tabs.get_mut(index) {
            tab.last_access_seq = seq;
        }
    }

    fn evict_inactive_tab_rows(&mut self) {
        // Keep table/query results resident so switching back to a tab feels instant.
    }

    fn begin_background_job(&mut self, job_id: u64, message: impl Into<String>) {
        let message = message.into();
        let is_retry = self.active_jobs.contains(&job_id);
        self.active_jobs.insert(job_id);

        if is_retry {
            self.push_activity(format!("Retrying: {}", message));
        } else {
            self.busy_message = Some(message.clone());
            self.push_activity(message);
        }
    }

    fn finish_background_job(&mut self, job_id: u64) {
        self.active_jobs.remove(&job_id);
        if self.active_jobs.is_empty() {
            self.busy_message = None;
        }
    }

    fn set_result_selection(&mut self, row: usize, col: usize) {
        self.selected_result_cell = Some(CellSelection { row, col });
        self.result_grid_has_focus = true;
        let total_columns = self.active_tab().result.columns.len();
        let max_offset = total_columns.saturating_sub(RESULT_COLUMNS_PER_PAGE);
        let current_offset = self.active_tab().column_page.min(max_offset);
        let next_offset = if col < current_offset {
            col
        } else if col >= current_offset + RESULT_COLUMNS_PER_PAGE {
            col + 1 - RESULT_COLUMNS_PER_PAGE
        } else {
            current_offset
        };
        self.active_tab_mut().column_page = next_offset.min(max_offset);
        self.sync_row_inspector();
    }

    fn clear_result_focus(&mut self) {
        self.result_grid_has_focus = false;
        self.editing_cell = None;
    }

    fn restore_active_tab_if_needed(&mut self) {
        let should_restore = matches!(&self.active_tab().kind, TabKind::Table { .. })
            && self.active_tab().result.rows.is_empty()
            && !self.active_tab().result.columns.is_empty();

        if should_restore {
            self.open_selected_table();
        }
    }

    fn sync_row_inspector(&mut self) {
        let Some(selection) = self.selected_result_cell else {
            self.row_inspector = None;
            return;
        };

        let Some(row_values) = self.active_tab().result.rows.get(selection.row).cloned() else {
            self.row_inspector = None;
            return;
        };

        let should_reset = self
            .row_inspector
            .as_ref()
            .map(|state| {
                state.row != selection.row
                    || state.original_values != row_values
                    || state.values.len() != row_values.len()
            })
            .unwrap_or(true);

        if should_reset {
            self.row_inspector_expanded = false;
            self.row_inspector = Some(RowInspectorState {
                row: selection.row,
                original_values: row_values.clone(),
                values: row_values,
            });
        }
    }

    fn rebuild_autocomplete_cache_for(&mut self, connection_index: usize) {
        let Some(connection) = self.connections.get(connection_index) else {
            return;
        };

        let mut table_entries = Vec::new();
        let mut columns_by_qualifier: BTreeMap<String, Vec<AutocompleteRecord>> = BTreeMap::new();

        let mut all_column_entries = Vec::new();

        for schema in &connection.schemas {
            for table in &schema.tables {
                let full_name = format!("{}.{}", table.schema, table.name);
                table_entries.push(AutocompleteRecord::new(AutocompleteItem {
                    label: full_name.clone(),
                    insert_text: full_name.clone(),
                    kind: AutocompleteKind::Table,
                }));

                let column_records = table
                    .columns
                    .iter()
                    .map(|column| {
                        let item = AutocompleteItem {
                            label: column.name.clone(),
                            insert_text: column.name.clone(),
                            kind: AutocompleteKind::Column,
                        };
                        AutocompleteRecord::new(item)
                    })
                    .collect::<Vec<_>>();

                all_column_entries.extend(column_records.clone());

                let qualified_column_records = table
                    .columns
                    .iter()
                    .map(|column| {
                        AutocompleteRecord::new(AutocompleteItem {
                            label: format!("{}.{}", table.name, column.name),
                            insert_text: format!("{}.{}", table.name, column.name),
                            kind: AutocompleteKind::Column,
                        })
                    })
                    .collect::<Vec<_>>();

                columns_by_qualifier
                    .entry(table.name.to_lowercase())
                    .or_default()
                    .extend(qualified_column_records.clone());
                columns_by_qualifier
                    .entry(full_name.to_lowercase())
                    .or_default()
                    .extend(qualified_column_records);
            }
        }

        self.autocomplete_cache.insert(
            connection_index,
            AutocompleteCatalog {
                table_entries,
                columns_by_qualifier,
                all_column_entries,
            },
        );
    }

    fn autocomplete_cache_for_selected_connection(&self) -> Option<&AutocompleteCatalog> {
        self.autocomplete_cache.get(&self.selected_connection)
    }

    fn queue_schema_refresh(&mut self, index: usize) {
        if index >= self.connections.len() {
            return;
        }
        if self.connections[index].source == ConnectionSource::Demo {
            return;
        }
        if self.schema_loading.contains(&index) {
            return;
        }

        let job_id = self.next_background_job_id();
        let profile = self.connections[index].clone();
        let connection_name = profile.name.clone();
        self.latest_schema_jobs.insert(index, job_id);
        self.schema_loading.insert(index);
        self.push_activity(format!("Loading schema for {}", connection_name));
        if self
            .worker_tx
            .send(BackgroundCommand::LoadSchemas {
                job_id,
                connection_index: index,
                profile,
            })
            .is_err()
        {
            self.latest_schema_jobs.remove(&index);
            self.schema_loading.remove(&index);
            self.push_activity("Background worker is unavailable.");
        }
    }

}

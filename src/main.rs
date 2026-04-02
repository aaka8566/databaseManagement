use clickhouse::Client as ClickHouseClient;
use duckdb::{Connection as DuckDbConnection, params};
use eframe::{
    App, CreationContext, NativeOptions,
    egui::{
        self, Align, Align2, Color32, FontId, Key, Layout, Margin, RichText, Sense, TextEdit, Vec2,
    },
};
use egui_extras::{Column, TableBuilder};
use mysql::{OptsBuilder, Pool, Row as MySqlRow, Value as MySqlValue, prelude::Queryable};
use postgres::{Client as PostgresClient, Config as PostgresConfig, NoTls, SimpleQueryMessage};
use rusqlite::Connection as SqliteConnection;
use serde::{Deserialize, Serialize};
use ssh2::Session;
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{Read as IoRead, Write as IoWrite},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

fn main() -> eframe::Result<()> {
    let options = NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Sharingan")
            .with_inner_size([1560.0, 980.0])
            .with_min_inner_size([1280.0, 780.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Sharingan",
        options,
        Box::new(|cc| Ok(Box::new(MangabaseApp::new(cc)))),
    )
}

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
        self.active_tab_mut().column_page = col / RESULT_COLUMNS_PER_PAGE;
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

    fn disconnect_connection(&mut self, index: usize) {
        let (profile, connection_name) = if let Some(profile) = self.connections.get_mut(index) {
            profile.schemas.clear();
            profile.is_disconnected = true;
            (profile.clone(), profile.name.clone())
        } else {
            return;
        };

        let _ = self.worker_tx.send(BackgroundCommand::Disconnect {
            connection_index: index,
            profile,
        });

        // Cancel any pending schema/database jobs for this connection
        if let Some(&job_id) = self.latest_schema_jobs.get(&index) {
            self.active_jobs.remove(&job_id);
        }
        if let Some(&job_id) = self.latest_database_jobs.get(&index) {
            self.active_jobs.remove(&job_id);
        }

        // Clear trackers for this connection
        self.latest_schema_jobs.remove(&index);
        self.latest_database_jobs.remove(&index);
        self.schema_loading.remove(&index);
        self.database_list_loading.remove(&index);
        self.autocomplete_cache.remove(&index);
        if self.database_list_connection == Some(index) {
            self.database_list.clear();
            self.database_list_connection = None;
        }

        self.push_activity(format!("Disconnected from {}", connection_name));

        self.restart_background_worker();

        // Identify and close all tabs belonging to this connection
        let active_tab_id = if self.active_tab < self.query_tabs.len() {
            Some(self.query_tabs[self.active_tab].id)
        } else {
            None
        };

        let mut removed_tab_ids = Vec::new();
        self.query_tabs.retain(|tab| {
            if let TabKind::Table {
                connection_index, ..
            } = &tab.kind
            {
                if *connection_index == index {
                    removed_tab_ids.push(tab.id);
                    false
                } else {
                    true
                }
            } else {
                true
            }
        });

        // Clear jobs associated with the removed tabs
        for tab_id in removed_tab_ids {
            if let Some(&job_id) = self.latest_query_jobs.get(&tab_id) {
                self.active_jobs.remove(&job_id);
            }
            if let Some(&job_id) = self.latest_preview_jobs.get(&tab_id) {
                self.active_jobs.remove(&job_id);
            }
            self.latest_query_jobs.remove(&tab_id);
            self.latest_preview_jobs.remove(&tab_id);
        }

        if self.active_jobs.is_empty() {
            self.busy_message = None;
        }

        // Restore active tab index if possible
        if let Some(id) = active_tab_id {
            if let Some(pos) = self.query_tabs.iter().position(|t| t.id == id) {
                self.active_tab = pos;
            } else {
                self.active_tab = self.query_tabs.len().saturating_sub(1);
            }
        }
        self.active_tab = self.active_tab.min(self.query_tabs.len().saturating_sub(1));

        // Reset global selection state if the disconnected connection was active
        if self.selected_connection == index {
            self.selected_table = TableSelection {
                schema_index: 0,
                table_index: 0,
            };
            self.selected_result_cell = None;
            self.row_inspector = None;
        }

        // Update autocomplete for the UI (now with empty schemas)
        self.rebuild_autocomplete_cache_for(index);
    }

    fn queue_active_query(&mut self, sql: String, connection_name: String) {
        if self.connection().is_disconnected {
            self.push_activity(format!(
                "Connection '{}' is disconnected. Click Connect to run this query.",
                connection_name
            ));
            return;
        }

        let tab_id = self.active_tab().id;
        let mut job_id = self.next_background_job_id();
        if let Some(&existing) = self.latest_query_jobs.get(&tab_id) {
            if self.active_jobs.contains(&existing) {
                job_id = existing;
            }
        }

        self.latest_query_jobs.insert(tab_id, job_id);
        self.begin_background_job(job_id, format!("Running query on {}", connection_name));
        if self
            .worker_tx
            .send(BackgroundCommand::ExecuteQuery {
                job_id,
                tab_id,
                connection_name,
                profile: self.connection().clone(),
                sql,
            })
            .is_err()
        {
            self.finish_background_job(job_id);
            self.push_activity("Background worker is unavailable.");
        }
    }

    fn restart_background_worker(&mut self) {
        let (worker_tx, worker_rx) = spawn_background_worker(self.ctx.clone());
        self.worker_tx = worker_tx;
        self.worker_rx = worker_rx;
    }

    fn apply_active_tab_raw_filter(&mut self) {
        if self.connection().is_disconnected {
            let name = self.connection().name.clone();
            self.push_activity(format!(
                "Connection '{}' is disconnected. Click Connect to apply filters.",
                name
            ));
            return;
        }
        let raw_filter = self.active_tab().filter_raw_sql.trim().to_owned();
        if raw_filter.is_empty() {
            self.refresh_active_view();
            return;
        }

        let TabKind::Table {
            connection_index,
            table_selection,
            table_ref,
        } = self.active_tab().kind.clone()
        else {
            self.push_activity("Raw SQL filter works on opened table tabs.");
            return;
        };

        let Some(table) = self
            .connections
            .get(connection_index)
            .and_then(|connection| connection.schemas.get(table_selection.schema_index))
            .and_then(|schema| schema.tables.get(table_selection.table_index))
            .cloned()
        else {
            self.push_activity("Selected table is unavailable for raw SQL filtering.");
            return;
        };

        let order_by = if table.primary_sort.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {} ASC", table.primary_sort)
        };
        let row_limit = match self.parsed_table_preview_limit() {
            Ok(limit) => limit,
            Err(error) => {
                self.push_activity(error);
                return;
            }
        };
        let sql = format!(
            "SELECT * FROM {}.{} WHERE {}{}{};",
            table_ref.schema,
            table_ref.table,
            raw_filter,
            order_by,
            limit_clause_inline(row_limit)
        );
        let connection_name = self.connections[connection_index].name.clone();

        self.active_tab_mut().result = QueryResult::message(
            "Applying filter",
            "Running raw SQL filter in the background.",
        );
        self.selected_result_cell = None;
        self.row_inspector = None;
        self.queue_active_query(sql, connection_name);
    }

    fn queue_table_preview(&mut self, table: TableInfo, table_selection: TableSelection) {
        if self.connection().is_disconnected {
            let name = self.connection().name.clone();
            self.push_activity(format!(
                "Connection '{}' is disconnected. Click Connect to preview tables.",
                name
            ));
            return;
        }
        let row_limit = match self.parsed_table_preview_limit() {
            Ok(limit) => limit,
            Err(error) => {
                self.push_activity(error);
                return;
            }
        };
        let tab_id = self.active_tab().id;
        let mut job_id = self.next_background_job_id();
        if let Some(&existing) = self.latest_preview_jobs.get(&tab_id) {
            if self.active_jobs.contains(&existing) {
                job_id = existing;
            }
        }

        self.latest_preview_jobs.insert(tab_id, job_id);
        self.begin_background_job(job_id, format!("Loading {}.{}", table.schema, table.name));
        if self
            .worker_tx
            .send(BackgroundCommand::PreviewTable {
                job_id,
                connection_index: self.selected_connection,
                tab_id,
                table_selection,
                row_limit,
                profile: self.connection().clone(),
                table,
            })
            .is_err()
        {
            self.finish_background_job(job_id);
            self.push_activity("Background worker is unavailable.");
        }
    }

    fn queue_row_save(
        &mut self,
        source: TableRef,
        columns: Vec<ResultColumn>,
        original_row: Vec<String>,
        updated_row: Vec<String>,
        row_index: usize,
    ) {
        let job_id = self.next_background_job_id();
        self.begin_background_job(
            job_id,
            format!(
                "Saving row {} to {}.{}",
                row_index + 1,
                source.schema,
                source.table
            ),
        );
        if self
            .worker_tx
            .send(BackgroundCommand::SaveRow {
                job_id,
                profile: self.connection().clone(),
                source,
                columns,
                original_row,
                updated_row,
                row_index,
            })
            .is_err()
        {
            self.finish_background_job(job_id);
            self.push_activity("Background worker is unavailable.");
        }
    }

    fn apply_query_result(
        &mut self,
        tab_id: usize,
        sql: String,
        connection_name: String,
        mut result: QueryResult,
    ) {
        let history_label = first_line(&sql);
        let summary = format!(
            "{} on {} returned {} rows",
            history_label,
            connection_name,
            result.rows.len()
        );
        let inferred_title = infer_tab_title(&sql, self.next_tab_id);
        if let Some(tab_index) = self.tab_index_by_id(tab_id) {
            let is_active = tab_index == self.active_tab;
            if let Some(table_ref) = match &self.query_tabs[tab_index].kind {
                TabKind::Table { table_ref, .. } => Some(table_ref.clone()),
                TabKind::Query => None,
            } {
                result.source = Some(table_ref);
            }

            {
                let tab = &mut self.query_tabs[tab_index];
                tab.result = result.clone();
                tab.column_page = 0;
                if tab.title == "scratch.sql" || tab.title.starts_with("Query ") {
                    tab.title = inferred_title.clone();
                }
            }
            self.touch_tab_index(tab_index);
            self.evict_inactive_tab_rows();

            if is_active {
                if !result.rows.is_empty() && !result.columns.is_empty() {
                    self.set_result_selection(0, 0);
                } else {
                    self.selected_result_cell = None;
                    self.row_inspector = None;
                }
            }
        } else if let Some((workspace_index, tab_index)) = self.workspace_tab_location_by_id(tab_id)
        {
            if let Some(table_ref) =
                match &self.workspaces[workspace_index].query_tabs[tab_index].kind {
                    TabKind::Table { table_ref, .. } => Some(table_ref.clone()),
                    TabKind::Query => None,
                }
            {
                result.source = Some(table_ref);
            }

            let workspace = &mut self.workspaces[workspace_index];
            let tab = &mut workspace.query_tabs[tab_index];
            tab.result = result.clone();
            tab.column_page = 0;
            if tab.title == "scratch.sql" || tab.title.starts_with("Query ") {
                tab.title = inferred_title.clone();
            }
            if workspace.active_tab == tab_index {
                if !result.rows.is_empty() && !result.columns.is_empty() {
                    workspace.selected_result_cell = Some(CellSelection { row: 0, col: 0 });
                } else {
                    workspace.selected_result_cell = None;
                    workspace.row_inspector = None;
                }
            }
        } else {
            return;
        }

        self.query_history.insert(
            0,
            HistoryEntry {
                title: history_label,
                sql,
                summary,
            },
        );
        self.query_history.truncate(24);
        self.push_activity(format!(
            "Ran query in {} ms on {}",
            result.duration_ms, connection_name
        ));
    }

    fn apply_table_preview(
        &mut self,
        connection_index: usize,
        tab_id: usize,
        table_selection: TableSelection,
        table: TableInfo,
        result: QueryResult,
    ) {
        if let Some(current_table) = self
            .connections
            .get_mut(connection_index)
            .and_then(|connection| connection.schemas.get_mut(table_selection.schema_index))
            .and_then(|schema| schema.tables.get_mut(table_selection.table_index))
        {
            current_table.primary_sort = table.primary_sort.clone();
            current_table.columns = table.columns.clone();
            current_table.indexes = table.indexes;
            current_table.index_entries = table.index_entries.clone();
            current_table.foreign_keys = table.foreign_keys.clone();
        }
        self.rebuild_autocomplete_cache_for(connection_index);

        let row_limit = self
            .workspaces
            .iter()
            .find(|workspace| workspace.connection_index == connection_index)
            .and_then(|workspace| {
                parse_table_preview_limit_input(&workspace.table_preview_limit_input).ok()
            })
            .unwrap_or(Some(DEFAULT_TABLE_PREVIEW_LIMIT));
        let query = table_preview_query_text(&table, row_limit);
        let title = format!("{}.sql", table.name);
        if let Some(tab_index) = self.tab_index_by_id(tab_id) {
            let is_active = tab_index == self.active_tab;

            {
                let tab = &mut self.query_tabs[tab_index];
                tab.title = title.clone();
                tab.sql = query.clone();
                tab.result = result.clone();
                tab.autocomplete_index = 0;
                tab.column_page = 0;
            }
            self.touch_tab_index(tab_index);
            self.evict_inactive_tab_rows();

            if is_active
                && self.selected_connection == connection_index
                && self.selected_table == table_selection
            {
                if !result.rows.is_empty() && !result.columns.is_empty() {
                    self.set_result_selection(0, 0);
                } else {
                    self.selected_result_cell = None;
                    self.row_inspector = None;
                }
            }
        } else if let Some((workspace_index, tab_index)) = self.workspace_tab_location_by_id(tab_id)
        {
            let workspace = &mut self.workspaces[workspace_index];
            let tab = &mut workspace.query_tabs[tab_index];
            tab.title = title;
            tab.sql = query;
            tab.result = result.clone();
            tab.autocomplete_index = 0;
            tab.column_page = 0;
            if workspace.active_tab == tab_index {
                if !result.rows.is_empty() && !result.columns.is_empty() {
                    workspace.selected_result_cell = Some(CellSelection { row: 0, col: 0 });
                } else {
                    workspace.selected_result_cell = None;
                    workspace.row_inspector = None;
                }
            }
        } else {
            return;
        }

        self.push_activity(format!("Opened {}.{}", table.schema, table.name));
    }

    fn process_background_events(&mut self) {
        while let Ok(event) = self.worker_rx.try_recv() {
            let job_id = match &event {
                BackgroundEvent::SchemasLoaded { job_id, .. } => Some(*job_id),
                BackgroundEvent::QueryFinished { job_id, .. } => Some(*job_id),
                BackgroundEvent::TablePreviewLoaded { job_id, .. } => Some(*job_id),
                BackgroundEvent::RowSaved { job_id, .. } => Some(*job_id),
                BackgroundEvent::DatabasesLoaded { job_id, .. } => Some(*job_id),
                BackgroundEvent::TableColumnsLoaded { job_id, .. } => Some(*job_id),
                BackgroundEvent::Disconnected { .. } => None,
            };

            if let Some(id) = job_id {
                self.finish_background_job(id);
            }

            match event {
                BackgroundEvent::SchemasLoaded {
                    job_id,
                    connection_index,
                    connection_name,
                    result,
                } => {
                    if self.latest_schema_jobs.get(&connection_index) != Some(&job_id) {
                        continue;
                    }
                    self.schema_loading.remove(&connection_index);
                    match result {
                        Ok(schemas) => {
                            if let Some(connection) = self.connections.get_mut(connection_index) {
                                connection.schemas = schemas;
                            }
                            self.rebuild_autocomplete_cache_for(connection_index);
                            if self.selected_connection == connection_index
                                && self.database_list_connection != Some(connection_index)
                            {
                                self.queue_database_list_refresh(connection_index);
                            }
                            if self.selected_connection == connection_index {
                                self.selected_table = TableSelection {
                                    schema_index: 0,
                                    table_index: 0,
                                };
                                let tab = self.active_tab_mut();
                                if tab.result.source.is_none() {
                                    tab.result = QueryResult::message(
                                        "Schema loaded",
                                        "Double-click a table in the schema list to preview rows.",
                                    );
                                }
                            }
                            self.push_activity(format!("Connected to {}", connection_name));
                        }
                        Err(error) => {
                            if let Some(connection) = self.connections.get_mut(connection_index) {
                                connection.is_disconnected = true;
                                connection.schemas.clear();
                            }
                            self.push_activity(format!("Connect failed: {}", error));
                        }
                    }
                }
                BackgroundEvent::QueryFinished {
                    job_id,
                    tab_id,
                    connection_name,
                    sql,
                    result,
                } => {
                    if self.latest_query_jobs.get(&tab_id) != Some(&job_id) {
                        continue;
                    }
                    match result {
                        Ok(result) => self.apply_query_result(tab_id, sql, connection_name, result),
                        Err(error) => {
                            self.apply_query_result(
                                tab_id,
                                sql,
                                connection_name,
                                QueryResult::message("Query failed", &error),
                            );
                        }
                    }
                }
                BackgroundEvent::TablePreviewLoaded {
                    job_id,
                    connection_index,
                    tab_id,
                    table_selection,
                    table,
                    result,
                } => {
                    if self.latest_preview_jobs.get(&tab_id) != Some(&job_id) {
                        continue;
                    }
                    match result {
                        Ok(result) => {
                            self.apply_table_preview(
                                connection_index,
                                tab_id,
                                table_selection,
                                table,
                                result,
                            );
                        }
                        Err(error) => {
                            if let Some(tab_index) = self.tab_index_by_id(tab_id) {
                                self.query_tabs[tab_index].result =
                                    QueryResult::message("Preview failed", &error);
                            } else if let Some((workspace_index, tab_index)) =
                                self.workspace_tab_location_by_id(tab_id)
                            {
                                self.workspaces[workspace_index].query_tabs[tab_index].result =
                                    QueryResult::message("Preview failed", &error);
                            }
                            if self.tab_index_by_id(tab_id).is_some()
                                || self.workspace_tab_location_by_id(tab_id).is_some()
                            {
                                self.push_activity(format!("Preview failed: {}", error));
                            }
                        }
                    }
                }
                BackgroundEvent::TableColumnsLoaded {
                    job_id: _,
                    connection_index,
                    schema_name,
                    table_name,
                    result,
                } => {
                    if let Ok(columns) = result {
                        if let Some(conn) = self.connections.get_mut(connection_index) {
                            let mut found = false;
                            for schema in &mut conn.schemas {
                                if schema.name == schema_name {
                                    if let Some(table) =
                                        schema.tables.iter_mut().find(|t| t.name == table_name)
                                    {
                                        table.columns = columns.clone();
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if found {
                                self.rebuild_autocomplete_cache_for(connection_index);
                                self.push_activity(format!(
                                    "Loaded columns for {}.{}",
                                    schema_name, table_name
                                ));
                            }
                        }
                    }
                }
                BackgroundEvent::DatabasesLoaded {
                    job_id,
                    connection_index,
                    result,
                } => {
                    self.database_list_loading.remove(&connection_index);
                    if self.latest_database_jobs.get(&connection_index) != Some(&job_id) {
                        continue;
                    }
                    match result {
                        Ok(databases) => {
                            if self.selected_connection == connection_index {
                                self.database_list = databases;
                                self.database_list_connection = Some(connection_index);
                            }
                            self.push_activity("Database list loaded.");
                        }
                        Err(error) => {
                            self.push_activity(format!("Failed to load databases: {}", error));
                        }
                    }
                }
                BackgroundEvent::RowSaved {
                    source,
                    row_index,
                    updated_row,
                    result,
                    ..
                } => match result {
                    Ok(()) => {
                        let mut updated_any = false;
                        if let Some(source_ref) = self.active_tab().result.source.clone() {
                            if source_ref.schema == source.schema
                                && source_ref.table == source.table
                            {
                                let tab = self.active_tab_mut();
                                if row_index < tab.result.rows.len() {
                                    tab.result.rows[row_index] = updated_row.clone();
                                    updated_any = true;
                                }
                            }
                        }

                        if let Some(inspector) = self.row_inspector.as_mut() {
                            if inspector.row == row_index {
                                inspector.original_values = updated_row.clone();
                                inspector.values = updated_row.clone();
                            }
                        }

                        for workspace in &mut self.workspaces {
                            for tab in &mut workspace.query_tabs {
                                if let Some(source_ref) = tab.result.source.as_ref() {
                                    if source_ref.schema == source.schema
                                        && source_ref.table == source.table
                                        && row_index < tab.result.rows.len()
                                    {
                                        tab.result.rows[row_index] = updated_row.clone();
                                        updated_any = true;
                                    }
                                }
                            }

                            if let Some(inspector) = workspace.row_inspector.as_mut() {
                                if inspector.row == row_index {
                                    inspector.original_values = updated_row.clone();
                                    inspector.values = updated_row.clone();
                                }
                            }
                        }

                        if updated_any {
                            self.push_activity(format!(
                                "Saved row to {}.{}",
                                source.schema, source.table
                            ));
                        }
                    }
                    Err(error) => {
                        self.push_activity(format!("Save failed: {}", error));
                    }
                },
                BackgroundEvent::Disconnected { connection_index } => {
                    let name = self
                        .connections
                        .get(connection_index)
                        .map(|c| c.name.clone());
                    if let Some(conn_name) = name {
                        self.push_activity(format!("Resources released for {}", conn_name));
                    }
                }
            }
        }
    }

    fn push_activity(&mut self, message: impl Into<String>) {
        self.activity_log.insert(0, message.into());
        self.activity_log.truncate(12);
    }

    fn open_selected_table(&mut self) {
        if !self.has_connections() {
            self.push_activity("Add a connection first.");
            return;
        }
        if self.connection().schemas.is_empty() || self.connection().schemas[0].tables.is_empty() {
            let tab = self.active_tab_mut();
            tab.kind = TabKind::Query;
            tab.title = "scratch.sql".to_owned();
            tab.sql = "-- No schema loaded for this connection yet.\nSELECT 1;".to_owned();
            tab.result = QueryResult::empty();
            self.selected_result_cell = None;
            self.row_inspector = None;
            self.push_activity("Opened empty connection profile.");
            return;
        }

        let table = self.selected_table().cloned().unwrap_or_else(|| TableInfo {
            schema: "public".to_owned(),
            name: "table".to_owned(),
            primary_sort: "id".to_owned(),
            row_count: 0,
            size: "-".to_owned(),
            indexes: 0,
            columns: Vec::new(),
            index_entries: Vec::new(),
            foreign_keys: Vec::new(),
            rows: Vec::new(),
        });
        let row_limit = match self.parsed_table_preview_limit() {
            Ok(limit) => limit,
            Err(error) => {
                self.push_activity(error);
                return;
            }
        };
        let query = table_preview_query_text(&table, row_limit);
        let selection = self.selected_table;
        let connection_index = self.selected_connection;
        let tab_index = self.ensure_table_tab(connection_index, selection, &table);
        let tab_id = self.query_tabs[tab_index].id;

        {
            let tab = &mut self.query_tabs[tab_index];
            tab.title = table.name.clone();
            tab.sql = query;
            tab.autocomplete_index = 0;
            tab.column_page = 0;
        }
        self.touch_tab_index(tab_index);

        if self.is_live_selected_connection() {
            self.query_tabs[tab_index].result =
                QueryResult::message("Loading table preview", "Fetching rows in the background.");
            self.selected_result_cell = None;
            self.row_inspector = None;
            self.active_tab = tab_index;
            self.queue_table_preview(table, selection);
        } else {
            let result = table.preview_result_with_limit(row_limit);
            self.query_tabs[tab_index].result = result.clone();
            self.active_tab = tab_index;
            self.touch_tab_index(tab_index);
            if !result.rows.is_empty() && !result.columns.is_empty() {
                self.set_result_selection(0, 0);
            } else {
                self.row_inspector = None;
            }
            self.push_activity(format!("Opened {}.{}", table.schema, table.name));
        }
        let _ = tab_id;
    }

    fn connect_to_index(&mut self, index: usize) {
        if index >= self.connections.len() {
            return;
        }

        let recently_disconnected = self
            .last_disconnect_time
            .map(|t| t.elapsed() < Duration::from_millis(500))
            .unwrap_or(false);

        if recently_disconnected {
            return;
        }

        self.connections[index].is_disconnected = false;
        self.open_workspace_for_connection(index);
        self.refresh_connection(index);
    }

    fn queue_database_list_refresh(&mut self, index: usize) {
        if index >= self.connections.len() {
            return;
        }
        if self.connections[index].source == ConnectionSource::Demo {
            return;
        }
        if self.database_list_loading.contains(&index) {
            return;
        }

        let job_id = self.next_background_job_id();
        let profile = self.connections[index].clone();
        self.latest_database_jobs.insert(index, job_id);
        self.database_list_loading.insert(index);
        if self
            .worker_tx
            .send(BackgroundCommand::LoadDatabases {
                job_id,
                connection_index: index,
                profile,
            })
            .is_err()
        {
            self.latest_database_jobs.remove(&index);
            self.database_list_loading.remove(&index);
            self.push_activity("Background worker is unavailable.");
        }
    }

    fn switch_database(&mut self, db_name: String) {
        let index = self.selected_connection;
        if index >= self.connections.len() {
            return;
        }
        let old_db = self.connections[index].database.clone();
        if old_db == db_name {
            self.push_activity(format!("Already on database {}", db_name));
            return;
        }
        self.connections[index].database = db_name.clone();
        self.connections[index].schemas.clear();
        self.autocomplete_cache.remove(&index);
        self.selected_table = TableSelection {
            schema_index: 0,
            table_index: 0,
        };
        let tab = self.active_tab_mut();
        tab.title = format!("{}.sql", db_name.to_lowercase());
        tab.sql = "-- Switching database. Loading schema...\nSELECT 1;".to_owned();
        tab.result = QueryResult::empty();
        self.touch_tab_index(self.active_tab);
        self.selected_result_cell = None;
        self.row_inspector = None;
        self.push_activity(format!("Switched to database {}", db_name));
        self.queue_schema_refresh(index);
    }

    fn select_table(&mut self, schema_index: usize, table_index: usize) {
        self.selected_table = TableSelection {
            schema_index,
            table_index,
        };
    }

    fn run_active_query(&mut self) {
        if !self.has_connections() {
            self.push_activity("Add a connection before running queries.");
            return;
        }
        if matches!(self.active_tab().kind, TabKind::Table { .. }) {
            self.push_activity("Table tabs show data directly. Use New Tab to run SQL.");
            return;
        }

        let sql = self.active_tab().sql.clone();
        let connection_name = self.connection().name.clone();
        if self.is_live_selected_connection() {
            self.active_tab_mut().result =
                QueryResult::message("Running query", "Executing in the background.");
            self.selected_result_cell = None;
            self.row_inspector = None;
            self.queue_active_query(sql, connection_name);
            return;
        }

        let result = { self.connection().execute_mock_query(&sql) };
        let history_label = first_line(&sql);
        let summary = format!(
            "{} on {} returned {} rows",
            history_label,
            connection_name,
            result.rows.len()
        );
        let inferred_title = infer_tab_title(&sql, self.next_tab_id);

        let tab = self.active_tab_mut();
        tab.result = result.clone();
        if tab.title == "scratch.sql" || tab.title.starts_with("Query ") {
            tab.title = inferred_title;
        }

        if !result.rows.is_empty() && !result.columns.is_empty() {
            self.set_result_selection(0, 0);
        } else {
            self.selected_result_cell = None;
            self.row_inspector = None;
        }

        self.query_history.insert(
            0,
            HistoryEntry {
                title: history_label,
                sql,
                summary,
            },
        );
        self.query_history.truncate(24);
        self.push_activity(format!(
            "Ran query in {} ms on {}",
            result.duration_ms, connection_name
        ));
    }

    fn add_query_tab(&mut self) {
        let id = self.next_tab_id;
        self.next_tab_id += 1;
        self.query_tabs.push(QueryTab::new(
            id,
            &format!("Query {}", id),
            "SELECT *\nFROM public.orders\nLIMIT 100;",
        ));
        self.active_tab = self.query_tabs.len() - 1;
        self.touch_tab_index(self.active_tab);
        self.clear_result_focus();
        self.push_activity("Opened a new query tab.");
    }

    fn close_tab(&mut self, index: usize) {
        if index >= self.query_tabs.len() {
            return;
        }
        self.query_tabs.remove(index);
        if self.query_tabs.is_empty() {
            if self.next_tab_id > 1 {
                self.next_tab_id -= 1;
            }
            self.add_query_tab();
        } else {
            if self.active_tab >= self.query_tabs.len() {
                self.active_tab = self.query_tabs.len() - 1;
            }
            self.touch_tab_index(self.active_tab);
            self.sync_context_from_active_tab();
        }
    }

    fn close_active_tab(&mut self) {
        self.close_tab(self.active_tab);
    }

    #[allow(dead_code)]
    fn save_active_query_as_bookmark(&mut self) {
        let sql = self.active_tab().sql.clone();
        let title = infer_tab_title(&sql, self.active_tab().id);
        self.bookmarks.insert(
            0,
            SavedQuery {
                name: title.clone(),
                description: "Saved from current tab".to_owned(),
                sql,
            },
        );
        self.bookmarks.truncate(16);
        self.push_activity(format!("Saved bookmark {}", title));
    }

    fn load_sql_into_active_tab(&mut self, title: String, sql: String) {
        let tab = self.active_tab_mut();
        tab.kind = TabKind::Query;
        tab.title = title;
        tab.sql = sql;
        tab.autocomplete_index = 0;
        self.touch_tab_index(self.active_tab);
        self.push_activity("Loaded query into active tab.");
    }

    fn begin_edit_selected_cell(&mut self) {
        if self.editing_cell.is_some() {
            return;
        }

        let Some(selection) = self.selected_result_cell else {
            return;
        };
        let result = &self.active_tab().result;
        if selection.row >= result.rows.len() || selection.col >= result.columns.len() {
            return;
        }

        self.editing_cell = Some(CellEditState {
            row: selection.row,
            col: selection.col,
            value: result.rows[selection.row][selection.col].clone(),
        });
    }

    fn commit_cell_edit(&mut self) {
        let Some(editing) = self.editing_cell.clone() else {
            return;
        };
        self.editing_cell = None;

        let result = &self.active_tab().result;
        if editing.row >= result.rows.len() || editing.col >= result.columns.len() {
            return;
        }

        let original_row = result.rows[editing.row].clone();
        let mut updated_row = original_row.clone();
        updated_row[editing.col] = editing.value.clone();

        if self.is_live_selected_connection() {
            let Some(source) = self.active_tab().result.source.clone() else {
                self.push_activity(
                    "Inline edits are only available for table previews, not ad-hoc queries.",
                );
                return;
            };
            let columns = result.columns.clone();
            self.start_pending_row_update(source, columns, original_row, updated_row, editing.row);
            return;
        }

        // Local (demo) result — update in place
        {
            let tab = self.active_tab_mut();
            if editing.row < tab.result.rows.len() {
                tab.result.rows[editing.row] = updated_row;
            }
        }
        self.sync_row_inspector();
        self.push_activity("Updated cell value in result grid.");
    }

    fn jump_to_foreign_key_target(&mut self, foreign_key: &TableForeignKey, value: &str) {
        let connection_index = match self.active_tab().kind {
            TabKind::Table {
                connection_index, ..
            } => connection_index,
            TabKind::Query => self.selected_connection,
        };
        let Some(connection) = self.connections.get(connection_index) else {
            return;
        };
        let engine = connection.engine;
        let connection_name = connection.name.clone();
        let Some(target_selection) = find_table_selection_by_name(
            connection,
            &foreign_key.referenced_schema,
            &foreign_key.referenced_table,
        ) else {
            self.push_activity(format!(
                "Couldn't find {}.{} in this connection.",
                foreign_key.referenced_schema, foreign_key.referenced_table
            ));
            return;
        };
        let Some(target_table) = connection
            .schemas
            .get(target_selection.schema_index)
            .and_then(|schema| schema.tables.get(target_selection.table_index))
            .cloned()
        else {
            self.push_activity("Referenced table is unavailable right now.");
            return;
        };

        self.selected_connection = connection_index;
        self.select_table(target_selection.schema_index, target_selection.table_index);
        let tab_index = self.ensure_table_tab(connection_index, target_selection, &target_table);
        let row_limit = match self.parsed_table_preview_limit() {
            Ok(limit) => limit,
            Err(error) => {
                self.push_activity(error);
                return;
            }
        };
        let identifier = identifier_for_engine(engine);
        let sql = if target_table.primary_sort.is_empty() {
            format!(
                "SELECT * FROM {}.{} WHERE {} = {}{};",
                identifier(&target_table.schema),
                identifier(&target_table.name),
                identifier(&foreign_key.referenced_column),
                sql_string_literal(value),
                limit_clause_inline(row_limit)
            )
        } else {
            format!(
                "SELECT * FROM {}.{} WHERE {} = {} ORDER BY {} ASC{};",
                identifier(&target_table.schema),
                identifier(&target_table.name),
                identifier(&foreign_key.referenced_column),
                sql_string_literal(value),
                identifier(&target_table.primary_sort),
                limit_clause_inline(row_limit)
            )
        };

        {
            let tab = &mut self.query_tabs[tab_index];
            tab.title = format!("{}.sql", target_table.name);
            tab.sql = sql.clone();
            tab.column_page = 0;
        }
        self.active_tab = tab_index;
        self.touch_tab_index(tab_index);
        self.sync_context_from_active_tab();

        if self.is_live_selected_connection() {
            self.query_tabs[tab_index].result = QueryResult::message(
                "Following foreign key",
                "Loading the referenced row in the background.",
            );
            self.selected_result_cell = None;
            self.row_inspector = None;
            self.queue_active_query(sql, connection_name);
        } else {
            let filtered_rows = target_table
                .rows
                .iter()
                .filter(|row| {
                    target_table
                        .columns
                        .iter()
                        .position(|column| {
                            column
                                .name
                                .eq_ignore_ascii_case(&foreign_key.referenced_column)
                        })
                        .and_then(|index| row.get(index))
                        .map(|cell| cell == value)
                        .unwrap_or(false)
                })
                .take(row_limit.unwrap_or(usize::MAX))
                .cloned()
                .collect::<Vec<_>>();
            let mut result = target_table.preview_result_with_limit(row_limit);
            result.rows = filtered_rows;
            self.query_tabs[tab_index].result = result.clone();
            if !result.rows.is_empty() && !result.columns.is_empty() {
                self.set_result_selection(0, 0);
            }
        }

        self.push_activity(format!(
            "Opened {}.{} for {} = {}",
            target_table.schema, target_table.name, foreign_key.referenced_column, value
        ));
    }

    fn confirm_pending_row_update(&mut self) {
        if let Some(pending) = self.pending_row_update.take() {
            self.queue_row_save(
                pending.source,
                pending.columns,
                pending.original_row,
                pending.updated_row,
                pending.row_index,
            );
        }
    }

    fn cancel_pending_row_update(&mut self) {
        if self.pending_row_update.take().is_some() {
            self.push_activity("Row update preview cancelled.");
        }
    }

    fn start_pending_row_update(
        &mut self,
        source: TableRef,
        columns: Vec<ResultColumn>,
        original_row: Vec<String>,
        updated_row: Vec<String>,
        row_index: usize,
    ) {
        let changed_indices = changed_column_indices(&original_row, &updated_row);
        if changed_indices.is_empty() {
            self.push_activity("No changes were detected in this row.");
            return;
        }
        let key_indices = row_identity_indices(&columns, &original_row);
        if key_indices.is_empty() {
            self.push_activity(
                "Cannot determine a stable key column for this row; update is blocked.",
            );
            return;
        }

        if let Some(sql) = format_update_sql(
            self.connection(),
            &source,
            &columns,
            &original_row,
            &updated_row,
            &changed_indices,
            &key_indices,
        ) {
            self.pending_row_update = Some(PendingRowUpdate {
                source,
                columns,
                original_row,
                updated_row,
                row_index,
                sql,
            });
            self.push_activity("Review the generated UPDATE statement before running.");
        } else {
            self.push_activity("Unable to build a SQL preview for this update.");
        }
    }

    fn save_selected_row(&mut self) {
        if !self.has_connections() {
            self.push_activity("Add a connection before saving rows.");
            return;
        }
        let Some(mut inspector) = self.row_inspector.clone() else {
            self.push_activity("Select a result row before saving.");
            return;
        };

        if !inspector.is_dirty() {
            self.push_activity("No row changes to save.");
            return;
        }

        let result = &self.active_tab().result;
        let Some(source) = result.source.clone() else {
            self.push_activity("Row saving is available only for opened table previews.");
            return;
        };

        if inspector.row >= result.rows.len() || inspector.values.len() != result.columns.len() {
            self.push_activity("Selected row is out of sync. Refresh the table and try again.");
            return;
        }

        if self.is_live_selected_connection() {
            self.start_pending_row_update(
                source,
                result.columns.clone(),
                inspector.original_values,
                inspector.values,
                inspector.row,
            );
            return;
        }

        {
            let tab = self.active_tab_mut();
            if inspector.row < tab.result.rows.len() {
                tab.result.rows[inspector.row] = inspector.values.clone();
            }
        }

        inspector.original_values = inspector.values.clone();
        self.row_inspector = Some(inspector);
        self.push_activity(format!("Saved row to {}.{}", source.schema, source.table));
    }

    fn refresh_active_view(&mut self) {
        if self.connection().is_disconnected {
            let name = self.connection().name.clone();
            self.push_activity(format!(
                "Connection '{}' is disconnected. Click Connect to refresh.",
                name
            ));
            return;
        }

        if !self.has_connections() {
            self.push_activity("Add a connection first.");
            return;
        }
        match self.active_tab().kind.clone() {
            TabKind::Table {
                connection_index,
                table_selection,
                ..
            } => {
                self.selected_connection = connection_index;
                self.selected_table = table_selection;
                self.open_selected_table();
            }
            TabKind::Query => self.run_active_query(),
        }
    }

    fn cancel_cell_edit(&mut self) {
        self.editing_cell = None;
    }

    fn open_connection_manager(&mut self) {
        self.connection_form = ConnectionForm::default();
        self.editing_connection_index = None;
        self.connection_manager_open = true;
    }

    fn open_edit_connection(&mut self, index: usize) {
        let profile = &self.connections[index];
        let (ssh_host, ssh_port, ssh_user, ssh_password, ssh_private_key_path, use_ssh) =
            if let Some(ref ssh) = profile.ssh_tunnel {
                (
                    ssh.host.clone(),
                    ssh.port.to_string(),
                    ssh.user.clone(),
                    ssh.password.clone(),
                    ssh.private_key_path.clone(),
                    true,
                )
            } else {
                (
                    "bastion.internal".to_owned(),
                    "22".to_owned(),
                    "ec2-user".to_owned(),
                    String::new(),
                    "~/.ssh/id_ed25519".to_owned(),
                    false,
                )
            };
        self.connection_form = ConnectionForm {
            connection_url: String::new(),
            name: profile.name.clone(),
            engine: profile.engine.clone(),
            host: profile.host.clone(),
            port: profile.port.to_string(),
            database: profile.database.clone(),
            user: profile.user.clone(),
            password: profile.password.clone(),
            path: profile.path.clone().unwrap_or_default(),
            use_ssh,
            ssh_host,
            ssh_port,
            ssh_user,
            ssh_password,
            ssh_private_key_path,
        };
        self.editing_connection_index = Some(index);
        self.connection_manager_open = true;
    }

    fn delete_connection(&mut self, index: usize) {
        if index >= self.connections.len() {
            return;
        }

        self.save_current_workspace();
        let removed_name = self.connections[index].name.clone();
        self.connections.remove(index);

        let mut shifted_cache = BTreeMap::new();
        for (cache_index, cache_value) in std::mem::take(&mut self.autocomplete_cache) {
            if cache_index == index {
                continue;
            }
            let new_index = if cache_index > index {
                cache_index - 1
            } else {
                cache_index
            };
            shifted_cache.insert(new_index, cache_value);
        }
        self.autocomplete_cache = shifted_cache;

        self.database_list_connection = match self.database_list_connection {
            Some(current) if current == index => {
                self.database_list.clear();
                None
            }
            Some(current) if current > index => Some(current - 1),
            other => other,
        };
        self.database_list_loading = self
            .database_list_loading
            .iter()
            .filter_map(|&current| {
                if current == index {
                    None
                } else if current > index {
                    Some(current - 1)
                } else {
                    Some(current)
                }
            })
            .collect();
        self.schema_loading = self
            .schema_loading
            .iter()
            .filter_map(|&current| {
                if current == index {
                    None
                } else if current > index {
                    Some(current - 1)
                } else {
                    Some(current)
                }
            })
            .collect();

        let mut next_active_workspace = None;
        let previous_active_workspace = self.active_workspace;
        let mut updated_workspaces = Vec::new();
        for (workspace_index, mut workspace) in
            std::mem::take(&mut self.workspaces).into_iter().enumerate()
        {
            if workspace.connection_index == index {
                continue;
            }
            if workspace.connection_index > index {
                workspace.connection_index -= 1;
            }
            reindex_query_tabs_after_connection_delete(&mut workspace.query_tabs, index);
            if workspace.query_tabs.is_empty() {
                let id = self.next_tab_id;
                self.next_tab_id += 1;
                let (title, sql) = self.workspace_seed_tab(workspace.connection_index);
                workspace.query_tabs.push(QueryTab::new(id, &title, &sql));
                workspace.active_tab = 0;
            } else {
                workspace.active_tab = workspace
                    .active_tab
                    .min(workspace.query_tabs.len().saturating_sub(1));
            }
            if previous_active_workspace == Some(workspace_index) {
                next_active_workspace = Some(updated_workspaces.len());
            }
            updated_workspaces.push(workspace);
        }
        self.workspaces = updated_workspaces;

        reindex_query_tabs_after_connection_delete(&mut self.query_tabs, index);

        if self.query_tabs.is_empty() {
            let id = self.next_tab_id;
            self.next_tab_id += 1;
            self.query_tabs.push(QueryTab::new(
                id,
                "scratch.sql",
                "-- Add a connection to get started.\nSELECT 1;",
            ));
            self.active_tab = 0;
        } else if self.active_tab >= self.query_tabs.len() {
            self.active_tab = self.query_tabs.len() - 1;
        }

        self.selected_result_cell = None;
        self.row_inspector = None;
        self.editing_cell = None;
        self.row_inspector_filter.clear();
        self.schema_filter.clear();

        if self.connections.is_empty() {
            self.workspaces.clear();
            self.active_workspace = None;
            self.selected_connection = 0;
            self.selected_table = TableSelection {
                schema_index: 0,
                table_index: 0,
            };
            let tab = self.active_tab_mut();
            tab.kind = TabKind::Query;
            tab.title = "scratch.sql".to_owned();
            tab.sql = "-- Add a connection to get started.\nSELECT 1;".to_owned();
            tab.result = QueryResult::empty();
        } else {
            let next_index = index.min(self.connections.len() - 1);
            if let Some(workspace_index) = next_active_workspace {
                self.restore_workspace(workspace_index);
            } else if !self.workspaces.is_empty() {
                self.restore_workspace(0);
            } else {
                self.open_workspace_for_connection(next_index);
            }
        }

        if let Err(error) = save_custom_connections(&self.connections[demo_connections().len()..]) {
            self.push_activity(format!("Deleted connection with warning: {}", error));
        } else {
            self.push_activity(format!("Deleted connection {}", removed_name));
        }
    }

    fn submit_connection_form(&mut self) {
        if self.connection_form.name.trim().is_empty() {
            self.push_activity("Connection name is required.");
            return;
        }

        if let Some(edit_index) = self.editing_connection_index {
            // Update existing connection in-place, preserving schemas
            let existing_schemas = self.connections[edit_index].schemas.clone();
            let mut updated = self.connection_form.build_profile(self.next_tab_id);
            updated.schemas = existing_schemas;
            let name = updated.name.clone();
            self.connections[edit_index] = updated;
            self.editing_connection_index = None;
            self.connection_manager_open = false;
            if let Err(error) =
                save_custom_connections(&self.connections[demo_connections().len()..])
            {
                self.push_activity(format!("Saved connection with warning: {}", error));
            }
            self.push_activity(format!("Updated connection {}", name));
        } else {
            let profile = self.connection_form.build_profile(self.next_tab_id);
            let name = profile.name.clone();
            self.connections.push(profile);
            self.open_workspace_for_connection(self.connections.len() - 1);
            self.connection_manager_open = false;
            if let Err(error) =
                save_custom_connections(&self.connections[demo_connections().len()..])
            {
                self.push_activity(format!("Saved connection with warning: {}", error));
            }
            self.push_activity(format!("Added connection {}", name));
        }
    }

    fn refresh_connection(&mut self, index: usize) {
        self.queue_schema_refresh(index);
    }

    fn handle_shortcuts(&mut self, ctx: &egui::Context) {
        if self.pending_row_update.is_some() {
            let confirm = ctx.input(|i| i.modifiers.command && i.key_pressed(Key::Enter));
            if confirm {
                self.confirm_pending_row_update();
                return;
            }
            if ctx.input(|i| i.key_pressed(Key::Escape)) {
                self.cancel_pending_row_update();
                return;
            }
        }

        let open_database_switcher =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::D));
        if open_database_switcher && !self.command_palette.open {
            self.open_command_palette(PaletteMode::Databases);
        }

        let open_palette =
            ctx.input(|i| i.modifiers.command && !i.modifiers.shift && i.key_pressed(Key::P));
        if open_palette {
            self.open_command_palette(PaletteMode::All);
        }

        let open_connection_palette =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::F));
        if open_connection_palette && !self.command_palette.open {
            self.open_command_palette(PaletteMode::Connections);
        }

        let open_filter_column_picker =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::K));
        if open_filter_column_picker
            && !self.command_palette.open
            && self.active_tab().filter_mode == ResultFilterMode::Column
            && !self.active_tab().draft_filter_rules.is_empty()
        {
            let initial_highlight = self.active_tab().draft_filter_rules[0]
                .column
                .map(|index| index + 1)
                .unwrap_or(0);
            self.active_tab_mut().draft_filter_rules[0].column_picker_highlight = initial_highlight;
            let popup_id = filter_column_popup_id(self.active_tab().id, 0);
            let search_id = filter_column_search_id(self.active_tab().id, 0);
            ctx.memory_mut(|mem| mem.open_popup(popup_id));
            ctx.memory_mut(|mem| mem.request_focus(search_id));
        }

        let open_schema_diagram =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::G));
        if open_schema_diagram && !self.command_palette.open {
            self.show_schema_diagram = true;
        }

        let toggle_left_sidebar =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::L));
        if toggle_left_sidebar {
            self.left_sidebar_open = !self.left_sidebar_open;
        }

        let toggle_right_sidebar =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::R));
        if toggle_right_sidebar {
            self.right_sidebar_open = !self.right_sidebar_open;
        }

        let new_tab = ctx.input(|i| i.modifiers.command && i.key_pressed(Key::T));
        if new_tab && !self.command_palette.open {
            self.add_query_tab();
        }

        let close_tab = ctx.input(|i| i.modifiers.command && i.key_pressed(Key::W));
        if close_tab && !self.command_palette.open {
            self.close_active_tab();
        }

        let add_connection_shortcut =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::N));
        if add_connection_shortcut {
            self.open_connection_manager();
        }

        let connect_shortcut =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::C));
        if connect_shortcut && self.has_connections() {
            self.connect_to_index(self.selected_connection);
        }

        let edit_connection_shortcut =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::E));
        if edit_connection_shortcut && self.has_connections() {
            self.open_edit_connection(self.selected_connection);
        }

        let delete_connection_shortcut =
            ctx.input(|i| i.modifiers.command && i.modifiers.shift && i.key_pressed(Key::Delete));
        if delete_connection_shortcut && self.has_connections() {
            self.delete_connection(self.selected_connection);
        }

        let run_query = ctx.input(|i| i.modifiers.command && i.key_pressed(Key::Enter));
        if run_query && !self.command_palette.open {
            self.run_active_query();
        }

        let save_row = ctx.input(|i| i.modifiers.command && i.key_pressed(Key::S));
        if save_row && !self.command_palette.open {
            if self.editing_cell.is_some() {
                self.commit_cell_edit();
            } else {
                self.save_selected_row();
            }
        }

        let structure_shortcuts_active = matches!(self.active_tab().kind, TabKind::Table { .. })
            && self.active_tab().table_detail_view == TableDetailView::Structure;
        if structure_shortcuts_active && !self.command_palette.open {
            let copy_structure_row =
                ctx.input(|i| i.modifiers.command && !i.modifiers.shift && i.key_pressed(Key::C));
            if copy_structure_row {
                self.copy_selected_structure_row();
                return;
            }
            let paste_structure_row =
                ctx.input(|i| i.modifiers.command && !i.modifiers.shift && i.key_pressed(Key::V));
            if paste_structure_row {
                self.paste_structure_row();
                return;
            }
        }

        let refresh = ctx.input(|i| i.modifiers.command && i.key_pressed(Key::R));
        if refresh && !self.command_palette.open {
            self.refresh_active_view();
        }

        if self.command_palette.open && ctx.input(|i| i.key_pressed(Key::Escape)) {
            self.command_palette = CommandPalette::default();
        }

        if self.active_tab().filter_mode == ResultFilterMode::Column
            && ctx.input(|i| i.key_pressed(Key::Escape))
        {
            for rule_index in 0..self.active_tab().draft_filter_rules.len() {
                let column_popup_id = filter_column_popup_id(self.active_tab().id, rule_index);
                let operator_popup_id = filter_operator_popup_id(self.active_tab().id, rule_index);
                if ctx.memory(|mem| mem.is_popup_open(column_popup_id))
                    || ctx.memory(|mem| mem.is_popup_open(operator_popup_id))
                {
                    ctx.memory_mut(|mem| mem.close_popup());
                    return;
                }
            }
        }

        if self.editing_cell.is_some() && ctx.input(|i| i.key_pressed(Key::Escape)) {
            self.cancel_cell_edit();
        }

        let filter_popup_open = any_filter_popup_open(
            ctx,
            self.active_tab().id,
            self.active_tab().draft_filter_rules.len(),
        );
        if self.result_grid_has_focus
            && self.editing_cell.is_none()
            && !self.command_palette.open
            && !filter_popup_open
        {
            let (row_len, col_len) = {
                let result = &self.active_tab().result;
                (result.rows.len(), result.columns.len())
            };
            if row_len == 0 || col_len == 0 {
                return;
            }

            let mut selection = self
                .selected_result_cell
                .unwrap_or(CellSelection { row: 0, col: 0 });
            let mut changed = false;
            if ctx.input(|i| i.key_pressed(Key::ArrowDown)) {
                selection.row = (selection.row + 1).min(row_len - 1);
                changed = true;
            }
            if ctx.input(|i| i.key_pressed(Key::ArrowUp)) {
                selection.row = selection.row.saturating_sub(1);
                changed = true;
            }
            if ctx.input(|i| i.key_pressed(Key::ArrowRight)) {
                selection.col = (selection.col + 1).min(col_len - 1);
                changed = true;
            }
            if ctx.input(|i| i.key_pressed(Key::ArrowLeft)) {
                selection.col = selection.col.saturating_sub(1);
                changed = true;
            }
            if changed {
                self.set_result_selection(selection.row, selection.col);
            }
            if ctx.input(|i| i.key_pressed(Key::Enter) || i.key_pressed(Key::E)) {
                self.begin_edit_selected_cell();
            }
        }
    }

    fn autocomplete_suggestions_at(&self, cursor_char_pos: usize) -> Vec<AutocompleteItem> {
        let sql = &self.active_tab().sql;
        let byte_pos = self.cursor_byte_index(cursor_char_pos);
        let sql_to_cursor = &sql[..byte_pos];
        let token = token_at_end(sql_to_cursor);
        let prefix_lower = token.fragment.to_lowercase();
        let selected_table = self.selected_table();
        let mut suggestions = Vec::new();
        let previous_token = previous_token_before(sql_to_cursor, token.start).to_uppercase();
        let table_context = matches!(
            previous_token.as_str(),
            "FROM" | "JOIN" | "UPDATE" | "INTO" | "TABLE"
        );
        let cache = self.autocomplete_cache_for_selected_connection();

        if token.fragment.contains('.') {
            let mut parts = token.fragment.split('.');
            let qualifier = parts.next().unwrap_or_default().to_lowercase();
            let column_prefix = parts.next().unwrap_or_default().to_lowercase();

            if let Some(cache) = cache {
                if let Some(entries) = cache.columns_by_qualifier.get(&qualifier) {
                    for entry in entries {
                        if column_prefix.is_empty()
                            || entry.label_lower.starts_with(&column_prefix)
                            || entry.insert_lower.ends_with(&format!(".{}", column_prefix))
                        {
                            suggestions.push(entry.item.clone());
                        }
                    }
                }
            }
        } else if table_context {
            if let Some(cache) = cache {
                for entry in &cache.table_entries {
                    if autocomplete_matches(entry, &prefix_lower) {
                        suggestions.push(entry.item.clone());
                    }
                }
            }
        } else {
            for keyword in SQL_KEYWORDS {
                if prefix_lower.is_empty() || keyword.to_lowercase().starts_with(&prefix_lower) {
                    suggestions.push(AutocompleteItem {
                        label: (*keyword).to_owned(),
                        insert_text: format!("{} ", keyword),
                        kind: AutocompleteKind::Keyword,
                    });
                }
            }

            if let Some(cache) = cache {
                // Prioritize columns from the selected sidebar table if it matches
                if let Some(selected_table) = &selected_table {
                    for column in &selected_table.columns {
                        if prefix_lower.is_empty()
                            || column.name.to_lowercase().starts_with(&prefix_lower)
                        {
                            suggestions.push(AutocompleteItem {
                                label: column.name.clone(),
                                insert_text: column.name.clone(),
                                kind: AutocompleteKind::ColumnSelected,
                            });
                        }
                    }
                }

                for entry in &cache.table_entries {
                    if autocomplete_matches(entry, &prefix_lower) {
                        suggestions.push(entry.item.clone());
                    }
                }

                // Global column suggestions
                for entry in &cache.all_column_entries {
                    if autocomplete_matches(entry, &prefix_lower) {
                        suggestions.push(entry.item.clone());
                    }
                }
            }
        }

        suggestions.sort_by(|a, b| {
            // Priority 1: Exact matches first
            let a_exact = a.label.to_lowercase() == prefix_lower;
            let b_exact = b.label.to_lowercase() == prefix_lower;
            if a_exact != b_exact {
                return b_exact.cmp(&a_exact);
            }

            // Priority 2: Kind (ColumnSelected > Column > Table > Keyword)
            if a.kind != b.kind {
                return a.kind.cmp(&b.kind);
            }

            // Priority 3: Alphabetical
            a.label.cmp(&b.label)
        });
        suggestions.dedup_by(|left, right| left.insert_text == right.insert_text);
        suggestions.truncate(100);
        suggestions
    }

    fn cursor_byte_index(&self, char_idx: usize) -> usize {
        let sql = &self.active_tab().sql;
        sql.char_indices()
            .nth(char_idx)
            .map(|(i, _)| i)
            .unwrap_or(sql.len())
    }

    fn byte_to_char_index(&self, byte_idx: usize) -> usize {
        let sql = &self.active_tab().sql;
        sql[..byte_idx.min(sql.len())].chars().count()
    }

    fn trigger_jit_column_loading(&mut self) {
        let connection_index = self.selected_connection;
        let Some(conn) = self.connections.get(connection_index) else {
            return;
        };
        let tab = self.active_tab();
        let sql = &tab.sql;

        // Simple regex to find potential table names after FROM, JOIN, UPDATE, INTO, TABLE
        let re = regex::Regex::new(r"(?i)\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+([a-zA-Z_0-9\.]+)")
            .unwrap();

        let mut tables_to_load = Vec::new();
        for cap in re.captures_iter(sql) {
            let full_name = &cap[1];
            let parts: Vec<&str> = full_name.split('.').collect();
            let (schema_name, table_name) = if parts.len() > 1 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                // Try to find the table in any schema if no schema prefix
                let mut found = None;
                for s in &conn.schemas {
                    if s.tables.iter().any(|t| t.name == full_name) {
                        found = Some((s.name.clone(), full_name.to_string()));
                        break;
                    }
                }
                match found {
                    Some(f) => f,
                    None => continue,
                }
            };

            // Only load if we don't have columns yet
            let already_has_columns = conn.schemas.iter().any(|s| {
                s.name == schema_name
                    && s.tables
                        .iter()
                        .any(|t| t.name == table_name && !t.columns.is_empty())
            });

            if !already_has_columns {
                tables_to_load.push((schema_name, table_name));
            }
        }

        for (schema_name, table_name) in tables_to_load {
            let job_id = self.next_job_id;
            self.next_job_id += 1;
            self.active_jobs.insert(job_id);
            self.worker_tx
                .send(BackgroundCommand::LoadTableColumns {
                    job_id,
                    connection_index,
                    schema_name,
                    table_name,
                    profile: conn.clone(),
                })
                .unwrap();
        }
    }

    fn apply_autocomplete(
        &mut self,
        ctx: &egui::Context,
        item: &AutocompleteItem,
        cursor_char_pos: usize,
    ) {
        let byte_pos = self.cursor_byte_index(cursor_char_pos);
        let sql = self.active_tab().sql.clone();
        let sql_to_cursor = &sql[..byte_pos];
        let token = token_at_end(sql_to_cursor);

        let tab = self.active_tab_mut();
        tab.sql
            .replace_range(token.start..byte_pos, &item.insert_text);
        tab.autocomplete_index = 0;

        // Update egui cursor state to point after the inserted text
        let mut state = egui::text_edit::TextEditState::load(ctx, egui::Id::new("sql_editor"))
            .unwrap_or_default();
        let new_byte_pos = token.start + item.insert_text.len();
        let new_char_pos = self.byte_to_char_index(new_byte_pos);

        state
            .cursor
            .set_char_range(Some(egui::text::CCursorRange::one(
                egui::text::CCursor::new(new_char_pos),
            )));
        state.store(ctx, egui::Id::new("sql_editor"));

        // Ensure focus returns to the editor
        ctx.memory_mut(|m| m.request_focus(egui::Id::new("sql_editor")));
    }

    fn palette_items(&self) -> Vec<PaletteItem> {
        let mut items = Vec::new();

        if let Some((connection_index, connection)) = self
            .connections
            .get(self.selected_connection)
            .map(|connection| (self.selected_connection, connection))
        {
            let database_label = if connection.database.trim().is_empty() {
                "No default database".to_owned()
            } else {
                connection.database.clone()
            };
            items.push(PaletteItem {
                title: connection.name.clone(),
                subtitle: format!(
                    "Current connection • {} • {} • {}",
                    connection.engine, connection.host, database_label
                ),
                action: PaletteAction::SelectConnection(connection_index),
            });

            if !connection.is_disconnected {
                for (schema_index, schema) in connection.schemas.iter().enumerate() {
                    for (table_index, table) in schema.tables.iter().enumerate() {
                        items.push(PaletteItem {
                            title: format!("{}.{}", schema.name, table.name),
                            subtitle: format!("Table • {} rows", format_count(table.row_count)),
                            action: PaletteAction::OpenTable {
                                connection_index,
                                schema_index,
                                table_index,
                            },
                        });
                    }
                }
            }
        }

        for entry in &self.query_history {
            items.push(PaletteItem {
                title: entry.title.clone(),
                subtitle: format!("Recent query • {}", entry.summary),
                action: PaletteAction::LoadSql {
                    title: entry.title.clone(),
                    sql: entry.sql.clone(),
                },
            });
        }

        for bookmark in &self.bookmarks {
            items.push(PaletteItem {
                title: bookmark.name.clone(),
                subtitle: format!("Saved query • {}", bookmark.description),
                action: PaletteAction::LoadSql {
                    title: bookmark.name.clone(),
                    sql: bookmark.sql.clone(),
                },
            });
        }

        for snippet in &self.snippets {
            items.push(PaletteItem {
                title: snippet.name.clone(),
                subtitle: format!("Snippet • {}", snippet.description),
                action: PaletteAction::LoadSql {
                    title: snippet.name.clone(),
                    sql: snippet.body.clone(),
                },
            });
        }

        items
    }

    fn connection_palette_items(&self) -> Vec<PaletteItem> {
        let mut items = Vec::new();
        for (connection_index, connection) in self.connections.iter().enumerate() {
            let database_label = if connection.database.trim().is_empty() {
                "No default database".to_owned()
            } else {
                connection.database.clone()
            };
            items.push(PaletteItem {
                title: connection.name.clone(),
                subtitle: format!(
                    "Connection • {} • {} • {}",
                    connection.engine, connection.host, database_label
                ),
                action: PaletteAction::SelectConnection(connection_index),
            });
        }

        items
    }

    fn palette_items_for_mode(&self, mode: PaletteMode) -> Vec<PaletteItem> {
        match mode {
            PaletteMode::All => self.palette_items(),
            PaletteMode::Connections => self.connection_palette_items(),
            PaletteMode::Databases => self
                .database_list
                .iter()
                .map(|db_name| {
                    let is_current = self
                        .connection_opt()
                        .map(|c| c.database == *db_name)
                        .unwrap_or(false);
                    PaletteItem {
                        title: db_name.clone(),
                        subtitle: if is_current {
                            "Current database".to_owned()
                        } else {
                            "Database".to_owned()
                        },
                        action: PaletteAction::SwitchDatabase(db_name.clone()),
                    }
                })
                .collect(),
        }
    }

    fn execute_palette_action(&mut self, action: PaletteAction) {
        match action {
            PaletteAction::SelectConnection(index) => self.connect_to_index(index),
            PaletteAction::OpenTable {
                connection_index,
                schema_index,
                table_index,
            } => {
                self.open_workspace_for_connection(connection_index);
                self.select_table(schema_index, table_index);
                self.open_selected_table();
            }
            PaletteAction::LoadSql { title, sql } => self.load_sql_into_active_tab(title, sql),
            PaletteAction::SwitchDatabase(db_name) => {
                self.switch_database(db_name);
            }
        }

        self.command_palette = CommandPalette::default();
    }

    fn ui_shortcuts_help(&mut self, ctx: &egui::Context) {
        let mut open = self.show_shortcuts_help;
        egui::Window::new("Keyboard Shortcuts")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                egui::Grid::new("shortcuts_grid")
                    .num_columns(2)
                    .spacing([40.0, 8.0])
                    .striped(true)
                    .show(ui, |ui| {
                        let shortcuts = [
                            (
                                "Cmd + Shift + F",
                                "Search connections and press Enter to connect",
                            ),
                            ("Cmd + Shift + C", "Connect to the selected connection"),
                            ("Cmd + Shift + E", "Edit the selected connection"),
                            ("Cmd + Shift + Delete", "Delete the selected connection"),
                            ("Cmd + Shift + N", "Add a new connection"),
                            ("Cmd + Shift + L", "Toggle the left sidebar"),
                            ("Cmd + Shift + R", "Toggle the right sidebar"),
                            ("Cmd + Shift + K", "Open the column filter dropdown"),
                            ("Cmd + Shift + G", "Open schema diagram"),
                            ("Cmd + Shift + D", "Select Database (List)"),
                            ("Cmd + P", "Command Palette / All Actions"),
                            ("Cmd + T", "New Query Tab"),
                            ("Cmd + W", "Close Active Tab"),
                            ("Cmd + Enter", "Run Query"),
                            ("Cmd + S", "Save Row / Commit Cell"),
                            ("Cmd + R", "Refresh View"),
                            ("Esc", "Close Palette / Cancel Edit"),
                        ];

                        for (key, desc) in shortcuts {
                            ui.label(
                                RichText::new(key)
                                    .strong()
                                    .color(Color32::from_rgb(191, 67, 59)),
                            );
                            ui.label(desc);
                            ui.end_row();
                        }
                    });

                ui.add_space(20.0);
                ui.label(RichText::new("Press Esc to close").weak());
            });
        self.show_shortcuts_help = open;
    }

    fn ui_top_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("top_bar")
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(245, 233, 231))
                    .inner_margin(Margin::symmetric(14, 8)),
            )
            .show(ctx, |ui| {
                let connection_summary = self.connection_opt().map(|connection| {
                    let database_label =
                        truncate_middle(&connection_database_label(connection), 18);
                    let tab_title = truncate_middle(&self.active_tab().title, 18);
                    format!(
                        "{}  |  {}  |  {}",
                        connection.engine, database_label, tab_title
                    )
                });
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Sharingan").strong().size(18.0));

                    ui.add_space(16.0);
                    let summary = connection_summary
                        .as_deref()
                        .unwrap_or("No connection selected  |  Add Connection to get started");
                    ui.add_sized(
                        [220.0, 24.0],
                        egui::Label::new(
                            RichText::new(summary)
                                .size(12.0)
                                .color(Color32::from_rgb(88, 88, 88)),
                        )
                        .truncate(),
                    );
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let run_resp = with_shortcut(
                            top_button(ui, "Run", Color32::from_rgb(86, 134, 214)),
                            "Cmd + Enter",
                        );
                        if run_resp.clicked() {
                            self.run_active_query();
                        }

                        let toggle_right_resp = with_shortcut(
                            soft_button(
                                ui,
                                if self.right_sidebar_open {
                                    "Hide Right"
                                } else {
                                    "Show Right"
                                },
                            ),
                            "Cmd + Shift + R",
                        );
                        if toggle_right_resp.clicked() {
                            self.right_sidebar_open = !self.right_sidebar_open;
                        }
                        let toggle_left_resp = with_shortcut(
                            soft_button(
                                ui,
                                if self.left_sidebar_open {
                                    "Hide Left"
                                } else {
                                    "Show Left"
                                },
                            ),
                            "Cmd + Shift + L",
                        );
                        if toggle_left_resp.clicked() {
                            self.left_sidebar_open = !self.left_sidebar_open;
                        }
                        if with_shortcut(
                            top_button(ui, "Add Connection", Color32::from_rgb(232, 235, 241)),
                            "Cmd + Shift + N",
                        )
                        .clicked()
                        {
                            self.open_connection_manager();
                        }
                        if with_shortcut(
                            top_button(ui, "Schema Diagram", Color32::from_rgb(232, 235, 241)),
                            "Cmd + Shift + G",
                        )
                        .clicked()
                        {
                            self.show_schema_diagram = true;
                        }
                        if with_shortcut(
                            top_button(ui, "New Tab", Color32::from_rgb(232, 235, 241)),
                            "Cmd + T",
                        )
                        .clicked()
                        {
                            self.add_query_tab();
                        }
                    });
                });
            });
    }

    fn ui_left_sidebar(&mut self, ctx: &egui::Context) {
        if !self.left_sidebar_open {
            return;
        }

        let mut clicked_connection: Option<usize> = None;
        let mut connect_connection: Option<usize> = None;
        let mut clicked_disconnect: Option<usize> = None;
        let mut delete_selected_connection = false;
        let mut clicked_table: Option<(usize, usize, bool)> = None;
        let mut edit_connection: Option<usize> = None;
        egui::SidePanel::left("left_sidebar")
            .default_width(250.0)
            .width_range(190.0..=320.0)
            .resizable(true)
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(247, 246, 244))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(218, 214, 210)))
                    .inner_margin(Margin::symmetric(10, 10)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading(
                        RichText::new("Connections")
                            .size(18.0)
                            .color(Color32::from_rgb(44, 53, 66)),
                    );
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if with_shortcut(soft_button(ui, "+ New"), "Cmd + Shift + N").clicked() {
                            self.open_connection_manager();
                        }
                    });
                });
                ui.label(
                    RichText::new(format!("{} profiles", self.connections.len()))
                        .color(Color32::from_rgb(120, 129, 140)),
                );
                if self.has_connections() {
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if let Some(conn) = self.connection_opt() {
                            if conn.is_disconnected || conn.schemas.is_empty() {
                                if with_shortcut(soft_button(ui, "Connect"), "Cmd + Shift + C")
                                    .clicked()
                                {
                                    connect_connection = Some(self.selected_connection);
                                }
                            } else {
                                if ui
                                    .button(
                                        RichText::new("Disconnect")
                                            .size(12.0)
                                            .color(Color32::from_rgb(180, 60, 50)),
                                    )
                                    .on_hover_text("Disconnect and release resources")
                                    .clicked()
                                {
                                    clicked_disconnect = Some(self.selected_connection);
                                }
                            }
                        }

                        if with_shortcut(soft_button(ui, "Edit"), "Cmd + Shift + E").clicked() {
                            edit_connection = Some(self.selected_connection);
                        }
                        if with_shortcut(soft_button(ui, "Delete"), "Cmd + Shift + Delete")
                            .clicked()
                        {
                            delete_selected_connection = true;
                        }
                    });
                }
                ui.add_space(10.0);

                egui::ScrollArea::vertical().show(ui, |ui| {
                    for index in 0..self.connections.len() {
                        let connection = &self.connections[index];
                        let selected = index == self.selected_connection;
                        let fill = if selected {
                            Color32::from_rgb(252, 243, 242)
                        } else {
                            Color32::from_rgb(255, 255, 255)
                        };

                        let response = egui::Frame::default()
                            .fill(fill)
                            .stroke(egui::Stroke::new(
                                1.0,
                                if selected {
                                    Color32::from_rgb(191, 67, 59)
                                } else {
                                    Color32::from_rgb(223, 219, 215)
                                },
                            ))
                            .corner_radius(4.0)
                            .inner_margin(Margin::symmetric(8, 4))
                            .show(ui, |ui| {
                                let connection_name = truncate_middle(&connection.name, 28);
                                ui.horizontal(|ui| {
                                    let name_color = if connection.is_disconnected {
                                        Color32::from_rgb(120, 120, 120)
                                    } else {
                                        Color32::from_rgb(32, 43, 57)
                                    };

                                    ui.add(
                                        egui::Label::new(
                                            RichText::new(connection_name)
                                                .strong()
                                                .size(14.0)
                                                .color(name_color),
                                        )
                                        .wrap_mode(egui::TextWrapMode::Truncate),
                                    );

                                    if connection.is_disconnected {
                                        ui.add_space(4.0);
                                        ui.label(
                                            RichText::new("(offline)")
                                                .size(11.0)
                                                .color(Color32::from_rgb(180, 60, 50))
                                                .italics(),
                                        );
                                    } else {
                                        ui.add_space(4.0);
                                        ui.add(egui::Label::new(
                                            RichText::new("•").color(Color32::from_rgb(0, 180, 0)),
                                        ));
                                    }
                                });
                            })
                            .response
                            .interact(Sense::click());

                        if response.clicked() {
                            clicked_connection = Some(index);
                        }
                        if response.double_clicked() {
                            connect_connection = Some(index);
                        }
                        ui.add_space(6.0);
                    }
                });

                if self.has_connections() && !self.database_list.is_empty() {
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        ui.heading(
                            RichText::new("Databases")
                                .size(18.0)
                                .color(Color32::from_rgb(44, 53, 66)),
                        );
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            ui.label(
                                RichText::new(format!("{} listed", self.database_list.len()))
                                    .color(Color32::from_rgb(120, 129, 140)),
                            );
                        });
                    });
                    ui.add_space(4.0);
                    ui.add(
                        egui::TextEdit::singleline(&mut self.database_list_filter)
                            .hint_text("Filter databases...")
                            .desired_width(f32::INFINITY),
                    );
                    ui.add_space(4.0);

                    egui::ScrollArea::vertical()
                        .id_salt("database_list_scroll")
                        .max_height(220.0)
                        .show(ui, |ui| {
                            let filter = self.database_list_filter.to_lowercase();
                            let current_db = self
                                .connection_opt()
                                .map(|c| c.database.clone())
                                .unwrap_or_default();
                            let mut next_db: Option<String> = None;

                            for db_name in &self.database_list {
                                if !filter.is_empty() && !db_name.to_lowercase().contains(&filter) {
                                    continue;
                                }

                                let selected = *db_name == current_db;
                                let text_color = if selected {
                                    Color32::from_rgb(191, 67, 59)
                                } else {
                                    Color32::from_rgb(44, 53, 66)
                                };

                                let resp = ui.add(egui::SelectableLabel::new(
                                    selected,
                                    RichText::new(db_name).color(text_color),
                                ));

                                if resp.clicked() {
                                    next_db = Some(db_name.clone());
                                }
                            }

                            if let Some(db) = next_db {
                                self.switch_database(db);
                            }
                        });
                }

                ui.add_space(10.0);
                ui.separator();
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    ui.heading(
                        RichText::new("Schema")
                            .size(18.0)
                            .color(Color32::from_rgb(44, 53, 66)),
                    );
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if with_shortcut(soft_button(ui, "Diagram"), "Cmd + Shift + G").clicked() {
                            self.show_schema_diagram = true;
                        }
                        ui.add_space(8.0);
                        let table_count = self
                            .connection_opt()
                            .map(|connection| {
                                connection
                                    .schemas
                                    .iter()
                                    .map(|schema| schema.tables.len())
                                    .sum::<usize>()
                            })
                            .unwrap_or(0);
                        ui.label(
                            RichText::new(format!("{} tables", table_count))
                                .color(Color32::from_rgb(120, 129, 140)),
                        );
                    });
                });

                if !self.has_connections() {
                    ui.add_space(10.0);
                    ui.label(
                        RichText::new("No connections yet. Add one to load schemas and tables.")
                            .color(Color32::from_rgb(120, 129, 140)),
                    );
                    return;
                }

                if self.connection().is_disconnected {
                    ui.add_space(20.0);
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new("Disconnected")
                                .size(16.0)
                                .strong()
                                .color(Color32::from_rgb(108, 119, 133)),
                        );
                        ui.add_space(8.0);
                        ui.label(
                            RichText::new("Click 'Connect' to load schemas and enable queries.")
                                .color(Color32::from_rgb(120, 129, 140)),
                        );
                    });
                    return;
                }

                if self.schema_loading.contains(&self.selected_connection) {
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        ui.add(egui::Spinner::new().size(14.0));
                        ui.label(
                            RichText::new("Loading schema...")
                                .strong()
                                .color(Color32::from_rgb(112, 122, 139)),
                        );
                    });
                    ui.add_space(6.0);
                    ui.label(
                        RichText::new("You can keep using the app while tables are loading.")
                            .color(Color32::from_rgb(112, 122, 139)),
                    );
                    return;
                }

                ui.add_space(8.0);
                ui.add(
                    TextEdit::singleline(&mut self.schema_filter)
                        .hint_text("Filter tables or schemas")
                        .desired_width(f32::INFINITY),
                );
                ui.add_space(8.0);

                let filter = self.schema_filter.to_lowercase();
                let schema_len = self.connection().schemas.len();
                egui::ScrollArea::vertical().show(ui, |ui| {
                    for schema_index in 0..schema_len {
                        let schema = &self.connection().schemas[schema_index];
                        let visible_tables = schema
                            .tables
                            .iter()
                            .enumerate()
                            .filter(|(_, table)| {
                                filter.is_empty()
                                    || schema.name.to_lowercase().contains(&filter)
                                    || table.name.to_lowercase().contains(&filter)
                            })
                            .collect::<Vec<_>>();

                        if visible_tables.is_empty() {
                            continue;
                        }

                        egui::CollapsingHeader::new(
                            RichText::new(format!("{} ({})", schema.name, schema.tables.len()))
                                .color(Color32::from_rgb(72, 82, 96)),
                        )
                        .default_open(false)
                        .show(ui, |ui| {
                            for (table_index, table) in visible_tables {
                                let selected = self.selected_table.schema_index == schema_index
                                    && self.selected_table.table_index == table_index;
                                let label =
                                    format!("{}  •  {}", table.name, format_count(table.row_count));
                                let response = ui.selectable_label(selected, label);
                                if response.clicked() {
                                    clicked_table = Some((schema_index, table_index, false));
                                }
                                if response.double_clicked() {
                                    clicked_table = Some((schema_index, table_index, true));
                                }
                            }
                        });
                    }
                });
            });

        if let Some(index) = clicked_disconnect {
            self.last_disconnect_time = Some(std::time::Instant::now());
            self.disconnect_connection(index);
        } else if let Some(index) = connect_connection {
            self.connect_to_index(index);
        } else if let Some(index) = clicked_connection {
            self.open_workspace_for_connection(index);
        }
        if let Some(index) = edit_connection {
            self.open_edit_connection(index);
        }
        if delete_selected_connection && self.has_connections() {
            self.delete_connection(self.selected_connection);
        }
        if let Some((schema_index, table_index, open)) = clicked_table {
            self.select_table(schema_index, table_index);
            if open {
                self.open_selected_table();
            }
        }
    }

    fn ui_right_sidebar(&mut self, ctx: &egui::Context) {
        if !self.right_sidebar_open {
            return;
        }

        egui::SidePanel::right("right_sidebar")
            .default_width(300.0)
            .width_range(240.0..=380.0)
            .resizable(true)
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(247, 246, 244))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(218, 214, 210)))
                    .inner_margin(Margin::symmetric(10, 10)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading(
                        RichText::new("Selected Row")
                            .size(18.0)
                            .color(Color32::from_rgb(44, 53, 66)),
                    );
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        chip(
                            ui,
                            "Row details",
                            Color32::from_rgb(235, 239, 246),
                            Color32::from_rgb(93, 104, 117),
                        );
                    });
                });
                ui.add_space(10.0);
                self.ui_inspector(ui);
            });
    }

    fn ui_inspector(&mut self, ui: &mut egui::Ui) {
        self.sync_row_inspector();
        let mut inspector = self.row_inspector.take();
        let mut save_row = false;
        let mut refresh = false;
        let table = self.selected_table().cloned();
        let table_label = match &table {
            Some(table) => format!("{}.{}", table.schema, table.name),
            None => "No table selected".to_owned(),
        };
        let connection_label = self
            .connection_opt()
            .map(|connection| {
                format!(
                    "{} • {}",
                    connection.name,
                    connection_database_label(connection)
                )
            })
            .unwrap_or_else(|| "No connection selected".to_owned());

        ui.horizontal_top(|ui| {
            ui.vertical(|ui| {
                ui.add_sized(
                    [ui.available_width().min(220.0), 18.0],
                    egui::Label::new(
                        RichText::new(truncate_middle(&table_label, 28))
                            .size(15.0)
                            .strong()
                            .color(Color32::from_rgb(54, 63, 76)),
                    )
                    .truncate(),
                );
                ui.add_sized(
                    [ui.available_width().min(240.0), 16.0],
                    egui::Label::new(
                        RichText::new(truncate_middle(&connection_label, 34))
                            .size(12.0)
                            .color(Color32::from_rgb(117, 126, 137)),
                    )
                    .truncate(),
                );
            });
            ui.with_layout(Layout::right_to_left(Align::TOP), |ui| {
                if soft_button(ui, "Refresh").clicked() {
                    refresh = true;
                }
                if soft_button(ui, "Save Row").clicked() {
                    save_row = true;
                }
            });
        });
        ui.add_space(8.0);
        ui.add_sized(
            [ui.available_width(), 30.0],
            TextEdit::singleline(&mut self.row_inspector_filter).hint_text("Search columns"),
        );
        ui.add_space(8.0);
        chip(
            ui,
            "Cmd+S saves • Cmd+R refreshes",
            Color32::from_rgb(241, 244, 250),
            Color32::from_rgb(92, 102, 114),
        );
        ui.add_space(8.0);

        let Some(state) = inspector.as_mut() else {
            inspector_card(
                ui,
                "Selected Row",
                &[
                    "Use the selector button at the start of a result row to inspect it here."
                        .to_owned(),
                ],
            );
            if refresh {
                self.refresh_active_view();
            }
            return;
        };

        let dirty_columns = state
            .values
            .iter()
            .zip(state.original_values.iter())
            .filter(|(left, right)| left != right)
            .count();
        let result_columns = &self.active_tab().result.columns;
        let result_column_count = result_columns.len();
        inspector_card(
            ui,
            "Selected Row",
            &[
                format!("Row: {}", state.row + 1),
                format!("Columns: {}", result_column_count),
                format!("Changed fields: {}", dirty_columns),
            ],
        );
        ui.add_space(8.0);

        let filter = self.row_inspector_filter.to_lowercase();
        let column_meta = table
            .as_ref()
            .map(|table| {
                table
                    .columns
                    .iter()
                    .map(|column| (column.name.to_lowercase(), column.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let matching_indices = result_columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                (filter.is_empty() || column.name.to_lowercase().contains(&filter)).then_some(index)
            })
            .collect::<Vec<_>>();
        let total_matching = matching_indices.len();
        let show_compact = filter.is_empty() && !self.row_inspector_expanded;
        let mut inspector_jump: Option<(TableForeignKey, String)> = None;
        let visible_indices = if show_compact {
            matching_indices
                .iter()
                .take(ROW_INSPECTOR_CARD_LIMIT)
                .copied()
                .collect::<Vec<_>>()
        } else {
            matching_indices.clone()
        };

        ui.horizontal(|ui| {
            ui.label(
                RichText::new(format!(
                    "Showing {} of {}",
                    visible_indices.len(),
                    total_matching
                ))
                .color(Color32::from_rgb(117, 126, 137)),
            );
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if total_matching > ROW_INSPECTOR_CARD_LIMIT {
                    let label = if self.row_inspector_expanded {
                        "Compact"
                    } else {
                        "Show All"
                    };
                    if soft_button(ui, label).clicked() {
                        self.row_inspector_expanded = !self.row_inspector_expanded;
                    }
                }
            });
        });
        ui.add_space(8.0);

        let result_columns = &self.active_tab().result.columns;
        let total_visible = visible_indices.len();
        egui::ScrollArea::vertical().show_rows(ui, 86.0, total_visible, |ui, row_range| {
            for visible_row in row_range {
                let index = visible_indices[visible_row];
                let column = &result_columns[index];
                let meta = column_meta.get(&column.name.to_lowercase());
                let foreign_key = table
                    .as_ref()
                    .and_then(|table| result_column_foreign_key(table, &column.name));
                let changed = state
                    .original_values
                    .get(index)
                    .zip(state.values.get(index))
                    .map(|(left, right)| left != right)
                    .unwrap_or(false);

                egui::Frame::default()
                    .fill(Color32::from_rgb(255, 255, 255))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(220, 216, 212)))
                    .corner_radius(2.0)
                    .inner_margin(Margin::same(8))
                    .show(ui, |ui| {
                        ui.label(
                            RichText::new(&column.name)
                                .size(12.0)
                                .color(Color32::from_rgb(110, 110, 110)),
                        );
                        ui.add_space(4.0);
                        ui.horizontal(|ui| {
                            if let Some(meta) = meta {
                                ui.label(
                                    RichText::new(&meta.kind)
                                        .size(11.0)
                                        .color(Color32::from_rgb(146, 146, 146)),
                                );
                            }
                            if changed {
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    ui.label(
                                        RichText::new("edited")
                                            .size(11.0)
                                            .color(Color32::from_rgb(185, 69, 59)),
                                    );
                                });
                            }
                        });
                        ui.add_space(6.0);

                        if let Some(value) = state.values.get_mut(index) {
                            let value: &mut String = value;
                            let enum_options =
                                meta.map(enum_options_for_column).unwrap_or_default();
                            if enum_options.is_empty() {
                                ui.add_sized(
                                    [ui.available_width(), 24.0],
                                    TextEdit::singleline(value)
                                        .desired_width(f32::INFINITY)
                                        .margin(Vec2::new(6.0, 2.0)),
                                );
                            } else {
                                ui.horizontal(|ui| {
                                    egui::ComboBox::from_id_salt((
                                        "inspector_enum",
                                        state.row,
                                        index,
                                    ))
                                    .selected_text(value.clone())
                                    .width((ui.available_width() - 26.0).max(120.0))
                                    .show_ui(ui, |ui| {
                                        for option in &enum_options {
                                            ui.selectable_value(value, option.clone(), option);
                                        }
                                    });
                                    ui.label(
                                        RichText::new("v")
                                            .size(11.0)
                                            .color(Color32::from_rgb(117, 126, 137)),
                                    );
                                });
                            }
                            if let Some(foreign_key) = foreign_key {
                                if !value.trim().is_empty() && !value.eq_ignore_ascii_case("NULL") {
                                    ui.add_space(4.0);
                                    if soft_button(
                                        ui,
                                        &format!(
                                            "Open {}.{} ->",
                                            foreign_key.referenced_schema,
                                            foreign_key.referenced_table
                                        ),
                                    )
                                    .clicked()
                                    {
                                        inspector_jump = Some((foreign_key.clone(), value.clone()));
                                    }
                                }
                            }
                        }

                        let subtitle = match meta {
                            Some(meta) if meta.nullable => "Nullable",
                            Some(_) => "Required",
                            None => "Value from current result row",
                        };
                        ui.label(
                            RichText::new(subtitle)
                                .size(11.0)
                                .color(Color32::from_rgb(146, 146, 146)),
                        );
                    });
                ui.add_space(6.0);
            }

            if total_matching == 0 {
                ui.label(
                    RichText::new("No columns match that search.")
                        .color(Color32::from_rgb(117, 126, 137)),
                );
            }
        });

        self.row_inspector = inspector;
        if let Some((foreign_key, value)) = inspector_jump {
            self.jump_to_foreign_key_target(&foreign_key, &value);
        }
        if save_row {
            self.save_selected_row();
        }
        if refresh {
            self.refresh_active_view();
        }
    }

    fn ui_center_workspace(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default()
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(247, 246, 244))
                    .inner_margin(Margin::same(8)),
            )
            .show(ctx, |ui| {
                if !self.has_connections() {
                    egui::Frame::default()
                        .fill(Color32::from_rgb(255, 255, 255))
                        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(216, 212, 208)))
                        .corner_radius(6.0)
                        .inner_margin(Margin::same(24))
                        .show(ui, |ui| {
                            ui.vertical_centered(|ui| {
                                ui.add_space(40.0);
                                ui.heading(
                                    RichText::new("No connections configured")
                                        .size(24.0)
                                        .color(Color32::from_rgb(38, 47, 60)),
                                );
                                ui.add_space(10.0);
                                ui.label(
                                    RichText::new(
                                        "Add a database connection to start browsing schemas, opening tables, and running queries.",
                                    )
                                    .color(Color32::from_rgb(115, 124, 136)),
                                );
                                ui.add_space(18.0);
                                if top_button(
                                    ui,
                                    "Add Connection",
                                    Color32::from_rgb(67, 133, 245),
                                )
                                .clicked()
                                {
                                    self.open_connection_manager();
                                }
                                ui.add_space(40.0);
                            });
                        });
                    return;
                }

                self.ui_workspace_strip(ui);
                ui.add_space(8.0);
                self.ui_tab_strip(ui);
                ui.add_space(8.0);

                match self.active_tab().kind {
                    TabKind::Table { .. } => {
                        self.ui_results(ui, ctx);
                    }
                    TabKind::Query => {
                        egui::TopBottomPanel::bottom("results_dock")
                            .resizable(true)
                            .default_height(300.0)
                            .min_height(180.0)
                            .show_inside(ui, |ui| {
                                self.ui_results(ui, ctx);
                            });

                        ui.add_space(10.0);
                        self.ui_editor(ui, ctx);
                    }
                }
            });
    }

    fn ui_workspace_strip(&mut self, ui: &mut egui::Ui) {
        if self.workspaces.is_empty() {
            return;
        }

        let mut clicked_workspace: Option<usize> = None;
        egui::ScrollArea::horizontal()
            .id_salt("workspace_tab_strip_scroll")
            .max_height(38.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    for workspace_index in 0..self.workspaces.len() {
                        let workspace = &self.workspaces[workspace_index];
                        let Some(connection) = self.connections.get(workspace.connection_index)
                        else {
                            continue;
                        };
                        let selected = self.active_workspace == Some(workspace_index);
                        let fill = if selected {
                            Color32::from_rgb(255, 255, 255)
                        } else {
                            Color32::from_rgb(238, 236, 233)
                        };
                        let label = if connection.is_disconnected {
                            format!("{} (offline)", truncate_middle(&connection.name, 24))
                        } else {
                            truncate_middle(&connection.name, 24)
                        };
                        let response = ui.add_sized(
                            [190.0, 30.0],
                            egui::Button::new(
                                RichText::new(label)
                                    .size(13.0)
                                    .color(Color32::from_rgb(54, 63, 77)),
                            )
                            .fill(fill)
                            .stroke(egui::Stroke::new(
                                1.0,
                                if selected {
                                    Color32::from_rgb(191, 67, 59)
                                } else {
                                    Color32::from_rgb(214, 210, 206)
                                },
                            ))
                            .corner_radius(6.0),
                        );
                        if response.clicked() {
                            clicked_workspace = Some(workspace.connection_index);
                        }
                    }
                });
            });

        if let Some(connection_index) = clicked_workspace {
            self.open_workspace_for_connection(connection_index);
        }
    }

    fn ui_tab_strip(&mut self, ui: &mut egui::Ui) {
        let mut clicked_tab: Option<usize> = None;
        let mut tab_to_close: Option<usize> = None;
        let mut add_tab = false;
        ui.horizontal(|ui| {
            egui::ScrollArea::horizontal()
                .id_salt("query_tab_strip_scroll")
                .max_height(36.0)
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        for index in 0..self.query_tabs.len() {
                            let title = truncate_middle(&self.query_tabs[index].title, 28);
                            let selected = index == self.active_tab;
                            let fill = if selected {
                                Color32::from_rgb(255, 255, 255)
                            } else {
                                Color32::from_rgb(232, 230, 227)
                            };
                            ui.scope(|ui| {
                                ui.spacing_mut().item_spacing.x = 0.0;
                                let resp = ui.add_sized(
                                    [160.0, 28.0],
                                    egui::Button::new(
                                        RichText::new(title)
                                            .color(Color32::from_rgb(69, 69, 69))
                                            .size(13.0),
                                    )
                                    .fill(fill)
                                    .stroke(egui::Stroke::new(
                                        1.0,
                                        if selected {
                                            Color32::from_rgb(201, 197, 192)
                                        } else {
                                            Color32::from_rgb(214, 210, 206)
                                        },
                                    ))
                                    .corner_radius(
                                        egui::CornerRadius {
                                            nw: 3,
                                            ne: 0,
                                            se: 0,
                                            sw: 3,
                                        },
                                    ),
                                );
                                if resp.clicked() {
                                    clicked_tab = Some(index);
                                }
                                let close_resp = ui.add_sized(
                                    [30.0, 28.0],
                                    egui::Button::new(RichText::new("x").size(14.0))
                                        .fill(fill)
                                        .stroke(egui::Stroke::new(
                                            1.0,
                                            if selected {
                                                Color32::from_rgb(201, 197, 192)
                                            } else {
                                                Color32::from_rgb(214, 210, 206)
                                            },
                                        ))
                                        .corner_radius(egui::CornerRadius {
                                            nw: 0,
                                            ne: 3,
                                            se: 3,
                                            sw: 0,
                                        }),
                                );
                                if close_resp.clicked() {
                                    tab_to_close = Some(index);
                                }
                            });
                        }
                    });
                });

            add_tab = ui
                .add(
                    egui::Button::new(RichText::new("+").size(16.0))
                        .fill(Color32::from_rgb(236, 233, 230))
                        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(214, 210, 206)))
                        .corner_radius(3.0),
                )
                .clicked();
        });
        if let Some(index) = tab_to_close {
            self.close_tab(index);
        }
        if add_tab {
            self.add_query_tab();
        }
        if let Some(index) = clicked_tab {
            self.active_tab = index;
            self.touch_tab_index(index);
            self.sync_context_from_active_tab();
            self.restore_active_tab_if_needed();
        }
    }

    fn ui_editor(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        let table = self.selected_table().cloned();
        let mut suggestions = Vec::new();
        let mut apply_suggestion: Option<(AutocompleteItem, usize)> = None;
        let mut current_cursor_pos = self.active_tab().sql.len();
        if let Some(state) = egui::text_edit::TextEditState::load(ctx, egui::Id::new("sql_editor"))
        {
            current_cursor_pos = state
                .cursor
                .char_range()
                .map(|r| r.primary.index)
                .unwrap_or(self.active_tab().sql.len());
        }
        let mut run_query = false;

        let mut editor_rect = None;
        let mut show_autocomplete = false;

        egui::Frame::default()
            .fill(Color32::from_rgb(255, 255, 255))
            .stroke(egui::Stroke::new(1.0, Color32::from_rgb(216, 212, 208)))
            .corner_radius(3.0)
            .inner_margin(Margin::same(10))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.heading(
                        RichText::new(match &table {
                            Some(table) => format!("Editing {}.{}", table.schema, table.name),
                            None => "Editing query".to_owned(),
                        })
                        .size(18.0)
                        .color(Color32::from_rgb(38, 47, 60)),
                    );
                    ui.label(
                        RichText::new(match &table {
                            Some(table) => format!(
                                "{} rows • {} columns",
                                format_count(table.row_count),
                                table.columns.len()
                            ),
                            None => "No table loaded".to_owned(),
                        })
                        .color(Color32::from_rgb(115, 124, 136)),
                    );
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if with_shortcut(soft_button(ui, "Run Query"), "Cmd + Enter").clicked() {
                            run_query = true;
                        }
                    });
                });

                ui.add_space(6.0);

                if self.autocomplete_open {
                    if ctx.input_mut(|i| i.consume_key(egui::Modifiers::NONE, Key::ArrowDown)) {
                        let len = self.autocomplete_suggestions_at(current_cursor_pos).len();
                        let tab = self.active_tab_mut();
                        tab.autocomplete_index =
                            (tab.autocomplete_index + 1).min(len.saturating_sub(1));
                    }
                    if ctx.input_mut(|i| i.consume_key(egui::Modifiers::NONE, Key::ArrowUp)) {
                        let tab = self.active_tab_mut();
                        tab.autocomplete_index = tab.autocomplete_index.saturating_sub(1);
                    }
                    let accepted = ctx.input_mut(|i| {
                        i.consume_key(egui::Modifiers::NONE, Key::Tab)
                            || i.consume_key(egui::Modifiers::NONE, Key::Enter)
                    });
                    if accepted {
                        let s = self.autocomplete_suggestions_at(current_cursor_pos);
                        if !s.is_empty() {
                            let idx = self.active_tab().autocomplete_index.min(s.len() - 1);
                            apply_suggestion = Some((s[idx].clone(), current_cursor_pos));
                        }
                    }
                    if ctx.input_mut(|i| i.consume_key(egui::Modifiers::NONE, Key::Escape)) {
                        self.autocomplete_open = false;
                        self.active_tab_mut().autocomplete_index = 0;
                    }
                    if ctx.input_mut(|i| i.consume_key(egui::Modifiers::COMMAND, Key::C)) {
                        let s = self.autocomplete_suggestions_at(current_cursor_pos);
                        if !s.is_empty() {
                            let idx = self.active_tab().autocomplete_index.min(s.len() - 1);
                            ctx.copy_text(s[idx].label.clone());
                        }
                    }
                }

                let mut cursor_pos = egui::Pos2::ZERO;
                let output = TextEdit::multiline(&mut self.active_tab_mut().sql)
                    .id(egui::Id::new("sql_editor"))
                    .font(egui::TextStyle::Monospace)
                    .code_editor()
                    .desired_width(ui.available_width())
                    .desired_rows(12)
                    .show(ui);

                editor_rect = Some(output.response.rect);
                if let Some(range) = output.cursor_range {
                    // Use the galley and galley_pos for absolutely accurate cursor position
                    let cursor_rect = output.galley.pos_from_cursor(&range.primary);
                    cursor_pos = output.galley_pos + cursor_rect.left_top().to_vec2();
                }

                if output.response.has_focus() {
                    self.result_grid_has_focus = false;

                    // Use cursor position for accurate token extraction
                    suggestions = self.autocomplete_suggestions_at(current_cursor_pos);
                    show_autocomplete = !suggestions.is_empty();
                    self.autocomplete_open = show_autocomplete;
                    if show_autocomplete {
                        self.trigger_jit_column_loading();
                    } else {
                        self.active_tab_mut().autocomplete_index = 0;
                    }
                } else {
                    self.autocomplete_open = false;
                }

                if show_autocomplete {
                    if let Some(rect) = editor_rect {
                        let popup_width = rect.width().min(420.0).max(320.0);
                        let popup_pos = autocomplete_popup_position(cursor_pos, rect, popup_width);

                        let selected_index = self
                            .active_tab()
                            .autocomplete_index
                            .min(suggestions.len().saturating_sub(1));

                        egui::Area::new("sql_autocomplete_dropdown".into())
                            .order(egui::Order::Foreground)
                            .fixed_pos(popup_pos)
                            .show(ctx, |ui| {
                                egui::Frame::default()
                                    .fill(Color32::from_rgb(255, 255, 255))
                                    .corner_radius(4.0)
                                    .stroke(egui::Stroke::new(
                                        1.0,
                                        Color32::from_rgb(200, 200, 200),
                                    ))
                                    .shadow(egui::Shadow {
                                        offset: [0, 4],
                                        blur: 8,
                                        spread: 0,
                                        color: Color32::from_black_alpha(20),
                                    })
                                    .inner_margin(egui::Margin::symmetric(0, 4))
                                    .show(ui, |ui| {
                                        ui.set_width(popup_width);
                                        egui::ScrollArea::vertical()
                                            .max_height(450.0)
                                            .id_salt("sql_autocomplete_scroll")
                                            .show(ui, |ui| {
                                                for (i, suggestion) in
                                                    suggestions.iter().enumerate()
                                                {
                                                    let is_selected = i == selected_index;
                                                    let response = egui::Frame::default()
                                                        .fill(if is_selected {
                                                            Color32::from_rgb(230, 240, 255)
                                                        } else {
                                                            Color32::TRANSPARENT
                                                        })
                                                        .inner_margin(Margin::symmetric(8, 4))
                                                        .show(ui, |ui| {
                                                            ui.label(
                                                                RichText::new(&suggestion.label)
                                                                    .color(Color32::BLACK),
                                                            )
                                                        })
                                                        .response;

                                                    let response = ui.interact(
                                                        response.rect,
                                                        response.id,
                                                        Sense::click(),
                                                    );
                                                    if response.clicked() {
                                                        apply_suggestion = Some((
                                                            suggestion.clone(),
                                                            current_cursor_pos,
                                                        ));
                                                    }
                                                    if is_selected {
                                                        response.scroll_to_me(None);
                                                    }
                                                }
                                            });
                                    });
                            });
                    }
                }
            });

        if let Some((item, pos)) = apply_suggestion {
            self.apply_autocomplete(ctx, &item, pos);
        }
        if run_query {
            self.run_active_query();
        }
    }

    fn ui_results(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        let selected = self.selected_result_cell;
        let selected_row = selected.map(|cell| cell.row);
        let editing = self.editing_cell.clone();
        let mut clicked_cell: Option<CellSelection> = None;
        let mut clicked_row: Option<usize> = None;
        let mut start_edit: Option<CellSelection> = None;
        let mut finish_edit = false;
        let mut draft = editing.clone();
        let available_height = ui.available_height();
        let mut apply_raw_sql_filter = false;
        let mut clear_filters = false;
        let mut apply_all_filters = false;
        let mut add_filter_rule = false;
        let mut remove_filter_rule: Option<usize> = None;
        let mut reload_table_preview = false;
        let mut switch_to_data = false;
        let mut switch_to_structure = false;
        let mut foreign_key_jump: Option<(TableForeignKey, String)> = None;
        let is_table_tab = matches!(self.active_tab().kind, TabKind::Table { .. });
        let active_table = self.active_table_info().cloned();

        let frame_response = egui::Frame::default()
            .fill(Color32::from_rgb(255, 255, 255))
            .stroke(egui::Stroke::new(1.0, Color32::from_rgb(216, 212, 208)))
            .corner_radius(3.0)
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                let column_names = self
                    .active_tab()
                    .result
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                ui.horizontal(|ui| {
                    ui.heading(
                        RichText::new("Results")
                            .size(18.0)
                            .color(Color32::from_rgb(38, 47, 60)),
                    );
                    {
                        let tab = self.active_tab();
                        ui.label(
                            RichText::new(format!(
                                "{} rows • {} ms",
                                format_count(tab.result.rows.len()),
                                tab.result.duration_ms
                            ))
                            .color(Color32::from_rgb(115, 124, 136)),
                        );
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if is_table_tab {
                            let is_structure =
                                self.active_tab().table_detail_view == TableDetailView::Structure;
                            let is_data =
                                self.active_tab().table_detail_view == TableDetailView::Data;
                            if ui.selectable_label(is_structure, "Structure").clicked() {
                                switch_to_structure = true;
                            }
                            if ui.selectable_label(is_data, "Data").clicked() {
                                switch_to_data = true;
                            }
                            ui.add_space(10.0);
                        }
                        chip(
                            ui,
                            "Arrow keys navigate • Enter edits",
                            Color32::from_rgb(241, 244, 250),
                            Color32::from_rgb(92, 102, 114),
                        );
                    });
                });
                if is_table_tab && self.active_tab().table_detail_view == TableDetailView::Structure
                {
                    ui.add_space(8.0);
                    self.ui_table_structure(ui);
                    return;
                }
                if is_table_tab {
                    ui.add_space(6.0);
                    ui.horizontal_wrapped(|ui| {
                        ui.label(RichText::new("Rows to load").size(12.0).strong());
                        ui.add_sized(
                            [90.0, 24.0],
                            TextEdit::singleline(&mut self.table_preview_limit_input)
                                .hint_text(&DEFAULT_TABLE_PREVIEW_LIMIT.to_string()),
                        );
                        ui.label(
                            RichText::new("Use 0 to load all rows")
                                .size(11.0)
                                .color(Color32::from_rgb(117, 126, 137)),
                        );
                        if soft_button(ui, "Reload").clicked() {
                            reload_table_preview = true;
                        }
                    });
                }
                ui.add_space(8.0);
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new("Filters").size(12.0).strong());
                    {
                        let selected_text = self.active_tab().filter_mode.label();
                        egui::ComboBox::from_id_salt("results_filter_mode")
                            .selected_text(selected_text)
                            .width(120.0)
                            .show_ui(ui, |ui| {
                                let tab = self.active_tab_mut();
                                ui.selectable_value(
                                    &mut tab.filter_mode,
                                    ResultFilterMode::Column,
                                    ResultFilterMode::Column.label(),
                                );
                                ui.selectable_value(
                                    &mut tab.filter_mode,
                                    ResultFilterMode::RawSql,
                                    ResultFilterMode::RawSql.label(),
                                );
                            });
                    }

                    if self.active_tab().filter_mode == ResultFilterMode::Column {
                        if soft_button(ui, "Apply All").clicked() {
                            apply_all_filters = true;
                        }
                        if soft_button(ui, "+ Filter").clicked() {
                            add_filter_rule = true;
                        }
                    } else {
                        ui.add_sized(
                            [360.0, 24.0],
                            TextEdit::singleline(&mut self.active_tab_mut().filter_raw_sql)
                                .hint_text("WHERE clause, e.g. status = 'paid'"),
                        );
                        if soft_button(ui, "Apply").clicked() {
                            apply_raw_sql_filter = true;
                        }
                    }

                    if soft_button(ui, "Clear").clicked() {
                        clear_filters = true;
                    }
                });

                if self.active_tab().filter_mode == ResultFilterMode::Column {
                    ui.add_space(6.0);
                    let rule_count = self.active_tab().draft_filter_rules.len();
                    for rule_index in 0..rule_count {
                        let column_search = self.active_tab().draft_filter_rules[rule_index]
                            .column_search
                            .clone();
                        let filtered_column_options =
                            column_candidates(&column_names, &column_search);

                        if !column_search.trim().is_empty() {
                            if let Some((index, _)) = filtered_column_options
                                .iter()
                                .find(|(_, name)| name.eq_ignore_ascii_case(column_search.trim()))
                            {
                                self.active_tab_mut().draft_filter_rules[rule_index].column =
                                    Some(*index);
                            }
                        }

                        let selected_text = self.active_tab().draft_filter_rules[rule_index]
                            .column
                            .and_then(|index| column_names.get(index))
                            .cloned()
                            .unwrap_or_else(|| "All columns".to_owned());
                        let tab_id = self.active_tab().id;

                        ui.horizontal_wrapped(|ui| {
                            let rule = &mut self.active_tab_mut().draft_filter_rules[rule_index];
                            let popup_id = filter_column_popup_id(tab_id, rule_index);
                            let search_id = filter_column_search_id(tab_id, rule_index);
                            let picker_open = ui.memory(|mem| mem.is_popup_open(popup_id));
                            let picker_label = format!(
                                "{} {}",
                                selected_text,
                                if picker_open { "^" } else { "v" }
                            );
                            let picker_response = with_shortcut(
                                ui.add_sized(
                                    [240.0, 24.0],
                                    egui::Button::new(
                                        RichText::new(picker_label)
                                            .size(12.0)
                                            .color(Color32::from_rgb(70, 74, 84)),
                                    )
                                    .fill(Color32::from_rgb(251, 251, 251))
                                    .stroke(egui::Stroke::new(
                                        1.0,
                                        Color32::from_rgb(216, 212, 208),
                                    ))
                                    .corner_radius(4.0),
                                ),
                                "Cmd + Shift + K",
                            );
                            if picker_response.clicked() {
                                if picker_open {
                                    ui.memory_mut(|mem| mem.close_popup());
                                } else {
                                    rule.column_picker_highlight =
                                        rule.column.map(|index| index + 1).unwrap_or(0);
                                    ui.memory_mut(|mem| mem.open_popup(popup_id));
                                    ui.memory_mut(|mem| mem.request_focus(search_id));
                                }
                            }

                            egui::popup::popup_below_widget(
                                ui,
                                popup_id,
                                &picker_response,
                                egui::popup::PopupCloseBehavior::CloseOnClickOutside,
                                |ui| {
                                    ui.set_min_width(240.0);
                                    let inner_search_response = ui.add_sized(
                                        [224.0, 26.0],
                                        TextEdit::singleline(&mut rule.column_search)
                                            .id(search_id)
                                            .hint_text("Search columns"),
                                    );
                                    let filtered =
                                        column_candidates(&column_names, &rule.column_search);
                                    let max_highlight = filtered.len();
                                    if inner_search_response.changed() {
                                        rule.column_picker_highlight =
                                            if filtered.is_empty() { 0 } else { 1 };
                                    }
                                    if ui.input(|i| i.key_pressed(Key::ArrowDown))
                                        && rule.column_picker_highlight < max_highlight
                                    {
                                        rule.column_picker_highlight += 1;
                                    }
                                    if ui.input(|i| i.key_pressed(Key::ArrowUp)) {
                                        rule.column_picker_highlight =
                                            rule.column_picker_highlight.saturating_sub(1);
                                    }
                                    rule.column_picker_highlight =
                                        rule.column_picker_highlight.min(max_highlight);
                                    let select_with_enter = ui.input_mut(|i| {
                                        i.consume_key(egui::Modifiers::NONE, Key::Enter)
                                    });

                                    if select_with_enter
                                        && (inner_search_response.has_focus() || picker_open)
                                    {
                                        if rule.column_picker_highlight == 0 {
                                            rule.column = None;
                                            rule.column_search.clear();
                                        } else if let Some((index, column_name)) =
                                            filtered.get(rule.column_picker_highlight - 1)
                                        {
                                            rule.column = Some(*index);
                                            rule.column_search = column_name.clone();
                                        } else if !filtered.is_empty() {
                                            commit_first_column_match(rule, &filtered);
                                        }
                                        ui.memory_mut(|mem| mem.close_popup());
                                    }

                                    ui.add_space(6.0);
                                    egui::ScrollArea::vertical()
                                        .max_height(260.0)
                                        .show(ui, |ui| {
                                            if ui
                                                .selectable_label(
                                                    rule.column_picker_highlight == 0,
                                                    "All columns",
                                                )
                                                .clicked()
                                            {
                                                rule.column_picker_highlight = 0;
                                                rule.column = None;
                                                rule.column_search.clear();
                                                ui.memory_mut(|mem| mem.close_popup());
                                            }
                                            for (filtered_index, (index, column_name)) in
                                                filtered.into_iter().enumerate()
                                            {
                                                let is_selected = rule.column_picker_highlight
                                                    == filtered_index + 1;
                                                if ui
                                                    .selectable_label(
                                                        is_selected,
                                                        column_name.as_str(),
                                                    )
                                                    .clicked()
                                                {
                                                    rule.column_picker_highlight =
                                                        filtered_index + 1;
                                                    rule.column = Some(index);
                                                    rule.column_search = column_name;
                                                    ui.memory_mut(|mem| mem.close_popup());
                                                }
                                            }
                                        });
                                },
                            );

                            let operator_label = rule.operator.label();
                            let operator_popup_id = filter_operator_popup_id(tab_id, rule_index);
                            let operator_popup_open =
                                ui.memory(|mem| mem.is_popup_open(operator_popup_id));
                            let operator_button_label = format!(
                                "{} {}",
                                operator_label,
                                if operator_popup_open { "^" } else { "v" }
                            );
                            let operator_response = ui.add_sized(
                                [190.0, 24.0],
                                egui::Button::new(
                                    RichText::new(operator_button_label)
                                        .size(12.0)
                                        .color(Color32::from_rgb(70, 74, 84)),
                                )
                                .fill(Color32::from_rgb(251, 251, 251))
                                .stroke(egui::Stroke::new(1.0, Color32::from_rgb(216, 212, 208)))
                                .corner_radius(4.0),
                            );
                            if operator_response.clicked() {
                                if operator_popup_open {
                                    ui.memory_mut(|mem| mem.close_popup());
                                } else {
                                    rule.operator_picker_highlight = all_result_filter_operators()
                                        .iter()
                                        .position(|operator| *operator == rule.operator)
                                        .unwrap_or(0);
                                    ui.memory_mut(|mem| mem.open_popup(operator_popup_id));
                                }
                            }

                            egui::popup::popup_below_widget(
                                ui,
                                operator_popup_id,
                                &operator_response,
                                egui::popup::PopupCloseBehavior::CloseOnClickOutside,
                                |ui| {
                                    ui.set_min_width(190.0);
                                    let operators = all_result_filter_operators();
                                    let last_index = operators.len().saturating_sub(1);
                                    if ui.input(|i| i.key_pressed(Key::ArrowDown))
                                        && rule.operator_picker_highlight < last_index
                                    {
                                        rule.operator_picker_highlight += 1;
                                    }
                                    if ui.input(|i| i.key_pressed(Key::ArrowUp)) {
                                        rule.operator_picker_highlight =
                                            rule.operator_picker_highlight.saturating_sub(1);
                                    }
                                    let select_with_enter = ui.input_mut(|i| {
                                        i.consume_key(egui::Modifiers::NONE, Key::Enter)
                                    });
                                    rule.operator_picker_highlight =
                                        rule.operator_picker_highlight.min(last_index);

                                    egui::ScrollArea::vertical()
                                        .max_height(280.0)
                                        .show(ui, |ui| {
                                            for (operator_index, operator) in
                                                operators.iter().copied().enumerate()
                                            {
                                                if operator_group_boundary(operator_index) {
                                                    ui.separator();
                                                }
                                                let is_selected = rule.operator_picker_highlight
                                                    == operator_index;
                                                if ui
                                                    .selectable_label(is_selected, operator.label())
                                                    .clicked()
                                                {
                                                    rule.operator_picker_highlight = operator_index;
                                                    rule.operator = operator;
                                                    ui.memory_mut(|mem| mem.close_popup());
                                                }
                                            }
                                        });

                                    if select_with_enter {
                                        if let Some(operator) =
                                            operators.get(rule.operator_picker_highlight).copied()
                                        {
                                            rule.operator = operator;
                                            ui.memory_mut(|mem| mem.close_popup());
                                        }
                                    }
                                },
                            );

                            ui.add_enabled_ui(rule.operator.requires_value(), |ui| {
                                let hint = match rule.operator {
                                    ResultFilterOperator::In | ResultFilterOperator::NotIn => {
                                        "Comma-separated values"
                                    }
                                    ResultFilterOperator::Between
                                    | ResultFilterOperator::NotBetween => "start, end",
                                    ResultFilterOperator::Like => "Use % and _ wildcards",
                                    _ => "Filter value",
                                };
                                ui.add_sized(
                                    [190.0, 24.0],
                                    TextEdit::singleline(&mut rule.value).hint_text(hint),
                                );
                            });

                            if self.active_tab().draft_filter_rules.len() > 1
                                && soft_button(ui, "-").clicked()
                            {
                                remove_filter_rule = Some(rule_index);
                            }
                        });
                        ui.add_space(4.0);
                    }
                }

                let tab = self.active_tab();
                let result = &tab.result;
                let total_columns = result.columns.len();
                let total_pages = total_columns.div_ceil(RESULT_COLUMNS_PER_PAGE).max(1);
                let current_page = tab.column_page.min(total_pages - 1);
                let column_start = current_page * RESULT_COLUMNS_PER_PAGE;
                let column_end = (column_start + RESULT_COLUMNS_PER_PAGE).min(total_columns);
                let visible_columns = &result.columns[column_start..column_end];
                let filtered_rows =
                    filtered_row_indices(result, tab.filter_mode, &tab.applied_filter_rules);

                ui.add_space(6.0);
                ui.label(
                    RichText::new(format!(
                        "Showing {} of {} rows",
                        format_count(filtered_rows.len()),
                        format_count(result.rows.len())
                    ))
                    .size(12.0)
                    .color(Color32::from_rgb(117, 126, 137)),
                );

                if total_columns > RESULT_COLUMNS_PER_PAGE {
                    ui.add_space(8.0);
                    ui.label(
                        RichText::new(format!(
                            "Columns {}-{} of {}",
                            column_start + 1,
                            column_end,
                            total_columns
                        ))
                        .color(Color32::from_rgb(117, 126, 137)),
                    );
                }
                ui.add_space(10.0);

                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.set_min_width(
                            ((visible_columns.len() + 1) as f32 * 160.0).max(ui.available_width()),
                        );

                        let mut table = TableBuilder::new(ui)
                            .striped(true)
                            .resizable(true)
                            .cell_layout(Layout::left_to_right(Align::Center))
                            .min_scrolled_height((available_height - 60.0).max(180.0));

                        table = table.column(Column::exact(44.0));
                        for _ in visible_columns {
                            table = table.column(Column::initial(160.0).at_least(130.0));
                        }

                        table
                            .header(26.0, |mut header| {
                                header.col(|ui| {
                                    egui::Frame::default()
                                        .fill(Color32::from_rgb(246, 244, 242))
                                        .stroke(egui::Stroke::new(
                                            1.0,
                                            Color32::from_rgb(220, 216, 212),
                                        ))
                                        .inner_margin(Margin::symmetric(6, 4))
                                        .show(ui, |ui| {
                                            ui.label(
                                                RichText::new("Row")
                                                    .size(12.0)
                                                    .color(Color32::from_rgb(88, 88, 88)),
                                            );
                                        });
                                });
                                for column in visible_columns {
                                    header.col(|ui| {
                                        egui::Frame::default()
                                            .fill(Color32::from_rgb(246, 244, 242))
                                            .stroke(egui::Stroke::new(
                                                1.0,
                                                Color32::from_rgb(220, 216, 212),
                                            ))
                                            .inner_margin(Margin::symmetric(6, 4))
                                            .show(ui, |ui| {
                                                ui.label(
                                                    RichText::new(&column.name)
                                                        .size(12.0)
                                                        .color(Color32::from_rgb(88, 88, 88)),
                                                );
                                            });
                                    });
                                }
                            })
                            .body(|body| {
                                body.rows(26.0, filtered_rows.len(), |mut row| {
                                    let row_index = filtered_rows[row.index()];
                                    row.col(|ui| {
                                        let is_selected_row = selected_row == Some(row_index);
                                        let response = ui.add_sized(
                                            [ui.available_width(), 20.0],
                                            egui::Button::new(
                                                RichText::new(if is_selected_row {
                                                    "●"
                                                } else {
                                                    "○"
                                                })
                                                .color(if is_selected_row {
                                                    Color32::from_rgb(255, 255, 255)
                                                } else {
                                                    Color32::from_rgb(150, 150, 150)
                                                }),
                                            )
                                            .fill(if is_selected_row {
                                                Color32::from_rgb(193, 63, 55)
                                            } else {
                                                Color32::from_rgb(250, 250, 250)
                                            })
                                            .stroke(egui::Stroke::new(
                                                1.0,
                                                if is_selected_row {
                                                    Color32::from_rgb(174, 50, 43)
                                                } else {
                                                    Color32::from_rgb(232, 228, 224)
                                                },
                                            ))
                                            .corner_radius(2.0),
                                        );
                                        if response.clicked() {
                                            clicked_row = Some(row_index);
                                        }
                                    });
                                    for (visible_index, _column) in
                                        visible_columns.iter().enumerate()
                                    {
                                        row.col(|ui| {
                                            let col_index = column_start + visible_index;
                                            let cell = CellSelection {
                                                row: row_index,
                                                col: col_index,
                                            };
                                            let is_selected = selected == Some(cell)
                                                || selected_row == Some(row_index);
                                            let is_editing = editing
                                                .as_ref()
                                                .map(|editor| {
                                                    editor.row == row_index
                                                        && editor.col == col_index
                                                })
                                                .unwrap_or(false);

                                            let column_name =
                                                result.columns[col_index].name.clone();
                                            let column_meta =
                                                active_table.as_ref().and_then(|table| {
                                                    result_column_meta(table, &column_name).cloned()
                                                });
                                            let foreign_key =
                                                active_table.as_ref().and_then(|table| {
                                                    result_column_foreign_key(table, &column_name)
                                                        .cloned()
                                                });
                                            let enum_options = column_meta
                                                .as_ref()
                                                .map(enum_options_for_column)
                                                .unwrap_or_default();
                                            let has_enum_picker = !enum_options.is_empty();

                                            if is_editing {
                                                let mut editor =
                                                    draft.clone().unwrap_or(CellEditState {
                                                        row: row_index,
                                                        col: col_index,
                                                        value: result.rows[row_index][col_index]
                                                            .clone(),
                                                    });
                                                if has_enum_picker {
                                                    let mut enum_changed = false;
                                                    let selected_text = if editor.value.is_empty() {
                                                        "Select value".to_owned()
                                                    } else {
                                                        editor.value.clone()
                                                    };
                                                    ui.set_min_width(ui.available_width());
                                                    egui::ComboBox::from_id_salt((
                                                        "result_enum_editor",
                                                        row_index,
                                                        col_index,
                                                    ))
                                                    .selected_text(selected_text)
                                                    .width(ui.available_width())
                                                    .show_ui(ui, |ui| {
                                                        for option in &enum_options {
                                                            if ui
                                                                .selectable_label(
                                                                    editor.value == *option,
                                                                    option,
                                                                )
                                                                .clicked()
                                                            {
                                                                editor.value = option.clone();
                                                                enum_changed = true;
                                                            }
                                                        }
                                                    });
                                                    if enum_changed {
                                                        finish_edit = true;
                                                    }
                                                } else {
                                                    let response = ui.add_sized(
                                                        [ui.available_width(), 22.0],
                                                        TextEdit::singleline(&mut editor.value)
                                                            .margin(Vec2::new(6.0, 2.0)),
                                                    );
                                                    if response.lost_focus()
                                                        && ctx.input(|i| i.key_pressed(Key::Enter))
                                                    {
                                                        finish_edit = true;
                                                    }
                                                }
                                                draft = Some(editor);
                                            } else {
                                                let is_row_selected =
                                                    selected_row == Some(row_index);
                                                let fill = if is_row_selected {
                                                    Color32::from_rgb(193, 63, 55)
                                                } else if is_selected {
                                                    Color32::from_rgb(245, 241, 239)
                                                } else {
                                                    Color32::TRANSPARENT
                                                };
                                                let cell_value =
                                                    result.rows[row_index][col_index].clone();
                                                ui.horizontal(|ui| {
                                                    ui.spacing_mut().item_spacing.x = 2.0;
                                                    let main_width = if foreign_key.is_some()
                                                        || has_enum_picker
                                                    {
                                                        (ui.available_width() - 34.0).max(40.0)
                                                    } else {
                                                        ui.available_width()
                                                    };
                                                    let response = ui.add_sized(
                                                        [main_width, 22.0],
                                                        egui::Button::new(
                                                            RichText::new(if has_enum_picker {
                                                                format!("{cell_value}  v")
                                                            } else {
                                                                cell_value.clone()
                                                            })
                                                            .size(12.0)
                                                            .color(if is_row_selected {
                                                                Color32::WHITE
                                                            } else {
                                                                Color32::from_rgb(72, 72, 72)
                                                            }),
                                                        )
                                                        .fill(fill)
                                                        .stroke(egui::Stroke::new(
                                                            1.0,
                                                            if is_row_selected {
                                                                Color32::from_rgb(174, 50, 43)
                                                            } else if is_selected {
                                                                Color32::from_rgb(208, 202, 198)
                                                            } else {
                                                                Color32::from_rgb(226, 222, 218)
                                                            },
                                                        ))
                                                        .corner_radius(0.0),
                                                    );

                                                    if response.clicked() {
                                                        clicked_cell = Some(cell);
                                                    }
                                                    if response.double_clicked() {
                                                        clicked_cell = Some(cell);
                                                        start_edit = Some(cell);
                                                    }

                                                    if has_enum_picker
                                                        && ui
                                                            .add_sized(
                                                                [14.0, 22.0],
                                                                egui::Button::new(
                                                                    RichText::new("v").size(11.0),
                                                                )
                                                                .fill(fill)
                                                                .stroke(egui::Stroke::new(
                                                                    1.0,
                                                                    if is_row_selected {
                                                                        Color32::from_rgb(
                                                                            174, 50, 43,
                                                                        )
                                                                    } else if is_selected {
                                                                        Color32::from_rgb(
                                                                            208, 202, 198,
                                                                        )
                                                                    } else {
                                                                        Color32::from_rgb(
                                                                            226, 222, 218,
                                                                        )
                                                                    },
                                                                ))
                                                                .corner_radius(0.0),
                                                            )
                                                            .clicked()
                                                    {
                                                        clicked_cell = Some(cell);
                                                        start_edit = Some(cell);
                                                    }

                                                    if let Some(foreign_key) = foreign_key.as_ref()
                                                    {
                                                        if !cell_value.trim().is_empty()
                                                            && !cell_value
                                                                .eq_ignore_ascii_case("NULL")
                                                            && ui
                                                                .add_sized(
                                                                    [18.0, 22.0],
                                                                    egui::Button::new(
                                                                        RichText::new("->")
                                                                            .size(12.0),
                                                                    )
                                                                    .fill(fill)
                                                                    .stroke(egui::Stroke::new(
                                                                        1.0,
                                                                        if is_row_selected {
                                                                            Color32::from_rgb(
                                                                                174, 50, 43,
                                                                            )
                                                                        } else if is_selected {
                                                                            Color32::from_rgb(
                                                                                208, 202, 198,
                                                                            )
                                                                        } else {
                                                                            Color32::from_rgb(
                                                                                226, 222, 218,
                                                                            )
                                                                        },
                                                                    ))
                                                                    .corner_radius(0.0),
                                                                )
                                                                .on_hover_text(format!(
                                                                    "Open {}.{} where {} = {}",
                                                                    foreign_key.referenced_schema,
                                                                    foreign_key.referenced_table,
                                                                    foreign_key.referenced_column,
                                                                    cell_value
                                                                ))
                                                                .clicked()
                                                        {
                                                            clicked_cell = Some(cell);
                                                            foreign_key_jump = Some((
                                                                foreign_key.clone(),
                                                                cell_value.clone(),
                                                            ));
                                                        }
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            });
                    });
            });
        let results_hovered = frame_response.response.hovered();

        if clear_filters {
            let tab = self.active_tab_mut();
            tab.filter_mode = ResultFilterMode::Column;
            tab.draft_filter_rules = vec![ResultFilterRule::default()];
            tab.applied_filter_rules = vec![ResultFilterRule::default()];
            tab.filter_raw_sql.clear();
        }
        if let Some(index) = remove_filter_rule {
            let tab = self.active_tab_mut();
            if index < tab.draft_filter_rules.len() {
                tab.draft_filter_rules.remove(index);
            }
            if tab.draft_filter_rules.is_empty() {
                tab.draft_filter_rules.push(ResultFilterRule::default());
            }
        }
        if add_filter_rule {
            self.active_tab_mut()
                .draft_filter_rules
                .push(ResultFilterRule::default());
        }
        if apply_all_filters {
            let rules = self.active_tab().draft_filter_rules.clone();
            self.active_tab_mut().applied_filter_rules = rules;
        }
        if apply_raw_sql_filter {
            self.apply_active_tab_raw_filter();
        }
        if reload_table_preview && is_table_tab {
            self.open_selected_table();
        }
        if switch_to_data {
            self.active_tab_mut().table_detail_view = TableDetailView::Data;
        }
        if switch_to_structure {
            self.active_tab_mut().table_detail_view = TableDetailView::Structure;
        }

        self.editing_cell = draft;
        if let Some(row_index) = clicked_row {
            self.set_result_selection(row_index, 0);
        }
        let total_columns = self.active_tab().result.columns.len();
        let total_pages = total_columns.div_ceil(RESULT_COLUMNS_PER_PAGE).max(1);
        let current_page = self.active_tab().column_page.min(total_pages - 1);
        let horizontal_scroll = ctx.input(|i| i.smooth_scroll_delta.x);
        if results_hovered
            && total_columns > RESULT_COLUMNS_PER_PAGE
            && horizontal_scroll.abs() > 8.0
            && self.last_results_page_change.elapsed() > Duration::from_millis(120)
        {
            if horizontal_scroll < 0.0 && current_page + 1 < total_pages {
                self.active_tab_mut().column_page = current_page + 1;
                self.last_results_page_change = Instant::now();
            } else if horizontal_scroll > 0.0 && current_page > 0 {
                self.active_tab_mut().column_page = current_page - 1;
                self.last_results_page_change = Instant::now();
            }
        }
        if let Some(cell) = clicked_cell {
            self.set_result_selection(cell.row, cell.col);
        }
        if let Some(cell) = start_edit {
            self.set_result_selection(cell.row, cell.col);
            self.begin_edit_selected_cell();
        }
        if let Some((foreign_key, value)) = foreign_key_jump {
            self.jump_to_foreign_key_target(&foreign_key, &value);
        }
        if finish_edit {
            self.commit_cell_edit();
        }
    }

    fn ui_table_structure(&mut self, ui: &mut egui::Ui) {
        let Some(table) = self.active_table_info().cloned() else {
            ui.label(
                RichText::new("Open a table tab to inspect its structure.")
                    .color(Color32::from_rgb(117, 126, 137)),
            );
            return;
        };

        let mut structure_filter = self.active_tab().structure_filter.clone();
        let mut structure_selected_row = self.active_tab().structure_selected_row;
        let mut columns = table.columns.clone();
        let mut foreign_key_values = columns
            .iter()
            .map(|column| {
                table
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.column_name.eq_ignore_ascii_case(&column.name))
                    .map(|fk| {
                        format!(
                            "{}.{}({})",
                            fk.referenced_schema, fk.referenced_table, fk.referenced_column
                        )
                    })
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        let primary_columns = columns
            .iter()
            .filter(|column| column.primary)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let unique_index_count = table
            .index_entries
            .iter()
            .map(|entry| entry.index_name.as_str())
            .collect::<HashSet<_>>()
            .len();
        let filter_lower = structure_filter.trim().to_lowercase();
        let visible_indices = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                let fk = foreign_key_values
                    .get(index)
                    .map(String::as_str)
                    .unwrap_or_default();
                let searchable = format!(
                    "{} {} {} {} {} {} {} {}",
                    column.name,
                    column.kind,
                    column.character_set,
                    column.collation,
                    column.default_value,
                    column.extra,
                    fk,
                    column.comment
                )
                .to_lowercase();
                (filter_lower.is_empty() || searchable.contains(&filter_lower)).then_some(index)
            })
            .collect::<Vec<_>>();
        if let Some(selected_row) = structure_selected_row {
            if selected_row >= columns.len() {
                structure_selected_row = None;
            }
        }

        ui.horizontal_wrapped(|ui| {
            chip(
                ui,
                &format!("Table {}", table.name),
                Color32::from_rgb(241, 244, 250),
                Color32::from_rgb(92, 102, 114),
            );
            chip(
                ui,
                &format!(
                    "Primary {}",
                    if primary_columns.is_empty() {
                        "-".to_owned()
                    } else {
                        primary_columns.join(", ")
                    }
                ),
                Color32::from_rgb(241, 244, 250),
                Color32::from_rgb(92, 102, 114),
            );
            chip(
                ui,
                &format!("{} columns", columns.len()),
                Color32::from_rgb(241, 244, 250),
                Color32::from_rgb(92, 102, 114),
            );
            chip(
                ui,
                &format!("{} indexes", unique_index_count),
                Color32::from_rgb(241, 244, 250),
                Color32::from_rgb(92, 102, 114),
            );
        });
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            ui.add_sized(
                [280.0, 28.0],
                TextEdit::singleline(&mut structure_filter).hint_text("Search columns"),
            );
            chip(
                ui,
                "Click row number to select • Cmd+C copies • Cmd+V duplicates",
                Color32::from_rgb(241, 244, 250),
                Color32::from_rgb(92, 102, 114),
            );
        });
        ui.add_space(8.0);

        egui::TopBottomPanel::bottom("structure_indexes_dock")
            .resizable(true)
            .default_height(220.0)
            .min_height(170.0)
            .show_inside(ui, |ui| {
                egui::Frame::default()
                    .fill(Color32::from_rgb(252, 252, 253))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(228, 232, 238)))
                    .corner_radius(10.0)
                    .inner_margin(Margin::same(8))
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("Indexes")
                                    .size(14.0)
                                    .strong()
                                    .color(Color32::from_rgb(54, 63, 76)),
                            );
                            ui.label(
                                RichText::new("Own scroll area")
                                    .size(11.0)
                                    .color(Color32::from_rgb(128, 137, 149)),
                            );
                        });
                        ui.add_space(6.0);

                        if table.index_entries.is_empty() {
                            ui.label(
                                RichText::new("No index metadata loaded for this table yet.")
                                    .color(Color32::from_rgb(117, 126, 137)),
                            );
                        } else {
                            let indexes_scroll_height = (ui.available_height() - 6.0).max(120.0);
                            egui::ScrollArea::both()
                                .id_salt("table_structure_indexes")
                                .max_height(indexes_scroll_height)
                                .auto_shrink([false, false])
                                .show(ui, |ui| {
                                    let min_width = 720.0_f32.max(ui.available_width());
                                    ui.set_min_width(min_width);
                                    TableBuilder::new(ui)
                                        .striped(true)
                                        .resizable(true)
                                        .cell_layout(Layout::left_to_right(Align::Center))
                                        .column(Column::initial(260.0).at_least(220.0))
                                        .column(Column::initial(140.0).at_least(120.0))
                                        .column(Column::initial(100.0).at_least(80.0))
                                        .column(Column::initial(240.0).at_least(180.0))
                                        .header(28.0, |mut header| {
                                            for label in [
                                                "index_name",
                                                "index_algorithm",
                                                "is_unique",
                                                "column_name",
                                            ] {
                                                header.col(|ui| {
                                                    ui.label(
                                                        RichText::new(label)
                                                            .strong()
                                                            .color(Color32::from_rgb(84, 92, 104)),
                                                    );
                                                });
                                            }
                                        })
                                        .body(|body| {
                                            body.rows(
                                                24.0,
                                                table.index_entries.len(),
                                                |mut row| {
                                                    let entry = &table.index_entries[row.index()];
                                                    for value in [
                                                        entry.index_name.clone(),
                                                        blank_to_null(&entry.index_algorithm),
                                                        if entry.is_unique {
                                                            "TRUE".to_owned()
                                                        } else {
                                                            "FALSE".to_owned()
                                                        },
                                                        entry.column_name.clone(),
                                                    ] {
                                                        row.col(|ui| {
                                                            ui.label(
                                                                RichText::new(value)
                                                                    .size(12.0)
                                                                    .color(Color32::from_rgb(
                                                                        74, 82, 94,
                                                                    )),
                                                            );
                                                        });
                                                    }
                                                },
                                            );
                                        });
                                });
                        }
                    });
            });

        ui.add_space(8.0);

        egui::Frame::default()
            .fill(Color32::from_rgb(252, 252, 253))
            .stroke(egui::Stroke::new(1.0, Color32::from_rgb(228, 232, 238)))
            .corner_radius(10.0)
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("Columns")
                            .size(14.0)
                            .strong()
                            .color(Color32::from_rgb(54, 63, 76)),
                    );
                    ui.label(
                        RichText::new("Own scroll area")
                            .size(11.0)
                            .color(Color32::from_rgb(128, 137, 149)),
                    );
                });
                ui.add_space(6.0);
                let columns_scroll_height = (ui.available_height() - 6.0).max(280.0);
                egui::ScrollArea::both()
                    .id_salt("table_structure_columns")
                    .max_height(columns_scroll_height)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let min_width = 1440.0_f32.max(ui.available_width());
                        ui.set_min_width(min_width);
                        TableBuilder::new(ui)
                            .striped(true)
                            .resizable(true)
                            .cell_layout(Layout::left_to_right(Align::Center))
                            .column(Column::exact(44.0))
                            .column(Column::initial(78.0).at_least(70.0))
                            .column(Column::initial(180.0).at_least(150.0))
                            .column(Column::initial(190.0).at_least(150.0))
                            .column(Column::initial(120.0).at_least(100.0))
                            .column(Column::initial(150.0).at_least(120.0))
                            .column(Column::initial(100.0).at_least(90.0))
                            .column(Column::initial(140.0).at_least(120.0))
                            .column(Column::initial(120.0).at_least(100.0))
                            .column(Column::initial(260.0).at_least(220.0))
                            .column(Column::initial(180.0).at_least(140.0))
                            .header(30.0, |mut header| {
                                for label in [
                                    "#",
                                    "key",
                                    "column_name",
                                    "data_type",
                                    "character_set",
                                    "collation",
                                    "is_nullable",
                                    "column_default",
                                    "extra",
                                    "foreign_key",
                                    "comment",
                                ] {
                                    header.col(|ui| {
                                        ui.label(
                                            RichText::new(label)
                                                .strong()
                                                .color(Color32::from_rgb(84, 92, 104)),
                                        );
                                    });
                                }
                            })
                            .body(|body| {
                                body.rows(30.0, visible_indices.len(), |mut row| {
                                    let actual_index = visible_indices[row.index()];
                                    let column = &mut columns[actual_index];
                                    let foreign_key_value = &mut foreign_key_values[actual_index];
                                    let selected = structure_selected_row == Some(actual_index);
                                    row.col(|ui| {
                                        if ui
                                            .selectable_label(
                                                selected,
                                                (actual_index + 1).to_string(),
                                            )
                                            .clicked()
                                        {
                                            structure_selected_row = Some(actual_index);
                                        }
                                    });
                                    row.col(|ui| {
                                        ui.horizontal(|ui| {
                                            ui.checkbox(&mut column.primary, "PK");
                                            if !foreign_key_value.trim().is_empty() {
                                                ui.label(
                                                    RichText::new("FK")
                                                        .size(11.0)
                                                        .color(Color32::from_rgb(95, 111, 171)),
                                                );
                                            }
                                        });
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.name),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.kind),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.character_set),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.collation),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.checkbox(&mut column.nullable, "");
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.default_value),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.extra),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(foreign_key_value)
                                                .hint_text("schema.table(column)"),
                                        );
                                    });
                                    row.col(|ui| {
                                        ui.add_sized(
                                            [ui.available_width(), 24.0],
                                            TextEdit::singleline(&mut column.comment),
                                        );
                                    });
                                });
                            });
                    });
            });

        self.active_tab_mut().structure_filter = structure_filter;
        self.active_tab_mut().structure_selected_row = structure_selected_row;
        if let Some(active_table) = self.active_table_info_mut() {
            active_table.columns = columns.clone();
            active_table.foreign_keys = columns
                .iter()
                .zip(foreign_key_values.iter())
                .filter_map(|(column, foreign_key_value)| {
                    parse_structure_foreign_key(
                        foreign_key_value,
                        &active_table.schema,
                        &column.name,
                    )
                })
                .collect();
        }
    }

    fn ui_status_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("status_bar")
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(248, 249, 252))
                    .inner_margin(Margin::symmetric(16, 10)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    let message = self
                        .activity_log
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "Idle".to_owned());
                    ui.label(
                        RichText::new(format!("Activity: {}", message))
                            .color(Color32::from_rgb(95, 105, 117)),
                    );
                    if let Some(busy) = &self.busy_message {
                        ui.add_space(10.0);
                        chip(
                            ui,
                            busy,
                            Color32::from_rgb(234, 240, 255),
                            Color32::from_rgb(70, 118, 210),
                        );
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let status_text = self
                            .connection_opt()
                            .map(|connection| {
                                format!("{} tabs • {}", self.query_tabs.len(), connection.engine)
                            })
                            .unwrap_or_else(|| {
                                format!("{} tabs • No connection", self.query_tabs.len())
                            });
                        ui.label(RichText::new(status_text).color(Color32::from_rgb(70, 118, 210)));
                    });
                });
            });
    }

    fn ui_busy_overlay(&mut self, ctx: &egui::Context) {
        let Some(busy) = self.busy_message.as_deref() else {
            return;
        };

        egui::Area::new("busy_overlay".into())
            .order(egui::Order::Foreground)
            .anchor(Align2::CENTER_TOP, Vec2::new(0.0, 78.0))
            .show(ctx, |ui| {
                egui::Frame::default()
                    .fill(Color32::from_rgba_unmultiplied(255, 255, 255, 245))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(220, 226, 235)))
                    .shadow(egui::epaint::Shadow {
                        offset: [0, 8],
                        blur: 24,
                        spread: 0,
                        color: Color32::from_rgba_unmultiplied(28, 36, 48, 26),
                    })
                    .corner_radius(14.0)
                    .inner_margin(Margin::symmetric(14, 10))
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.add(egui::Spinner::new().size(16.0));
                            ui.add_space(6.0);
                            ui.label(
                                RichText::new(busy)
                                    .size(14.0)
                                    .color(Color32::from_rgb(46, 57, 72)),
                            );
                            if self.active_jobs.len() > 1 {
                                ui.add_space(8.0);
                                chip(
                                    ui,
                                    &format!("{} jobs", self.active_jobs.len()),
                                    Color32::from_rgb(234, 240, 255),
                                    Color32::from_rgb(70, 118, 210),
                                );
                            }
                        });
                    });
            });
    }

    fn ui_command_palette(&mut self, ctx: &egui::Context) {
        if !self.command_palette.open {
            return;
        }

        let mode = self.command_palette.mode;
        let items = self.palette_items_for_mode(mode);
        let query = self.command_palette.query.to_lowercase();
        let mut matches = items
            .into_iter()
            .filter_map(|item| {
                let haystack = format!(
                    "{} {}",
                    item.title.to_lowercase(),
                    item.subtitle.to_lowercase()
                );
                fuzzy_score(&query, &haystack).map(|score| (score, item))
            })
            .collect::<Vec<_>>();

        matches.sort_by(|left, right| right.0.cmp(&left.0));
        let filtered = matches
            .into_iter()
            .map(|(_, item)| item)
            .take(12)
            .collect::<Vec<_>>();

        if self.command_palette.selection >= filtered.len() && !filtered.is_empty() {
            self.command_palette.selection = filtered.len() - 1;
        }

        if ctx.input(|i| i.key_pressed(Key::ArrowDown)) && !filtered.is_empty() {
            self.command_palette.selection =
                (self.command_palette.selection + 1).min(filtered.len() - 1);
        }
        if ctx.input(|i| i.key_pressed(Key::ArrowUp)) && !filtered.is_empty() {
            self.command_palette.selection = self.command_palette.selection.saturating_sub(1);
        }

        let mut chosen: Option<PaletteAction> = None;
        let (title, hint) = match mode {
            PaletteMode::All => ("Go To Anything", "Tables, saved queries, connections"),
            PaletteMode::Connections => ("Switch Connection", "Available server connections"),
            PaletteMode::Databases => ("Switch Database", "Available databases on this server"),
        };
        egui::Window::new(title)
            .collapsible(false)
            .resizable(false)
            .default_width(620.0)
            .anchor(Align2::CENTER_TOP, Vec2::new(0.0, 60.0))
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(255, 255, 255))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(225, 230, 237)))
                    .corner_radius(14.0)
                    .inner_margin(Margin::same(14)),
            )
            .show(ctx, |ui| {
                let response = ui.add_sized(
                    [ui.available_width(), 34.0],
                    TextEdit::singleline(&mut self.command_palette.query).hint_text(hint),
                );
                if self.command_palette.focus_requested {
                    response.request_focus();
                    self.command_palette.focus_requested = false;
                }

                ui.add_space(4.0);
                ui.label(
                    RichText::new(title)
                        .size(13.0)
                        .color(Color32::from_rgb(108, 117, 129)),
                );

                ui.add_space(10.0);
                for (index, item) in filtered.iter().enumerate() {
                    let selected = index == self.command_palette.selection;
                    let fill = if selected {
                        Color32::from_rgb(234, 240, 255)
                    } else {
                        Color32::from_rgb(250, 250, 252)
                    };

                    egui::Frame::default()
                        .fill(fill)
                        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(234, 237, 242)))
                        .corner_radius(10.0)
                        .inner_margin(Margin::same(10))
                        .show(ui, |ui| {
                            let response = ui.allocate_response(
                                Vec2::new(ui.available_width(), 40.0),
                                Sense::click(),
                            );
                            if response.clicked() {
                                chosen = Some(item.action.clone());
                            }

                            let rect = response.rect;
                            let painter = ui.painter();
                            painter.text(
                                rect.left_center() + Vec2::new(10.0, -8.0),
                                Align2::LEFT_CENTER,
                                &item.title,
                                FontId::proportional(15.0),
                                Color32::from_rgb(43, 52, 66),
                            );
                            painter.text(
                                rect.left_center() + Vec2::new(10.0, 10.0),
                                Align2::LEFT_CENTER,
                                &item.subtitle,
                                FontId::proportional(12.0),
                                Color32::from_rgb(108, 117, 129),
                            );
                        });
                    ui.add_space(4.0);
                }
            });

        if ctx.input(|i| i.key_pressed(Key::Enter)) && !filtered.is_empty() {
            chosen = Some(filtered[self.command_palette.selection].action.clone());
        }

        if let Some(action) = chosen {
            self.execute_palette_action(action);
        }
    }

    fn ui_schema_diagram(&mut self, ctx: &egui::Context) {
        if !self.show_schema_diagram {
            return;
        }

        let mut open = self.show_schema_diagram;
        egui::Window::new("Schema Diagram")
            .open(&mut open)
            .collapsible(false)
            .resizable(true)
            .default_width(1180.0)
            .default_height(760.0)
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(252, 252, 253))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(220, 224, 230)))
                    .corner_radius(16.0)
                    .shadow(egui::Shadow {
                        offset: [0, 12],
                        blur: 36,
                        spread: 8,
                        color: Color32::from_rgba_unmultiplied(0, 0, 0, 28),
                    })
                    .inner_margin(Margin::same(14)),
            )
            .show(ctx, |ui| {
                let connection_index = self.selected_connection;
                let Some(connection) = self.connections.get(connection_index).cloned() else {
                    ui.vertical_centered(|ui| {
                        ui.add_space(80.0);
                        ui.heading("No connection selected");
                        ui.add_space(8.0);
                        ui.label("Add or select a connection to explore its schema visually.");
                    });
                    return;
                };

                let mut fit_view = false;
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.heading(
                            RichText::new(format!("{} schema diagram", connection.name))
                                .size(20.0)
                                .color(Color32::from_rgb(40, 50, 64)),
                        );
                        ui.label(
                            RichText::new(format!(
                                "{} • {}",
                                connection.engine,
                                connection_database_label(&connection)
                            ))
                            .size(12.0)
                            .color(Color32::from_rgb(116, 124, 136)),
                        );
                    });
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if soft_button(ui, "Fit View").clicked() {
                            fit_view = true;
                        }
                        ui.add(
                            egui::Slider::new(&mut self.schema_diagram_zoom, 0.55..=1.5)
                                .text("Zoom")
                                .step_by(0.05),
                        );
                        ui.checkbox(
                            &mut self.schema_diagram_current_schema_only,
                            "Current schema only",
                        );
                    });
                });

                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [340.0, 32.0],
                        TextEdit::singleline(&mut self.schema_diagram_filter)
                            .hint_text("Filter schemas, tables, or loaded columns"),
                    );
                    chip(
                        ui,
                        "Exact FK lines • Single-click selects • Double-click opens table",
                        Color32::from_rgb(241, 244, 250),
                        Color32::from_rgb(92, 102, 114),
                    );
                });
                ui.add_space(12.0);

                if connection.is_disconnected {
                    ui.vertical_centered(|ui| {
                        ui.add_space(90.0);
                        ui.heading(
                            RichText::new("Connection is offline")
                                .size(22.0)
                                .color(Color32::from_rgb(88, 97, 110)),
                        );
                        ui.add_space(10.0);
                        ui.label(
                            RichText::new(
                                "Reconnect first, then open the schema diagram to explore schemas, tables, and real foreign keys.",
                            )
                            .color(Color32::from_rgb(120, 129, 140)),
                        );
                    });
                    return;
                }

                if self.schema_loading.contains(&connection_index) {
                    ui.vertical_centered(|ui| {
                        ui.add_space(90.0);
                        ui.add(egui::Spinner::new().size(18.0));
                        ui.add_space(8.0);
                        ui.label(
                            RichText::new("Loading schema metadata...")
                                .strong()
                                .color(Color32::from_rgb(88, 97, 110)),
                        );
                    });
                    return;
                }

                let selected_schema_name = connection
                    .schemas
                    .get(self.selected_table.schema_index)
                    .map(|schema| schema.name.to_lowercase());
                let filter = self.schema_diagram_filter.trim().to_lowercase();
                let active_connection_index = connection_index;
                let mut open_table_tabs = self
                    .query_tabs
                    .iter()
                    .filter_map(|tab| match &tab.kind {
                        TabKind::Table {
                            connection_index,
                            table_selection,
                            ..
                        } if *connection_index == active_connection_index => Some(*table_selection),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if open_table_tabs.is_empty()
                    && connection
                        .schemas
                        .get(self.selected_table.schema_index)
                        .and_then(|schema| schema.tables.get(self.selected_table.table_index))
                        .is_some()
                {
                    open_table_tabs.push(self.selected_table);
                }
                let diagram_selections =
                    collect_schema_diagram_selections(&connection, &open_table_tabs);
                let mut visible_tables = Vec::new();
                for table_selection in diagram_selections {
                    let Some(schema) = connection.schemas.get(table_selection.schema_index) else {
                        continue;
                    };
                    let Some(table) = schema.tables.get(table_selection.table_index) else {
                        continue;
                    };
                    let schema_name_lower = schema.name.to_lowercase();
                    if self.schema_diagram_current_schema_only
                        && selected_schema_name.as_deref() != Some(schema_name_lower.as_str())
                    {
                        continue;
                    }
                    let matches = filter.is_empty()
                        || schema_name_lower.contains(&filter)
                        || table.name.to_lowercase().contains(&filter)
                        || table
                            .columns
                            .iter()
                            .any(|column| column.name.to_lowercase().contains(&filter));
                    if !matches {
                        continue;
                    }

                    visible_tables.push(SchemaDiagramNode::from_table(
                        table_selection.schema_index,
                        table_selection.table_index,
                        table,
                    ));
                }

                let visible_edges = schema_diagram_edges(&visible_tables);
                let loaded_column_count = visible_tables
                    .iter()
                    .map(|node| node.columns.len())
                    .sum::<usize>();
                let relationship_count = visible_edges.len();

                ui.horizontal(|ui| {
                    inspector_card(
                        ui,
                        "Visible scope",
                        &[
                            format!("Tables: {}", visible_tables.len()),
                            format!("Schemas: {}", count_distinct_schemas(&visible_tables)),
                        ],
                    );
                    inspector_card(
                        ui,
                        "Loaded metadata",
                        &[
                            format!("Columns loaded: {}", loaded_column_count),
                            format!("Foreign keys shown: {}", relationship_count),
                        ],
                    );
                    inspector_card(
                        ui,
                        "Tips",
                        &[
                            "Opened tables expand to include directly related tables.".to_owned(),
                            "Double-click any table card to open and load its full preview."
                                .to_owned(),
                        ],
                    );
                });
                ui.add_space(12.0);

                if visible_tables.is_empty() {
                    ui.vertical_centered(|ui| {
                        ui.add_space(90.0);
                        ui.heading(
                            RichText::new("No open table tabs")
                                .size(22.0)
                                .color(Color32::from_rgb(88, 97, 110)),
                        );
                        ui.add_space(8.0);
                        ui.label(
                            RichText::new(
                                "Open or select a table first, then the diagram will expand around it with directly related tables.",
                            )
                            .color(Color32::from_rgb(120, 129, 140)),
                        );
                    });
                    return;
                }

                let diagram_view_width = ui.available_width().max(680.0);
                let diagram_view_height = ui.available_height().max(420.0);
                if fit_view {
                    let base_layout =
                        layout_schema_diagram(&visible_tables, 1.0, diagram_view_width);
                    let width_fit = (diagram_view_width / base_layout.canvas_size.x).min(1.5);
                    let height_fit = (diagram_view_height / base_layout.canvas_size.y).min(1.5);
                    self.schema_diagram_zoom = width_fit.min(height_fit).clamp(0.55, 1.5);
                }

                let layout =
                    layout_schema_diagram(&visible_tables, self.schema_diagram_zoom, diagram_view_width);
                egui::Frame::default()
                    .fill(Color32::from_rgb(247, 248, 250))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(229, 232, 238)))
                    .corner_radius(12.0)
                    .inner_margin(Margin::same(10))
                    .show(ui, |ui| {
                        egui::ScrollArea::both()
                            .id_salt("schema_diagram_scroll")
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                let (response, painter) =
                                    ui.allocate_painter(layout.canvas_size, Sense::hover());
                                let origin = response.rect.min;

                                for schema_group in &layout.schema_groups {
                                    let schema_rect = schema_group.rect.translate(origin.to_vec2());
                                    painter.rect_filled(
                                        schema_rect,
                                        18.0,
                                        Color32::from_rgb(243, 246, 250),
                                    );
                                    painter.rect_stroke(
                                        schema_rect,
                                        18.0,
                                        egui::Stroke::new(
                                            1.0,
                                            Color32::from_rgb(221, 226, 233),
                                        ),
                                        egui::StrokeKind::Inside,
                                    );
                                    painter.text(
                                        schema_rect.min + Vec2::new(16.0, 14.0),
                                        Align2::LEFT_TOP,
                                        format!("Schema · {}", schema_group.name),
                                        FontId::proportional(
                                            (14.0 * self.schema_diagram_zoom).clamp(12.0, 17.0),
                                        ),
                                        Color32::from_rgb(70, 80, 94),
                                    );
                                }

                                for edge in &visible_edges {
                                    let Some(from) = layout.node_rects.get(edge.from_index) else {
                                        continue;
                                    };
                                    let Some(to) = layout.node_rects.get(edge.to_index) else {
                                        continue;
                                    };
                                    let from_center = from.center();
                                    let to_center = to.center();
                                    let horizontal = (from_center.x - to_center.x).abs()
                                        >= (from_center.y - to_center.y).abs();
                                    let (start, end, bend_a, bend_b) = if horizontal {
                                        let start = if to_center.x >= from_center.x {
                                            egui::pos2(from.right(), from.center().y)
                                        } else {
                                            egui::pos2(from.left(), from.center().y)
                                        } + origin.to_vec2();
                                        let end = if to_center.x >= from_center.x {
                                            egui::pos2(to.left(), to.center().y)
                                        } else {
                                            egui::pos2(to.right(), to.center().y)
                                        } + origin.to_vec2();
                                        let mid_x = (start.x + end.x) * 0.5;
                                        (
                                            start,
                                            end,
                                            egui::pos2(mid_x, start.y),
                                            egui::pos2(mid_x, end.y),
                                        )
                                    } else {
                                        let start = if to_center.y >= from_center.y {
                                            egui::pos2(from.center().x, from.bottom())
                                        } else {
                                            egui::pos2(from.center().x, from.top())
                                        } + origin.to_vec2();
                                        let end = if to_center.y >= from_center.y {
                                            egui::pos2(to.center().x, to.top())
                                        } else {
                                            egui::pos2(to.center().x, to.bottom())
                                        } + origin.to_vec2();
                                        let mid_y = (start.y + end.y) * 0.5;
                                        (
                                            start,
                                            end,
                                            egui::pos2(start.x, mid_y),
                                            egui::pos2(end.x, mid_y),
                                        )
                                    };
                                    let stroke = egui::Stroke::new(
                                        1.5,
                                        Color32::from_rgba_unmultiplied(97, 118, 167, 170),
                                    );
                                    painter.line_segment([start, bend_a], stroke);
                                    painter.line_segment([bend_a, bend_b], stroke);
                                    painter.line_segment([bend_b, end], stroke);
                                    painter.circle_filled(end, 3.0, Color32::from_rgb(97, 118, 167));
                                    painter.text(
                                        egui::pos2(
                                            (start.x + end.x) * 0.5 + 6.0,
                                            (start.y + end.y) * 0.5 - 6.0,
                                        ),
                                        Align2::LEFT_CENTER,
                                        &edge.label,
                                        FontId::proportional((11.0 * self.schema_diagram_zoom).clamp(10.0, 14.0)),
                                        Color32::from_rgb(96, 106, 120),
                                    );
                                }

                                let mut open_table = None;
                                let mut select_table = None;
                                for (index, node) in visible_tables.iter().enumerate() {
                                    let Some(node_rect) = layout.node_rects.get(index) else {
                                        continue;
                                    };
                                    let card_rect = node_rect.translate(origin.to_vec2());
                                    let response = ui.interact(
                                        card_rect,
                                        egui::Id::new(("schema_map_card", node.schema_index, node.table_index)),
                                        Sense::click(),
                                    );
                                    if response.clicked() {
                                        select_table = Some((node.schema_index, node.table_index));
                                    }
                                    if response.double_clicked() {
                                        open_table = Some((node.schema_index, node.table_index));
                                    }

                                    let selected = self.selected_table.schema_index == node.schema_index
                                        && self.selected_table.table_index == node.table_index;
                                    let fill = if selected {
                                        Color32::from_rgb(255, 252, 249)
                                    } else if response.hovered() {
                                        Color32::from_rgb(255, 255, 255)
                                    } else {
                                        Color32::from_rgb(250, 251, 252)
                                    };
                                    let stroke = if selected {
                                        egui::Stroke::new(2.0, Color32::from_rgb(191, 67, 59))
                                    } else {
                                        egui::Stroke::new(1.0, Color32::from_rgb(219, 223, 230))
                                    };
                                    painter.rect_filled(card_rect, 14.0, fill);
                                    painter.rect_stroke(
                                        card_rect,
                                        12.0,
                                        stroke,
                                        egui::StrokeKind::Inside,
                                    );
                                    let header_rect = egui::Rect::from_min_max(
                                        card_rect.min,
                                        egui::pos2(card_rect.max.x, card_rect.min.y + 26.0),
                                    );
                                    painter.rect_filled(
                                        header_rect,
                                        egui::CornerRadius {
                                            nw: 12,
                                            ne: 12,
                                            sw: 0,
                                            se: 0,
                                        },
                                        if selected {
                                            Color32::from_rgb(97, 152, 176)
                                        } else {
                                            Color32::from_rgb(106, 154, 178)
                                        },
                                    );

                                    let title_pos = card_rect.min + Vec2::new(12.0, 8.0);
                                    painter.text(
                                        title_pos,
                                        Align2::LEFT_TOP,
                                        truncate_middle(&node.name, 24),
                                        FontId::proportional(
                                            (13.5 * self.schema_diagram_zoom).clamp(12.0, 16.0),
                                        ),
                                        Color32::WHITE,
                                    );
                                    painter.text(
                                        title_pos + Vec2::new(0.0, 28.0),
                                        Align2::LEFT_TOP,
                                        format!(
                                            "{} pk • {} fk",
                                            node.columns.iter().filter(|column| column.primary).count(),
                                            node.foreign_keys.len()
                                        ),
                                        FontId::proportional(
                                            (10.5 * self.schema_diagram_zoom).clamp(9.5, 12.0),
                                        ),
                                        Color32::from_rgb(112, 121, 134),
                                    );

                                    let card_lines = schema_diagram_card_lines(node);
                                    let line_height =
                                        (16.5 * self.schema_diagram_zoom).clamp(15.0, 20.0);
                                    for (line_index, (line, is_primary)) in card_lines
                                        .iter()
                                        .take(SCHEMA_DIAGRAM_COLUMN_PREVIEW)
                                        .enumerate()
                                    {
                                        painter.text(
                                            title_pos
                                                + Vec2::new(
                                                    0.0,
                                                    50.0 + line_index as f32 * line_height,
                                                ),
                                            Align2::LEFT_TOP,
                                            truncate_middle(line, 42),
                                            FontId::proportional(
                                                (10.0 * self.schema_diagram_zoom).clamp(9.0, 11.5),
                                            ),
                                            if *is_primary {
                                                Color32::from_rgb(185, 69, 59)
                                            } else {
                                                Color32::from_rgb(90, 100, 114)
                                            },
                                        );
                                    }

                                    if card_lines.len() > SCHEMA_DIAGRAM_COLUMN_PREVIEW {
                                        painter.text(
                                            card_rect.right_bottom() - Vec2::new(14.0, 14.0),
                                            Align2::RIGHT_BOTTOM,
                                            format!(
                                                "+{} more relations",
                                                card_lines.len() - SCHEMA_DIAGRAM_COLUMN_PREVIEW
                                            ),
                                            FontId::proportional((10.5 * self.schema_diagram_zoom).clamp(10.0, 12.0)),
                                            Color32::from_rgb(118, 126, 138),
                                        );
                                    }
                                }

                                if let Some((schema_index, table_index)) = select_table {
                                    self.select_table(schema_index, table_index);
                                }
                                if let Some((schema_index, table_index)) = open_table {
                                    self.select_table(schema_index, table_index);
                                    self.open_selected_table();
                                }
                            });
                    });
            });
        self.show_schema_diagram = open;
    }

    fn ui_sql_preview(&mut self, ctx: &egui::Context) {
        let sql_preview = match self.pending_row_update.as_ref() {
            Some(pending) => pending.sql.clone(),
            None => return,
        };
        let mut sql_preview = sql_preview;

        egui::Window::new("Review SQL before executing")
            .collapsible(false)
            .resizable(true)
            .default_width(620.0)
            .default_height(280.0)
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(252, 252, 253))
                    .stroke(egui::Stroke::new(1.5, Color32::from_rgb(220, 224, 230)))
                    .corner_radius(16.0)
                    .shadow(egui::Shadow {
                        offset: [0, 12],
                        blur: 36,
                        spread: 8,
                        color: Color32::from_rgba_unmultiplied(0, 0, 0, 40),
                    }),
            )
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    ui.heading(
                        RichText::new("Confirm row update SQL")
                            .size(18.0)
                            .color(Color32::from_rgb(34, 44, 62)),
                    );
                    ui.add_space(4.0);
                    ui.label(
                        RichText::new(
                            "An UPDATE statement will be issued for this row. Review it before executing.",
                        )
                        .size(13.0)
                        .color(Color32::from_rgb(118, 125, 138)),
                    );
                    ui.add_space(12.0);
                    egui::Frame::default()
                        .fill(Color32::from_rgb(250, 250, 252))
                        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(223, 227, 235)))
                        .corner_radius(10.0)
                    .inner_margin(Margin::symmetric(12, 10))
                        .show(ui, |ui| {
                            ui.add(
                                TextEdit::multiline(&mut sql_preview)
                                    .code_editor()
                                    .font(egui::TextStyle::Monospace)
                                    .interactive(false)
                                    .desired_rows(8)
                                    .desired_width(ui.available_width()),
                            );
                        });
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    RichText::new("Run · Cmd + Enter")
                                        .size(14.0)
                                        .color(Color32::WHITE),
                                )
                                .fill(Color32::from_rgb(67, 133, 245))
                                .stroke(egui::Stroke::new(1.0, Color32::from_rgb(52, 101, 205)))
                                .corner_radius(8.0),
                            )
                            .clicked()
                        {
                            self.confirm_pending_row_update();
                        }
                        ui.add_space(8.0);
                        if ui
                            .add(
                                egui::Button::new("Cancel")
                                    .fill(Color32::from_rgb(237, 238, 239))
                                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(210, 213, 218)))
                                    .corner_radius(8.0),
                            )
                            .clicked()
                        {
                            self.cancel_pending_row_update();
                        }
                    });
                });
            });
    }

    fn ui_connection_manager(&mut self, ctx: &egui::Context) {
        if !self.connection_manager_open {
            return;
        }

        let mut submit = false;
        let mut import_url = false;
        let is_editing = self.editing_connection_index.is_some();
        let window_title = if is_editing {
            "Edit Connection"
        } else {
            "New Connection"
        };
        let submit_label = if is_editing { "Save Changes" } else { "Create" };
        egui::Window::new(window_title)
            .collapsible(false)
            .resizable(false)
            .default_width(520.0)
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .frame(
                egui::Frame::default()
                    .fill(Color32::from_rgb(255, 255, 255))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(225, 230, 237)))
                    .corner_radius(14.0)
                    .inner_margin(Margin::same(16)),
            )
            .show(ctx, |ui| {
                ui.label(
                    RichText::new(if is_editing {
                        "Edit connection profile"
                    } else {
                        "Create a connection profile"
                    })
                    .size(18.0)
                    .strong()
                    .color(Color32::from_rgb(43, 52, 66)),
                );
                ui.add_space(10.0);

                egui::Frame::default()
                    .fill(Color32::from_rgb(248, 250, 253))
                    .stroke(egui::Stroke::new(1.0, Color32::from_rgb(229, 233, 239)))
                    .corner_radius(10.0)
                    .inner_margin(Margin::same(12))
                    .show(ui, |ui| {
                        ui.label(
                            RichText::new("Import from URL")
                                .strong()
                                .color(Color32::from_rgb(58, 68, 82)),
                        );
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("URL")
                                    .size(13.0)
                                    .color(Color32::from_rgb(92, 101, 113)),
                            );
                            ui.add_sized(
                                [ui.available_width() - 90.0, 30.0],
                                TextEdit::singleline(&mut self.connection_form.connection_url)
                                    .hint_text("mysql://user:pass@host:3306/db or postgres://..."),
                            );
                            if soft_button(ui, "Import URL").clicked() {
                                import_url = true;
                            }
                        });
                    });

                ui.add_space(10.0);

                field_row(ui, "Name", &mut self.connection_form.name);
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("Engine")
                            .size(13.0)
                            .color(Color32::from_rgb(92, 101, 113)),
                    );
                    egui::ComboBox::from_id_salt("connection_engine")
                        .selected_text(format!("{}", self.connection_form.engine))
                        .width(ui.available_width())
                        .show_ui(ui, |ui| {
                            for engine in ENGINE_OPTIONS {
                                let selected = self.connection_form.engine == *engine;
                                if ui
                                    .selectable_label(selected, format!("{}", engine))
                                    .clicked()
                                {
                                    self.connection_form.engine = *engine;
                                    self.connection_form.port =
                                        default_port(self.connection_form.engine).to_string();
                                }
                            }
                        });
                });

                let is_file_based = matches!(
                    self.connection_form.engine,
                    ConnectionEngine::SQLite | ConnectionEngine::DuckDB
                );

                if is_file_based {
                    field_row(ui, "File Path", &mut self.connection_form.path);
                } else {
                    field_row(ui, "Host", &mut self.connection_form.host);
                    field_row(ui, "Port", &mut self.connection_form.port);
                    field_row(ui, "Username", &mut self.connection_form.user);
                    password_row(ui, "Password", &mut self.connection_form.password);
                }

                field_row(
                    ui,
                    "Database (optional)",
                    &mut self.connection_form.database,
                );

                ui.add_space(6.0);
                ui.checkbox(
                    &mut self.connection_form.use_ssh,
                    "Connect through SSH tunnel",
                );
                if self.connection_form.use_ssh {
                    ui.add_space(8.0);
                    egui::Frame::default()
                        .fill(Color32::from_rgb(248, 250, 253))
                        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(229, 233, 239)))
                        .corner_radius(10.0)
                        .inner_margin(Margin::same(12))
                        .show(ui, |ui| {
                            ui.label(
                                RichText::new("SSH settings")
                                    .strong()
                                    .color(Color32::from_rgb(58, 68, 82)),
                            );
                            ui.add_space(8.0);
                            field_row(ui, "SSH Host", &mut self.connection_form.ssh_host);
                            field_row(ui, "SSH Port", &mut self.connection_form.ssh_port);
                            field_row(ui, "SSH Username", &mut self.connection_form.ssh_user);
                            password_row(
                                ui,
                                "SSH Password",
                                &mut self.connection_form.ssh_password,
                            );
                            field_row(
                                ui,
                                "Private Key",
                                &mut self.connection_form.ssh_private_key_path,
                            );
                        });
                }

                ui.add_space(12.0);
                ui.horizontal(|ui| {
                    if soft_button(ui, "Cancel").clicked() {
                        self.connection_manager_open = false;
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if top_button(ui, submit_label, Color32::from_rgb(67, 133, 245)).clicked() {
                            submit = true;
                        }
                    });
                });
            });

        if submit {
            self.submit_connection_form();
        }
        if import_url {
            match self.connection_form.import_connection_url() {
                Ok(()) => self.push_activity("Imported connection details from URL."),
                Err(error) => self.push_activity(format!("URL import failed: {}", error)),
            }
        }
    }
}

impl App for MangabaseApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_background_events();
        self.handle_shortcuts(ctx);
        self.ui_top_bar(ctx);
        self.ui_left_sidebar(ctx);
        self.ui_right_sidebar(ctx);
        self.ui_center_workspace(ctx);
        self.ui_schema_diagram(ctx);
        self.ui_busy_overlay(ctx);
        self.ui_status_bar(ctx);
        self.ui_command_palette(ctx);
        self.ui_shortcuts_help(ctx);
        self.ui_connection_manager(ctx);
        self.ui_sql_preview(ctx);
    }
}

fn configure_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::light();
    visuals.panel_fill = Color32::from_rgb(243, 241, 239);
    visuals.window_fill = Color32::from_rgb(255, 255, 255);
    visuals.extreme_bg_color = Color32::from_rgb(255, 255, 255);
    visuals.faint_bg_color = Color32::from_rgb(243, 241, 239);
    visuals.code_bg_color = Color32::from_rgb(249, 249, 249);
    visuals.selection.bg_fill = Color32::from_rgb(203, 58, 50);
    visuals.selection.stroke = egui::Stroke::new(1.0, Color32::from_rgb(181, 44, 38));
    visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(255, 255, 255);
    visuals.widgets.inactive.bg_fill = Color32::from_rgb(255, 255, 255);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(244, 242, 241);
    visuals.widgets.active.bg_fill = Color32::from_rgb(236, 233, 230);
    visuals.widgets.noninteractive.corner_radius = 0.0.into();
    visuals.widgets.inactive.corner_radius = 0.0.into();
    visuals.widgets.hovered.corner_radius = 0.0.into();
    visuals.widgets.active.corner_radius = 0.0.into();
    visuals.widgets.open.corner_radius = 0.0.into();
    visuals.window_shadow = egui::epaint::Shadow::NONE;
    visuals.override_text_color = Some(Color32::from_rgb(58, 58, 58));
    ctx.set_visuals(visuals);
}

fn top_button(ui: &mut egui::Ui, label: &str, fill: Color32) -> egui::Response {
    let text_color = if fill == Color32::from_rgb(232, 235, 241) {
        Color32::from_rgb(51, 60, 74)
    } else if fill == Color32::from_rgb(253, 187, 45) {
        Color32::from_rgb(79, 56, 11)
    } else {
        Color32::WHITE
    };

    ui.add(
        egui::Button::new(RichText::new(label).size(12.0).color(text_color))
            .fill(fill)
            .stroke(egui::Stroke::new(1.0, Color32::from_rgb(214, 210, 206)))
            .corner_radius(4.0),
    )
}

fn soft_button(ui: &mut egui::Ui, label: &str) -> egui::Response {
    ui.add(
        egui::Button::new(
            RichText::new(label)
                .size(12.0)
                .color(Color32::from_rgb(84, 84, 84)),
        )
        .fill(Color32::from_rgb(251, 251, 251))
        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(216, 212, 208)))
        .corner_radius(4.0),
    )
}

fn with_shortcut(resp: egui::Response, shortcut: impl AsRef<str>) -> egui::Response {
    resp.on_hover_text(format!("Shortcut: {}", shortcut.as_ref()))
}

fn chip(ui: &mut egui::Ui, label: &str, fill: Color32, text: Color32) {
    egui::Frame::default()
        .fill(fill)
        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(218, 214, 210)))
        .corner_radius(4.0)
        .inner_margin(Margin::symmetric(8, 3))
        .show(ui, |ui| {
            ui.label(RichText::new(label).size(12.0).color(text));
        });
}

fn truncate_middle(value: &str, max_chars: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        return value.to_owned();
    }
    if max_chars <= 3 {
        return "...".to_owned();
    }

    let prefix_len = (max_chars - 3) / 2;
    let suffix_len = max_chars - 3 - prefix_len;
    let prefix = chars.iter().take(prefix_len).collect::<String>();
    let suffix = chars
        .iter()
        .skip(chars.len().saturating_sub(suffix_len))
        .collect::<String>();
    format!("{}...{}", prefix, suffix)
}

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

fn field_row(ui: &mut egui::Ui, label: &str, value: &mut String) {
    ui.horizontal(|ui| {
        ui.label(
            RichText::new(label)
                .size(13.0)
                .color(Color32::from_rgb(92, 101, 113)),
        );
        ui.add_sized([ui.available_width(), 30.0], TextEdit::singleline(value));
    });
}

fn password_row(ui: &mut egui::Ui, label: &str, value: &mut String) {
    ui.horizontal(|ui| {
        ui.label(
            RichText::new(label)
                .size(13.0)
                .color(Color32::from_rgb(92, 101, 113)),
        );
        ui.add_sized(
            [ui.available_width(), 30.0],
            TextEdit::singleline(value).password(true),
        );
    });
}

fn inspector_card(ui: &mut egui::Ui, title: &str, rows: &[String]) {
    egui::Frame::default()
        .fill(Color32::from_rgb(255, 255, 255))
        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(233, 236, 242)))
        .corner_radius(12.0)
        .inner_margin(Margin::same(12))
        .show(ui, |ui| {
            ui.label(
                RichText::new(title)
                    .size(15.0)
                    .strong()
                    .color(Color32::from_rgb(54, 63, 76)),
            );
            ui.add_space(6.0);
            for row in rows {
                ui.label(RichText::new(row).color(Color32::from_rgb(117, 126, 137)));
            }
        });
}

fn first_line(sql: &str) -> String {
    sql.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("Untitled query")
        .trim()
        .to_owned()
}

fn infer_tab_title(sql: &str, fallback: usize) -> String {
    let sql_lower = sql.to_lowercase();
    for marker in ["from ", "update ", "into "] {
        if let Some(index) = sql_lower.find(marker) {
            let tail = &sql_lower[index + marker.len()..];
            let table = tail
                .split(|ch: char| ch.is_whitespace() || ch == ';' || ch == ',')
                .next()
                .unwrap_or_default();
            if !table.is_empty() {
                return format!("{}.sql", table.replace('.', "_"));
            }
        }
    }

    format!("Query {}", fallback)
}

fn format_count(value: usize) -> String {
    let raw = value.to_string();
    let mut formatted = String::new();

    for (index, ch) in raw.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(ch);
    }

    formatted.chars().rev().collect()
}

fn blank_to_null(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "NULL".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn parse_structure_foreign_key(
    value: &str,
    default_schema: &str,
    column_name: &str,
) -> Option<TableForeignKey> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("empty")
        || trimmed.eq_ignore_ascii_case("null")
    {
        return None;
    }

    let (table_part, column_part) = trimmed.split_once('(')?;
    let referenced_column = column_part.trim_end_matches(')').trim();
    if referenced_column.is_empty() {
        return None;
    }
    let (referenced_schema, referenced_table) =
        if let Some((schema, table)) = table_part.rsplit_once('.') {
            (schema.trim().to_owned(), table.trim().to_owned())
        } else {
            (default_schema.to_owned(), table_part.trim().to_owned())
        };
    if referenced_table.is_empty() {
        return None;
    }

    Some(TableForeignKey {
        column_name: column_name.to_owned(),
        referenced_schema,
        referenced_table,
        referenced_column: referenced_column.to_owned(),
    })
}

fn count_distinct_schemas(nodes: &[SchemaDiagramNode]) -> usize {
    let mut schemas = HashSet::new();
    for node in nodes {
        schemas.insert(node.schema.as_str());
    }
    schemas.len()
}

fn collect_schema_diagram_selections(
    connection: &ConnectionProfile,
    seed_tables: &[TableSelection],
) -> Vec<TableSelection> {
    let mut ordered = Vec::new();
    let mut seen = HashSet::<(usize, usize)>::new();

    let mut push_unique = |selection: TableSelection, ordered: &mut Vec<TableSelection>| {
        if seen.insert((selection.schema_index, selection.table_index)) {
            ordered.push(selection);
        }
    };

    for selection in seed_tables {
        push_unique(*selection, &mut ordered);
    }

    for selection in seed_tables {
        let Some(source_table) = connection
            .schemas
            .get(selection.schema_index)
            .and_then(|schema| schema.tables.get(selection.table_index))
        else {
            continue;
        };

        for foreign_key in &source_table.foreign_keys {
            if let Some(target_selection) = find_table_selection_by_name(
                connection,
                &foreign_key.referenced_schema,
                &foreign_key.referenced_table,
            ) {
                push_unique(target_selection, &mut ordered);
            }
        }
    }

    for (schema_index, schema) in connection.schemas.iter().enumerate() {
        for (table_index, table) in schema.tables.iter().enumerate() {
            let references_seed = table.foreign_keys.iter().any(|foreign_key| {
                seed_tables.iter().any(|seed| {
                    connection
                        .schemas
                        .get(seed.schema_index)
                        .and_then(|seed_schema| seed_schema.tables.get(seed.table_index))
                        .map(|seed_table| {
                            foreign_key
                                .referenced_schema
                                .eq_ignore_ascii_case(&seed_table.schema)
                                && foreign_key
                                    .referenced_table
                                    .eq_ignore_ascii_case(&seed_table.name)
                        })
                        .unwrap_or(false)
                })
            });
            if references_seed {
                push_unique(
                    TableSelection {
                        schema_index,
                        table_index,
                    },
                    &mut ordered,
                );
            }
        }
    }

    ordered
}

fn find_table_selection_by_name(
    connection: &ConnectionProfile,
    schema_name: &str,
    table_name: &str,
) -> Option<TableSelection> {
    connection
        .schemas
        .iter()
        .enumerate()
        .find_map(|(schema_index, schema)| {
            if !schema.name.eq_ignore_ascii_case(schema_name) {
                return None;
            }
            schema
                .tables
                .iter()
                .enumerate()
                .find_map(|(table_index, table)| {
                    table
                        .name
                        .eq_ignore_ascii_case(table_name)
                        .then_some(TableSelection {
                            schema_index,
                            table_index,
                        })
                })
        })
}

fn schema_diagram_card_lines(node: &SchemaDiagramNode) -> Vec<(String, bool)> {
    let mut lines = Vec::new();

    for column in node.columns.iter().filter(|column| column.primary) {
        lines.push((format!("PK  {}", column.name), true));
    }

    for foreign_key in &node.foreign_keys {
        lines.push((
            format!(
                "FK  {} -> {}.{}",
                foreign_key.column_name,
                foreign_key.referenced_table,
                foreign_key.referenced_column
            ),
            false,
        ));
    }

    if lines.is_empty() {
        lines.push(("No FK / PK metadata loaded".to_owned(), false));
    }

    lines
}

fn schema_diagram_edges(nodes: &[SchemaDiagramNode]) -> Vec<SchemaDiagramEdge> {
    let mut node_lookup = BTreeMap::<(String, String), usize>::new();
    for (index, node) in nodes.iter().enumerate() {
        node_lookup.insert(
            (node.schema.to_lowercase(), node.name.to_lowercase()),
            index,
        );
    }

    let mut edges = Vec::new();
    for (from_index, source) in nodes.iter().enumerate() {
        for foreign_key in &source.foreign_keys {
            let key = (
                foreign_key.referenced_schema.to_lowercase(),
                foreign_key.referenced_table.to_lowercase(),
            );
            let Some(to_index) = node_lookup.get(&key).copied() else {
                continue;
            };
            if from_index == to_index {
                continue;
            }
            edges.push(SchemaDiagramEdge {
                from_index,
                to_index,
                label: format!(
                    "{} -> {}",
                    foreign_key.column_name, foreign_key.referenced_column
                ),
            });
        }
    }
    edges
}

fn layout_schema_diagram(
    nodes: &[SchemaDiagramNode],
    zoom: f32,
    available_width: f32,
) -> SchemaDiagramLayout {
    let zoom = zoom.clamp(0.55, 1.5);
    let mut grouped = BTreeMap::<String, Vec<usize>>::new();
    for (index, node) in nodes.iter().enumerate() {
        grouped.entry(node.schema.clone()).or_default().push(index);
    }

    let card_width = 280.0 * zoom;
    let card_gap_x = 18.0 * zoom;
    let row_gap = 18.0 * zoom;
    let schema_gap_x = 34.0 * zoom;
    let schema_gap_y = 34.0 * zoom;
    let schema_header_height = 42.0 * zoom;
    let schema_padding_x = 14.0 * zoom;
    let schema_padding_top = 14.0 * zoom;
    let schema_padding_bottom = 18.0 * zoom;
    let mut node_rects = vec![egui::Rect::NOTHING; nodes.len()];
    let mut schema_groups = Vec::new();
    let mut current_x = 24.0 * zoom;
    let mut current_y = 20.0 * zoom;
    let mut row_max_height = 0.0;
    let mut max_used_right: f32 = 0.0;
    let max_row_width = available_width.max(680.0) - 24.0 * zoom;

    for (schema_name, indices) in grouped {
        let schema_count = indices.len();
        let schema_columns = ((schema_count as f32).sqrt().ceil() as usize)
            .clamp(1, 4)
            .max(if schema_count >= 6 { 2 } else { 1 });
        let schema_inner_width = schema_columns as f32 * card_width
            + (schema_columns.saturating_sub(1) as f32) * card_gap_x;
        let schema_width = schema_inner_width + schema_padding_x * 2.0;
        if current_x + schema_width > max_row_width && current_x > 24.0 * zoom {
            current_x = 24.0 * zoom;
            current_y += row_max_height + schema_gap_y;
            row_max_height = 0.0;
        }

        let schema_left = current_x;
        let schema_top = current_y;
        let mut column_heights = vec![0.0; schema_columns];
        for node_index in &indices {
            let node = &nodes[*node_index];
            let line_count = schema_diagram_card_lines(node)
                .len()
                .min(SCHEMA_DIAGRAM_COLUMN_PREVIEW);
            let card_height = (82.0 + line_count as f32 * 18.0) * zoom
                + if schema_diagram_card_lines(node).len() > SCHEMA_DIAGRAM_COLUMN_PREVIEW {
                    18.0 * zoom
                } else {
                    0.0
                };
            let column_index = column_heights
                .iter()
                .enumerate()
                .min_by(|left, right| {
                    left.1
                        .partial_cmp(right.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(index, _)| index)
                .unwrap_or(0);
            let rect = egui::Rect::from_min_size(
                egui::pos2(
                    schema_left
                        + schema_padding_x
                        + column_index as f32 * (card_width + card_gap_x),
                    schema_top
                        + schema_padding_top
                        + schema_header_height
                        + column_heights[column_index],
                ),
                Vec2::new(card_width, card_height),
            );
            node_rects[*node_index] = rect;
            column_heights[column_index] += card_height + row_gap;
        }
        let content_height = column_heights.into_iter().fold(0.0_f32, f32::max)
            - if schema_count == 0 { 0.0 } else { row_gap };
        let schema_rect = egui::Rect::from_min_max(
            egui::pos2(schema_left, schema_top),
            egui::pos2(
                schema_left + schema_width,
                (schema_top
                    + schema_padding_top
                    + schema_header_height
                    + content_height
                    + schema_padding_bottom)
                    .max(schema_top + 150.0 * zoom),
            ),
        );
        row_max_height = row_max_height.max(schema_rect.height());
        max_used_right = max_used_right.max(schema_rect.right());
        current_x = schema_rect.right() + schema_gap_x;
        schema_groups.push(SchemaDiagramSchemaGroupLayout {
            name: schema_name,
            rect: schema_rect,
        });
    }

    SchemaDiagramLayout {
        canvas_size: Vec2::new(
            (max_used_right + 24.0 * zoom).max(available_width.max(680.0)),
            (current_y + row_max_height + 24.0 * zoom).max(420.0),
        ),
        node_rects,
        schema_groups,
    }
}

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
    rect: egui::Rect,
    popup_width: f32,
) -> egui::Pos2 {
    let x_limit = (rect.width() - popup_width - 12.0).max(8.1);

    // Position it slightly below the cursor
    let x = (cursor_pos.x - rect.left() + 4.0).clamp(8.0, x_limit);
    let y = cursor_pos.y - rect.top() + 20.0;

    rect.left_top() + egui::Vec2::new(x, y)
}

fn connection_database_label(connection: &ConnectionProfile) -> String {
    let database = connection.database.trim();
    if database.is_empty() {
        "all schemas".to_owned()
    } else {
        database.to_owned()
    }
}

fn parse_table_preview_limit_input(value: &str) -> Result<Option<usize>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Some(DEFAULT_TABLE_PREVIEW_LIMIT));
    }

    let parsed = trimmed
        .parse::<usize>()
        .map_err(|_| "Rows to load must be a whole number. Use 0 for all rows.".to_owned())?;

    if parsed == 0 {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

fn limit_clause(limit: Option<usize>) -> String {
    match limit {
        Some(limit) => format!("\nLIMIT {};", limit),
        None => ";".to_owned(),
    }
}

fn enum_options_for_column(column: &TableColumn) -> Vec<String> {
    let kind = column.kind.trim();
    if !kind.to_ascii_lowercase().starts_with("enum(") {
        return Vec::new();
    }

    let Some(start) = kind.find('(') else {
        return Vec::new();
    };
    let Some(end) = kind.rfind(')') else {
        return Vec::new();
    };
    let inner = &kind[start + 1..end];
    let mut values = Vec::new();
    let mut chars = inner.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '\'' {
            continue;
        }

        let mut value = String::new();
        while let Some(next) = chars.next() {
            match next {
                '\'' => {
                    if matches!(chars.peek(), Some('\'')) {
                        value.push('\'');
                        chars.next();
                    } else {
                        break;
                    }
                }
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        value.push(escaped);
                    }
                }
                other => value.push(other),
            }
        }
        values.push(value);
    }

    if column.nullable
        && !values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("NULL"))
    {
        values.push("NULL".to_owned());
    }
    values
}

fn result_column_meta<'a>(table: &'a TableInfo, column_name: &str) -> Option<&'a TableColumn> {
    table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
}

fn result_column_foreign_key<'a>(
    table: &'a TableInfo,
    column_name: &str,
) -> Option<&'a TableForeignKey> {
    table
        .foreign_keys
        .iter()
        .find(|fk| fk.column_name.eq_ignore_ascii_case(column_name))
}

fn limit_clause_inline(limit: Option<usize>) -> String {
    match limit {
        Some(limit) => format!(" LIMIT {}", limit),
        None => String::new(),
    }
}

fn table_preview_query_text(table: &TableInfo, row_limit: Option<usize>) -> String {
    let select_columns = if table.columns.is_empty() {
        "*".to_owned()
    } else {
        table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut sql = format!(
        "SELECT {}\nFROM {}.{}",
        select_columns, table.schema, table.name
    );
    if !table.primary_sort.is_empty() {
        sql.push_str(&format!("\nORDER BY {} ASC", table.primary_sort));
    }
    sql.push_str(&limit_clause(row_limit));
    sql
}

fn filtered_row_indices(
    result: &QueryResult,
    filter_mode: ResultFilterMode,
    rules: &[ResultFilterRule],
) -> Vec<usize> {
    if matches!(filter_mode, ResultFilterMode::RawSql) {
        return (0..result.rows.len()).collect();
    }

    let active_rules = rules
        .iter()
        .filter(|rule| !rule.operator.requires_value() || !rule.value.trim().is_empty())
        .collect::<Vec<_>>();
    if active_rules.is_empty() {
        return (0..result.rows.len()).collect();
    }

    result
        .rows
        .iter()
        .enumerate()
        .filter_map(|(row_index, row)| {
            let matches = active_rules.iter().all(|rule| {
                if let Some(column_index) = rule.column {
                    row.get(column_index)
                        .map(|value| rule.operator.matches(value, rule.value.trim()))
                        .unwrap_or(false)
                } else {
                    row.iter()
                        .any(|value| rule.operator.matches(value, rule.value.trim()))
                }
            });
            matches.then_some(row_index)
        })
        .collect()
}

fn column_candidates(column_names: &[String], filter: &str) -> Vec<(usize, String)> {
    let needle = filter.to_lowercase();
    column_names
        .iter()
        .enumerate()
        .filter_map(|(index, name)| {
            if needle.is_empty() || name.to_lowercase().contains(&needle) {
                Some((index, name.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn commit_first_column_match(rule: &mut ResultFilterRule, filtered_options: &[(usize, String)]) {
    if let Some((index, column_name)) = filtered_options.first() {
        rule.column = Some(*index);
        rule.column_search = column_name.clone();
    }
}

fn filter_column_popup_id(tab_id: usize, rule_index: usize) -> egui::Id {
    egui::Id::new(("results_filter_column_popup", tab_id, rule_index))
}

fn filter_column_search_id(tab_id: usize, rule_index: usize) -> egui::Id {
    egui::Id::new(("results_filter_column_search", tab_id, rule_index))
}

fn filter_operator_popup_id(tab_id: usize, rule_index: usize) -> egui::Id {
    egui::Id::new(("results_filter_operator_popup", tab_id, rule_index))
}

fn normalized_filter_value(value: &str) -> &str {
    let trimmed = value.trim();
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        trimmed[1..trimmed.len().saturating_sub(1)].trim()
    } else {
        trimmed
    }
}

fn looks_like_null(value: &str) -> bool {
    let normalized = normalized_filter_value(value);
    normalized.is_empty() || normalized.eq_ignore_ascii_case("null")
}

fn split_filter_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(normalized_filter_value)
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_owned())
        .collect()
}

fn parse_filter_between(value: &str) -> Option<(String, String)> {
    let parts = regex::Regex::new(r"(?i)\s+and\s+|,")
        .ok()?
        .split(value)
        .map(normalized_filter_value)
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_owned())
        .collect::<Vec<_>>();
    if parts.len() == 2 {
        Some((parts[0].clone(), parts[1].clone()))
    } else {
        None
    }
}

fn compare_filter_values(left: &str, right: &str) -> std::cmp::Ordering {
    match (left.trim().parse::<f64>(), right.trim().parse::<f64>()) {
        (Ok(left_number), Ok(right_number)) => left_number
            .partial_cmp(&right_number)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => left.to_lowercase().cmp(&right.to_lowercase()),
    }
}

fn sql_like_matches(value: &str, pattern: &str) -> bool {
    let mut regex_pattern = String::from("(?i)^");
    for ch in pattern.chars() {
        match ch {
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            _ => regex_pattern.push_str(&regex::escape(&ch.to_string())),
        }
    }
    regex_pattern.push('$');
    regex::Regex::new(&regex_pattern)
        .map(|regex| regex.is_match(value))
        .unwrap_or(false)
}

fn any_filter_popup_open(ctx: &egui::Context, tab_id: usize, rule_count: usize) -> bool {
    (0..rule_count).any(|rule_index| {
        let column_popup_id = filter_column_popup_id(tab_id, rule_index);
        let operator_popup_id = filter_operator_popup_id(tab_id, rule_index);
        ctx.memory(|mem| mem.is_popup_open(column_popup_id))
            || ctx.memory(|mem| mem.is_popup_open(operator_popup_id))
    })
}

fn all_result_filter_operators() -> &'static [ResultFilterOperator] {
    &[
        ResultFilterOperator::Equals,
        ResultFilterOperator::NotEquals,
        ResultFilterOperator::LessThan,
        ResultFilterOperator::GreaterThan,
        ResultFilterOperator::LessThanOrEqual,
        ResultFilterOperator::GreaterThanOrEqual,
        ResultFilterOperator::In,
        ResultFilterOperator::NotIn,
        ResultFilterOperator::IsNull,
        ResultFilterOperator::IsNotNull,
        ResultFilterOperator::Between,
        ResultFilterOperator::NotBetween,
        ResultFilterOperator::Like,
        ResultFilterOperator::Contains,
        ResultFilterOperator::NotContains,
        ResultFilterOperator::ContainsCaseSensitive,
        ResultFilterOperator::NotContainsCaseSensitive,
        ResultFilterOperator::HasPrefix,
        ResultFilterOperator::HasSuffix,
        ResultFilterOperator::HasPrefixCaseSensitive,
        ResultFilterOperator::HasSuffixCaseSensitive,
    ]
}

fn operator_group_boundary(index: usize) -> bool {
    matches!(index, 6 | 13)
}

fn reindex_query_tabs_after_connection_delete(
    query_tabs: &mut Vec<QueryTab>,
    deleted_index: usize,
) {
    query_tabs.retain(|tab| {
        !matches!(
            tab.kind,
            TabKind::Table {
                connection_index,
                ..
            } if connection_index == deleted_index
        )
    });

    for tab in query_tabs {
        if let TabKind::Table {
            ref mut connection_index,
            ..
        } = tab.kind
        {
            if *connection_index > deleted_index {
                *connection_index -= 1;
            }
        }
    }
}

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

#[derive(Clone)]
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

#[derive(Clone, Copy, PartialEq, Eq)]
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

#[derive(Clone, Copy, PartialEq, Eq)]
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
            kind: TabKind::Table {
                connection_index,
                table_selection,
                table_ref,
            },
        }
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
const ROW_INSPECTOR_CARD_LIMIT: usize = 20;
const DEFAULT_TABLE_PREVIEW_LIMIT: usize = 500;
const SCHEMA_DIAGRAM_COLUMN_PREVIEW: usize = 8;

fn demo_history() -> Vec<HistoryEntry> {
    vec![
        HistoryEntry {
            title: "Recent paid orders".to_owned(),
            sql: "SELECT id, customer_email, total_cents\nFROM public.orders\nWHERE status = 'paid'\nORDER BY created_at DESC\nLIMIT 20;".to_owned(),
            summary: "Returned 20 rows from Production Cluster".to_owned(),
        },
        HistoryEntry {
            title: "Session duration breakdown".to_owned(),
            sql: "SELECT device, AVG(duration_sec)\nFROM analytics.sessions\nGROUP BY device\nORDER BY AVG(duration_sec) DESC;".to_owned(),
            summary: "Grouped sessions by device".to_owned(),
        },
    ]
}

fn demo_bookmarks() -> Vec<SavedQuery> {
    vec![
        SavedQuery {
            name: "High value orders".to_owned(),
            description: "Orders above 100 dollars".to_owned(),
            sql: "SELECT id, customer_email, total_cents, created_at\nFROM public.orders\nWHERE total_cents > 10000\nORDER BY created_at DESC\nLIMIT 50;".to_owned(),
        },
        SavedQuery {
            name: "Pending invoices".to_owned(),
            description: "Billing follow-up queue".to_owned(),
            sql: "SELECT invoice_id, customer, amount_usd, state\nFROM finance.invoices\nWHERE state = 'pending'\nORDER BY issued_at DESC;".to_owned(),
        },
    ]
}

fn demo_snippets() -> Vec<QuerySnippet> {
    vec![
        QuerySnippet {
            name: "Pagination template".to_owned(),
            description: "Offset pagination starter".to_owned(),
            body: "SELECT *\nFROM public.orders\nORDER BY created_at DESC\nLIMIT 100 OFFSET 0;".to_owned(),
        },
        QuerySnippet {
            name: "Health check".to_owned(),
            description: "Quick connection validation".to_owned(),
            body: "SELECT NOW() AS server_time, COUNT(*) AS total_rows\nFROM public.orders;".to_owned(),
        },
        QuerySnippet {
            name: "Aggregate revenue".to_owned(),
            description: "Group totals by status".to_owned(),
            body: "SELECT status, SUM(total_cents) AS revenue_cents\nFROM public.orders\nGROUP BY status\nORDER BY revenue_cents DESC;".to_owned(),
        },
    ]
}

fn demo_connections() -> Vec<ConnectionProfile> {
    Vec::new()
}

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

fn with_live_endpoint<T, F>(
    profile: &ConnectionProfile,
    resources: &mut WorkerResources,
    action: F,
) -> Result<T, String>
where
    F: FnOnce(&str, u16, &mut WorkerResources) -> Result<T, String>,
{
    let (host, port) = live_endpoint(profile, resources)?;

    action(&host, port, resources)
}

fn ssh_auth(
    session: &mut Session,
    user: &str,
    password: &str,
    private_key_path: &str,
) -> Result<(), String> {
    if !password.is_empty() {
        session
            .userauth_password(user, password)
            .map_err(|e| format!("SSH password auth failed: {e}"))?;
    } else if !private_key_path.trim().is_empty() {
        let key_path = expand_tilde(private_key_path);
        session
            .userauth_pubkey_file(user, None, std::path::Path::new(&key_path), None)
            .map_err(|e| format!("SSH key auth failed: {e}"))?;
    } else {
        session
            .userauth_agent(user)
            .map_err(|e| format!("SSH agent auth failed: {e}"))?;
    }
    if !session.authenticated() {
        return Err("SSH authentication failed".to_owned());
    }
    Ok(())
}

fn new_ssh_session(
    ssh_host: &str,
    ssh_port: u16,
    ssh_user: &str,
    ssh_password: &str,
    ssh_private_key_path: &str,
) -> Result<Session, String> {
    let tcp = TcpStream::connect(format!("{ssh_host}:{ssh_port}"))
        .map_err(|e| format!("SSH TCP connect failed: {e}"))?;
    let mut session = Session::new().map_err(|e| format!("SSH session init failed: {e}"))?;
    session.set_tcp_stream(tcp);
    session
        .handshake()
        .map_err(|e| format!("SSH handshake failed: {e}"))?;
    ssh_auth(&mut session, ssh_user, ssh_password, ssh_private_key_path)?;
    Ok(session)
}

fn live_endpoint(
    profile: &ConnectionProfile,
    resources: &mut WorkerResources,
) -> Result<(String, u16), String> {
    let Some(ssh) = &profile.ssh_tunnel else {
        return Ok((profile.host.clone(), profile.port));
    };

    let cache_key = format!(
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

    // Check if we already have a tunnel cached for this key
    if let Some(tunnel) = resources.tunnels.get(&cache_key) {
        return Ok(("127.0.0.1".to_owned(), tunnel.local_port));
    }

    // Validate credentials work before binding the listener
    let ssh_host = ssh.host.clone();
    let ssh_port = ssh.port;
    let ssh_user = ssh.user.clone();
    let ssh_password = ssh.password.clone();
    let ssh_private_key_path = ssh.private_key_path.clone();
    let remote_host = profile.host.clone();
    let remote_port = profile.port;

    // Probe-connect to surface auth errors immediately
    new_ssh_session(
        &ssh_host,
        ssh_port,
        &ssh_user,
        &ssh_password,
        &ssh_private_key_path,
    )?;

    let local_port = find_free_local_port()?;
    let listener = TcpListener::bind(("127.0.0.1", local_port))
        .map_err(|e| format!("Failed to bind local port: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("set_nonblocking failed: {e}"))?;

    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    let handle = thread::spawn(move || {
        let mut connection_threads: Vec<thread::JoinHandle<()>> = Vec::new();

        loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }

            match listener.accept() {
                Ok((local_stream, _)) => {
                    // Each connection gets its own independent SSH session so there
                    // is zero shared mutable state — this is what eliminates the
                    // "Packets out of sync" codec error.
                    let ssh_host2 = ssh_host.clone();
                    let ssh_user2 = ssh_user.clone();
                    let ssh_password2 = ssh_password.clone();
                    let ssh_key2 = ssh_private_key_path.clone();
                    let remote_host2 = remote_host.clone();

                    let h = thread::spawn(move || {
                        eprintln!(
                            "[SSH] new connection → opening session to {ssh_host2}:{ssh_port}"
                        );
                        let session = match new_ssh_session(
                            &ssh_host2,
                            ssh_port,
                            &ssh_user2,
                            &ssh_password2,
                            &ssh_key2,
                        ) {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("[SSH] session failed: {e}");
                                return;
                            }
                        };
                        eprintln!(
                            "[SSH] session ok → opening channel to {remote_host2}:{remote_port}"
                        );
                        forward_connection(session, local_stream, &remote_host2, remote_port);
                        eprintln!("[SSH] channel closed for {remote_host2}:{remote_port}");
                    });
                    connection_threads.push(h);
                    connection_threads.retain(|h| !h.is_finished());
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }

        for h in connection_threads {
            let _ = h.join();
        }
    });

    let tunnel = TemporaryTunnel {
        local_port,
        stop_signal: stop_tx,
        thread_handle: Some(handle),
    };

    resources.tunnels.insert(cache_key, tunnel);
    Ok(("127.0.0.1".to_owned(), local_port))
}

/// Forward one local TCP connection through a dedicated SSH channel.
///
/// Uses libssh2 non-blocking mode with session.block_directions() to
/// correctly multiplex reads and writes on the same channel without a mutex.
/// This is the approach recommended by the libssh2 documentation and avoids
/// the deadlock where a blocking read prevents writes during TLS handshake.
fn forward_connection(session: Session, local: TcpStream, remote_host: &str, remote_port: u16) {
    // Everything in blocking mode — no spin loops, no select(), zero idle CPU.
    //
    // The deadlock problem (Thread A holds channel mutex while blocking on read,
    // preventing Thread B from writing) is solved with an intermediate pipe:
    //
    //   Thread L→C : local.read()  →  pipe_in  (blocks on local socket)
    //   Main loop  : pipe_in.read() → channel.write()   ─┐ alternating,
    //              : channel.read() → pipe_out.write()   ─┘ no mutex needed
    //   Thread C→L : pipe_out.read() → local.write()   (blocks on pipe)
    //
    // The channel is only ever touched by the main loop on a single thread,
    // so there is no shared state and no locking at all.
    session.set_blocking(true);
    local.set_nonblocking(false).ok();

    let mut channel = match session.channel_direct_tcpip(remote_host, remote_port, None) {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("[SSH] channel_direct_tcpip failed: {e}");
            return;
        }
    };

    // Create two OS pipes as intermediaries.
    let (mut pipe_in_r, mut pipe_in_w) = match os_pipe() {
        Ok(p) => p,
        Err(_) => return,
    };
    let (mut pipe_out_r, mut pipe_out_w) = match os_pipe() {
        Ok(p) => p,
        Err(_) => return,
    };

    // Clone local socket for the writer thread
    let mut local_r = local;
    let mut local_w = match local_r.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    // Thread 1: local → pipe_in  (pure blocking copy, zero CPU when idle)
    let t_local_to_pipe = thread::spawn(move || {
        let mut buf = [0u8; 32768];
        loop {
            match local_r.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if pipe_in_w.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        // Closing the write end signals EOF to the main loop's pipe_in read
    });

    // Thread 2: pipe_out → local  (pure blocking copy, zero CPU when idle)
    let t_pipe_to_local = thread::spawn(move || {
        let mut buf = [0u8; 32768];
        loop {
            match pipe_out_r.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if local_w.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = local_w.shutdown(std::net::Shutdown::Both);
    });

    // Main loop: shuttle between channel and the two pipes.
    // session.set_timeout gives the channel read a deadline so we can also
    // service the client→server direction without spinning.
    let mut lbuf = [0u8; 32768];
    let mut cbuf = [0u8; 32768];
    // 50 ms timeout on libssh2 blocking calls — low CPU, low latency
    session.set_timeout(50);

    use std::os::unix::io::AsRawFd;
    let pipe_in_fd = pipe_in_r.as_raw_fd();

    loop {
        // channel → pipe_out  (server → client)
        // session.set_timeout makes this return after 50 ms if no data
        match channel.read(&mut cbuf) {
            Ok(0) => {}
            Ok(n) => {
                if pipe_out_w.write_all(&cbuf[..n]).is_err() {
                    break;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => break,
        }

        if channel.eof() {
            break;
        }

        // pipe_in → channel  (client → server)
        // Use poll() with 0ms timeout so we don't block if no client data
        let ready = {
            let mut pfd = libc::pollfd {
                fd: pipe_in_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            unsafe { libc::poll(&mut pfd, 1, 0) > 0 && (pfd.revents & libc::POLLIN) != 0 }
        };

        if ready {
            match pipe_in_r.read(&mut lbuf) {
                Ok(0) => break, // local closed
                Ok(n) => {
                    let mut written = 0;
                    while written < n {
                        match channel.write(&lbuf[written..n]) {
                            Ok(w) => written += w,
                            Err(_) => break,
                        }
                    }
                    let _ = channel.flush();
                }
                Err(_) => break,
            }
        }
    }

    // Drop pipe ends to unblock the helper threads
    drop(pipe_in_r);
    drop(pipe_out_w);

    let _ = channel.send_eof();
    let _ = channel.wait_eof();
    let _ = channel.close();
    let _ = channel.wait_close();

    let _ = t_local_to_pipe.join();
    let _ = t_pipe_to_local.join();
}

fn os_pipe() -> Result<(std::fs::File, std::fs::File), ()> {
    use std::os::unix::io::FromRawFd;
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(());
    }
    let r = unsafe { std::fs::File::from_raw_fd(fds[0]) };
    let w = unsafe { std::fs::File::from_raw_fd(fds[1]) };
    Ok((r, w))
}

fn find_free_local_port() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("port bind failed: {}", error))?;
    listener
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|error| format!("local addr failed: {}", error))
}

fn expand_tilde(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home, stripped);
        }
    }

    path.to_owned()
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

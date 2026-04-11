impl MangabaseApp {
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
                tab.invalidate_filtered_row_cache();
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

}

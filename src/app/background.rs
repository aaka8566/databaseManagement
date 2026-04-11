impl MangabaseApp {
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
        self.jit_column_loading
            .retain(|(connection_index, _, _)| *connection_index != index);
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
                    self.jit_column_loading.remove(&(
                        connection_index,
                        schema_name.clone(),
                        table_name.clone(),
                    ));
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
                                    tab.invalidate_filtered_row_cache();
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
                                        tab.invalidate_filtered_row_cache();
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
}

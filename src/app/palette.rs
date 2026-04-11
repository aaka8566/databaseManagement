impl MangabaseApp {
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

    fn should_open_autocomplete_at(&self, cursor_char_pos: usize) -> bool {
        let sql = &self.active_tab().sql;
        let byte_pos = self.cursor_byte_index(cursor_char_pos);
        let sql_to_cursor = &sql[..byte_pos];
        let token = token_at_end(sql_to_cursor);

        if token.fragment.is_empty() {
            return false;
        }

        token
            .fragment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
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

    fn toggle_sql_line_comment(&mut self, ctx: &egui::Context) {
        let state = egui::text_edit::TextEditState::load(ctx, egui::Id::new("sql_editor"))
            .unwrap_or_default();
        let range = state.cursor.char_range();
        let sql = self.active_tab().sql.clone();
        if sql.is_empty() {
            return;
        }

        let (mut start_char, mut end_char) = range
            .map(|r| (r.primary.index, r.secondary.index))
            .unwrap_or((sql.chars().count(), sql.chars().count()));
        if start_char > end_char {
            std::mem::swap(&mut start_char, &mut end_char);
        }

        let start_byte = self.cursor_byte_index(start_char);
        let end_byte = self.cursor_byte_index(end_char);
        let line_start = sql[..start_byte].rfind('\n').map(|idx| idx + 1).unwrap_or(0);
        let line_end = sql[end_byte..]
            .find('\n')
            .map(|offset| end_byte + offset)
            .unwrap_or(sql.len());

        let selected = &sql[line_start..line_end];
        let lines: Vec<&str> = selected.split('\n').collect();
        let should_uncomment = lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .all(|line| line.trim_start().starts_with("--"));

        let transformed = lines
            .into_iter()
            .map(|line| {
                if line.trim().is_empty() {
                    return line.to_owned();
                }
                let indent_len = line.len() - line.trim_start_matches([' ', '\t']).len();
                let (indent, body) = line.split_at(indent_len);
                if should_uncomment {
                    if let Some(stripped) = body.strip_prefix("-- ") {
                        format!("{}{}", indent, stripped)
                    } else if let Some(stripped) = body.strip_prefix("--") {
                        format!("{}{}", indent, stripped)
                    } else {
                        line.to_owned()
                    }
                } else {
                    format!("{}-- {}", indent, body)
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        let tab = self.active_tab_mut();
        tab.sql.replace_range(line_start..line_end, &transformed);
        tab.autocomplete_index = 0;
        self.autocomplete_open = false;

        let mut next_state = egui::text_edit::TextEditState::load(ctx, egui::Id::new("sql_editor"))
            .unwrap_or_default();
        let cursor_byte = line_start + transformed.len();
        let cursor_char = self.byte_to_char_index(cursor_byte);
        next_state
            .cursor
            .set_char_range(Some(egui::text::CCursorRange::one(
                egui::text::CCursor::new(cursor_char),
            )));
        next_state.store(ctx, egui::Id::new("sql_editor"));
        ctx.memory_mut(|m| m.request_focus(egui::Id::new("sql_editor")));
    }

    fn trigger_jit_column_loading(&mut self) {
        let connection_index = self.selected_connection;
        let Some(conn) = self.connections.get(connection_index) else {
            return;
        };
        let profile = conn.clone();
        let schemas = conn.schemas.clone();
        let tab = self.active_tab();
        let sql = &tab.sql;

        // Simple regex to find potential table names after FROM, JOIN, UPDATE, INTO, TABLE
        let re = regex::Regex::new(r"(?i)\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+([a-zA-Z_0-9\.]+)")
            .unwrap();

        let mut tables_to_load = Vec::new();
        let mut seen_tables: HashSet<(String, String)> = HashSet::new();
        for cap in re.captures_iter(sql) {
            let full_name = &cap[1];
            let parts: Vec<&str> = full_name.split('.').collect();
            let (schema_name, table_name) = if parts.len() > 1 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                // Try to find the table in any schema if no schema prefix
                let mut found = None;
                for s in &schemas {
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
            let already_has_columns = schemas.iter().any(|s| {
                s.name == schema_name
                    && s.tables
                        .iter()
                        .any(|t| t.name == table_name && !t.columns.is_empty())
            });

            if !already_has_columns && seen_tables.insert((schema_name.clone(), table_name.clone())) {
                tables_to_load.push((schema_name, table_name));
            }
        }

        for (schema_name, table_name) in tables_to_load {
            let loading_key = (connection_index, schema_name.clone(), table_name.clone());
            if self.jit_column_loading.contains(&loading_key) {
                continue;
            }
            let job_id = self.next_job_id;
            self.next_job_id += 1;
            self.active_jobs.insert(job_id);
            self.jit_column_loading.insert(loading_key.clone());
            if self
                .worker_tx
                .send(BackgroundCommand::LoadTableColumns {
                    job_id,
                    connection_index,
                    schema_name,
                    table_name,
                    profile: profile.clone(),
                })
                .is_err()
            {
                self.active_jobs.remove(&job_id);
                self.jit_column_loading.remove(&loading_key);
            }
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

}

impl MangabaseApp {
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
                                egui::ComboBox::from_id_salt(("inspector_enum", state.row, index))
                                    .selected_text(value.clone())
                                    .width(ui.available_width().max(120.0))
                                    .show_ui(ui, |ui| {
                                        for option in &enum_options {
                                            ui.selectable_value(value, option.clone(), option);
                                        }
                                    });
                            }
                            if let Some(foreign_key) = foreign_key {
                                if !value.trim().is_empty() && !value.eq_ignore_ascii_case("NULL") {
                                    ui.add_space(4.0);
                                    if soft_button(
                                        ui,
                                        &format!(
                                            "Open {}.{}",
                                            foreign_key.referenced_schema,
                                            foreign_key.referenced_table,
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
        let autocomplete_allowed = self.active_tab().sql.len() <= MAX_SQL_AUTOCOMPLETE_BYTES;
        if let Some(state) = egui::text_edit::TextEditState::load(ctx, egui::Id::new("sql_editor"))
        {
            current_cursor_pos = state
                .cursor
                .char_range()
                .map(|r| r.primary.index)
                .unwrap_or(self.active_tab().sql.len());
        }
        let mut run_query = false;
        let mut toggle_comment = false;

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
                        if with_shortcut(soft_button(ui, "Comment"), "Cmd + /").clicked() {
                            toggle_comment = true;
                        }
                    });
                });

                ui.add_space(6.0);

                if autocomplete_allowed && self.autocomplete_open {
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
                let editor_height = ui.available_height().max(220.0);
                let mut output = None;
                egui::ScrollArea::both()
                    .id_salt("sql_editor_scroll")
                    .auto_shrink([false, false])
                    .max_height(editor_height)
                    .show(ui, |ui| {
                        output = Some(
                            TextEdit::multiline(&mut self.active_tab_mut().sql)
                                .id(egui::Id::new("sql_editor"))
                                .font(egui::TextStyle::Monospace)
                                .code_editor()
                                .desired_width(ui.available_width())
                                .desired_rows(12)
                                .show(ui),
                        );
                    });
                let output = output.expect("sql editor output");

                editor_rect = Some(output.response.rect);
                if let Some(range) = output.cursor_range {
                    // Use the galley and galley_pos for absolutely accurate cursor position
                    let cursor_rect = output.galley.pos_from_cursor(&range.primary);
                    cursor_pos = output.galley_pos + cursor_rect.left_top().to_vec2();
                }

                if output.response.has_focus() {
                    self.result_grid_has_focus = false;
                    if ctx.input_mut(|i| i.consume_key(egui::Modifiers::COMMAND, Key::Slash)) {
                        toggle_comment = true;
                        self.autocomplete_open = false;
                    }

                    if autocomplete_allowed && self.should_open_autocomplete_at(current_cursor_pos)
                    {
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
                        self.active_tab_mut().autocomplete_index = 0;
                    }
                } else {
                    self.autocomplete_open = false;
                }

                if !autocomplete_allowed {
                    ui.add_space(6.0);
                    ui.label(
                        RichText::new(
                            "Autocomplete pauses for very large queries so scrolling stays responsive.",
                        )
                        .size(11.0)
                        .color(Color32::from_rgb(117, 126, 137)),
                    );
                }

                if show_autocomplete {
                    if let Some(rect) = editor_rect {
                        let popup_width = rect.width().clamp(420.0, 640.0);
                        let visible_rows = suggestions.len().clamp(1, 12) as f32;
                        let max_popup_height =
                            (ctx.screen_rect().height() * 0.45).clamp(220.0, 520.0);
                        let popup_height =
                            (visible_rows * 30.0 + 12.0).clamp(180.0, max_popup_height);
                        let popup_pos = autocomplete_popup_position(
                            cursor_pos,
                            rect,
                            ctx.screen_rect(),
                            popup_width,
                            popup_height,
                        );

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
                                        ui.set_min_height(popup_height);
                                        egui::ScrollArea::vertical()
                                            .max_height(popup_height)
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
        if toggle_comment {
            self.toggle_sql_line_comment(ctx);
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
        let mut direct_enum_edit: Option<CellEditState> = None;
        let is_table_tab = matches!(self.active_tab().kind, TabKind::Table { .. });
        let active_table_metadata = self
            .active_table_info()
            .map(|table| (table.columns.clone(), table.foreign_keys.clone()));

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
                            let picker_label = format!("{selected_text}   ");
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
                            paint_dropdown_indicator(
                                ui,
                                picker_response.rect,
                                Color32::from_rgb(70, 74, 84),
                                picker_open,
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
                            let operator_button_label = format!("{operator_label}   ");
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
                            paint_dropdown_indicator(
                                ui,
                                operator_response.rect,
                                Color32::from_rgb(70, 74, 84),
                                operator_popup_open,
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

                self.active_tab_mut().ensure_filtered_row_cache();
                let tab = self.active_tab();
                let result = &tab.result;
                let total_columns = result.columns.len();
                let max_column_offset = total_columns.saturating_sub(RESULT_COLUMNS_PER_PAGE);
                let column_start = tab.column_page.min(max_column_offset);
                let column_end = (column_start + RESULT_COLUMNS_PER_PAGE).min(total_columns);
                let visible_columns = &result.columns[column_start..column_end];
                let filtered_rows = tab.filtered_rows();
                let visible_column_details = visible_columns
                    .iter()
                    .map(|column| {
                        let foreign_key = active_table_metadata
                            .as_ref()
                            .and_then(|(_, foreign_keys)| {
                                foreign_keys.iter().find(|fk| {
                                    fk.column_name.eq_ignore_ascii_case(&column.name)
                                })
                            })
                            .cloned();
                        let enum_options = active_table_metadata
                            .as_ref()
                            .and_then(|(columns, _)| {
                                columns
                                    .iter()
                                    .find(|table_column| {
                                        table_column.name.eq_ignore_ascii_case(&column.name)
                                    })
                            })
                            .map(enum_options_for_column)
                            .unwrap_or_default();
                        (foreign_key, enum_options)
                    })
                    .collect::<Vec<_>>();

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

                                            let (foreign_key, enum_options) =
                                                &visible_column_details[visible_index];
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
                                                        for option in enum_options {
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
                                                    result.rows[row_index][col_index].as_str();
                                                let has_foreign_key_jump = foreign_key
                                                    .as_ref()
                                                    .map(|_| {
                                                        !cell_value.trim().is_empty()
                                                            && !cell_value
                                                                .eq_ignore_ascii_case("NULL")
                                                    })
                                                    .unwrap_or(false);
                                                ui.horizontal(|ui| {
                                                    ui.spacing_mut().item_spacing.x = 2.0;
                                                    let accessory_width =
                                                        if has_foreign_key_jump { 18.0 + 2.0 } else { 0.0 };
                                                    let main_width =
                                                        (ui.available_width() - accessory_width)
                                                            .max(40.0);

                                                    if has_enum_picker {
                                                        let mut enum_value = cell_value.to_owned();
                                                        let combo_response = egui::ComboBox::from_id_salt((
                                                            "result_enum_cell",
                                                            row_index,
                                                            col_index,
                                                        ))
                                                        .selected_text(enum_value.clone())
                                                        .width(main_width)
                                                        .show_ui(ui, |ui| {
                                                            for option in enum_options {
                                                                ui.selectable_value(
                                                                    &mut enum_value,
                                                                    option.to_owned(),
                                                                    option.as_str(),
                                                                );
                                                            }
                                                        });
                                                        if combo_response.response.clicked() {
                                                            clicked_cell = Some(cell);
                                                        }
                                                        if enum_value != cell_value {
                                                            clicked_cell = Some(cell);
                                                            direct_enum_edit = Some(CellEditState {
                                                                row: row_index,
                                                                col: col_index,
                                                                value: enum_value,
                                                            });
                                                        }
                                                    } else {
                                                        let response = ui.add_sized(
                                                            [main_width, 22.0],
                                                            egui::Button::new(
                                                                RichText::new(cell_value)
                                                                    .size(12.0)
                                                                    .color(if is_row_selected {
                                                                        Color32::WHITE
                                                                    } else {
                                                                        Color32::from_rgb(
                                                                            72, 72, 72,
                                                                        )
                                                                    }),
                                                            )
                                                            .fill(fill)
                                                            .stroke(egui::Stroke::new(
                                                                1.0,
                                                                if is_row_selected {
                                                                    Color32::from_rgb(174, 50, 43)
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
                                                        );

                                                        if response.clicked() {
                                                            clicked_cell = Some(cell);
                                                        }
                                                        if response.double_clicked() {
                                                            clicked_cell = Some(cell);
                                                            start_edit = Some(cell);
                                                        }
                                                    }

                                                    if let Some(foreign_key) = foreign_key.as_ref() {
                                                        if has_foreign_key_jump {
                                                            let jump_response = ui
                                                                .add_sized(
                                                                    [18.0, 22.0],
                                                                    egui::Button::new("")
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
                                                                ));
                                                            paint_external_link_indicator(
                                                                ui,
                                                                jump_response.rect,
                                                                if is_row_selected {
                                                                    Color32::WHITE
                                                                } else {
                                                                    Color32::from_rgb(72, 72, 72)
                                                                },
                                                            );
                                                                if jump_response.clicked() {
                                                                    clicked_cell = Some(cell);
                                                                    foreign_key_jump = Some((
                                                                        foreign_key.clone(),
                                                                        cell_value.to_owned(),
                                                                    ));
                                                                }
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
        let max_column_offset = total_columns.saturating_sub(RESULT_COLUMNS_PER_PAGE);
        let current_offset = self.active_tab().column_page.min(max_column_offset);
        let horizontal_scroll = ctx.input(|i| i.smooth_scroll_delta.x);
        if results_hovered
            && total_columns > RESULT_COLUMNS_PER_PAGE
            && horizontal_scroll.abs() > 6.0
            && self.last_results_page_change.elapsed() > Duration::from_millis(80)
        {
            if horizontal_scroll < 0.0 && current_offset < max_column_offset {
                self.active_tab_mut().column_page =
                    (current_offset + RESULT_COLUMN_SCROLL_STEP).min(max_column_offset);
                self.last_results_page_change = Instant::now();
            } else if horizontal_scroll > 0.0 && current_offset > 0 {
                self.active_tab_mut().column_page =
                    current_offset.saturating_sub(RESULT_COLUMN_SCROLL_STEP);
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
        if let Some(editing) = direct_enum_edit {
            self.set_result_selection(editing.row, editing.col);
            self.editing_cell = Some(editing);
            self.commit_cell_edit();
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

}

impl MangabaseApp {
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

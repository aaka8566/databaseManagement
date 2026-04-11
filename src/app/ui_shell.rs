impl MangabaseApp {
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

}

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

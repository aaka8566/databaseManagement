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

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

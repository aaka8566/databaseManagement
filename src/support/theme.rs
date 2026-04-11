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

fn paint_dropdown_indicator(ui: &egui::Ui, rect: egui::Rect, color: Color32, open: bool) {
    let center = egui::pos2(rect.right() - 12.0, rect.center().y);
    let points = if open {
        vec![
            egui::pos2(center.x - 4.0, center.y + 2.0),
            egui::pos2(center.x + 4.0, center.y + 2.0),
            egui::pos2(center.x, center.y - 2.5),
        ]
    } else {
        vec![
            egui::pos2(center.x - 4.0, center.y - 2.0),
            egui::pos2(center.x + 4.0, center.y - 2.0),
            egui::pos2(center.x, center.y + 2.5),
        ]
    };
    ui.painter().add(egui::Shape::convex_polygon(
        points,
        color,
        egui::Stroke::NONE,
    ));
}

fn paint_external_link_indicator(ui: &egui::Ui, rect: egui::Rect, color: Color32) {
    let stroke = egui::Stroke::new(1.3, color);
    let origin = egui::pos2(rect.left() + 5.0, rect.bottom() - 5.0);
    let target = egui::pos2(rect.right() - 5.0, rect.top() + 5.0);
    ui.painter().line_segment([origin, target], stroke);
    ui.painter().line_segment(
        [egui::pos2(target.x - 4.0, target.y), target],
        stroke,
    );
    ui.painter().line_segment(
        [egui::pos2(target.x, target.y + 4.0), target],
        stroke,
    );
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

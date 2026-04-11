use clickhouse::Client as ClickHouseClient;
use duckdb::{Connection as DuckDbConnection, params};
use eframe::{
    App, CreationContext, NativeOptions,
    egui::{
        self, Align, Align2, Color32, FontId, Key, Layout, Margin, RichText, Sense, TextEdit, Vec2,
    },
};
use egui_extras::{Column, TableBuilder};
use mysql::{OptsBuilder, Pool, Row as MySqlRow, Value as MySqlValue, prelude::Queryable};
use postgres::{Client as PostgresClient, Config as PostgresConfig, NoTls, SimpleQueryMessage};
use rusqlite::Connection as SqliteConnection;
use serde::{Deserialize, Serialize};
use ssh2::Session;
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{Read as IoRead, Write as IoWrite},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

include!("models/query_state.rs");
include!("models/connection.rs");
include!("models/demo.rs");
include!("support/theme.rs");
include!("support/forms.rs");
include!("support/schema_diagram.rs");
include!("support/parsing.rs");
include!("support/query_tools.rs");
include!("data/storage.rs");
include!("data/worker.rs");
include!("data/live.rs");
include!("data/sql.rs");
include!("data/transport.rs");
include!("data/engines.rs");
include!("app/state.rs");
include!("app/background.rs");
include!("app/actions.rs");
include!("app/palette.rs");
include!("app/ui_shell.rs");
include!("app/ui_workspace.rs");
include!("app/ui_overlays.rs");

fn main() -> eframe::Result<()> {
    let options = NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Sharingan")
            .with_inner_size([1560.0, 980.0])
            .with_min_inner_size([1280.0, 780.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Sharingan",
        options,
        Box::new(|cc| Ok(Box::new(MangabaseApp::new(cc)))),
    )
}

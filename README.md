# Sharingan

Sharingan is a native Rust desktop database client for macOS and Windows. It supports multiple database connections, SSH tunneling, tabs, filters, autocomplete, and editing rows directly from a query result.

## Features

- Multiple connections (stored locally)
- SSH tunneling (`+ssh` URL scheme and `ssh_*` query parameters)
- Query tabs (history, bookmarks, snippets)
- Table browsing (schemas, table previews)
- Result grid with row/cell inspection
- Autocomplete for SQL
- Schema diagrams (optional)

## Supported Database Engines

Sharingan recognizes these engines in connection profiles:

- MySQL
- PostgreSQL
- ClickHouse
- DuckDB
- SQLite
- MariaDB
- SQL Server
- Redshift
- BigQuery
- Cassandra
- DynamoDB
- LibSQL
- Cloudflare D1
- MongoDB
- Snowflake
- Redis
- Oracle
- CockroachDB
- Greenplum
- Vertica

## Connection URL Format

Sharingan can import connections using URL-like strings, for example:

- `mysql://user:pass@host:3306/dbname`
- `postgres://user:pass@host:5432/dbname`
- `sqlite://user@/path/to/db.sqlite` (SQLite uses the parsed URL path/db field)

### SSH Tunneling

You can enable SSH in two ways:

1. Add `+ssh` to the scheme:
   - `postgres+ssh://user:pass@host:5432/dbname?...`

2. Or pass SSH details via query parameters:
   - Use `ssh=1` (or `true/yes/on`) plus:
     - `ssh_host`
     - `ssh_port`
     - `ssh_user`
     - `ssh_password`
     - `ssh_private_key_path` (or `ssh_key_path` / `ssh_private_key`)

Example:

```text
postgres://user:pass@db.example.com:5432/mydb?ssh=1&ssh_host=bastion.example.com&ssh_port=22&ssh_user=ec2-user&ssh_private_key_path=~/.ssh/id_ed25519
```

## Stored Connections

Sharingan saves your custom connections to:

- macOS: `~/Library/Application Support/Sharingan/sharingan_connections.json`
- Windows: `%APPDATA%/Sharingan/sharingan_connections.json`

It also auto-loads the legacy file `mangabase_connections.json` so existing saved connections can be migrated.

## Development

### Run (debug)

```bash
cargo run
```

### Build (release)

```bash
cargo build --release
```

## Build for Multiple Platforms

### macOS (DMG)

From the project root:

```bash
chmod +x scripts/build-macos-dmg.sh
./scripts/build-macos-dmg.sh
```

Optional: build an app bundle directly (recommended when `cargo-bundle` is installed):

```bash
cargo install cargo-bundle
cargo bundle --release --format osx
```

### Windows (MSI or ZIP)

Build Windows packages on Windows (or in Windows CI). From PowerShell:

```powershell
.\scripts\build-windows.ps1
```

If you want full installer generation (MSI), ensure `cargo-bundle` is installed:

```powershell
cargo install cargo-bundle
```

## Packaging

See `PACKAGING.md` for DMG/MSI/ZIP build instructions.

- macOS DMG / app bundle
- Windows packaging scripts

## Notes

- The UI includes a starter workspace and example query on first launch (until you add connections).
- No connection is required at compile time; connections are configured at runtime.
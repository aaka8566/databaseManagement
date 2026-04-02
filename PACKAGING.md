# Packaging Sharingan

## macOS DMG

From the project root:

```bash
chmod +x scripts/build-macos-dmg.sh
./scripts/build-macos-dmg.sh
```

What it does:

- If `cargo-bundle` is installed, it builds a proper macOS app bundle using the bundle metadata in `Cargo.toml` and then wraps it into a DMG
- If `cargo-bundle` is not installed, it falls back to a manual `.app` bundle and creates `dist/Sharingan.dmg`

Recommended tool for the best macOS icon/bundle handling:

```bash
cargo install cargo-bundle
```

Then build an app bundle directly:

```bash
cargo bundle --release --format osx
```

## Windows package

Build Windows packages on Windows, or in Windows CI.

From PowerShell on Windows:

```powershell
.\scripts\build-windows.ps1
```

What it does:

- If `cargo-bundle` is installed, it creates an MSI
- Otherwise it creates a distributable ZIP with `Sharingan.exe`

For a full Windows installer, install one of these on Windows:

```powershell
cargo install cargo-bundle
```

or

```powershell
cargo install cargo-wix
```

## App identity

- App name: `Sharingan`
- Bundle identifier: `com.sharingan.desktop`
- Packaging icon source: `assets/sharingan-square.png`

## Notes

- `Cargo.toml` already contains bundle metadata for `cargo-bundle`
- The app now saves connections to `sharingan_connections.json`
- It still reads the old `mangabase_connections.json` automatically so existing saved connections continue to load

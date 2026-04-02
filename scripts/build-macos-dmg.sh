#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="Sharingan"
DIST_DIR="$ROOT_DIR/dist"
APP_DIR="$DIST_DIR/$APP_NAME.app"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"
DMG_STAGING_DIR="$DIST_DIR/dmg"
DMG_PATH="$DIST_DIR/$APP_NAME.dmg"

mkdir -p "$DIST_DIR"

if cargo bundle --help >/dev/null 2>&1; then
  if cargo bundle --help 2>&1 | grep -Eq '(^|[[:space:]])osx([[:space:]]|$)'; then
    echo "Using cargo-bundle to create a macOS app bundle with app metadata."
    cargo bundle --release --format osx

    BUNDLED_APP="$ROOT_DIR/target/release/bundle/osx/$APP_NAME.app"
    if [[ -d "$BUNDLED_APP" ]]; then
      rm -rf "$DMG_STAGING_DIR" "$DMG_PATH"
      mkdir -p "$DMG_STAGING_DIR"
      cp -R "$BUNDLED_APP" "$DMG_STAGING_DIR/"
      ln -s /Applications "$DMG_STAGING_DIR/Applications"
      hdiutil create -volname "$APP_NAME" -srcfolder "$DMG_STAGING_DIR" -ov -format UDZO "$DMG_PATH"
      echo "DMG created at: $DMG_PATH"
      exit 0
    fi

    echo "cargo-bundle completed, but $BUNDLED_APP was not found. Falling back to manual packaging."
  elif cargo bundle --help 2>&1 | grep -Eq '(^|[[:space:]])dmg([[:space:]]|$)'; then
    echo "Using cargo-bundle to create a macOS DMG."
    cargo bundle --release --format dmg
    echo "DMG created under target/release/bundle/dmg/"
    exit 0
  fi
fi

echo "Falling back to manual .app/.dmg packaging."
echo "Install cargo-bundle later for better icon handling:"
echo "  cargo install cargo-bundle"

cargo build --release

rm -rf "$APP_DIR" "$DMG_STAGING_DIR" "$DMG_PATH"
mkdir -p "$MACOS_DIR" "$RESOURCES_DIR" "$DMG_STAGING_DIR"

cp "$ROOT_DIR/target/release/sharingan" "$MACOS_DIR/$APP_NAME"
chmod +x "$MACOS_DIR/$APP_NAME"

if [[ -f "$ROOT_DIR/assets/sharingan.icns" ]]; then
  cp "$ROOT_DIR/assets/sharingan.icns" "$RESOURCES_DIR/sharingan.icns"
  ICON_PLIST=$'    <key>CFBundleIconFile</key>\n    <string>sharingan.icns</string>'
else
  cp "$ROOT_DIR/assets/sharingan-square.png" "$RESOURCES_DIR/sharingan-square.png"
  ICON_PLIST=$'    <key>CFBundleIconFile</key>\n    <string>sharingan-square.png</string>'
fi

cat > "$CONTENTS_DIR/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleDisplayName</key>
    <string>$APP_NAME</string>
    <key>CFBundleExecutable</key>
    <string>$APP_NAME</string>
$ICON_PLIST
    <key>CFBundleIdentifier</key>
    <string>com.sharingan.desktop</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>$APP_NAME</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.0</string>
    <key>CFBundleVersion</key>
    <string>0.1.0</string>
    <key>LSMinimumSystemVersion</key>
    <string>13.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
EOF

cp -R "$APP_DIR" "$DMG_STAGING_DIR/"
ln -s /Applications "$DMG_STAGING_DIR/Applications"

hdiutil create -volname "$APP_NAME" -srcfolder "$DMG_STAGING_DIR" -ov -format UDZO "$DMG_PATH"

echo "DMG created at: $DMG_PATH"

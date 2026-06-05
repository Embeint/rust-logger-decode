#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <rust-target-triple>" >&2
  exit 64
fi

TARGET="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAME="${MACOS_APP_NAME:-Infuse Decoder}"
BUNDLE_ID="${MACOS_BUNDLE_ID:-iot.infuse.decoder}"
BIN_NAME="${MACOS_BIN_NAME:-infuse_decoder}"
EXECUTABLE_NAME="${MACOS_EXECUTABLE_NAME:-infuse_decoder}"
SIGNING_IDENTITY="${MACOS_SIGNING_IDENTITY:-${CODESIGN_IDENTITY:-}}"
REQUIRE_SIGNING="${MACOS_REQUIRE_SIGNING:-false}"
ENTITLEMENTS="${MACOS_ENTITLEMENTS:-$ROOT_DIR/packaging/macos/entitlements.plist}"
ICON_PNG="${MACOS_ICON_PNG:-$ROOT_DIR/assets/icon.png}"

cd "$ROOT_DIR"

VERSION="$(sed -nE 's/^version[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' Cargo.toml | head -n 1)"
if [[ -z "$VERSION" ]]; then
  echo "Could not determine package version from Cargo.toml" >&2
  exit 1
fi

if [[ "$REQUIRE_SIGNING" == "true" || "$REQUIRE_SIGNING" == "1" ]]; then
  if [[ -z "$SIGNING_IDENTITY" ]]; then
    echo "MACOS_SIGNING_IDENTITY is required when MACOS_REQUIRE_SIGNING is true" >&2
    exit 1
  fi
fi

if [[ -n "$SIGNING_IDENTITY" && ! -f "$ENTITLEMENTS" ]]; then
  echo "Entitlements file not found: $ENTITLEMENTS" >&2
  exit 1
fi

for required_tool in cargo codesign hdiutil iconutil sips; do
  if ! command -v "$required_tool" >/dev/null 2>&1; then
    echo "Required macOS packaging tool not found: $required_tool" >&2
    exit 1
  fi
done

APP_BASENAME="${APP_NAME// /-}"
BUILD_ROOT="$ROOT_DIR/target/macos-dmg/$TARGET"
APP_PATH="$BUILD_ROOT/$APP_NAME.app"
DMG_ROOT="$BUILD_ROOT/dmg-root"
DMG_PATH="$BUILD_ROOT/$APP_BASENAME-$VERSION-$TARGET.dmg"
ICONSET="$BUILD_ROOT/AppIcon.iconset"

cargo build --release --bin "$BIN_NAME" --target "$TARGET"

rm -rf "$APP_PATH" "$DMG_ROOT" "$DMG_PATH" "$ICONSET"
mkdir -p "$APP_PATH/Contents/MacOS" "$APP_PATH/Contents/Resources" "$DMG_ROOT" "$ICONSET"

cp "$ROOT_DIR/target/$TARGET/release/$BIN_NAME" "$APP_PATH/Contents/MacOS/$EXECUTABLE_NAME"
chmod 755 "$APP_PATH/Contents/MacOS/$EXECUTABLE_NAME"

make_icon() {
  local size="$1"
  local output="$2"

  sips -z "$size" "$size" "$ICON_PNG" --out "$ICONSET/$output" >/dev/null
}

make_icon 16 "icon_16x16.png"
make_icon 32 "icon_16x16@2x.png"
make_icon 32 "icon_32x32.png"
make_icon 64 "icon_32x32@2x.png"
make_icon 128 "icon_128x128.png"
make_icon 256 "icon_128x128@2x.png"
make_icon 256 "icon_256x256.png"
make_icon 512 "icon_256x256@2x.png"
make_icon 512 "icon_512x512.png"
make_icon 1024 "icon_512x512@2x.png"
iconutil -c icns "$ICONSET" -o "$APP_PATH/Contents/Resources/AppIcon.icns"

cat > "$APP_PATH/Contents/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleDisplayName</key>
  <string>$APP_NAME</string>
  <key>CFBundleExecutable</key>
  <string>$EXECUTABLE_NAME</string>
  <key>CFBundleIconFile</key>
  <string>AppIcon</string>
  <key>CFBundleIdentifier</key>
  <string>$BUNDLE_ID</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>$APP_NAME</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>$VERSION</string>
  <key>CFBundleVersion</key>
  <string>$VERSION</string>
  <key>LSApplicationCategoryType</key>
  <string>public.app-category.utilities</string>
  <key>NSHighResolutionCapable</key>
  <true/>
  <key>NSPrincipalClass</key>
  <string>NSApplication</string>
  <key>NSSupportsAutomaticGraphicsSwitching</key>
  <true/>
</dict>
</plist>
EOF

printf 'APPL????' > "$APP_PATH/Contents/PkgInfo"

if [[ -n "$SIGNING_IDENTITY" ]]; then
  codesign --force \
    --timestamp \
    --options runtime \
    --entitlements "$ENTITLEMENTS" \
    --sign "$SIGNING_IDENTITY" \
    "$APP_PATH/Contents/MacOS/$EXECUTABLE_NAME"

  codesign --force \
    --timestamp \
    --options runtime \
    --entitlements "$ENTITLEMENTS" \
    --sign "$SIGNING_IDENTITY" \
    "$APP_PATH"
else
  echo "MACOS_SIGNING_IDENTITY is not set; creating an ad-hoc signed DMG for local validation only." >&2
  codesign --force --sign - "$APP_PATH/Contents/MacOS/$EXECUTABLE_NAME"
  codesign --force --sign - "$APP_PATH"
fi

codesign --verify --strict --deep --verbose=2 "$APP_PATH"

ditto "$APP_PATH" "$DMG_ROOT/$APP_NAME.app"
ln -s /Applications "$DMG_ROOT/Applications"

hdiutil create \
  -volname "$APP_NAME" \
  -srcfolder "$DMG_ROOT" \
  -ov \
  -format UDZO \
  "$DMG_PATH"
hdiutil verify "$DMG_PATH"

if [[ -n "$SIGNING_IDENTITY" ]]; then
  codesign --force --timestamp --sign "$SIGNING_IDENTITY" "$DMG_PATH"
  codesign --verify --verbose=2 "$DMG_PATH"
fi

echo "$DMG_PATH"

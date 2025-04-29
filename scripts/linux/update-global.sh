#!/usr/bin/env bash
set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────────
# URL for .NET 8 release metadata
METADATA_URL="https://dotnetcli.blob.core.windows.net/dotnet/release-metadata/8.0/releases.json"
GLOBAL_JSON="global.json"   # path to your global.json
# ──────────────────────────────────────────────────────────────────────────────

# Check dependencies
command -v curl >/dev/null 2>&1 || { echo >&2 "Error: curl is required."; exit 1; }
command -v jq >/dev/null 2>&1  || { echo >&2 "Error: jq is required.";  exit 1; }

echo "🔍 Fetching .NET 8 release metadata..."
json=$(curl -sSL "$METADATA_URL")

# Extract the very latest SDK version (first release in array)
latest_sdk=$(jq -r '.releases[0].sdk.version' <<<"$json")
if [[ -z "$latest_sdk" ]]; then
  echo "Error: could not parse latest SDK version." >&2
  exit 1
fi
echo "Latest SDK version → $latest_sdk"

# Backup existing global.json
cp "$GLOBAL_JSON" "${GLOBAL_JSON}.bak-$(date +%Y%m%d%H%M%S)"

# Update the version field in-place
jq --arg v "$latest_sdk" '.sdk.version = $v' "$GLOBAL_JSON" > "${GLOBAL_JSON}.tmp" \
  && mv "${GLOBAL_JSON}.tmp" "$GLOBAL_JSON"

echo "✅ Updated $GLOBAL_JSON to SDK $latest_sdk"

#!/usr/bin/env bash

set -euo pipefail

show_help() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --tag <name>          Image tag. Repeatable. (default: ravendb/quill:dev)
  --push                Push to registry instead of loading locally
  --platform <plat>     Single platform: linux/amd64 (default) or linux/arm64
  --payload <path>      Quill payload (.tar.bz2). Auto-detected from artifacts/ if omitted.
  --no-cache            Force a full rebuild, ignoring layer cache
  --dry-run             Build locally without pushing (overrides --push)
  -h, --help            Show this help

Examples:
  $(basename "$0") --tag ravendb/quill:dev
  $(basename "$0") --tag ravendb/quill:0.1.0 --push
  $(basename "$0") --platform linux/arm64 --payload artifacts/Quill-7.2.6-quill-nightly-...-linux-arm64.tar.bz2 --push
EOF
}

TAGS=()
PLATFORM="linux/amd64"
PAYLOAD=""
PUSH="false"
NO_CACHE="false"
DRY_RUN="false"

DOCKERFILE="docker/quill/Dockerfile"

require_value() {
  if [ "$2" -lt 2 ]; then
    echo "error: $1 requires a value" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)       require_value --tag "$#";      TAGS+=("$2"); shift 2 ;;
    --push)      PUSH="true"; shift ;;
    --platform)  require_value --platform "$#"; PLATFORM="$2"; shift 2 ;;
    --payload)   require_value --payload "$#";  PAYLOAD="$2"; shift 2 ;;
    --no-cache)  NO_CACHE="true"; shift ;;
    --dry-run)   DRY_RUN="true"; shift ;;
    -h|--help)   show_help; exit 0 ;;
    *) echo "Unknown flag: $1" >&2; show_help >&2; exit 1 ;;
  esac
done

case "$PLATFORM" in
  linux/amd64) RID_ARCH="linux-x64" ;;
  linux/arm64) RID_ARCH="linux-arm64" ;;
  *)
    echo "error: --platform must be linux/amd64 or linux/arm64 (got: $PLATFORM)." >&2
    echo "       The payload is arch-specific; build one platform per call." >&2
    exit 1
    ;;
esac

if [ ${#TAGS[@]} -eq 0 ]; then
  TAGS=("ravendb/quill:dev")
fi

if [ "$DRY_RUN" = "true" ]; then
  PUSH="false"
fi

cd "$(git rev-parse --show-toplevel)"

if [ -z "$PAYLOAD" ]; then
  PAYLOAD=$(ls -1 artifacts/Quill-*-"$RID_ARCH".tar.bz2 2>/dev/null | head -n1 || true)
fi

if [ -z "$PAYLOAD" ] || [ ! -e "$PAYLOAD" ]; then
  echo "error: no Quill payload for $RID_ARCH (looked for artifacts/Quill-*-$RID_ARCH.tar.bz2)." >&2
  echo "       Run 'pwsh ./build.ps1 -Quill' first, or pass --payload <path>." >&2
  exit 1
fi

echo "Building Quill image for ${PLATFORM} from ${PAYLOAD}"

cmd=(docker buildx build --pull --platform "$PLATFORM" -f "$DOCKERFILE" --build-arg "QUILL_PAYLOAD=$PAYLOAD")

for tag in "${TAGS[@]}"; do
  cmd+=(-t "$tag")
done

if [ "$PUSH" = "true" ]; then
  cmd+=(--push)
else
  cmd+=(--load)
fi

if [ "$NO_CACHE" = "true" ]; then
  cmd+=(--no-cache)
fi

cmd+=(.)

exec "${cmd[@]}"

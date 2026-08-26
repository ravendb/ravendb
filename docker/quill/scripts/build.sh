#!/usr/bin/env bash
# Build the Quill docker image.
# Used both locally and by CI.

set -euo pipefail

show_help() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --tag <name>          Image tag. Repeatable. (default: ravendb/quill:dev)
  --push                Push to registry instead of loading locally
  --platforms <list>    Comma-separated platforms (default: linux/amd64)
  --mode <mode>         Build mode: source|nightly (default: source)
                        source  -> docker/quill/Dockerfile.source (compile
                                   Raven.Server + Raven.Studio from source)
                        nightly -> docker/quill/Dockerfile (pull RavenDB
                                   binaries from ravendb/ravendb-nightly)
  --no-cache            Force a full rebuild, ignoring layer cache
  --dry-run             Build locally without pushing (overrides --push)
  -h, --help            Show this help

Examples:
  $(basename "$0")
  $(basename "$0") --tag ravendb/quill:smoke
  $(basename "$0") --tag ravendb/quill:0.1.0 --tag ravendb/quill:latest --push
  $(basename "$0") --tag ravendb/quill:0.1.0 --platforms linux/amd64,linux/arm64 --push
  $(basename "$0") --tag ravendb/quill:nightly --mode nightly --push
EOF
}

TAGS=()
PLATFORMS="linux/amd64"
MODE="source"
PUSH="false"
NO_CACHE="false"
DRY_RUN="false"

require_value() {
  if [ "$2" -lt 2 ]; then
    echo "error: $1 requires a value" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)       require_value --tag "$#";       TAGS+=("$2"); shift 2 ;;
    --push)      PUSH="true"; shift ;;
    --platforms) require_value --platforms "$#"; PLATFORMS="$2"; shift 2 ;;
    --mode)      require_value --mode "$#";      MODE="$2"; shift 2 ;;
    --no-cache)  NO_CACHE="true"; shift ;;
    --dry-run)   DRY_RUN="true"; shift ;;
    -h|--help)   show_help; exit 0 ;;
    *) echo "Unknown flag: $1" >&2; show_help >&2; exit 1 ;;
  esac
done

case "$MODE" in
  source)  DOCKERFILE="docker/quill/Dockerfile.source" ;;
  nightly) DOCKERFILE="docker/quill/Dockerfile" ;;
  *) echo "error: --mode must be source or nightly (got: $MODE)" >&2; exit 1 ;;
esac

# default tag if none was given
if [ ${#TAGS[@]} -eq 0 ]; then
  TAGS=("ravendb/quill:dev")
fi

if [ "$DRY_RUN" = "true" ]; then
  PUSH="false"
fi

if [ "$PUSH" != "true" ]; then
  case "$PLATFORMS" in
    *,*)
      echo "error: multi-platform build (--platforms $PLATFORMS) requires --push." >&2
      echo "       buildx --load only supports a single platform." >&2
      exit 1
      ;;
  esac
fi

cd "$(git rev-parse --show-toplevel)"

cmd=(docker buildx build --pull --platform "$PLATFORMS" -f "$DOCKERFILE")

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

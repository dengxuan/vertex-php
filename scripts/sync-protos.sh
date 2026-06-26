#!/usr/bin/env bash
# Sync vendored .proto files from the Vertex spec repo at the pinned SHA,
# then regenerate the PHP protobuf message classes via protoc (--php_out).
# CI runs this with --check-only + `git diff --exit-code` to enforce that the
# checked-in vendored protos track the pinned spec SHA.
#
# Note: we do NOT use grpc_php_plugin. The Vertex gRPC bidi stream is a raw
# HTTP/2 stream carrying our own TransportFrame messages; the transport is
# hand-written on Swoole's coroutine HTTP/2 client (see src/Transport/Grpc),
# so we only need protoc to emit the protobuf *message* classes, not gRPC
# service stubs.
#
# To bump the spec: edit scripts/.spec-ref, re-run this script, commit.
set -euo pipefail

CHECK_ONLY=0
if [[ "${1:-}" == "--check-only" ]]; then
  CHECK_ONLY=1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SHA_FILE="$SCRIPT_DIR/.spec-ref"

if [[ ! -f "$SHA_FILE" ]]; then
  echo "error: $SHA_FILE not found" >&2
  exit 1
fi

SPEC_SHA="$(tr -d '[:space:]' < "$SHA_FILE")"
if [[ -z "$SPEC_SHA" ]]; then
  echo "error: $SHA_FILE is empty; put a dengxuan/Vertex commit SHA in it" >&2
  exit 1
fi

# List of proto files to vendor (spec-relative paths). Keep in lockstep with
# vertex-go / vertex-dotnet so all implementations pin the same wire surface.
PROTOS=(
  "protos/vertex/transport/grpc/v1/bidi.proto"
)

SPEC_RAW_BASE="https://raw.githubusercontent.com/dengxuan/Vertex/$SPEC_SHA"

for p in "${PROTOS[@]}"; do
  dst="$REPO_ROOT/$p"
  mkdir -p "$(dirname "$dst")"
  echo "syncing $p @ $SPEC_SHA"
  curl -fsSL "$SPEC_RAW_BASE/$p" -o "$dst"
done

# --check-only skips protoc regen. Used by CI where installing an exact protoc
# version isn't worth the complexity — tool version differences leak into
# generated file headers and false-positive the drift check. Dev contributors
# omit the flag and run the full regeneration locally.
if [[ "$CHECK_ONLY" -eq 1 ]]; then
  echo "check-only mode; skipping protoc regeneration (CI path)"
  exit 0
fi

# Regenerate PHP message classes from the vendored protos. Generated classes
# land under generated/ (PSR-4 mapped to Vertex\ + GPBMetadata\ in
# composer.json). No --grpc_out: the transport is hand-written (see header).
echo "regenerating PHP message classes via protoc"
cd "$REPO_ROOT"

if ! command -v protoc >/dev/null; then
  echo "error: protoc not found on PATH; install protobuf compiler" >&2
  exit 1
fi

mkdir -p generated
protoc \
  --proto_path=protos \
  --php_out=generated \
  "${PROTOS[@]}"

echo "done. If git diff is non-empty the vendored protos or generated PHP drifted; commit the update."

#!/bin/sh
set -eu
version="${RUNNEL_PROTO_VERSION:-v0.0.4-beta.1}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git -c advice.detachedHead=false clone -q --depth 1 --branch "$version" https://github.com/kinoforge/runnel "$work/runnel"
find src/main/proto/runnel/v1 -name '*.proto' -delete
cp "$work"/runnel/proto/runnel/v1/*.proto src/main/proto/runnel/v1/

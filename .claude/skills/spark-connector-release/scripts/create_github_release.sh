#!/usr/bin/env bash
# Modifications Copyright 2021 StarRocks Limited.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib.sh"

NOTES=
YES="${CONFIRM_DRAFT:-}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --yes) YES=yes ;;
    -*) die "unknown option: $1" ;;
    *) [ -z "$NOTES" ] || die "unexpected extra argument: $1"; NOTES="$1" ;;
  esac
  shift
done
[ -n "$NOTES" ] || die "usage: create_github_release.sh <release-notes.md> [--yes]"
[ -f "$NOTES" ] || die "release notes file not found: $NOTES"
NOTES="$(cd "$(dirname "$NOTES")" && pwd)/$(basename "$NOTES")"
grep -Fq "## What's Changed" "$NOTES" || die "release notes are missing the What's Changed section"
grep -Fq '## Contributors' "$NOTES" || die "release notes are missing the Contributors section"

REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"
assert_primary_checkout "$REPO_ROOT"
require_command gh

VERSION="$(pom_connector_version "$REPO_ROOT")"
pom_is_snapshot "$REPO_ROOT" && die "pom.xml is still a SNAPSHOT"
verify_head_is_tag "$REPO_ROOT" "$VERSION"
COMMIT="$(git rev-parse HEAD)"
verify_tag_on_origin "$REPO_ROOT" "$VERSION" "$COMMIT"
TAG="v$VERSION"
RELEASE_REPO="${RELEASE_REPO:-StarRocks/starrocks-connector-for-apache-spark}"
STATE_DIR="$(release_state_dir "$REPO_ROOT" "$VERSION")"

gh auth status >/dev/null 2>&1 || die "GitHub CLI is not authenticated"
gh release view "$TAG" --repo "$RELEASE_REPO" >/dev/null 2>&1 \
  && die "a GitHub release already exists for $TAG; edit or remove the existing draft instead of duplicating it"

mapfile -t versions < <(supported_spark_versions "$REPO_ROOT")
declare -a assets=()
for spark in "${versions[@]}"; do
  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  filename="$(artifact_filename "$VERSION" "$spark" "$scala")"
  jar="$STATE_DIR/central/$filename"
  meta="$STATE_DIR/central-verified-$spark.env"
  [ -f "$jar" ] && [ -f "$meta" ] \
    || die "Spark $spark has not been verified from Maven Central; run verify_central.sh $VERSION"
  [ "$(sed -n 's/^commit=//p' "$meta")" = "$COMMIT" ] \
    || die "Central verification for Spark $spark belongs to another commit"
  expected_sha="$(sed -n 's/^sha256=//p' "$meta")"
  actual_sha="$(sha256sum "$jar" | awk '{print $1}')"
  [ "$actual_sha" = "$expected_sha" ] || die "$filename changed after Central verification"
  assets+=("$jar")
done

echo
info "Will create draft GitHub release $TAG on $RELEASE_REPO"
printf 'Notes: %s\n' "$NOTES"
printf 'Assets:\n'
for asset in "${assets[@]}"; do printf '  %s\n' "$(basename "$asset")"; done
if [ -t 0 ]; then
  printf 'Create the draft release? [y/N] '
  read -r answer
  [ "$answer" = y ] || [ "$answer" = Y ] || die "draft creation aborted"
else
  [ "$YES" = yes ] || die "non-interactive draft creation requires --yes or CONFIRM_DRAFT=yes"
fi

gh release create "$TAG" \
  --repo "$RELEASE_REPO" \
  --title "Release $VERSION" \
  --notes-file "$NOTES" \
  --draft \
  --verify-tag \
  "${assets[@]}"

info "${C_GREEN}DRAFT GITHUB RELEASE CREATED${C_RESET}: $TAG"
printf 'A maintainer must review and publish the draft manually.\n'

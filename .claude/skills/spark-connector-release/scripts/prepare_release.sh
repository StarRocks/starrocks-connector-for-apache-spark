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

VERSION="${1:-}"
[ -n "$VERSION" ] || die "usage: prepare_release.sh <version>"
validate_release_version "$VERSION"

REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"
assert_primary_checkout "$REPO_ROOT"
assert_clean_checkout "$REPO_ROOT"
require_command mvn

BRANCH="release-$VERSION"
TAG="v$VERSION"
git check-ref-format --branch "$BRANCH" >/dev/null \
  || die "invalid release branch name: $BRANCH"

info "Fetching the latest main branch and tags"
git fetch origin main --tags

git show-ref --verify --quiet "refs/heads/$BRANCH" \
  && die "local branch $BRANCH already exists; inspect it instead of overwriting it"
git show-ref --verify --quiet "refs/tags/$TAG" \
  && die "local tag $TAG already exists; tags are never overwritten by this workflow"
[ -z "$(git ls-remote --heads origin "refs/heads/$BRANCH")" ] \
  || die "origin branch $BRANCH already exists"
[ -z "$(git ls-remote --tags origin "refs/tags/$TAG")" ] \
  || die "origin tag $TAG already exists; a published release version cannot be reused"

current="$(git show origin/main:pom.xml | awk '
  index($0, "<artifactId>starrocks-spark-connector-${env.SPARK_FEATURE_VERSION}_${env.STARROCKS_SCALA_VERSION}</artifactId>") { found = 1; next }
  found && /<version>[^<]+<\/version>/ {
    line = $0
    sub(/^.*<version>/, "", line)
    sub(/<\/version>.*$/, "", line)
    print line
    exit
  }
')"
[ -n "$current" ] || die "cannot determine connector version from origin/main"
[[ "$current" == *-SNAPSHOT ]] \
  || die "origin/main version $current is not a SNAPSHOT; inspect repository state before releasing"

info "Creating $BRANCH from origin/main"
git switch --create "$BRANCH" origin/main

info "Setting connector version $current -> $VERSION"
mvn --batch-mode versions:set \
  -DnewVersion="$VERSION" \
  -DgenerateBackupPoms=false

[ "$(pom_connector_version "$REPO_ROOT")" = "$VERSION" ] \
  || die "pom.xml did not resolve to release version $VERSION"
pom_is_snapshot "$REPO_ROOT" && die "pom.xml is still a SNAPSHOT after versions:set"

mapfile -t changed < <(git status --porcelain | awk '{print $2}')
[ "${#changed[@]}" -eq 1 ] && [ "${changed[0]}" = pom.xml ] \
  || die "version update changed unexpected files: ${changed[*]:-none}"

git add pom.xml
git commit -m "Bump version to $VERSION"
git tag "$TAG"

head="$(git rev-parse HEAD)"
verify_head_is_tag "$REPO_ROOT" "$VERSION"
pass "$TAG points to $head"

echo
info "${C_GREEN}RELEASE COMMIT AND LOCAL TAG READY${C_RESET}"
printf 'Branch: %s\nTag: %s\nCommit: %s\n' "$BRANCH" "$TAG" "$head"
printf 'Nothing was pushed. Next run: scripts/build_verify.sh\n'

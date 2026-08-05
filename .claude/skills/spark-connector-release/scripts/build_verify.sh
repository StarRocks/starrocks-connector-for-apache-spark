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

REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"
assert_primary_checkout "$REPO_ROOT"
assert_clean_checkout "$REPO_ROOT"
assert_central_dry_run_contract "$REPO_ROOT"
trap 'cleanup_maven_generated_files "$REPO_ROOT"' EXIT

VERSION="$(pom_connector_version "$REPO_ROOT")"
[ -n "$VERSION" ] || die "cannot determine connector version"
pom_is_snapshot "$REPO_ROOT" && die "pom.xml is still a SNAPSHOT; run prepare_release.sh first"
verify_head_is_tag "$REPO_ROOT" "$VERSION"
COMMIT="$(git rev-parse HEAD)"
SDK_VERSION="$(stream_load_sdk_version "$REPO_ROOT")"
MAVEN_CMD="$(release_maven_command)"

mapfile -t versions < <(resolve_versions "$REPO_ROOT" "$@")
[ "${#versions[@]}" -gt 0 ] || die "no Spark versions selected"

STATE_DIR="$(release_state_dir "$REPO_ROOT" "$VERSION")"
ARTIFACT_DIR="$STATE_DIR/artifacts"
BUNDLE_DIR="$STATE_DIR/bundles"
mkdir -p "$ARTIFACT_DIR" "$BUNDLE_DIR"
if [ -f "$STATE_DIR/commit" ]; then
  [ "$(cat "$STATE_DIR/commit")" = "$COMMIT" ] \
    || die "release state belongs to another commit; inspect and remove $STATE_DIR before continuing"
else
  printf '%s\n' "$COMMIT" > "$STATE_DIR/commit"
fi

info "Building connector $VERSION at $COMMIT"
declare -a results=()
overall=0

for spark in "${versions[@]}"; do
  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  java="$(spark_java_major "$REPO_ROOT" "$spark")"
  filename="$(artifact_filename "$VERSION" "$spark" "$scala")"
  source_jar="$REPO_ROOT/target/$filename"
  staged_jar="$ARTIFACT_DIR/$filename"
  source_bundle="$REPO_ROOT/target/central-publishing/central-bundle.zip"
  staged_bundle="$BUNDLE_DIR/central-bundle-spark-$spark.zip"

  info "Building the signed Central bundle for Spark $spark / Scala $scala / JDK $java without publishing"
  activate_java_for_spark "$REPO_ROOT" "$spark"
  if ! CUSTOM_MVN="$MAVEN_CMD --batch-mode --no-transfer-progress -DskipPublishing=true" \
      bash deploy.sh "$spark"; then
    fail "deploy.sh dry-run failed for Spark $spark"
    results+=("Spark $spark FAIL(dry-run)")
    overall=1
    continue
  fi
  if [ ! -f "$source_jar" ]; then
    fail "expected primary JAR not found: $source_jar"
    results+=("Spark $spark FAIL(no-jar)")
    overall=1
    continue
  fi
  if [ ! -f "$source_bundle" ]; then
    fail "Central plugin did not create $source_bundle"
    results+=("Spark $spark FAIL(no-bundle)")
    overall=1
    continue
  fi
  if ! "$SCRIPT_DIR/verify_bundle.sh" \
      "$source_bundle" "$source_jar" "$COMMIT" "$VERSION" "$spark" "$scala" "$SDK_VERSION"; then
    results+=("Spark $spark FAIL(bundle-verify)")
    overall=1
    continue
  fi

  cp "$source_jar" "$staged_jar"
  cp "$source_bundle" "$staged_bundle"
  sha="$(sha256sum "$staged_jar" | awk '{print $1}')"
  bundle_sha="$(sha256sum "$staged_bundle" | awk '{print $1}')"
  {
    printf 'commit=%s\n' "$COMMIT"
    printf 'spark=%s\n' "$spark"
    printf 'scala=%s\n' "$scala"
    printf 'java=%s\n' "$java"
    printf 'artifact=%s\n' "$filename"
    printf 'sha256=%s\n' "$sha"
    printf 'bundle=%s\n' "$(basename "$staged_bundle")"
    printf 'bundle_sha256=%s\n' "$bundle_sha"
  } > "$STATE_DIR/verified-$spark.env"
  results+=("Spark $spark OK")
done

declare -a verified=()
mapfile -t supported < <(supported_spark_versions "$REPO_ROOT")
for spark in "${supported[@]}"; do
  meta="$STATE_DIR/verified-$spark.env"
  [ -f "$meta" ] || continue
  meta_commit="$(sed -n 's/^commit=//p' "$meta")"
  [ "$meta_commit" = "$COMMIT" ] || continue
  verified+=("$spark")
done

(
  cd "$ARTIFACT_DIR"
  : > "$STATE_DIR/SHA256SUMS"
  for jar in *.jar; do
    [ -f "$jar" ] || continue
    sha256sum "$jar" >> "$STATE_DIR/SHA256SUMS"
  done
)
{
  printf 'commit=%s\n' "$COMMIT"
  printf 'version=%s\n' "$VERSION"
  printf 'versions=%s\n' "${verified[*]}"
} > "$STATE_DIR/verified.env"

echo
info "Build verification summary"
for result in "${results[@]}"; do
  printf '  %s\n' "$result"
done

if [ "$overall" -ne 0 ]; then
  die "one or more selected Spark builds failed; verified versions remain: ${verified[*]:-none}"
fi

info "${C_GREEN}SELECTED VERSIONS VERIFIED${C_RESET}: ${versions[*]}"
printf 'Verification state: %s\n' "$STATE_DIR"
printf 'Push the tag only after every intended version is verified: git push origin v%s\n' "$VERSION"

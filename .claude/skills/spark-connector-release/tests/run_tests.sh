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

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "$TEST_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
SCRIPTS="$SKILL_DIR/scripts"

passed=0
failed=0

pass_test() {
  printf 'PASS %s\n' "$1"
  passed=$((passed + 1))
}

fail_test() {
  printf 'FAIL %s: %s\n' "$1" "$2" >&2
  failed=$((failed + 1))
}

assert_success() {
  local name="$1"
  shift
  if "$@"; then
    pass_test "$name"
  else
    fail_test "$name" "command failed: $*"
  fi
}

assert_failure_with() {
  local name="$1" expected="$2"
  shift 2
  local output status
  set +e
  output="$("$@" 2>&1)"
  status=$?
  set -e
  if [ "$status" -ne 0 ] && [[ "$output" == *"$expected"* ]]; then
    pass_test "$name"
  else
    fail_test "$name" "expected failure containing '$expected'; status=$status output=$output"
  fi
}

make_fake_jar() {
  local destination="$1" version="$2" spark="$3" scala="$4" commit="$5" sdk_version="$6"
  local fixture artifact
  fixture="$(mktemp -d)"
  artifact="starrocks-spark-connector-${spark}_${scala}"
  mkdir -p \
    "$fixture/META-INF/maven/com.starrocks/$artifact" \
    "$fixture/META-INF/maven/com.starrocks/starrocks-stream-load-sdk" \
    "$fixture/com/starrocks/connector/spark" \
    "$fixture/com/starrocks/data/load/stream"
  printf 'groupId=com.starrocks\nartifactId=%s\nversion=%s\n' "$artifact" "$version" \
    > "$fixture/META-INF/maven/com.starrocks/$artifact/pom.properties"
  printf 'git.build.version=%s\ngit.commit.id=%s\n' "$version" "$commit" \
    > "$fixture/starrocks-spark-connector-git.properties"
  printf 'groupId=com.starrocks\nartifactId=starrocks-stream-load-sdk\nversion=%s\n' "$sdk_version" \
    > "$fixture/META-INF/maven/com.starrocks/starrocks-stream-load-sdk/pom.properties"
  printf 'git.build.version=%s\ngit.commit.id=1111111111111111111111111111111111111111\n' "$sdk_version" \
    > "$fixture/stream-load-sdk-git.properties"
  printf 'connector-class' > "$fixture/com/starrocks/connector/spark/Fixture.class"
  printf 'sdk' > "$fixture/com/starrocks/data/load/stream/Chunk.class"
  (cd "$fixture" && zip -qr "$destination" .)
  rm -rf "$fixture"
}

test_required_files() {
  local file
  for file in \
    SKILL.md \
    references/release-guide.md \
    assets/release-note-template.md \
    scripts/lib.sh \
    scripts/preflight.sh \
    scripts/prepare_release.sh \
    scripts/build_verify.sh \
    scripts/publish.sh \
    scripts/verify_central.sh \
    scripts/create_github_release.sh \
    scripts/verify_bundle.sh \
    scripts/verify_jar.sh; do
    [ -f "$SKILL_DIR/$file" ] || return 1
  done
}

test_scripts_are_executable() {
  local script
  for script in "$SCRIPTS"/*.sh; do
    [ -x "$script" ] || return 1
  done
}

test_supported_versions_come_from_common() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local actual expected
  actual="$(supported_spark_versions "$REPO_ROOT" | paste -sd ' ' -)"
  expected="$(CUSTOM_MVN=true bash -c 'source "$1/common.sh" >/dev/null; printf "%s\\n" "${SUPPORTED_SPARK_VERSIONS[@]}"' _ "$REPO_ROOT" | paste -sd ' ' -)"
  [ "$actual" = "$expected" ] && [ -n "$actual" ]
}

test_profile_mappings() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local spark expected_java expected_scala
  while IFS= read -r spark; do
    case "$spark" in
      3.*) expected_java=8; expected_scala=2.12 ;;
      4.*) expected_java=17; expected_scala=2.13 ;;
      *) return 1 ;;
    esac
    [ "$(spark_java_major "$REPO_ROOT" "$spark")" = "$expected_java" ]
    [ "$(spark_scala_binary "$REPO_ROOT" "$spark")" = "$expected_scala" ]
  done < <(supported_spark_versions "$REPO_ROOT")
}

test_version_selection_guards() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local spark
  spark="$(supported_spark_versions "$REPO_ROOT" | head -1)"
  [ -n "$spark" ]
  assert_failure_with "duplicate Spark selection is rejected" "selected more than once" \
    resolve_versions "$REPO_ROOT" "$spark" "$spark"
}

test_custom_maven_settings() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local actual
  actual="$(CUSTOM_MVN='mvn -o' MAVEN_SETTINGS=/tmp/release-settings.xml release_maven_command)"
  [ "$actual" = 'mvn -o --settings /tmp/release-settings.xml' ]
}

test_central_dry_run_contract() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  [ "$(central_plugin_version "$REPO_ROOT")" = 0.8.0 ]
  assert_central_dry_run_contract "$REPO_ROOT"
}

test_linked_worktree_rejected() {
  # This repository is intentionally the development linked worktree.
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  assert_primary_checkout "$REPO_ROOT"
}

test_jar_validation() {
  local work commit spark3 spark4 snapshot_sdk
  work="$(mktemp -d)"
  commit=0123456789abcdef0123456789abcdef01234567
  spark3="$work/starrocks-spark-connector-3.5_2.12-1.2.0.jar"
  spark4="$work/starrocks-spark-connector-4.1_2.13-1.2.0.jar"
  snapshot_sdk="$work/starrocks-spark-connector-3.5_2.12-1.2.0.jar.snapshot-sdk"
  make_fake_jar "$spark3" 1.2.0 3.5 2.12 "$commit" 1.0
  make_fake_jar "$spark4" 1.2.0 4.1 2.13 "$commit" 1.0
  cp "$spark3" "$snapshot_sdk"
  fixture="$(mktemp -d)"
  (cd "$fixture" && unzip -q "$snapshot_sdk")
  printf 'git.build.version=1.0-SNAPSHOT\ngit.commit.id=1111111111111111111111111111111111111111\n' \
    > "$fixture/stream-load-sdk-git.properties"
  (cd "$fixture" && zip -qr "$snapshot_sdk" .)
  rm -rf "$fixture"
  "$SCRIPTS/verify_jar.sh" "$spark3" "$commit" 1.2.0 3.5 2.12 1.0 >/dev/null
  "$SCRIPTS/verify_jar.sh" "$spark4" "$commit" 1.2.0 4.1 2.13 1.0 >/dev/null
  assert_failure_with "jar rejects snapshot Stream Load SDK metadata" "SDK Git version" \
    "$SCRIPTS/verify_jar.sh" "$snapshot_sdk" "$commit" 1.2.0 3.5 2.12 1.0
  rm -rf "$work"
}

test_irreversible_stage_guards() {
  rg -q --fixed-strings -- '-DskipPublishing=true' "$SCRIPTS/build_verify.sh"
  rg -q --fixed-strings 'CONFIRM_PUBLISH' "$SCRIPTS/publish.sh"
  rg -q --fixed-strings 'publishing-$spark.env' "$SCRIPTS/publish.sh"
  rg -q --fixed-strings -- '--draft' "$SCRIPTS/create_github_release.sh"
  ! rg -q --fixed-strings -- '--draft=false' "$SCRIPTS/create_github_release.sh"
}

assert_success "required files exist" test_required_files
assert_success "release scripts are executable" test_scripts_are_executable
assert_success "supported versions come from common.sh" test_supported_versions_come_from_common
assert_success "Spark profiles map to the required JDK and Scala" test_profile_mappings
assert_success "duplicate Spark versions cannot be selected" test_version_selection_guards
assert_success "custom Maven settings reach deploy.sh" test_custom_maven_settings
assert_success "Central no-upload rehearsal is pinned to its verified plugin" test_central_dry_run_contract
assert_failure_with "linked worktree is rejected" "linked Git worktree" test_linked_worktree_rejected
assert_success "JAR verifier validates Spark release metadata" test_jar_validation
assert_success "publish and GitHub stages retain irreversible-action guards" test_irreversible_stage_guards

for script in "$SCRIPTS"/*.sh "$TEST_DIR"/*.sh; do
  if bash -n "$script"; then
    pass_test "bash syntax: $(basename "$script")"
  else
    fail_test "bash syntax: $(basename "$script")" "bash -n failed"
  fi
done

printf '\n%d passed, %d failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]

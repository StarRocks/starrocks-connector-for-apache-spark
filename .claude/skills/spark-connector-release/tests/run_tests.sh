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
    "$fixture/META-INF/maven/com.starrocks/starrocks-stream-load-sdk"
  printf 'groupId=com.starrocks\nartifactId=%s\nversion=%s\n' "$artifact" "$version" \
    > "$fixture/META-INF/maven/com.starrocks/$artifact/pom.properties"
  printf 'git.build.version=%s\ngit.commit.id=%s\n' "$version" "$commit" \
    > "$fixture/starrocks-spark-connector-git.properties"
  printf 'groupId=com.starrocks\nartifactId=starrocks-stream-load-sdk\nversion=%s\n' "$sdk_version" \
    > "$fixture/META-INF/maven/com.starrocks/starrocks-stream-load-sdk/pom.properties"
  printf 'git.build.version=%s\ngit.commit.id=1111111111111111111111111111111111111111\n' "$sdk_version" \
    > "$fixture/stream-load-sdk-git.properties"
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

test_codex_project_skill_is_discoverable() {
  local codex_skills codex_skill
  codex_skills="$REPO_ROOT/.agents/skills"
  codex_skill="$REPO_ROOT/.agents/skills/spark-connector-release"
  [ -L "$codex_skills" ] \
    && [ "$(readlink "$codex_skills")" = '../.claude/skills' ] \
    && [ -f "$codex_skill/SKILL.md" ] \
    && [ "$codex_skill/SKILL.md" -ef "$SKILL_DIR/SKILL.md" ]
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

write_version_doc() {
  local destination="$1" version="$2"
  mkdir -p "$(dirname "$destination")"
  printf '%s\n' \
    '# Connector documentation' \
    '' \
    'This prose mentions 9.9.9 but is not a compatibility declaration.' \
    '' \
    '## Version requirements' \
    '' \
    '| Spark connector | Spark | Java | Scala |' \
    '|-----------------|-------|------|-------|' \
    "| $version | 3.5 | 8 | 2.12 |" \
    '' \
    '## Usage' \
    > "$destination"
}

test_docs_version_gate_accepts_exact_rows() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local work
  work="$(mktemp -d)"
  write_version_doc "$work/docs/connector-read.md" 1.1.4-RC0
  write_version_doc "$work/docs/connector-write.md" 1.1.4-RC0
  if ! check_docs_version "$work" 1.1.4-RC0 >/dev/null; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_docs_version_gate_requires_exact_confirmation() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local work output status
  work="$(mktemp -d)"
  write_version_doc "$work/docs/connector-read.md" 1.1.4
  write_version_doc "$work/docs/connector-write.md" 1.1.4

  set +e
  output="$(CONFIRM_DOCS_VERSION=1.1.4 check_docs_version "$work" 1.1.4-RC0 2>&1)"
  status=$?
  set -e
  rm -rf "$work"

  [ "$status" -ne 0 ] && [[ "$output" == *"ASK THE USER"* ]]
}

test_docs_version_gate_accepts_user_confirmation() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local work
  work="$(mktemp -d)"
  write_version_doc "$work/docs/connector-read.md" 1.1.4
  write_version_doc "$work/docs/connector-write.md" 1.1.4
  if ! CONFIRM_DOCS_VERSION=1.1.4-RC0 \
      check_docs_version "$work" 1.1.4-RC0 >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_prepare_release_checks_origin_main_docs_before_branching() {
  local work origin repo fake_bin output status
  work="$(mktemp -d)"
  origin="$work/origin.git"
  repo="$work/repo"
  fake_bin="$work/bin"
  git init --quiet --bare "$origin"
  git init --quiet --initial-branch=main "$repo"
  git -C "$repo" config user.name 'Release Test'
  git -C "$repo" config user.email 'release-test@example.com'
  printf '%s\n' \
    '<project>' \
    '  <artifactId>starrocks-spark-connector-${env.SPARK_FEATURE_VERSION}_${env.STARROCKS_SCALA_VERSION}</artifactId>' \
    '  <version>1.2.3-SNAPSHOT</version>' \
    '</project>' > "$repo/pom.xml"
  printf '%s\n' 'SUPPORTED_SPARK_VERSIONS=(3.5)' > "$repo/common.sh"
  write_version_doc "$repo/docs/connector-read.md" 1.2.2
  write_version_doc "$repo/docs/connector-write.md" 1.2.2
  git -C "$repo" add pom.xml common.sh docs
  git -C "$repo" commit --quiet -m fixture
  git -C "$repo" remote add origin "$origin"
  git -C "$repo" push --quiet --set-upstream origin main
  git -C "$repo" switch --quiet --create local-docs
  write_version_doc "$repo/docs/connector-read.md" 1.2.3
  write_version_doc "$repo/docs/connector-write.md" 1.2.3
  git -C "$repo" add docs
  git -C "$repo" commit --quiet -m 'local docs only'
  mkdir -p "$fake_bin"
  printf '%s\n' '#!/usr/bin/env bash' 'echo "mvn must not run before the docs gate" >&2' 'exit 99' \
    > "$fake_bin/mvn"
  chmod +x "$fake_bin/mvn"

  set +e
  output="$(PATH="$fake_bin:$PATH" CONFIRM_DOCS_VERSION= CONNECTOR_REPO="$repo" \
    "$SCRIPTS/prepare_release.sh" 1.2.3 2>&1)"
  status=$?
  set -e

  if [ "$status" -eq 0 ] \
      || [[ "$output" != *"ASK THE USER"* ]] \
      || git -C "$repo" show-ref --verify --quiet refs/heads/release-1.2.3; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_linked_worktree_rejected() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local fixture_root linked_checkout output status
  fixture_root="$(mktemp -d)"
  linked_checkout="$fixture_root/linked-checkout"
  git -C "$REPO_ROOT" worktree add --quiet --detach "$linked_checkout" HEAD || {
    rmdir "$fixture_root"
    return 1
  }

  set +e
  output="$(assert_primary_checkout "$linked_checkout" 2>&1)"
  status=$?
  set -e

  git -C "$REPO_ROOT" worktree remove --force "$linked_checkout" >/dev/null
  rmdir "$fixture_root"
  [ "$status" -ne 0 ] && [[ "$output" == *"linked Git worktree"* ]]
}

test_publish_rechecks_official_origin() {
  local work repo fork output status
  work="$(mktemp -d)"
  repo="$work/repo"
  fork="$work/lookalike/StarRocks/starrocks-connector-for-apache-spark-fork.git"
  mkdir -p "$repo"
  mkdir -p "$(dirname "$fork")"
  git init --quiet --bare "$fork"
  git init --quiet "$repo"
  git -C "$repo" config user.name 'Release Test'
  git -C "$repo" config user.email 'release-test@example.com'
  printf '%s\n' \
    '<project>' \
    '  <artifactId>starrocks-spark-connector-${env.SPARK_FEATURE_VERSION}_${env.STARROCKS_SCALA_VERSION}</artifactId>' \
    '  <version>1.2.3</version>' \
    '</project>' > "$repo/pom.xml"
  printf '%s\n' 'SUPPORTED_SPARK_VERSIONS=(3.5)' > "$repo/common.sh"
  git -C "$repo" add pom.xml common.sh
  git -C "$repo" commit --quiet -m fixture
  git -C "$repo" tag v1.2.3
  git -C "$repo" remote add origin "$fork"
  git -C "$repo" push --quiet origin HEAD 'refs/tags/v1.2.3'

  set +e
  output="$(CONNECTOR_REPO="$repo" "$SCRIPTS/publish.sh" 3.5 2>&1)"
  status=$?
  set -e
  rm -rf "$work"

  [ "$status" -ne 0 ] \
    && [[ "$output" == *"origin is not the StarRocks Spark connector repository"* ]]
}

test_verified_bundle_upload() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local work bundle curl_config call_log marker bundle_sha
  work="$(mktemp -d)"
  bundle="$work/verified-central-bundle.zip"
  curl_config="$work/curl.conf"
  call_log="$work/curl.log"
  marker="$work/publishing.env"
  printf 'verified bundle bytes' > "$bundle"
  printf 'header = "Authorization: Bearer test"\n' > "$curl_config"
  printf 'started=yes\n' > "$marker"

  curl() {
    local output='' argument previous='' url
    url="${*: -1}"
    printf '%s\n' "$*" >> "$call_log"
    for argument in "$@"; do
      if [ "$previous" = --output ]; then
        output="$argument"
      fi
      previous="$argument"
    done
    [ -n "$output" ] || return 2
    case "$url" in
      */api/v1/publisher/upload\?*)
        printf '01234567-89ab-cdef-0123-456789abcdef' > "$output"
        printf '201'
        ;;
      */api/v1/publisher/status\?id=*)
        printf '{"deploymentState":"PUBLISHED"}\n' > "$output"
        printf '200'
        ;;
      *) return 3 ;;
    esac
  }

  if ! publish_verified_bundle "$bundle" 'spark-3.5 release' "$curl_config" "$marker" >/dev/null; then
    unset -f curl
    rm -rf "$work"
    return 1
  fi
  bundle_sha="$(sha256sum "$bundle" | awk '{print $1}')"
  if ! rg -Fxq 'deployment_id=01234567-89ab-cdef-0123-456789abcdef' "$marker" \
      || ! rg -Fxq "bundle_sha256=$bundle_sha" "$marker" \
      || ! rg -Fq -- "--form bundle=@$bundle;type=application/octet-stream" "$call_log" \
      || ! rg -Fq -- 'publishingType=AUTOMATIC' "$call_log" \
      || ! rg -Fq -- '--connect-timeout 20' "$call_log" \
      || ! rg -Fq -- '--max-time 600' "$call_log" \
      || ! rg -Fq -- '--max-time 60' "$call_log"; then
    unset -f curl
    rm -rf "$work"
    return 1
  fi
  unset -f curl
  rm -rf "$work"
}

test_central_api_credentials_from_maven_settings() {
  # shellcheck disable=SC1091
  source "$SCRIPTS/lib.sh"
  local work settings curl_config
  work="$(mktemp -d)"
  settings="$work/settings.xml"
  curl_config="$work/curl.conf"
  printf '%s\n' \
    '<settings>' \
    '  <servers>' \
    '    <server>' \
    '      <id>central</id>' \
    '      <username>test-user</username>' \
    '      <password>test-pass</password>' \
    '    </server>' \
    '  </servers>' \
    '</settings>' > "$settings"

  MAVEN_SETTINGS="$settings" CENTRAL_USERNAME= CENTRAL_PASSWORD= \
    write_central_curl_config "$curl_config" || {
      rm -rf "$work"
      return 1
    }
  if [ "$(stat -c '%a' "$curl_config")" != 600 ] \
      || ! rg -Fxq 'header = "Authorization: Bearer dGVzdC11c2VyOnRlc3QtcGFzcw=="' "$curl_config"; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_jar_validation() {
  local work commit spark3 spark4 snapshot_sdk fixture
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
  rg -q --fixed-strings -- '-DskipPublishing=true' "$SCRIPTS/build_verify.sh" || return 1
  rg -q --fixed-strings 'CONFIRM_PUBLISH' "$SCRIPTS/publish.sh" || return 1
  rg -q --fixed-strings 'publishing-$spark.env' "$SCRIPTS/publish.sh" || return 1
  rg -q --fixed-strings 'publish_verified_bundle' "$SCRIPTS/publish.sh" || return 1
  ! rg -q --fixed-strings 'bash deploy.sh' "$SCRIPTS/publish.sh" || return 1
  rg -q --fixed-strings -- '--draft' "$SCRIPTS/create_github_release.sh" || return 1
  ! rg -q --fixed-strings -- '--draft=false' "$SCRIPTS/create_github_release.sh" || return 1
}

assert_success "required files exist" test_required_files
assert_success "Codex discovers the project skill from .agents/skills" test_codex_project_skill_is_discoverable
assert_success "release scripts are executable" test_scripts_are_executable
assert_success "supported versions come from common.sh" test_supported_versions_come_from_common
assert_success "Spark profiles map to the required JDK and Scala" test_profile_mappings
assert_success "duplicate Spark versions cannot be selected" test_version_selection_guards
assert_success "custom Maven settings reach deploy.sh" test_custom_maven_settings
assert_success "Central no-upload rehearsal is pinned to its verified plugin" test_central_dry_run_contract
assert_success "docs gate accepts exact version rows" test_docs_version_gate_accepts_exact_rows
assert_success "docs gate requires confirmation for an exact missing version" test_docs_version_gate_requires_exact_confirmation
assert_success "docs gate accepts the user's version-scoped confirmation" test_docs_version_gate_accepts_user_confirmation
assert_success "prepare release checks origin/main docs before creating a branch" test_prepare_release_checks_origin_main_docs_before_branching
assert_success "linked-worktree rejection uses an isolated fixture" test_linked_worktree_rejected
assert_success "publish rejects a matching tag from a non-official origin" test_publish_rechecks_official_origin
assert_success "Central API credentials come from Maven settings without logging them" test_central_api_credentials_from_maven_settings
assert_success "verified Central bundle is uploaded directly" test_verified_bundle_upload
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

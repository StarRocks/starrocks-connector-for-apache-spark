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

if [ -t 1 ]; then
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'
  C_RESET=$'\033[0m'
else
  C_RED=
  C_GREEN=
  C_YELLOW=
  C_BLUE=
  C_RESET=
fi

info() { printf '%s==>%s %s\n' "$C_BLUE" "$C_RESET" "$*"; }
warn() { printf '%sWARN%s %s\n' "$C_YELLOW" "$C_RESET" "$*" >&2; }
pass() { printf '  %sPASS%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
fail() { printf '  %sFAIL%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }
die()  { printf '%sABORT%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 1; }

resolve_repo() {
  local root
  if [ -n "${CONNECTOR_REPO:-}" ]; then
    root="$CONNECTOR_REPO"
  else
    root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  fi
  [ -n "$root" ] && [ -f "$root/pom.xml" ] && [ -f "$root/common.sh" ] \
    || die "Cannot find the Spark connector repository; run inside it or set CONNECTOR_REPO"
  grep -Fq '<artifactId>starrocks-spark-connector-${env.SPARK_FEATURE_VERSION}_${env.STARROCKS_SCALA_VERSION}</artifactId>' "$root/pom.xml" \
    || die "$root is not starrocks-connector-for-apache-spark"
  printf '%s\n' "$root"
}

pom_connector_version() {
  awk '
    index($0, "<artifactId>starrocks-spark-connector-${env.SPARK_FEATURE_VERSION}_${env.STARROCKS_SCALA_VERSION}</artifactId>") { found = 1; next }
    found && /<version>[^<]+<\/version>/ {
      line = $0
      sub(/^.*<version>/, "", line)
      sub(/<\/version>.*$/, "", line)
      print line
      exit
    }
  ' "$1/pom.xml"
}

pom_is_snapshot() {
  [[ "$(pom_connector_version "$1")" == *-SNAPSHOT ]]
}

pom_root_property() {
  local root="$1" property="$2"
  awk -v property="$property" '
    /<profiles>/ { exit }
    index($0, "<" property ">") {
      line = $0
      sub("^.*<" property ">", "", line)
      sub("</" property ">.*$", "", line)
      print line
      exit
    }
  ' "$root/pom.xml"
}

pom_profile_property() {
  local root="$1" spark="$2" property="$3"
  awk -v profile="spark-$spark" -v property="$property" '
    index($0, "<id>" profile "</id>") { inside = 1; next }
    inside && index($0, "<" property ">") {
      line = $0
      sub("^.*<" property ">", "", line)
      sub("</" property ">.*$", "", line)
      print line
      exit
    }
    inside && /<\/profile>/ { exit }
  ' "$root/pom.xml"
}

supported_spark_versions() {
  local root="$1"
  (
    export CUSTOM_MVN=true
    # common.sh is the sole source of the supported-version list.
    # shellcheck disable=SC1090
    source "$root/common.sh" >/dev/null
    [ "${#SUPPORTED_SPARK_VERSIONS[@]}" -gt 0 ] \
      || die "common.sh did not define SUPPORTED_SPARK_VERSIONS"
    printf '%s\n' "${SUPPORTED_SPARK_VERSIONS[@]}"
  )
}

resolve_versions() {
  local root="$1"
  shift
  local -a supported requested
  local candidate item found
  declare -A supported_set=()
  declare -A requested_set=()
  mapfile -t supported < <(supported_spark_versions "$root")
  [ "${#supported[@]}" -gt 0 ] || die "no supported Spark versions found in common.sh"
  for item in "${supported[@]}"; do
    [ -z "${supported_set[$item]:-}" ] \
      || die "common.sh lists Spark $item more than once"
    supported_set[$item]=1
  done
  if [ "$#" -eq 0 ]; then
    printf '%s\n' "${supported[@]}"
    return
  fi
  requested=("$@")
  for candidate in "${requested[@]}"; do
    [ -z "${requested_set[$candidate]:-}" ] \
      || die "Spark $candidate was selected more than once"
    requested_set[$candidate]=1
    found=0
    for item in "${supported[@]}"; do
      if [ "$candidate" = "$item" ]; then
        found=1
        break
      fi
    done
    [ "$found" -eq 1 ] || die "Spark $candidate is not supported by common.sh (supported: ${supported[*]})"
    printf '%s\n' "$candidate"
  done
}

spark_java_major() {
  local root="$1" spark="$2" actual expected
  actual="$(pom_profile_property "$root" "$spark" java.target.version)"
  [ -n "$actual" ] || actual="$(pom_root_property "$root" java.target.version)"
  case "$spark" in
    3.*) expected=8 ;;
    4.*) expected=17 ;;
    *) die "No JDK compatibility rule is defined for Spark $spark" ;;
  esac
  [ "$actual" = "$expected" ] \
    || die "Spark $spark profile targets Java $actual, expected Java $expected"
  printf '%s\n' "$actual"
}

spark_scala_binary() {
  local root="$1" spark="$2" actual expected
  actual="$(pom_profile_property "$root" "$spark" env.STARROCKS_SCALA_VERSION)"
  case "$spark" in
    3.*) expected=2.12 ;;
    4.*) expected=2.13 ;;
    *) die "No Scala compatibility rule is defined for Spark $spark" ;;
  esac
  [ "$actual" = "$expected" ] \
    || die "Spark $spark profile uses Scala ${actual:-<missing>}, expected $expected"
  printf '%s\n' "$actual"
}

spark_full_version() {
  local value
  value="$(pom_profile_property "$1" "$2" env.STARROCKS_SPARK_VERSION)"
  [ -n "$value" ] || die "Spark $2 profile is missing env.STARROCKS_SPARK_VERSION"
  printf '%s\n' "$value"
}

stream_load_sdk_version() {
  awk '
    /<dependency>/ { inside = 1; artifact = ""; version = "" }
    inside && /<artifactId>starrocks-stream-load-sdk<\/artifactId>/ { artifact = "yes" }
    inside && /<version>[^<]+<\/version>/ {
      line = $0
      sub(/^.*<version>/, "", line)
      sub(/<\/version>.*$/, "", line)
      version = line
    }
    inside && /<\/dependency>/ {
      if (artifact == "yes") { print version; exit }
      inside = 0
    }
  ' "$1/pom.xml"
}

central_plugin_version() {
  awk '
    /<plugin>/ { inside = 1; central = 0 }
    inside && /<artifactId>central-publishing-maven-plugin<\/artifactId>/ { central = 1 }
    inside && central && /<version>[^<]+<\/version>/ {
      line = $0
      sub(/^.*<version>/, "", line)
      sub(/<\/version>.*$/, "", line)
      print line
      exit
    }
    inside && /<\/plugin>/ { inside = 0 }
  ' "$1/pom.xml"
}

assert_central_dry_run_contract() {
  local root="$1" version
  version="$(central_plugin_version "$root")"
  [ "$version" = 0.8.0 ] \
    || die "skipPublishing dry-run behavior is verified only for Central plugin 0.8.0; found ${version:-<missing>}"
  ! grep -q 'skipPublishing' "$root/deploy.sh" \
    || die "deploy.sh defines skipPublishing itself; review the dry-run contract before release"
}

expected_class_major() {
  case "$1" in
    8) printf '52\n' ;;
    17) printf '61\n' ;;
    *) die "No class-file mapping is defined for Java $1" ;;
  esac
}

java_major_at_home() {
  local home="$1" line version
  [ -x "$home/bin/java" ] || return 1
  line="$("$home/bin/java" -version 2>&1 | head -1)"
  version="$(printf '%s\n' "$line" | sed -n 's/.*version "\([^"]*\)".*/\1/p')"
  [ -n "$version" ] || return 1
  case "$version" in
    1.*) printf '%s\n' "$version" | cut -d. -f2 ;;
    *) printf '%s\n' "$version" | cut -d. -f1 ;;
  esac
}

find_java_home() {
  local major="$1" variable configured candidate detected executable
  local -a candidates=()
  declare -A seen=()
  variable="JAVA${major}_HOME"
  configured="${!variable:-}"
  [ -z "$configured" ] || candidates+=("$configured")
  [ -z "${JAVA_HOME:-}" ] || candidates+=("$JAVA_HOME")
  if command -v java >/dev/null 2>&1; then
    executable="$(readlink -f "$(command -v java)")"
    candidates+=("$(dirname "$(dirname "$executable")")")
  fi
  for candidate in /usr/lib/jvm/* /usr/java/*; do
    [ -d "$candidate" ] && candidates+=("$candidate")
  done
  if command -v update-alternatives >/dev/null 2>&1; then
    while IFS= read -r executable; do
      [ -x "$executable" ] && candidates+=("$(dirname "$(dirname "$(readlink -f "$executable")")")")
    done < <(update-alternatives --list java 2>/dev/null || true)
  fi
  for candidate in "${candidates[@]}"; do
    [ -n "$candidate" ] || continue
    candidate="$(readlink -f "$candidate" 2>/dev/null || true)"
    [ -n "$candidate" ] && [ -z "${seen[$candidate]:-}" ] || continue
    seen[$candidate]=1
    detected="$(java_major_at_home "$candidate" 2>/dev/null || true)"
    if [ "$detected" = "$major" ]; then
      printf '%s\n' "$candidate"
      return
    fi
  done
  die "JDK $major not found; set $variable to a JDK $major installation"
}

activate_java_for_spark() {
  local root="$1" spark="$2" major home detected
  major="$(spark_java_major "$root" "$spark")"
  home="$(find_java_home "$major")"
  export JAVA_HOME="$home"
  export PATH="$JAVA_HOME/bin:$PATH"
  detected="$(java_major_at_home "$JAVA_HOME")"
  [ "$detected" = "$major" ] || die "Spark $spark requires JDK $major, but $JAVA_HOME reports JDK $detected"
  pass "Spark $spark uses JDK $major ($JAVA_HOME)"
}

assert_primary_checkout() {
  local root="$1" git_dir common_dir
  git_dir="$(git -C "$root" rev-parse --absolute-git-dir 2>/dev/null)" \
    || die "$root is not a Git checkout"
  common_dir="$(git -C "$root" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" \
    || die "cannot resolve the Git common directory"
  [ "$git_dir" = "$common_dir" ] \
    || die "linked Git worktree is not supported for release builds; use a primary checkout because git-commit-id-plugin 4.0.2 omits the connector fingerprint here"
}

assert_clean_checkout() {
  [ -z "$(git -C "$1" status --porcelain)" ] \
    || die "working tree is not clean; commit, stash, or remove local changes before continuing"
}

verify_head_is_tag() {
  local root="$1" version="$2" tag_commit head
  tag_commit="$(git -C "$root" rev-parse -q --verify "refs/tags/v${version}^{commit}" 2>/dev/null || true)"
  [ -n "$tag_commit" ] || die "local tag v$version does not exist"
  head="$(git -C "$root" rev-parse HEAD)"
  [ "$head" = "$tag_commit" ] || die "HEAD $head is not tag v$version ($tag_commit)"
}

verify_tag_on_origin() {
  local root="$1" version="$2" expected="$3" remote
  remote="$(git -C "$root" ls-remote --tags origin "refs/tags/v$version" "refs/tags/v$version^{}" | tail -1 | awk '{print $1}')"
  [ -n "$remote" ] || die "origin does not have tag v$version; push the tag only after local verification"
  [ "$remote" = "$expected" ] || die "origin tag v$version points to $remote, expected $expected"
}

release_state_root() {
  local root="$1" path
  path="$(git -C "$root" rev-parse --git-path spark-connector-release)"
  case "$path" in
    /*) printf '%s\n' "$path" ;;
    *) printf '%s/%s\n' "$root" "$path" ;;
  esac
}

release_state_dir() {
  printf '%s/v%s\n' "$(release_state_root "$1")" "$2"
}

artifact_id() {
  printf 'starrocks-spark-connector-%s_%s\n' "$1" "$2"
}

artifact_filename() {
  printf '%s-%s.jar\n' "$(artifact_id "$2" "$3")" "$1"
}

central_artifact_url() {
  local version="$1" spark="$2" scala="$3" artifact
  artifact="$(artifact_id "$spark" "$scala")"
  printf 'https://repo.maven.apache.org/maven2/com/starrocks/%s/%s/%s-%s.jar\n' \
    "$artifact" "$version" "$artifact" "$version"
}

property_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }'
}

validate_release_version() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z][0-9A-Za-z.-]*)?$ ]] \
    || die "invalid release version '$1' (expected semantic version such as 1.2.0 or 1.2.0-RC1)"
  [[ "$1" != *SNAPSHOT* ]] || die "release version must not contain SNAPSHOT"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

release_maven_command() {
  local command="${CUSTOM_MVN:-mvn}"
  if [ -n "${MAVEN_SETTINGS:-}" ]; then
    [[ "$MAVEN_SETTINGS" != *[[:space:]]* ]] \
      || die "MAVEN_SETTINGS path must not contain whitespace because deploy.sh expands CUSTOM_MVN as words"
    command="$command --settings $MAVEN_SETTINGS"
  fi
  printf '%s\n' "$command"
}

cleanup_maven_generated_files() {
  # flatten-maven-plugin writes this untracked file at the repository root.
  # A clean checkout was required before the lifecycle started, so it is safe
  # to remove this exact generated path when a build or publication exits.
  rm -f "$1/.flattened-pom.xml"
}

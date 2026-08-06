#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
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

ci_error() {
  printf 'ERROR %s\n' "$*" >&2
}

spark_full_version() {
  case "${1:-}" in
    3.3) printf '3.3.2\n' ;;
    3.4) printf '3.4.0\n' ;;
    3.5) printf '3.5.0\n' ;;
    4.0) printf '4.0.0\n' ;;
    4.1) printf '4.1.0\n' ;;
    *)
      ci_error "unsupported Spark minor: ${1:-<empty>}"
      return 1
      ;;
  esac
}

spark_scala_binary() {
  case "${1:-}" in
    3.3|3.4|3.5) printf '2.12\n' ;;
    4.0|4.1) printf '2.13\n' ;;
    *)
      ci_error "unsupported Spark minor: ${1:-<empty>}"
      return 1
      ;;
  esac
}

spark_build_java_major() {
  case "${1:-}" in
    3.3|3.4|3.5) printf '8\n' ;;
    4.0|4.1) printf '17\n' ;;
    *)
      ci_error "unsupported Spark minor: ${1:-<empty>}"
      return 1
      ;;
  esac
}

spark_runtime_java_major() {
  case "${1:-}" in
    3.3|3.4|3.5) printf '11\n' ;;
    4.0|4.1) printf '17\n' ;;
    *)
      ci_error "unsupported Spark minor: ${1:-<empty>}"
      return 1
      ;;
  esac
}

spark_official_image() {
  case "${1:-}" in
    3.3)
      printf '%s\n' \
        'apache/spark-py:v3.3.2@sha256:5bb83c62a12546bc0abd5196df301ec64085aa5c38cc9e144c6135d740596c7a'
      ;;
    3.4)
      printf '%s\n' \
        'apache/spark:3.4.0@sha256:55e67077c859e14b6ea5600dc9dfd67219054c6feed0689558ff07bb4b2c9579'
      ;;
    3.5)
      printf '%s\n' \
        'apache/spark:3.5.0@sha256:0ed5154e6b32ac3af1272d4d65e9f65b13afcfe80b41ad10bd059bcd6317863c'
      ;;
    4.0)
      printf '%s\n' \
        'apache/spark:4.0.0@sha256:a89782d90529a623fc4471cdddb0f9c32d6eed10a81b256a80207b23c3b1df00'
      ;;
    4.1)
      printf '%s\n' \
        'apache/spark:4.1.0-scala2.13-java17-python3-ubuntu@sha256:deaccd823f4bb2b6ed48443abd1ab35669bf9d6c226cf17098d6203182797252'
      ;;
    *)
      ci_error "unsupported Spark minor: ${1:-<empty>}"
      return 1
      ;;
  esac
}

first_line() {
  local value="${1:-}"
  printf '%s\n' "${value%%$'\n'*}"
}

java_output_matches_major() {
  local expected_major="${1:-}" output="${2:-}" version_line
  version_line="$(first_line "$output")"
  case "$expected_major:$version_line" in
    8:*'"1.8.'*) ;;
    11:*'"11.'*) ;;
    17:*'"17.'*) ;;
    *) return 1 ;;
  esac
}

spark_output_matches_version() {
  local expected_version="${1:-}" output="${2:-}"
  [ -n "$expected_version" ] \
    && printf '%s\n' "$output" | grep -Fq "version $expected_version"
}

retry_command() {
  local max_attempts="${1:-}" delay_seconds="${2:-}" attempt=1
  shift 2
  [ "$max_attempts" -gt 0 ] 2>/dev/null && [ "$#" -gt 0 ] || {
    ci_error "usage: retry_command <attempts> <delay-seconds> <command> [args...]"
    return 1
  }

  while ! "$@"; do
    if [ "$attempt" -ge "$max_attempts" ]; then
      return 1
    fi
    ci_error "command failed on attempt $attempt/$max_attempts; retrying"
    attempt=$((attempt + 1))
    sleep "$delay_seconds"
  done
}

find_mysql_jar() {
  local maven_repo="${1:-}" expected
  [ -n "$maven_repo" ] || {
    ci_error "usage: find_mysql_jar <maven-repository>"
    return 1
  }
  expected="$maven_repo/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
  [ -f "$expected" ] || {
    ci_error "MySQL connector JAR is missing: $expected"
    return 1
  }
  printf '%s\n' "$expected"
}

spark_worker_is_alive() {
  local status_file="${1:-}"
  [ -f "$status_file" ] || return 1
  grep -Eq '"state"[[:space:]]*:[[:space:]]*"ALIVE"' "$status_file"
}

executor_evidence_uses_host() {
  local application_log="${1:-}" expected_host="${2:-}" line evidence entry
  local -a entries=()
  [ -f "$application_log" ] && [ -n "$expected_host" ] || return 1
  line="$(grep -F 'CHECK distributed_execution PASS ' "$application_log" | tail -1)"
  [ -n "$line" ] || return 1
  evidence="${line#*CHECK distributed_execution PASS }"
  IFS=',' read -r -a entries <<< "$evidence"
  [ "${#entries[@]}" -gt 0 ] || return 1
  for entry in "${entries[@]}"; do
    [[ "$entry" == *"@$expected_host:"* ]] || return 1
  done
}

starrocks_backend_is_alive() {
  local status_file="${1:-}"
  [ -f "$status_file" ] || return 1
  awk -F '\t' '
    NR == 1 {
      for (column = 1; column <= NF; column++) {
        if ($column == "Alive") {
          alive_column = column
        }
      }
      next
    }
    alive_column && tolower($alive_column) == "true" { found = 1 }
    END { exit !found }
  ' "$status_file"
}

http_response_is_ready() {
  [[ "${1:-}" =~ ^[1-5][0-9][0-9]$ ]]
}

find_connector_jar() {
  local root="${1:-}" spark="${2:-}" scala="${3:-}"
  local target candidate basename candidate_count=0 selected=""

  [ -n "$root" ] && [ -n "$spark" ] && [ -n "$scala" ] || {
    ci_error "usage: find_connector_jar <repo-root> <spark-minor> <scala-binary>"
    return 1
  }
  target="$root/target"
  [ -d "$target" ] || {
    ci_error "connector target directory does not exist: $target"
    return 1
  }

  while IFS= read -r candidate; do
    [ -n "$candidate" ] || continue
    basename="$(basename "$candidate")"
    case "$basename" in
      original-*|*-sources.jar|*-javadoc.jar) continue ;;
    esac
    candidate_count=$((candidate_count + 1))
    selected="$candidate"
  done < <(find "$target" -maxdepth 1 -type f \
    -name "starrocks-spark-connector-${spark}_${scala}-*.jar" -print | sort)

  if [ "$candidate_count" -ne 1 ]; then
    ci_error "expected exactly one final Spark $spark / Scala $scala connector JAR in $target, found $candidate_count"
    return 1
  fi

  printf '%s\n' "$selected"
}

verify_connector_jar() {
  local jar="${1:-}" spark="${2:-}" scala="${3:-}"
  local artifact basename entries required entry

  [ -f "$jar" ] || {
    ci_error "connector JAR does not exist: ${jar:-<empty>}"
    return 1
  }
  artifact="starrocks-spark-connector-${spark}_${scala}"
  basename="$(basename "$jar")"
  case "$basename" in
    "$artifact"-*.jar) ;;
    *)
      ci_error "$basename is not a final $artifact JAR"
      return 1
      ;;
  esac
  case "$basename" in
    original-*|*-sources.jar|*-javadoc.jar)
      ci_error "$basename is not a final connector JAR"
      return 1
      ;;
  esac

  entries="$(jar tf "$jar")" || {
    ci_error "cannot read connector JAR: $jar"
    return 1
  }

  required="com/starrocks/connector/spark/sql/StarRocksDataSourceProvider.class
META-INF/maven/com.starrocks/$artifact/pom.properties"
  while IFS= read -r entry; do
    if ! grep -Fxq "$entry" <<< "$entries"; then
      ci_error "Spark $spark connector is missing required entry: $entry"
      return 1
    fi
  done <<< "$required"

  if grep -Eq '^(org/apache/spark|scala)/.*\.class$' <<< "$entries"; then
    ci_error "Spark $spark connector unexpectedly bundles Spark or Scala implementation classes"
    return 1
  fi

  printf 'Verified packaged connector: %s (Spark %s / Scala %s)\n' "$basename" "$spark" "$scala"
}

package_smoke_jar() {
  local root="${1:-}" output="${2:-}" classes_root class relative output_dir
  local -a smoke_classes=()

  [ -n "$root" ] && [ -n "$output" ] || {
    ci_error "usage: package_smoke_jar <repo-root> <output-jar>"
    return 1
  }
  classes_root="$root/target/test-classes"
  [ -d "$classes_root" ] || {
    ci_error "compiled test class directory does not exist: $classes_root"
    return 1
  }

  while IFS= read -r -d '' class; do
    relative="${class#"$classes_root"/}"
    smoke_classes+=("$relative")
  done < <(find "$classes_root/com/starrocks/connector/spark/ci" -maxdepth 1 \
    -type f -name 'ConnectorDocsSmoke*.class' -print0 2>/dev/null | sort -z)

  if [ "${#smoke_classes[@]}" -eq 0 ]; then
    ci_error "no compiled ConnectorDocsSmoke classes were found below $classes_root"
    return 1
  fi

  mkdir -p "$(dirname "$output")"
  output_dir="$(cd "$(dirname "$output")" && pwd -P)"
  output="$output_dir/$(basename "$output")"
  (cd "$classes_root" && jar cf "$output" "${smoke_classes[@]}") || {
    ci_error "failed to package smoke application JAR: $output"
    return 1
  }

  printf '%s\n' "$output"
}

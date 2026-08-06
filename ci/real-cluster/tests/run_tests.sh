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

set -uo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CI_DIR="$(cd "$TEST_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CI_DIR/../.." && pwd)"

# shellcheck disable=SC1091
source "$CI_DIR/lib.sh"
if [ -f "$CI_DIR/spark-containers.sh" ]; then
  # shellcheck disable=SC1091
  source "$CI_DIR/spark-containers.sh"
fi

passed=0
failed=0

pass() {
  printf 'PASS %s\n' "$1"
  passed=$((passed + 1))
}

fail() {
  printf 'FAIL %s: %s\n' "$1" "$2" >&2
  failed=$((failed + 1))
}

assert_success() {
  local name="$1"
  shift
  if "$@"; then
    pass "$name"
  else
    fail "$name" "expected success"
  fi
}

assert_failure() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    fail "$name" "expected failure"
  else
    pass "$name"
  fi
}

array_contains_sequence() {
  local array_name="$1"
  shift
  local -n values="$array_name"
  local start expected_index
  for ((start = 0; start <= ${#values[@]} - $#; start++)); do
    for ((expected_index = 1; expected_index <= $#; expected_index++)); do
      if [ "${values[start + expected_index - 1]}" != "${!expected_index}" ]; then
        break
      fi
    done
    if [ "$expected_index" -gt "$#" ]; then
      return 0
    fi
  done
  return 1
}

array_contains_value() {
  local array_name="$1" expected="$2" value
  local -n values="$array_name"
  for value in "${values[@]}"; do
    [ "$value" = "$expected" ] && return 0
  done
  return 1
}

array_contains_mount_target() {
  local array_name="$1" target="$2" value
  local -n values="$array_name"
  for value in "${values[@]}"; do
    case "$value" in
      *:"$target"|*:"$target":*) return 0 ;;
    esac
  done
  return 1
}

make_fixture_jar() {
  local destination="$1" spark="$2"
  local fixture scala artifact
  fixture="$(mktemp -d)"
  scala="$(spark_scala_binary "$spark")"
  artifact="starrocks-spark-connector-${spark}_${scala}"
  mkdir -p "$fixture/com/starrocks/connector/spark/sql"
  mkdir -p "$fixture/META-INF/maven/com.starrocks/$artifact"
  printf 'connector' > "$fixture/com/starrocks/connector/spark/sql/StarRocksDataSourceProvider.class"
  printf 'groupId=com.starrocks\nartifactId=%s\nversion=1.2.0\n' "$artifact" \
    > "$fixture/META-INF/maven/com.starrocks/$artifact/pom.properties"

  (cd "$fixture" && jar cf "$destination" .)
  rm -rf "$fixture"
}

test_version_mappings() {
  local expected actual spark
  declare -F spark_build_java_major >/dev/null || return 1
  declare -F spark_runtime_java_major >/dev/null || return 1
  expected=$'3.3|3.3.2|2.12|8|11\n3.4|3.4.0|2.12|8|11\n3.5|3.5.0|2.12|8|11\n4.0|4.0.0|2.13|17|17\n4.1|4.1.0|2.13|17|17'
  actual=""
  for spark in 3.3 3.4 3.5 4.0 4.1; do
    actual+="${spark}|$(spark_full_version "$spark")|$(spark_scala_binary "$spark")|$(spark_build_java_major "$spark")|$(spark_runtime_java_major "$spark")"$'\n'
  done
  [ "${actual%$'\n'}" = "$expected" ]
}

test_official_spark_images_are_pinned() {
  local expected actual spark
  declare -F spark_official_image >/dev/null || return 1
  expected=$'3.3|apache/spark-py:v3.3.2@sha256:5bb83c62a12546bc0abd5196df301ec64085aa5c38cc9e144c6135d740596c7a\n3.4|apache/spark:3.4.0@sha256:55e67077c859e14b6ea5600dc9dfd67219054c6feed0689558ff07bb4b2c9579\n3.5|apache/spark:3.5.0@sha256:0ed5154e6b32ac3af1272d4d65e9f65b13afcfe80b41ad10bd059bcd6317863c\n4.0|apache/spark:4.0.0@sha256:a89782d90529a623fc4471cdddb0f9c32d6eed10a81b256a80207b23c3b1df00\n4.1|apache/spark:4.1.0-scala2.13-java17-python3-ubuntu@sha256:deaccd823f4bb2b6ed48443abd1ab35669bf9d6c226cf17098d6203182797252'
  actual=""
  for spark in 3.3 3.4 3.5 4.0 4.1; do
    actual+="${spark}|$(spark_official_image "$spark")"$'\n'
  done
  [ "${actual%$'\n'}" = "$expected" ] \
    && ! spark_official_image 3.2 >/dev/null 2>&1
}

test_java_output_is_parsed_without_a_pipe() {
  [ "$(first_line $'openjdk version "17.0.20"\nRuntime line\nVM line')" = \
      'openjdk version "17.0.20"' ] \
    && java_output_matches_major 17 $'openjdk version "17.0.20"\nRuntime line' \
    && java_output_matches_major 11 $'openjdk version "11.0.20"\nRuntime line' \
    && java_output_matches_major 8 $'openjdk version "1.8.0_502"\nRuntime line' \
    && ! java_output_matches_major 8 'openjdk version "17.0.20"'
}

test_spark_version_output_is_parsed() {
  declare -F spark_output_matches_version >/dev/null || return 1
  spark_output_matches_version 3.3.2 $'Welcome to\nversion 3.3.2\n' \
    && spark_output_matches_version 4.1.0 $'Welcome to\nversion 4.1.0\n' \
    && ! spark_output_matches_version 4.1.0 $'Welcome to\nversion 4.0.0\n'
}

retry_test_attempts=0

succeed_on_third_attempt() {
  retry_test_attempts=$((retry_test_attempts + 1))
  [ "$retry_test_attempts" -eq 3 ]
}

test_retry_command_retries_transient_failures() {
  retry_test_attempts=0
  retry_command 3 0 succeed_on_third_attempt 2>/dev/null \
    && [ "$retry_test_attempts" -eq 3 ]
}

test_unsupported_version_rejected() {
  ! spark_full_version 3.2 >/dev/null 2>&1
}

test_find_connector_jar() {
  local work expected actual
  work="$(mktemp -d)"
  mkdir -p "$work/target"
  touch \
    "$work/target/original-starrocks-spark-connector-4.1_2.13-1.2.0.jar" \
    "$work/target/starrocks-spark-connector-4.1_2.13-1.2.0-sources.jar" \
    "$work/target/starrocks-spark-connector-4.1_2.13-1.2.0-javadoc.jar"
  expected="$work/target/starrocks-spark-connector-4.1_2.13-1.2.0.jar"
  touch "$expected"
  actual="$(find_connector_jar "$work" 4.1 2.13)" || return 1
  rm -rf "$work"
  [ "$actual" = "$expected" ]
}

test_missing_connector_jar_rejected() {
  local work
  work="$(mktemp -d)"
  mkdir -p "$work/target"
  if find_connector_jar "$work" 4.1 2.13 >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_multiple_connector_jars_rejected() {
  local work
  work="$(mktemp -d)"
  mkdir -p "$work/target"
  touch \
    "$work/target/starrocks-spark-connector-4.1_2.13-1.2.0.jar" \
    "$work/target/starrocks-spark-connector-4.1_2.13-1.2.1.jar"
  if find_connector_jar "$work" 4.1 2.13 >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_connector_jars_for_all_versions_accepted() {
  local work jar spark scala
  work="$(mktemp -d)"
  for spark in 3.3 3.4 3.5 4.0 4.1; do
    scala="$(spark_scala_binary "$spark")"
    jar="$work/starrocks-spark-connector-${spark}_${scala}-1.2.0.jar"
    make_fixture_jar "$jar" "$spark"
    verify_connector_jar "$jar" "$spark" "$scala" >/dev/null || {
      rm -rf "$work"
      return 1
    }
  done
  rm -rf "$work"
}

test_smoke_jar_requires_compiled_classes() {
  local work
  work="$(mktemp -d)"
  mkdir -p "$work/target/test-classes"
  if package_smoke_jar "$work" "$work/smoke.jar" >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_smoke_jar_contains_only_smoke_classes() {
  local work classes jar entries
  work="$(mktemp -d)"
  classes="$work/target/test-classes/com/starrocks/connector/spark/ci"
  jar="$work/smoke.jar"
  mkdir -p "$classes"
  printf 'main' > "$classes/ConnectorDocsSmoke.class"
  printf 'closure' > "$classes/ConnectorDocsSmoke\$\$anonfun\$1.class"
  printf 'not-smoke' > "$classes/AnotherTest.class"
  package_smoke_jar "$work" "$jar" >/dev/null || return 1
  entries="$(jar tf "$jar")" || return 1
  rm -rf "$work"
  printf '%s\n' "$entries" \
    | grep -Fxq 'com/starrocks/connector/spark/ci/ConnectorDocsSmoke.class' \
    && printf '%s\n' "$entries" \
      | grep -Fxq 'com/starrocks/connector/spark/ci/ConnectorDocsSmoke$$anonfun$1.class' \
    && ! printf '%s\n' "$entries" | grep -Fq 'AnotherTest.class' \
    && ! printf '%s\n' "$entries" | grep -Eq '^(org/apache/spark|scala|com/mysql)/'
}

test_find_mysql_jar() {
  local work expected actual
  work="$(mktemp -d)"
  expected="$work/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
  mkdir -p "$(dirname "$expected")"
  touch "$expected"
  actual="$(find_mysql_jar "$work")" || return 1
  rm -rf "$work"
  [ "$actual" = "$expected" ]
}

test_missing_mysql_jar_rejected() {
  local work
  work="$(mktemp -d)"
  if find_mysql_jar "$work" >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
}

test_spark_worker_readiness_parser() {
  local work
  work="$(mktemp -d)"
  printf '{"workers":[{"id":"worker-1","state":"ALIVE"}]}\n' > "$work/alive.json"
  printf '{"workers":[]}\n' > "$work/empty.json"
  spark_worker_is_alive "$work/alive.json" \
    && ! spark_worker_is_alive "$work/empty.json"
  local status=$?
  rm -rf "$work"
  return "$status"
}

test_starrocks_backend_readiness_parser() {
  local work
  work="$(mktemp -d)"
  printf 'BackendId\tIP\tAlive\n10001\t127.0.0.1\ttrue\n' > "$work/alive.tsv"
  printf 'BackendId\tIP\tAlive\n10001\t127.0.0.1\tfalse\n' > "$work/dead.tsv"
  starrocks_backend_is_alive "$work/alive.tsv" \
    && ! starrocks_backend_is_alive "$work/dead.tsv"
  local status=$?
  rm -rf "$work"
  return "$status"
}

test_fe_http_readiness_accepts_authentication_response() {
  http_response_is_ready 200 \
    && http_response_is_ready 401 \
    && ! http_response_is_ready 000 \
    && ! http_response_is_ready error
}

test_cluster_runner_rejects_bad_arguments() {
  ! "$CI_DIR/run.sh" >/dev/null 2>&1 \
    && ! "$CI_DIR/run.sh" 3.2 >/dev/null 2>&1
}

test_spark_roles_use_one_container_network() {
  declare -F spark_master_container_args >/dev/null || return 1
  declare -F spark_worker_container_args >/dev/null || return 1
  declare -F spark_driver_container_args >/dev/null || return 1

  local -a master_args=() worker_args=() driver_args=()
  spark_master_container_args master_args master-id cluster-net official-spark-image 28080
  spark_worker_container_args \
    worker_args worker-id cluster-net official-spark-image /shared /worker
  spark_driver_container_args \
    driver_args driver-id cluster-net official-spark-image /artifacts /shared /events

  array_contains_sequence master_args --network cluster-net \
    && array_contains_sequence master_args --hostname spark-master \
    && array_contains_sequence master_args --publish 127.0.0.1:28080:8080 \
    && array_contains_sequence master_args --entrypoint /opt/spark/bin/spark-class \
      official-spark-image org.apache.spark.deploy.master.Master \
    && array_contains_sequence worker_args --network cluster-net \
    && array_contains_sequence worker_args --hostname spark-worker \
    && array_contains_sequence worker_args --entrypoint /opt/spark/bin/spark-class \
      official-spark-image org.apache.spark.deploy.worker.Worker \
    && array_contains_sequence driver_args --network cluster-net \
    && array_contains_sequence driver_args --hostname spark-driver \
    && array_contains_sequence driver_args --entrypoint /opt/spark/bin/spark-submit \
      official-spark-image \
    && ! array_contains_mount_target master_args /opt/spark \
    && ! array_contains_mount_target worker_args /opt/spark \
    && ! array_contains_mount_target driver_args /opt/spark
}

test_executor_evidence_requires_worker_container() {
  local work good bad
  work="$(mktemp -d)"
  good="$work/good.log"
  bad="$work/bad.log"
  printf '%s\n' \
    'CHECK distributed_execution PASS 1@spark-worker:10:spark://driver/jars/connector.jar,1@spark-worker:20:spark://driver/jars/connector.jar' \
    > "$good"
  printf '%s\n' \
    'CHECK distributed_execution PASS 1@runner-host:10:file:/workspace/connector.jar' \
    > "$bad"

  executor_evidence_uses_host "$good" spark-worker \
    && ! executor_evidence_uses_host "$bad" spark-worker
  local status=$?
  rm -rf "$work"
  return "$status"
}

test_worker_has_shared_data_but_not_driver_artifacts() {
  declare -F spark_worker_container_args >/dev/null || return 1
  declare -F spark_driver_container_args >/dev/null || return 1

  local -a worker_args=() driver_args=()
  spark_worker_container_args \
    worker_args worker-id cluster-net official-spark-image /shared /worker
  spark_driver_container_args \
    driver_args driver-id cluster-net official-spark-image /artifacts /shared /events

  array_contains_value worker_args /shared:/opt/spark-shared \
    && ! array_contains_value worker_args /artifacts:/opt/spark-driver:ro \
    && array_contains_value driver_args /artifacts:/opt/spark-driver:ro \
    && array_contains_value driver_args /shared:/opt/spark-shared \
    && array_contains_value driver_args /events:/opt/spark-events
}

test_diagnostics_permissions_are_fixed_in_a_container() {
  declare -F spark_diagnostics_permissions_container_args >/dev/null || return 1

  local -a permissions_args=()
  spark_diagnostics_permissions_container_args \
    permissions_args official-spark-image /diagnostics-on-host

  array_contains_sequence permissions_args run --rm \
    --volume /diagnostics-on-host:/diagnostics \
    --user 0:0 --entrypoint chmod official-spark-image -R a+rX /diagnostics
}

test_runner_uses_official_image_without_host_distribution() {
  local runner="$CI_DIR/run.sh"
  ! grep -Fq 'java -version 2>&1 | head -1' "$runner" \
    && [ "$(grep -Fc 'retry_command 3 5 docker_command pull' "$runner")" -eq 2 ] \
    && grep -Fq 'spark_diagnostics_permissions_container_args' "$runner" \
    && ! grep -Eq 'SPARK_CACHE_DIR|SPARK_HOME_OVERRIDE|acquire_spark|sha512sum|spark-[^ ]+-bin-hadoop3\\.tgz' "$runner"
}

test_spark_readiness_uses_canonical_json_endpoint() {
  grep -Fq 'http://127.0.0.1:${SPARK_MASTER_UI_PORT}/json/' "$CI_DIR/run.sh"
}

test_starrocks_uses_explicit_port_mappings() {
  local runner="$CI_DIR/run.sh" mapping internal host default_port
  ! grep -Fq -- '--network host' "$runner" || return 1
  for mapping in \
      STARROCKS_FE_HTTP_PORT,STARROCKS_FE_HTTP_HOST_PORT \
      STARROCKS_FE_JDBC_PORT,STARROCKS_FE_JDBC_HOST_PORT \
      STARROCKS_BE_HTTP_PORT,STARROCKS_BE_HTTP_HOST_PORT \
      STARROCKS_BE_THRIFT_PORT,STARROCKS_BE_THRIFT_HOST_PORT \
      STARROCKS_BE_BRPC_PORT,STARROCKS_BE_BRPC_HOST_PORT; do
    IFS=',' read -r internal host <<< "$mapping"
    grep -Fq -- "--publish \"\${$host}:\${$internal}\"" "$runner" || return 1
  done
  for mapping in \
      STARROCKS_FE_HTTP_HOST_PORT,18030 \
      STARROCKS_FE_JDBC_HOST_PORT,19030 \
      STARROCKS_BE_HTTP_HOST_PORT,18040 \
      STARROCKS_BE_THRIFT_HOST_PORT,19060 \
      STARROCKS_BE_BRPC_HOST_PORT,18060; do
    IFS=',' read -r host default_port <<< "$mapping"
    grep -Fq "${host}=\"\${${host}:-$default_port}\"" "$runner" || return 1
  done
}

test_starrocks_registers_bridge_address() {
  local bootstrap="$CI_DIR/start-starrocks.sh" runner="$CI_DIR/run.sh"
  [ -f "$bootstrap" ] || return 1
  grep -Fq \
    'starrocks/allin1-ubuntu:3.5.5@sha256:711dbcdec06a93858bf37ab6e197f36fa707e0488bd4f9c06fa1c666d9ef8149' \
    "$runner" || return 1
  grep -Fq 'hostname -i' "$bootstrap" || return 1
  grep -Fq 'priority_networks = ${container_ip}/32' "$bootstrap" || return 1
  grep -Fq 'MYHOST=${container_ip}' "$bootstrap" || return 1
  grep -Fq -- '--entrypoint /bin/bash' "$runner" || return 1
  grep -Fq 'start-starrocks.sh:/opt/starrocks-ci/start-starrocks.sh:ro' "$runner" || return 1
}

test_github_real_cluster_matrix() {
  local workflow="$REPO_ROOT/.github/workflows/ci-pipeline.yml" tuple
  grep -Fq 'fail-fast: false' "$workflow" || return 1
  grep -Fq 'workflow_dispatch:' "$workflow" || return 1
  grep -Fq 'actions/checkout@v7' "$workflow" || return 1
  grep -Fq 'actions/setup-java@v5' "$workflow" || return 1
  ! grep -Fq 'actions/cache@' "$workflow" || return 1
  ! grep -Fq 'Cache Spark' "$workflow" || return 1
  grep -Fq 'actions/upload-artifact@v7' "$workflow" || return 1
  grep -Fq 'if: always()' "$workflow" || return 1
  grep -Fq './ci/real-cluster/run.sh "${{ matrix.spark }}"' "$workflow" || return 1
  grep -Eq '^[[:space:]]+docker info[[:space:]]*$' "$workflow" || return 1
  ! grep -Fq 'docker info || true' "$workflow" || return 1

  for tuple in \
      '3.3|3.3.2|2.12|8' \
      '3.4|3.4.0|2.12|8' \
      '3.5|3.5.0|2.12|8' \
      '4.0|4.0.0|2.13|17' \
      '4.1|4.1.0|2.13|17'; do
    IFS='|' read -r spark full scala java <<< "$tuple"
    grep -A4 -F -- "- spark: '$spark'" "$workflow" \
      | grep -Fq "full-version: '$full'" || return 1
    grep -A4 -F -- "- spark: '$spark'" "$workflow" \
      | grep -Fq "scala: '$scala'" || return 1
    grep -A4 -F -- "- spark: '$spark'" "$workflow" \
      | grep -Fq "java: '$java'" || return 1
  done
}

assert_success "exact Spark, Scala, build JDK, and runtime JDK mappings" test_version_mappings
assert_success "Apache official Spark images are pinned by digest" test_official_spark_images_are_pinned
assert_success "Java version output is parsed without a truncating pipe" test_java_output_is_parsed_without_a_pipe
assert_success "Spark version output is parsed" test_spark_version_output_is_parsed
assert_success "transient commands are retried" test_retry_command_retries_transient_failures
assert_success "unsupported Spark version is rejected" test_unsupported_version_rejected
assert_success "primary connector JAR is selected" test_find_connector_jar
assert_success "missing connector JAR is rejected" test_missing_connector_jar_rejected
assert_success "multiple connector JARs are rejected" test_multiple_connector_jars_rejected
assert_success "connector packages use one validation contract across Spark versions" test_connector_jars_for_all_versions_accepted
assert_success "smoke JAR requires compiled smoke classes" test_smoke_jar_requires_compiled_classes
assert_success "smoke JAR contains only smoke application classes" test_smoke_jar_contains_only_smoke_classes
assert_success "MySQL connector JAR is resolved from Maven cache" test_find_mysql_jar
assert_success "missing MySQL connector JAR is rejected" test_missing_mysql_jar_rejected
assert_success "Spark worker readiness parses an ALIVE worker" test_spark_worker_readiness_parser
assert_success "StarRocks backend readiness parses the Alive column" test_starrocks_backend_readiness_parser
assert_success "FE HTTP readiness accepts an authenticated endpoint response" test_fe_http_readiness_accepts_authentication_response
assert_success "cluster runner rejects missing and unsupported versions" test_cluster_runner_rejects_bad_arguments
assert_success "Spark master, worker, and driver use one container network" test_spark_roles_use_one_container_network
assert_success "worker receives shared data without driver artifact mounts" test_worker_has_shared_data_but_not_driver_artifacts
assert_success "container-created diagnostics are made readable" test_diagnostics_permissions_are_fixed_in_a_container
assert_success "runner uses the official image without a host Spark distribution" test_runner_uses_official_image_without_host_distribution
assert_success "distributed evidence comes from the worker container" test_executor_evidence_requires_worker_container
assert_success "Spark readiness uses the canonical JSON endpoint" test_spark_readiness_uses_canonical_json_endpoint
assert_success "StarRocks exposes FE and BE ports without host networking" test_starrocks_uses_explicit_port_mappings
assert_success "StarRocks advertises its reachable bridge address" test_starrocks_registers_bridge_address
assert_success "GitHub workflow runs the five-version real-cluster matrix" test_github_real_cluster_matrix

printf '\n%d passed, %d failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]

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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/spark-containers.sh"

if [ "$#" -ne 1 ]; then
  ci_error "usage: $0 <spark-minor>"
  exit 1
fi

SPARK_MINOR="$1"
SPARK_FULL_VERSION="$(spark_full_version "$SPARK_MINOR")"
SCALA_BINARY="$(spark_scala_binary "$SPARK_MINOR")"
EXPECTED_BUILD_JAVA="$(spark_build_java_major "$SPARK_MINOR")"
EXPECTED_RUNTIME_JAVA="$(spark_runtime_java_major "$SPARK_MINOR")"

MAVEN_REPO="${MAVEN_REPO:-${HOME}/.m2/repository}"
SMOKE_LOG_DIR="${SMOKE_LOG_DIR:-${RUNNER_TEMP:-/tmp}/spark-connector-smoke-${SPARK_MINOR}-$$}"
STARROCKS_IMAGE_DEFAULT="starrocks/allin1-ubuntu:3.5.5@sha256:711dbcdec06a93858bf37ab6e197f36fa707e0488bd4f9c06fa1c666d9ef8149"
STARROCKS_IMAGE="${STARROCKS_IMAGE:-$STARROCKS_IMAGE_DEFAULT}"
SPARK_IMAGE="${SPARK_IMAGE:-$(spark_official_image "$SPARK_MINOR")}"
SPARK_MASTER_UI_PORT="${SPARK_MASTER_UI_PORT:-28080}"
STARROCKS_FE_HTTP_PORT="${STARROCKS_FE_HTTP_PORT:-8030}"
STARROCKS_FE_JDBC_PORT="${STARROCKS_FE_JDBC_PORT:-9030}"
STARROCKS_BE_HTTP_PORT="${STARROCKS_BE_HTTP_PORT:-8040}"
STARROCKS_BE_THRIFT_PORT="${STARROCKS_BE_THRIFT_PORT:-9060}"
STARROCKS_BE_BRPC_PORT="${STARROCKS_BE_BRPC_PORT:-8060}"
STARROCKS_FE_HTTP_HOST_PORT="${STARROCKS_FE_HTTP_HOST_PORT:-18030}"
STARROCKS_FE_JDBC_HOST_PORT="${STARROCKS_FE_JDBC_HOST_PORT:-19030}"
STARROCKS_BE_HTTP_HOST_PORT="${STARROCKS_BE_HTTP_HOST_PORT:-18040}"
STARROCKS_BE_THRIFT_HOST_PORT="${STARROCKS_BE_THRIFT_HOST_PORT:-19060}"
STARROCKS_BE_BRPC_HOST_PORT="${STARROCKS_BE_BRPC_HOST_PORT:-18060}"
STARROCKS_STARTUP_TIMEOUT_SECONDS="${STARROCKS_STARTUP_TIMEOUT_SECONDS:-480}"
SPARK_STARTUP_TIMEOUT_SECONDS="${SPARK_STARTUP_TIMEOUT_SECONDS:-120}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-900}"

read -r -a DOCKER_COMMAND_PARTS <<< "${DOCKER_COMMAND:-docker}"
RUN_TOKEN="${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}-${SPARK_MINOR//./-}"
CLUSTER_NETWORK="spark-ci-$RUN_TOKEN"
STARROCKS_CONTAINER="spark-ci-starrocks-$RUN_TOKEN"
SPARK_MASTER_CONTAINER="spark-ci-master-$RUN_TOKEN"
SPARK_WORKER_CONTAINER="spark-ci-worker-$RUN_TOKEN"
SPARK_DRIVER_CONTAINER="spark-ci-driver-$RUN_TOKEN"
NETWORK_CREATED=false

mkdir -p "$SMOKE_LOG_DIR" "$SMOKE_LOG_DIR/spark" "$SMOKE_LOG_DIR/spark-events" \
  "$SMOKE_LOG_DIR/spark-work" "$SMOKE_LOG_DIR/shared-work" \
  "$SMOKE_LOG_DIR/driver-artifacts"
chmod a+rwx "$SMOKE_LOG_DIR/spark-events" "$SMOKE_LOG_DIR/spark-work" \
  "$SMOKE_LOG_DIR/shared-work"

docker_command() {
  "${DOCKER_COMMAND_PARTS[@]}" "$@"
}

collect_container_diagnostics() {
  local label="$1" container="$2"
  if docker_command inspect "$container" >/dev/null 2>&1; then
    docker_command logs "$container" > "$SMOKE_LOG_DIR/$label.log" 2>&1
    docker_command inspect "$container" > "$SMOKE_LOG_DIR/$label-inspect.json" 2>&1
  fi
}

collect_diagnostics() {
  collect_container_diagnostics spark-master "$SPARK_MASTER_CONTAINER"
  collect_container_diagnostics spark-worker "$SPARK_WORKER_CONTAINER"
  collect_container_diagnostics spark-driver "$SPARK_DRIVER_CONTAINER"
  collect_container_diagnostics starrocks-container "$STARROCKS_CONTAINER"
  if docker_command inspect "$STARROCKS_CONTAINER" >/dev/null 2>&1; then
    docker_command cp "$STARROCKS_CONTAINER:/data/deploy/starrocks/fe/log" \
      "$SMOKE_LOG_DIR/starrocks-fe-log" >/dev/null 2>&1
    docker_command cp "$STARROCKS_CONTAINER:/data/deploy/starrocks/be/log" \
      "$SMOKE_LOG_DIR/starrocks-be-log" >/dev/null 2>&1
  fi
}

cleanup() {
  local status=$? container
  local -a diagnostics_permissions_args=()
  trap - EXIT
  set +e
  collect_diagnostics
  for container in \
      "$SPARK_DRIVER_CONTAINER" "$SPARK_WORKER_CONTAINER" \
      "$SPARK_MASTER_CONTAINER" "$STARROCKS_CONTAINER"; do
    if docker_command inspect "$container" >/dev/null 2>&1; then
      docker_command rm --force "$container" >> "$SMOKE_LOG_DIR/cleanup.log" 2>&1
    fi
  done
  if docker_command image inspect "$SPARK_IMAGE" >/dev/null 2>&1; then
    spark_diagnostics_permissions_container_args \
      diagnostics_permissions_args "$SPARK_IMAGE" "$SMOKE_LOG_DIR"
    docker_command "${diagnostics_permissions_args[@]}" \
      >> "$SMOKE_LOG_DIR/cleanup.log" 2>&1
  fi
  if [ "$NETWORK_CREATED" = true ]; then
    docker_command network rm "$CLUSTER_NETWORK" >> "$SMOKE_LOG_DIR/cleanup.log" 2>&1
  fi
  printf 'cleanup_exit_status=%s\n' "$status" >> "$SMOKE_LOG_DIR/cleanup.log"
  exit "$status"
}
trap cleanup EXIT

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    ci_error "required command is unavailable: $1"
    exit 1
  }
}

for command_name in cp curl jar java sha256sum timeout; do
  require_command "$command_name"
done
require_command "${DOCKER_COMMAND_PARTS[0]}"

if ! JAVA_VERSION_OUTPUT="$(java -version 2>&1)"; then
  ci_error "failed to run the host Java runtime"
  exit 1
fi
if ! java_output_matches_major "$EXPECTED_BUILD_JAVA" "$JAVA_VERSION_OUTPUT"; then
  ci_error "Spark $SPARK_MINOR connector build requires JDK $EXPECTED_BUILD_JAVA, but java reports: $(first_line "$JAVA_VERSION_OUTPUT")"
  exit 1
fi

docker_command info > "$SMOKE_LOG_DIR/docker-info.txt"

CONNECTOR_JAR="$(find_connector_jar "$REPO_ROOT" "$SPARK_MINOR" "$SCALA_BINARY")"
verify_connector_jar "$CONNECTOR_JAR" "$SPARK_MINOR" "$SCALA_BINARY" \
  | tee "$SMOKE_LOG_DIR/connector-verification.txt"
sha256sum "$CONNECTOR_JAR" > "$SMOKE_LOG_DIR/connector.sha256"
jar tf "$CONNECTOR_JAR" > "$SMOKE_LOG_DIR/connector-entries.txt"

MYSQL_JAR="$(find_mysql_jar "$MAVEN_REPO")"

DRIVER_ARTIFACT_DIR="$SMOKE_LOG_DIR/driver-artifacts"
CONNECTOR_BASENAME="$(basename "$CONNECTOR_JAR")"
MYSQL_BASENAME="$(basename "$MYSQL_JAR")"
SMOKE_BASENAME="connector-docs-smoke-${SPARK_MINOR}.jar"
SMOKE_JAR="$DRIVER_ARTIFACT_DIR/$SMOKE_BASENAME"
package_smoke_jar "$REPO_ROOT" "$SMOKE_JAR" >/dev/null
jar tf "$SMOKE_JAR" > "$SMOKE_LOG_DIR/smoke-jar-entries.txt"
cp "$CONNECTOR_JAR" "$DRIVER_ARTIFACT_DIR/$CONNECTOR_BASENAME"
cp "$MYSQL_JAR" "$DRIVER_ARTIFACT_DIR/$MYSQL_BASENAME"

printf 'Starting Apache official Spark image %s\n' "$SPARK_IMAGE"
if docker_command image inspect "$SPARK_IMAGE" >/dev/null 2>&1; then
  printf 'Using locally available Apache Spark image %s\n' "$SPARK_IMAGE" \
    | tee "$SMOKE_LOG_DIR/spark-image-pull.log"
else
  retry_command 3 5 docker_command pull --platform linux/amd64 "$SPARK_IMAGE" \
    | tee "$SMOKE_LOG_DIR/spark-image-pull.log"
fi
if ! CONTAINER_JAVA_OUTPUT="$(
    docker_command run --rm --platform linux/amd64 \
      --entrypoint java "$SPARK_IMAGE" -version 2>&1
  )"; then
  ci_error "failed to run Java from Apache Spark image: $SPARK_IMAGE"
  exit 1
fi
if ! java_output_matches_major "$EXPECTED_RUNTIME_JAVA" "$CONTAINER_JAVA_OUTPUT"; then
  ci_error "Spark $SPARK_MINOR official image should use JDK $EXPECTED_RUNTIME_JAVA, but reports: $(first_line "$CONTAINER_JAVA_OUTPUT")"
  exit 1
fi
if ! CONTAINER_SPARK_OUTPUT="$(
    docker_command run --rm --platform linux/amd64 \
      --entrypoint /opt/spark/bin/spark-submit "$SPARK_IMAGE" --version 2>&1
  )"; then
  ci_error "failed to run spark-submit from Apache Spark image: $SPARK_IMAGE"
  exit 1
fi
if ! spark_output_matches_version "$SPARK_FULL_VERSION" "$CONTAINER_SPARK_OUTPUT"; then
  ci_error "Apache Spark image does not report expected version $SPARK_FULL_VERSION"
  exit 1
fi
printf '%s\n' "$CONTAINER_JAVA_OUTPUT" > "$SMOKE_LOG_DIR/spark-image-java.txt"
printf '%s\n' "$CONTAINER_SPARK_OUTPUT" > "$SMOKE_LOG_DIR/spark-image-version.txt"
printf 'Verified Apache Spark %s image with JDK %s\n' \
  "$SPARK_FULL_VERSION" "$EXPECTED_RUNTIME_JAVA"
docker_command network create "$CLUSTER_NETWORK" \
  > "$SMOKE_LOG_DIR/cluster-network-id.txt"
NETWORK_CREATED=true

printf 'Starting StarRocks image %s\n' "$STARROCKS_IMAGE"
if docker_command image inspect "$STARROCKS_IMAGE" >/dev/null 2>&1; then
  printf 'Using cached StarRocks image %s\n' "$STARROCKS_IMAGE" \
    | tee "$SMOKE_LOG_DIR/starrocks-pull.log"
else
  retry_command 3 5 docker_command pull "$STARROCKS_IMAGE" \
    | tee "$SMOKE_LOG_DIR/starrocks-pull.log"
fi
docker_command run --detach --rm --platform linux/amd64 \
  --network "$CLUSTER_NETWORK" \
  --network-alias starrocks \
  --hostname starrocks \
  --publish "${STARROCKS_FE_HTTP_HOST_PORT}:${STARROCKS_FE_HTTP_PORT}" \
  --publish "${STARROCKS_FE_JDBC_HOST_PORT}:${STARROCKS_FE_JDBC_PORT}" \
  --publish "${STARROCKS_BE_HTTP_HOST_PORT}:${STARROCKS_BE_HTTP_PORT}" \
  --publish "${STARROCKS_BE_THRIFT_HOST_PORT}:${STARROCKS_BE_THRIFT_PORT}" \
  --publish "${STARROCKS_BE_BRPC_HOST_PORT}:${STARROCKS_BE_BRPC_PORT}" \
  --volume "$SCRIPT_DIR/start-starrocks.sh:/opt/starrocks-ci/start-starrocks.sh:ro" \
  --entrypoint /bin/bash \
  --name "$STARROCKS_CONTAINER" "$STARROCKS_IMAGE" \
  /opt/starrocks-ci/start-starrocks.sh \
  > "$SMOKE_LOG_DIR/starrocks-container-id.txt"

STARROCKS_DEADLINE=$((SECONDS + STARROCKS_STARTUP_TIMEOUT_SECONDS))
while [ "$SECONDS" -lt "$STARROCKS_DEADLINE" ]; do
  http_ready=false
  backend_ready=false
  http_code="$(curl --silent --show-error --max-time 5 \
      "http://127.0.0.1:${STARROCKS_FE_HTTP_HOST_PORT}/" \
      --output "$SMOKE_LOG_DIR/starrocks-fe-http.txt" --write-out '%{http_code}' \
      2>> "$SMOKE_LOG_DIR/readiness.log" || true)"
  if http_response_is_ready "$http_code"; then
    http_ready=true
  fi
  if docker_command exec "$STARROCKS_CONTAINER" mysql --batch \
      -h127.0.0.1 -P"$STARROCKS_FE_JDBC_PORT" -uroot \
      -e 'SHOW BACKENDS' > "$SMOKE_LOG_DIR/starrocks-backends.tsv" \
      2>> "$SMOKE_LOG_DIR/readiness.log" \
      && starrocks_backend_is_alive "$SMOKE_LOG_DIR/starrocks-backends.tsv"; then
    backend_ready=true
  fi
  if [ "$http_ready" = true ] && [ "$backend_ready" = true ]; then
    break
  fi
  sleep 2
done
if [ "${http_ready:-false}" != true ] || [ "${backend_ready:-false}" != true ]; then
  ci_error "StarRocks did not become ready within ${STARROCKS_STARTUP_TIMEOUT_SECONDS}s"
  exit 1
fi
printf 'StarRocks FE and BE are ready\n' | tee -a "$SMOKE_LOG_DIR/readiness.log"

declare -a SPARK_MASTER_ARGS=() SPARK_WORKER_ARGS=() SPARK_DRIVER_ARGS=()
spark_master_container_args \
  SPARK_MASTER_ARGS "$SPARK_MASTER_CONTAINER" "$CLUSTER_NETWORK" \
  "$SPARK_IMAGE" "$SPARK_MASTER_UI_PORT"
docker_command "${SPARK_MASTER_ARGS[@]}" | tee "$SMOKE_LOG_DIR/spark-master-id.txt"

spark_worker_container_args \
  SPARK_WORKER_ARGS "$SPARK_WORKER_CONTAINER" "$CLUSTER_NETWORK" \
  "$SPARK_IMAGE" "$SMOKE_LOG_DIR/shared-work" \
  "$SMOKE_LOG_DIR/spark-work"
docker_command "${SPARK_WORKER_ARGS[@]}" | tee "$SMOKE_LOG_DIR/spark-worker-id.txt"

SPARK_DEADLINE=$((SECONDS + SPARK_STARTUP_TIMEOUT_SECONDS))
while [ "$SECONDS" -lt "$SPARK_DEADLINE" ]; do
  if curl --fail --silent --show-error --max-time 5 \
      "http://127.0.0.1:${SPARK_MASTER_UI_PORT}/json/" \
      > "$SMOKE_LOG_DIR/spark-master-status.json" \
      2>> "$SMOKE_LOG_DIR/readiness.log" \
      && spark_worker_is_alive "$SMOKE_LOG_DIR/spark-master-status.json"; then
    break
  fi
  sleep 2
done
if ! spark_worker_is_alive "$SMOKE_LOG_DIR/spark-master-status.json"; then
  ci_error "Spark worker did not register within ${SPARK_STARTUP_TIMEOUT_SECONDS}s"
  exit 1
fi
printf 'Spark standalone worker is ALIVE\n' | tee -a "$SMOKE_LOG_DIR/readiness.log"

RUN_ID="${GITHUB_RUN_ID:-$$}_${GITHUB_RUN_ATTEMPT:-1}"
DATABASE="ci_spark_${SPARK_MINOR//./_}_${RUN_ID}"
APPLICATION_LOG="$SMOKE_LOG_DIR/application.log"

spark_driver_container_args \
  SPARK_DRIVER_ARGS "$SPARK_DRIVER_CONTAINER" "$CLUSTER_NETWORK" \
  "$SPARK_IMAGE" "$DRIVER_ARTIFACT_DIR" \
  "$SMOKE_LOG_DIR/shared-work" "$SMOKE_LOG_DIR/spark-events"
SPARK_DRIVER_ARGS+=(
  --master spark://spark-master:7077
  --deploy-mode client
  --class com.starrocks.connector.spark.ci.ConnectorDocsSmoke
  --conf spark.driver.host=spark-driver
  --conf spark.driver.bindAddress=0.0.0.0
  --conf spark.cores.max=2
  --conf spark.executor.cores=2
  --conf spark.executor.memory=512m
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///opt/spark-events
  --jars "/opt/spark-driver/$CONNECTOR_BASENAME,/opt/spark-driver/$MYSQL_BASENAME"
  "/opt/spark-driver/$SMOKE_BASENAME"
  "$DATABASE"
  /opt/spark-shared
  starrocks:8030
  jdbc:mysql://starrocks:9030
  "$CONNECTOR_BASENAME"
)

set +e
timeout "$SMOKE_TIMEOUT_SECONDS" \
  "${DOCKER_COMMAND_PARTS[@]}" "${SPARK_DRIVER_ARGS[@]}" \
  2>&1 | tee "$APPLICATION_LOG"
SUBMIT_STATUS=${PIPESTATUS[0]}
set -e

if [ "$SUBMIT_STATUS" -ne 0 ]; then
  ci_error "Spark $SPARK_MINOR smoke application failed with status $SUBMIT_STATUS"
  exit "$SUBMIT_STATUS"
fi
if ! executor_evidence_uses_host "$APPLICATION_LOG" spark-worker; then
  ci_error "distributed execution did not run exclusively in the Spark worker container"
  exit 1
fi

CHECK_COUNT="$(grep -Ec '^CHECK [a-z_]+ PASS ' "$APPLICATION_LOG" || true)"
if [ "$CHECK_COUNT" -ne 8 ]; then
  ci_error "Spark $SPARK_MINOR smoke application reported $CHECK_COUNT checks, expected 8"
  exit 1
fi
if ! grep -Fq "VALIDATION_RESULT PASS spark=$SPARK_FULL_VERSION database=$DATABASE" "$APPLICATION_LOG"; then
  ci_error "Spark $SPARK_MINOR smoke application did not report the expected final PASS record"
  exit 1
fi

{
  printf 'spark_minor=%s\n' "$SPARK_MINOR"
  printf 'spark_version=%s\n' "$SPARK_FULL_VERSION"
  printf 'scala_binary=%s\n' "$SCALA_BINARY"
  printf 'build_java=%s\n' "$(first_line "$JAVA_VERSION_OUTPUT")"
  printf 'runtime_java=%s\n' "$(first_line "$CONTAINER_JAVA_OUTPUT")"
  printf 'connector=%s\n' "$CONNECTOR_JAR"
  printf 'connector_sha256=%s\n' "$(awk '{print $1}' "$SMOKE_LOG_DIR/connector.sha256")"
  printf 'spark_image=%s\n' "$SPARK_IMAGE"
  printf 'spark_topology=containerized-master-worker-driver\n'
  printf 'starrocks_image=%s\n' "$STARROCKS_IMAGE"
  printf 'checks=%s\n' "$CHECK_COUNT"
  printf 'result=PASS\n'
} > "$SMOKE_LOG_DIR/summary.txt"

cat "$SMOKE_LOG_DIR/summary.txt"

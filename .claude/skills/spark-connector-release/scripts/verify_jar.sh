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

JAR="${1:-}"
EXPECTED_COMMIT="${2:-}"
EXPECTED_VERSION="${3:-}"
SPARK_MINOR="${4:-}"
SCALA_BINARY="${5:-}"
SDK_VERSION="${6:-}"

[ "$#" -eq 6 ] \
  || die "usage: verify_jar.sh <jar> <commit> <version> <spark-minor> <scala-binary> <sdk-version>"
[ -f "$JAR" ] || die "JAR not found: $JAR"
[[ "$EXPECTED_COMMIT" =~ ^[0-9a-fA-F]{40}$ ]] || die "expected commit must be a full 40-character Git SHA"

require_command unzip

ARTIFACT="$(artifact_id "$SPARK_MINOR" "$SCALA_BINARY")"
EXPECTED_FILE="$(artifact_filename "$EXPECTED_VERSION" "$SPARK_MINOR" "$SCALA_BINARY")"
POM_PROPERTIES="META-INF/maven/com.starrocks/$ARTIFACT/pom.properties"
CONNECTOR_PROPERTIES="starrocks-spark-connector-git.properties"
SDK_POM_PROPERTIES="META-INF/maven/com.starrocks/starrocks-stream-load-sdk/pom.properties"
SDK_GIT_PROPERTIES="stream-load-sdk-git.properties"

ok=0
bad=0
yes() { pass "$1"; ok=$((ok + 1)); }
no()  { fail "$1"; bad=$((bad + 1)); }

case "$(basename "$JAR")" in
  original-*|*-sources.jar|*-javadoc.jar)
    die "$(basename "$JAR") is not the primary shaded connector JAR"
    ;;
esac

info "Verifying $(basename "$JAR") for Spark $SPARK_MINOR / Scala $SCALA_BINARY"
if [ "$(basename "$JAR")" = "$EXPECTED_FILE" ]; then
  yes "filename matches $EXPECTED_FILE"
else
  no "filename $(basename "$JAR") != expected $EXPECTED_FILE"
fi

LIST="$(unzip -Z1 "$JAR" 2>/dev/null)" || die "cannot read ZIP directory from $JAR"

pom="$(unzip -p "$JAR" "$POM_PROPERTIES" 2>/dev/null || true)"
if [ -n "$pom" ]; then
  yes "$POM_PROPERTIES is present"
  pom_group="$(printf '%s\n' "$pom" | property_value groupId)"
  pom_artifact="$(printf '%s\n' "$pom" | property_value artifactId)"
  pom_version="$(printf '%s\n' "$pom" | property_value version)"
  [ "$pom_group" = com.starrocks ] && yes "groupId is com.starrocks" || no "groupId is ${pom_group:-<missing>}"
  [ "$pom_artifact" = "$ARTIFACT" ] && yes "artifactId is $ARTIFACT" || no "artifactId is ${pom_artifact:-<missing>}, expected $ARTIFACT"
  [ "$pom_version" = "$EXPECTED_VERSION" ] && yes "artifact version is $EXPECTED_VERSION" || no "artifact version is ${pom_version:-<missing>}, expected $EXPECTED_VERSION"
  [[ "$pom_version" != *SNAPSHOT* ]] && yes "artifact version is not a snapshot" || no "artifact version contains SNAPSHOT"
else
  no "$POM_PROPERTIES is missing"
fi

connector="$(unzip -p "$JAR" "$CONNECTOR_PROPERTIES" 2>/dev/null || true)"
if [ -n "$connector" ]; then
  yes "$CONNECTOR_PROPERTIES is present"
  connector_version="$(printf '%s\n' "$connector" | property_value git.build.version)"
  connector_commit="$(printf '%s\n' "$connector" | property_value git.commit.id)"
  [ "$connector_version" = "$EXPECTED_VERSION" ] \
    && yes "embedded connector version is $EXPECTED_VERSION" \
    || no "embedded connector version is ${connector_version:-<missing>}, expected $EXPECTED_VERSION"
  [[ "$connector_version" != *SNAPSHOT* ]] \
    && yes "embedded connector version is not a snapshot" \
    || no "embedded connector version contains SNAPSHOT"
  [ "$connector_commit" = "$EXPECTED_COMMIT" ] \
    && yes "embedded connector commit matches the release tag" \
    || no "embedded connector commit is ${connector_commit:-<missing>}, expected $EXPECTED_COMMIT"
else
  no "$CONNECTOR_PROPERTIES is missing; do not release from a linked worktree"
fi

sdk_pom="$(unzip -p "$JAR" "$SDK_POM_PROPERTIES" 2>/dev/null || true)"
if [ -n "$sdk_pom" ]; then
  yes "$SDK_POM_PROPERTIES is present"
  bundled_sdk_version="$(printf '%s\n' "$sdk_pom" | property_value version)"
  [ "$bundled_sdk_version" = "$SDK_VERSION" ] \
    && yes "bundled Stream Load SDK version is $SDK_VERSION" \
    || no "bundled Stream Load SDK version is ${bundled_sdk_version:-<missing>}, expected $SDK_VERSION"
else
  no "$SDK_POM_PROPERTIES is missing"
fi

sdk_git="$(unzip -p "$JAR" "$SDK_GIT_PROPERTIES" 2>/dev/null || true)"
if [ -n "$sdk_git" ]; then
  yes "$SDK_GIT_PROPERTIES is present"
  sdk_git_version="$(printf '%s\n' "$sdk_git" | property_value git.build.version)"
  [ "$sdk_git_version" = "$SDK_VERSION" ] \
    && yes "embedded Stream Load SDK Git version is $SDK_VERSION" \
    || no "embedded Stream Load SDK Git version is ${sdk_git_version:-<missing>}, expected $SDK_VERSION"
  [[ "$sdk_git_version" != *SNAPSHOT* ]] \
    && yes "embedded Stream Load SDK Git version is not a snapshot" \
    || no "embedded Stream Load SDK Git version contains SNAPSHOT"
else
  no "$SDK_GIT_PROPERTIES is missing"
fi

echo
if [ "$bad" -eq 0 ]; then
  info "${C_GREEN}ALL $ok CHECKS PASSED${C_RESET} — $(basename "$JAR")"
else
  die "$bad check(s) failed, $ok passed — DO NOT PUBLISH $(basename "$JAR")"
fi

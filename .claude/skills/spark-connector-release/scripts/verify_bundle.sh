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

BUNDLE="${1:-}"
LOCAL_JAR="${2:-}"
EXPECTED_COMMIT="${3:-}"
EXPECTED_VERSION="${4:-}"
SPARK_MINOR="${5:-}"
SCALA_BINARY="${6:-}"
SDK_VERSION="${7:-}"
[ "$#" -eq 7 ] \
  || die "usage: verify_bundle.sh <bundle.zip> <local-jar> <commit> <version> <spark-minor> <scala-binary> <sdk-version>"
[ -s "$BUNDLE" ] || die "Central bundle not found: $BUNDLE"
[ -s "$LOCAL_JAR" ] || die "local primary JAR not found: $LOCAL_JAR"

for command in unzip gpg md5sum sha1sum sha256sum sha512sum; do
  require_command "$command"
done

ARTIFACT="$(artifact_id "$SPARK_MINOR" "$SCALA_BINARY")"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
unzip -q "$BUNDLE" -d "$WORK"
BASE="$WORK/com/starrocks/$ARTIFACT/$EXPECTED_VERSION/$ARTIFACT-$EXPECTED_VERSION"

for file in "$BASE.pom" "$BASE.jar" "$BASE-sources.jar" "$BASE-javadoc.jar"; do
  [ -s "$file" ] || die "Central bundle is missing ${file#"$WORK/"}"
  [ -s "$file.asc" ] || die "Central bundle is missing signature ${file#"$WORK/"}.asc"
  gpg --batch --verify "$file.asc" "$file" >/dev/null 2>&1 \
    || die "GPG verification failed for ${file#"$WORK/"}"
  for algorithm in md5 sha1 sha256 sha512; do
    checksum_file="$file.$algorithm"
    [ -s "$checksum_file" ] || die "Central bundle is missing ${checksum_file#"$WORK/"}"
    expected="$(tr -d '[:space:]' < "$checksum_file")"
    actual="$("${algorithm}sum" "$file" | awk '{print $1}')"
    [ "$actual" = "$expected" ] \
      || die "$algorithm checksum mismatch for ${file#"$WORK/"}"
  done
done

grep -q '<version>.*SNAPSHOT.*</version>' "$BASE.pom" \
  && die "flattened release POM still contains a project SNAPSHOT version"
cmp -s "$LOCAL_JAR" "$BASE.jar" \
  || die "primary JAR in Central bundle differs from target/ primary JAR"

"$SCRIPT_DIR/verify_jar.sh" \
  "$BASE.jar" "$EXPECTED_COMMIT" "$EXPECTED_VERSION" "$SPARK_MINOR" \
  "$SCALA_BINARY" "$SDK_VERSION"
pass "Central bundle contains signed primary, sources, Javadocs, POM, and checksums"

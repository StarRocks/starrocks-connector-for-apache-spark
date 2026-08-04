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
[ -n "$VERSION" ] || die "usage: verify_central.sh <version> [spark-minor ...]"
validate_release_version "$VERSION"
shift

REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"
assert_primary_checkout "$REPO_ROOT"
[ "$(pom_connector_version "$REPO_ROOT")" = "$VERSION" ] \
  || die "current pom version does not equal $VERSION; checkout tag v$VERSION"
verify_head_is_tag "$REPO_ROOT" "$VERSION"
COMMIT="$(git rev-parse HEAD)"
verify_tag_on_origin "$REPO_ROOT" "$VERSION" "$COMMIT"
pass "origin tag v$VERSION points to $COMMIT"
SDK_VERSION="$(stream_load_sdk_version "$REPO_ROOT")"
require_command curl
require_command gpg

mapfile -t versions < <(resolve_versions "$REPO_ROOT" "$@")
[ "${#versions[@]}" -gt 0 ] || die "no Spark versions selected"

STATE_DIR="$(release_state_dir "$REPO_ROOT" "$VERSION")"
DOWNLOAD_DIR="$STATE_DIR/central"
mkdir -p "$DOWNLOAD_DIR"

declare -a results=()
overall=0

download_file() {
  local url="$1" destination="$2" attempt
  for attempt in 1 2 3 4 5 6; do
    if curl -fsSL --connect-timeout 20 --max-time 180 -o "$destination" "$url"; then
      return
    fi
    warn "download attempt $attempt failed; Central may still be synchronizing: $url"
    [ "$attempt" -eq 6 ] || sleep 15
  done
  return 1
}

for spark in "${versions[@]}"; do
  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  java="$(spark_java_major "$REPO_ROOT" "$spark")"
  artifact="$(artifact_id "$spark" "$scala")"
  filename="$(artifact_filename "$VERSION" "$spark" "$scala")"
  destination="$DOWNLOAD_DIR/$filename"
  url="$(central_artifact_url "$VERSION" "$spark" "$scala")"
  base_url="${url%.jar}"

  info "Downloading published Spark $spark artifacts and signatures"
  downloaded=1
  for suffix in .pom .jar -sources.jar -javadoc.jar; do
    published_file="$DOWNLOAD_DIR/$artifact-$VERSION$suffix"
    if ! download_file "$base_url$suffix" "$published_file" \
        || ! download_file "$base_url$suffix.asc" "$published_file.asc"; then
      downloaded=0
      break
    fi
    if ! gpg --batch --verify "$published_file.asc" "$published_file" >/dev/null 2>&1; then
      fail "GPG verification failed for $(basename "$published_file")"
      downloaded=0
      break
    fi
  done
  if [ "$downloaded" -ne 1 ]; then
    fail "could not verify the complete published artifact set for Spark $spark"
    results+=("Spark $spark FAIL(download)")
    overall=1
    continue
  fi
  grep -q '<version>.*SNAPSHOT.*</version>' "$DOWNLOAD_DIR/$artifact-$VERSION.pom" \
    && die "published POM for Spark $spark contains a project SNAPSHOT version"

  if "$SCRIPT_DIR/verify_jar.sh" \
      "$destination" "$COMMIT" "$VERSION" "$spark" "$scala" "$java" "$SDK_VERSION"; then
    sha="$(sha256sum "$destination" | awk '{print $1}')"
    {
      printf 'commit=%s\n' "$COMMIT"
      printf 'version=%s\n' "$VERSION"
      printf 'spark=%s\n' "$spark"
      printf 'artifact=%s\n' "$filename"
      printf 'sha256=%s\n' "$sha"
    } > "$STATE_DIR/central-verified-$spark.env"
    results+=("Spark $spark OK")
  else
    results+=("Spark $spark FAIL(verify)")
    overall=1
  fi
done

echo
info "Maven Central verification summary"
for result in "${results[@]}"; do printf '  %s\n' "$result"; done

if [ "$overall" -ne 0 ]; then
  die "one or more published artifacts failed verification"
fi
info "${C_GREEN}PUBLISHED ARTIFACTS VERIFIED${C_RESET}: ${versions[*]}"

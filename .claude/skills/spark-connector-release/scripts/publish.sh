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
assert_official_origin "$REPO_ROOT"
CENTRAL_CURL_CONFIG=''
trap '[ -z "$CENTRAL_CURL_CONFIG" ] || rm -f "$CENTRAL_CURL_CONFIG"' EXIT
for command in curl jq base64 sha256sum; do require_command "$command"; done

VERSION="$(pom_connector_version "$REPO_ROOT")"
[ -n "$VERSION" ] || die "cannot determine connector version"
pom_is_snapshot "$REPO_ROOT" && die "pom.xml is still a SNAPSHOT"
verify_head_is_tag "$REPO_ROOT" "$VERSION"
COMMIT="$(git rev-parse HEAD)"
verify_tag_on_origin "$REPO_ROOT" "$VERSION" "$COMMIT"
pass "origin tag v$VERSION points to $COMMIT"

mapfile -t versions < <(resolve_versions "$REPO_ROOT" "$@")
[ "${#versions[@]}" -gt 0 ] || die "no Spark versions selected"

STATE_DIR="$(release_state_dir "$REPO_ROOT" "$VERSION")"
MARKER="$STATE_DIR/verified.env"
[ -f "$MARKER" ] || die "no local verification marker; run build_verify.sh first"
[ "$(sed -n 's/^commit=//p' "$MARKER")" = "$COMMIT" ] \
  || die "verification marker does not match current commit $COMMIT"
[ "$(sed -n 's/^version=//p' "$MARKER")" = "$VERSION" ] \
  || die "verification marker does not match connector version $VERSION"
verified="$(sed -n 's/^versions=//p' "$MARKER")"
SDK_VERSION="$(stream_load_sdk_version "$REPO_ROOT")"

for spark in "${versions[@]}"; do
  case " $verified " in
    *" $spark "*) ;;
    *) die "Spark $spark was not verified by build_verify.sh (verified: ${verified:-none})" ;;
  esac
  [ ! -f "$STATE_DIR/published-$spark.env" ] \
    || die "local state says Spark $spark was already published; never redeploy the same coordinates"
  [ ! -f "$STATE_DIR/publishing-$spark.env" ] \
    || die "a previous Spark $spark publication was interrupted; inspect Central Portal, then remove the in-flight marker only after proving the coordinate was not uploaded"

  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  java="$(spark_java_major "$REPO_ROOT" "$spark")"
  filename="$(artifact_filename "$VERSION" "$spark" "$scala")"
  jar="$STATE_DIR/artifacts/$filename"
  bundle="$STATE_DIR/bundles/central-bundle-spark-$spark.zip"
  meta="$STATE_DIR/verified-$spark.env"
  [ -f "$jar" ] && [ -f "$bundle" ] && [ -f "$meta" ] \
    || die "verified artifact state is incomplete for Spark $spark"
  [ "$(sed -n 's/^commit=//p' "$meta")" = "$COMMIT" ] \
    || die "verification metadata for Spark $spark belongs to another commit"
  [ "$(sed -n 's/^spark=//p' "$meta")" = "$spark" ] \
    || die "verification metadata has the wrong Spark version for $spark"
  [ "$(sed -n 's/^scala=//p' "$meta")" = "$scala" ] \
    || die "verification metadata has the wrong Scala version for Spark $spark"
  [ "$(sed -n 's/^java=//p' "$meta")" = "$java" ] \
    || die "verification metadata has the wrong Java version for Spark $spark"
  [ "$(sed -n 's/^artifact=//p' "$meta")" = "$filename" ] \
    || die "verification metadata has the wrong artifact for Spark $spark"
  [ "$(sed -n 's/^bundle=//p' "$meta")" = "$(basename "$bundle")" ] \
    || die "verification metadata has the wrong Central bundle for Spark $spark"
  expected_sha="$(sed -n 's/^sha256=//p' "$meta")"
  actual_sha="$(sha256sum "$jar" | awk '{print $1}')"
  [ "$actual_sha" = "$expected_sha" ] || die "$filename changed after verification"
  expected_bundle_sha="$(sed -n 's/^bundle_sha256=//p' "$meta")"
  actual_bundle_sha="$(sha256sum "$bundle" | awk '{print $1}')"
  [ "$actual_bundle_sha" = "$expected_bundle_sha" ] \
    || die "Central bundle for Spark $spark changed after verification"
  "$SCRIPT_DIR/verify_bundle.sh" \
    "$bundle" "$jar" "$COMMIT" "$VERSION" "$spark" "$scala" "$SDK_VERSION" >/dev/null
  pass "verified signed Central bundle is intact for Spark $spark"

  url="$(central_artifact_url "$VERSION" "$spark" "$scala")"
  status="$(curl -sS --connect-timeout 15 --max-time 30 -o /dev/null -w '%{http_code}' "$url" || true)"
  case "$status" in
    404) pass "coordinates are not present on Maven Central for Spark $spark" ;;
    200) die "$url already exists; Maven Central releases are immutable" ;;
    000|'') die "could not check Maven Central for existing Spark $spark coordinates" ;;
    *) die "unexpected Maven Central response $status while checking $url" ;;
  esac
done

echo
warn "This will publish immutable artifacts through Central Publisher Portal:"
for spark in "${versions[@]}"; do
  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  printf '  com.starrocks:%s:%s\n' "$(artifact_id "$spark" "$scala")" "$VERSION"
done
if [ -t 0 ]; then
  printf 'Type the release version "%s" to publish: ' "$VERSION"
  read -r answer
  [ "$answer" = "$VERSION" ] || die "confirmation mismatch; publication aborted"
else
  [ "${CONFIRM_PUBLISH:-}" = "$VERSION" ] \
    || die "non-interactive publication requires CONFIRM_PUBLISH=$VERSION"
fi

CENTRAL_CURL_CONFIG="$(mktemp /tmp/spark-connector-central-curl.XXXXXX)"
write_central_curl_config "$CENTRAL_CURL_CONFIG"
pass "Central Portal API credentials are loaded without logging them"

declare -a results=()
for spark in "${versions[@]}"; do
  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  artifact="$(artifact_id "$spark" "$scala")"
  bundle="$STATE_DIR/bundles/central-bundle-spark-$spark.zip"
  bundle_sha="$(sha256sum "$bundle" | awk '{print $1}')"
  in_flight_marker="$STATE_DIR/publishing-$spark.env"
  info "Publishing the verified Central bundle for Spark $spark"
  {
    printf 'commit=%s\n' "$COMMIT"
    printf 'version=%s\n' "$VERSION"
    printf 'spark=%s\n' "$spark"
    printf 'bundle=%s\n' "$(basename "$bundle")"
    printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$in_flight_marker"
  publish_verified_bundle \
    "$bundle" "com.starrocks:$artifact:$VERSION" "$CENTRAL_CURL_CONFIG" "$in_flight_marker"
  deployment_id="$(sed -n 's/^deployment_id=//p' "$in_flight_marker")"
  {
    printf 'commit=%s\n' "$COMMIT"
    printf 'version=%s\n' "$VERSION"
    printf 'spark=%s\n' "$spark"
    printf 'bundle=%s\n' "$(basename "$bundle")"
    printf 'bundle_sha256=%s\n' "$bundle_sha"
    printf 'deployment_id=%s\n' "$deployment_id"
    printf 'published_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$STATE_DIR/published-$spark.env"
  rm -f "$in_flight_marker"
  results+=("Spark $spark PUBLISHED")
  pass "published Spark $spark from Central deployment $deployment_id"
done

echo
info "${C_GREEN}PUBLICATION COMPLETE${C_RESET}"
for result in "${results[@]}"; do printf '  %s\n' "$result"; done
printf 'Next run: scripts/verify_central.sh %s\n' "$VERSION"

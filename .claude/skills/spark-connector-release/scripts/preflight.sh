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

CENTRAL_CREDENTIAL_PROBE=''
trap '[ -z "$CENTRAL_CREDENTIAL_PROBE" ] || rm -f "$CENTRAL_CREDENTIAL_PROBE"' EXIT

REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"

info "Checking Spark connector release environment"
assert_primary_checkout "$REPO_ROOT"
pass "repository is a primary checkout"
assert_clean_checkout "$REPO_ROOT"
pass "working tree is clean"

for command in git mvn gpg curl jq base64 unzip zip sha256sum gh; do
  require_command "$command"
  pass "$command is available"
done

origin_url="$(git remote get-url origin 2>/dev/null || true)"
[[ "$origin_url" == *StarRocks/starrocks-connector-for-apache-spark* ]] \
  || die "origin is not the StarRocks Spark connector repository: ${origin_url:-<missing>}"
pass "origin points to StarRocks/starrocks-connector-for-apache-spark"

mapfile -t versions < <(supported_spark_versions "$REPO_ROOT")
[ "${#versions[@]}" -gt 0 ] || die "common.sh contains no supported Spark versions"
pass "common.sh supports: ${versions[*]}"

for spark in "${versions[@]}"; do
  full="$(spark_full_version "$REPO_ROOT" "$spark")"
  [[ "$full" == "$spark".* ]] || die "Spark $spark profile resolves full version $full"
  scala="$(spark_scala_binary "$REPO_ROOT" "$spark")"
  java="$(spark_java_major "$REPO_ROOT" "$spark")"
  java_home="$(find_java_home "$java")"
  [ "$(java_major_at_home "$java_home")" = "$java" ] \
    || die "detected Java home $java_home does not provide JDK $java"
  pass "Spark $spark ($full) -> Scala $scala, JDK $java ($java_home)"
done

sdk="$(stream_load_sdk_version "$REPO_ROOT")"
[ -n "$sdk" ] || die "cannot determine starrocks-stream-load-sdk version from pom.xml"
[[ "$sdk" != *SNAPSHOT* ]] || die "Stream Load SDK dependency must be a released version, found $sdk"
pass "Stream Load SDK dependency is release $sdk"

grep -Fq '<artifactId>central-publishing-maven-plugin</artifactId>' pom.xml \
  || die "Central Publisher Portal Maven plugin is not configured"
grep -Fq '<groupId>org.sonatype.central</groupId>' pom.xml \
  || die "Central Publisher Portal plugin groupId must be org.sonatype.central"
grep -Fq '<extensions>true</extensions>' pom.xml \
  || die "Central Publisher Portal Maven extension must be enabled"
grep -Fq '<publishingServerId>central</publishingServerId>' pom.xml \
  || die "Central publishingServerId must be central"
grep -Fq '<autoPublish>true</autoPublish>' pom.xml \
  || die "Central plugin must auto-publish successful validated deployments"
grep -Fq '<waitUntil>published</waitUntil>' pom.xml \
  || die "Central plugin must wait until publication completes before deploy.sh succeeds"
assert_central_dry_run_contract "$REPO_ROOT"
pass "Central Publisher Portal plugin 0.8.0 and its no-upload rehearsal contract are configured"

grep -Fq '<arg>--pinentry-mode</arg>' pom.xml \
  || die "release signing is missing GPG loopback pinentry configuration"
grep -Fq '<arg>loopback</arg>' pom.xml \
  || die "release signing is missing GPG loopback pinentry configuration"
pass "GPG loopback signing is configured"

CENTRAL_CREDENTIAL_PROBE="$(mktemp /tmp/spark-connector-central-auth.XXXXXX)"
write_central_curl_config "$CENTRAL_CREDENTIAL_PROBE"
rm -f "$CENTRAL_CREDENTIAL_PROBE"
CENTRAL_CREDENTIAL_PROBE=''
pass "Central Portal API credentials are available"

secret_keys="$(gpg --batch --list-secret-keys --with-colons 2>/dev/null || true)"
grep -q '^sec:' <<< "$secret_keys" \
  || die "no GPG secret key is available for Maven signing"
pass "a GPG secret key is available"

gh auth status >/dev/null 2>&1 || die "GitHub CLI is not authenticated; run gh auth login"
pass "GitHub CLI authentication is available"

echo
info "${C_GREEN}PREFLIGHT PASSED${C_RESET}"
printf 'Connector version in pom.xml: %s\n' "$(pom_connector_version "$REPO_ROOT")"
printf 'Supported Spark versions: %s\n' "${versions[*]}"

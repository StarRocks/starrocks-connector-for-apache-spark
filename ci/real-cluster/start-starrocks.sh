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

STARROCKS_HOME="${SR_HOME:-/data/deploy/starrocks}"
IMAGE_ENTRYPOINT="/data/deploy/entrypoint.sh"
DIRECTOR_SCRIPT="$STARROCKS_HOME/director/run.sh"
container_ip="$(hostname -i | awk '{print $1}')"

if [ -z "$container_ip" ]; then
  printf 'Unable to resolve the StarRocks container bridge address\n' >&2
  exit 1
fi

# The all-in-one image deliberately binds FE and BE to loopback. That makes
# redirects unusable from an external Spark executor, so register both services
# on the container's bridge address for this real-cluster test.
sed -i \
  "s|priority_networks = 127.0.0.1/32|priority_networks = ${container_ip}/32|g" \
  "$IMAGE_ENTRYPOINT"
sed -i \
  "s|^MYHOST=127.0.0.1$|MYHOST=${container_ip}|" \
  "$DIRECTOR_SCRIPT"

exec "$IMAGE_ENTRYPOINT"

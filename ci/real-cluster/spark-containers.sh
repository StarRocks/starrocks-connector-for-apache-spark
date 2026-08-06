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

spark_container_common_args() {
  local output_name="$1" container_name="$2" hostname="$3" network="$4"
  local -n output="$output_name"

  output=(
    run
    --init
    --platform linux/amd64
    --network "$network"
    --name "$container_name"
    --hostname "$hostname"
    --network-alias "$hostname"
  )
}

spark_master_container_args() {
  local output_name="$1" container_name="$2" network="$3" image="$4"
  local web_ui_host_port="$5"
  local -n output="$output_name"

  spark_container_common_args \
    "$output_name" "$container_name" spark-master "$network"
  output+=(
    --publish "127.0.0.1:${web_ui_host_port}:8080"
    --detach
    "$image"
    /opt/spark/bin/spark-class
    org.apache.spark.deploy.master.Master
    --host spark-master
    --port 7077
    --webui-port 8080
  )
}

spark_worker_container_args() {
  local output_name="$1" container_name="$2" network="$3" image="$4"
  local shared_work="$5" worker_work="$6"
  local -n output="$output_name"

  spark_container_common_args \
    "$output_name" "$container_name" spark-worker "$network"
  output+=(
    --volume "$shared_work:/opt/spark-shared"
    --volume "$worker_work:/opt/spark-work"
    --env SPARK_WORKER_DIR=/opt/spark-work
    --detach
    "$image"
    /opt/spark/bin/spark-class
    org.apache.spark.deploy.worker.Worker
    --host spark-worker
    --port 7078
    --webui-port 8081
    --cores 2
    --memory 1g
    spark://spark-master:7077
  )
}

spark_driver_container_args() {
  local output_name="$1" container_name="$2" network="$3" image="$4"
  local artifacts="$5" shared_work="$6" events="$7"
  local -n output="$output_name"

  spark_container_common_args \
    "$output_name" "$container_name" spark-driver "$network"
  output+=(
    --volume "$artifacts:/opt/spark-driver:ro"
    --volume "$shared_work:/opt/spark-shared"
    --volume "$events:/opt/spark-events"
    "$image"
    /opt/spark/bin/spark-submit
  )
}

spark_diagnostics_permissions_container_args() {
  local output_name="$1" image="$2" diagnostics="$3"
  local -n output="$output_name"

  output=(
    run
    --rm
    --volume "$diagnostics:/diagnostics"
    --user 0:0
    --entrypoint chmod
    "$image"
    -R
    a+rX
    /diagnostics
  )
}

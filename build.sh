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

##############################################################
# This script is used to compile spark-starrocks-connector
# Usage:
#    sh build.sh
#
##############################################################

set -eo pipefail

# check maven
MVN_CMD=mvn
if [[ ! -z ${CUSTOM_MVN} ]]; then
    MVN_CMD=${CUSTOM_MVN}
fi
if ! ${MVN_CMD} --version; then
    echo "Error: mvn is not found"
    exit 1
fi
export MVN_CMD

if [ ! $1 ]
then
    echo "Usage: sh build.sh <spark_version>"
    echo "       spark version options: 2 or 3"
    exit 1
fi

if [ $1 == 2 ]; then
    export STARROCKS_SPARK_BASE_VERSION=2
    export STARROCKS_SPARK_VERSION=2.3.4
    export STARROCKS_SCALA_VERSION=2.11
elif [ $1 == 3 ]; then
    export STARROCKS_SPARK_BASE_VERSION=3
    export STARROCKS_SPARK_VERSION=3.1.2
    export STARROCKS_SCALA_VERSION=2.12
else
    echo "Error: spark version options: 2 or 3"
    exit 1
fi

${MVN_CMD} clean package

mkdir -p output/
cp target/starrocks-spark*.jar ./output/

echo "********************************************"
echo "Successfully build Spark StarRocks Connector"
echo "********************************************"

exit 0

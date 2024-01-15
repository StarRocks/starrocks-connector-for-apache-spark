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
# This script is used to compile Spark StarRocks Connector
# Usage:
#    sh build.sh <spark_version>
#    spark version options: 2 or 3
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

supported_spark_version=("3.1" "3.2" "3.3" "3.4" "3.5")
version_msg=$(IFS=, ; echo "${supported_spark_version[*]}")
if [ ! $1 ]
then
    echo "Usage:"
    echo "   sh build.sh <spark_version>"
    echo "   supported spark version: ${version_msg}"
    exit 1
fi

spark_version=$1
if [[ " ${supported_spark_version[*]} " == *" $spark_version "* ]];
then
    echo "Compiling connector for spark version $spark_version"
else
    echo "Error: only support spark version: ${version_msg}"
    exit 1
fi

${MVN_CMD} clean package -DskipTests -Pspark-${spark_version}

echo "*********************************************************************"
echo "Successfully build Spark StarRocks Connector for Spark $spark_version"
echo "You can find the connector jar under the \"target\" directory"
echo "*********************************************************************"

exit 0

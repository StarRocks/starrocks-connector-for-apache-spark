// Modifications Copyright 2021 StarRocks Limited.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql

import com.starrocks.connector.spark.rdd.{AbstractStarRocksRDD, AbstractStarRocksRDDIterator, ScalaValueReader, StarRocksPartition}
import com.starrocks.connector.spark.rest.PartitionDefinition
import com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.sql.Row

private[spark] class ScalaStarRocksRowRDD(
  sc: SparkContext,
  params: Map[String, String] = Map.empty)
  extends AbstractStarRocksRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaStarRocksRowRDDIterator = {
    new ScalaStarRocksRowRDDIterator(context, split.asInstanceOf[StarRocksPartition].starRocksPartition)
  }
}

private[spark] class ScalaStarRocksRowRDDIterator(
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractStarRocksRDDIterator[Row](context, partition) {

  override def newReader(config: ReadStarRocksConfig): ScalaValueReader = new ScalaStarRocksRowValueReader(partition, config)

  override def createValue(value: Object): Row = {
    value.asInstanceOf[ScalaStarRocksRow]
  }
}

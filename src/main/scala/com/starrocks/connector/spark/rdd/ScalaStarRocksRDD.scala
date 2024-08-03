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

package com.starrocks.connector.spark.rdd

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_VALUE_READER_CLASS
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rest.RpcPartition
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

private[spark] class ScalaStarRocksRDD[T: ClassTag](
    sc: SparkContext,
    params: Map[String, String] = Map.empty)
    extends AbstractStarRocksRDD[T](sc, params) {
  override def compute(split: Partition, context: TaskContext): ScalaStarRocksRDDIterator[T] = {
    new ScalaStarRocksRDDIterator(context, split.asInstanceOf[StarRocksPartition].starrocksPartition)
  }
}

private[spark] class ScalaStarRocksRDDIterator[T](
    context: TaskContext,
    partition: RpcPartition)
    extends AbstractStarRocksRDDIterator[T](context, partition, null) {

  override def initReader(settings: Settings) = {
    settings.setProperty(STARROCKS_VALUE_READER_CLASS, classOf[RpcValueReader].getName)
  }

  override def createValue(value: Object): T = {
    value.asInstanceOf[T]
  }
}

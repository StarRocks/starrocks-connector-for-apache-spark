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

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import com.starrocks.connector.spark.rest.{PartitionDefinition, RestService}
import com.starrocks.connector.spark.sql.conf.StarRocksConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext}

private[spark] abstract class AbstractStarRocksRDD[T: ClassTag](
    @transient private var sc: SparkContext,
    val params: Map[String, String] = Map.empty)
    extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    starRocksPartitions.zipWithIndex.map { case (starRocksPartition, idx) =>
      new StarRocksPartition(id, idx, starRocksPartition)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val starRocksSplit = split.asInstanceOf[StarRocksPartition]
    Seq(starRocksSplit.starRocksPartition.getBeAddress)
  }

  override def checkpoint(): Unit = {
    // Do nothing. StarRocks RDD should not be checkpointed.
  }

  /**
   * starrocks configuration get from rdd parameters and spark conf.
   */
  @transient private[spark] lazy val starRocksCfg = StarRocksConfig.createConfig(params)

  @transient private[spark] lazy val starRocksPartitions = {
    RestService.findPartitions(starRocksCfg, log)
  }
}

private[spark] class StarRocksPartition(rddId: Int, idx: Int, val starRocksPartition: PartitionDefinition)
    extends Partition {

  override def hashCode(): Int = 31 * (31 * (31 + rddId) + idx) + starRocksPartition.hashCode()

  override val index: Int = idx
}

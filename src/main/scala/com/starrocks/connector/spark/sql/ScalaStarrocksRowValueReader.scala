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

import scala.collection.JavaConverters._

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_READ_FIELD
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.exception.ShouldNeverHappenException
import com.starrocks.connector.spark.rdd.ScalaValueReader
import com.starrocks.connector.spark.rest.PartitionDefinition
import com.starrocks.connector.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE

import org.apache.spark.internal.Logging

class ScalaStarrocksRowValueReader(
  partition: PartitionDefinition,
  settings: Settings)
  extends ScalaValueReader(partition, settings) with Logging {

  val rowOrder: Seq[String] = settings.getProperty(STARROCKS_READ_FIELD).split(",")

  override def next: AnyRef = {
    if (!hasNext) {
      logError(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    val row: ScalaStarrocksRow = new ScalaStarrocksRow(rowOrder)
    rowBatch.next.asScala.zipWithIndex.foreach{
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }
}

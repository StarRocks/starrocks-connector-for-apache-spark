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

import com.starrocks.connector.spark.exception.ShouldNeverHappenException
import com.starrocks.connector.spark.rdd.ScalaValueReader
import com.starrocks.connector.spark.rest.PartitionDefinition
import com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig
import com.starrocks.connector.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class ScalaStarRocksRowValueReader(partition: PartitionDefinition, config: ReadStarRocksConfig)
  extends ScalaValueReader(partition, config) with Logging {

  val rowOrder: Seq[String] = {
    var columns = config.getColumns
    if (columns == null) {
      columns = Array("*")
    }
    columns
  }

  override def next: AnyRef = {
    if (!hasNext) {
      logError(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    val row: ScalaStarRocksRow = new ScalaStarRocksRow(rowOrder)
    rowBatch.next.asScala.zipWithIndex.foreach {
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }
}

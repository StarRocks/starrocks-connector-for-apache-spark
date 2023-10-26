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

package com.starrocks.connector.spark.read

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_READ_FIELD
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rdd.ScalaValueReader
import com.starrocks.connector.spark.rest.PartitionDefinition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader

import scala.collection.JavaConverters._

abstract class StarRocksReader [Record <: StarRocksGenericRow](
  partition: PartitionDefinition,
  settings: Settings)
  extends ScalaValueReader(partition, settings)
  with Logging  with PartitionReader[Record]{
    var rowOrder: Seq[String] = settings.getProperty(STARROCKS_READ_FIELD).split(",")
    var currentRow: Array[Any] = Array.fill(rowOrder.size)(null)

    override def getNextRecord: AnyRef = {
      if (!hasNext) {
        return null
      }
      rowBatch.next.asScala.zipWithIndex.foreach {
        case (s, index) if index < currentRow.size => currentRow.update(index, s)
        case _ => // nothing
      }
      currentRow
    }

  override def next(): Boolean = {
    val hasNext = rowBatchHasNext || rowHasNext
    if (hasNext) {
      getNextRecord
    }
    rowBatchHasNext || rowHasNext
  }

  override def get: Record = decode(currentRow)

  def decode(values: Array[Any]): Record
}

// For v1.0 RDD Reader
class StarrocksRowValueReader(partition: PartitionDefinition, settings: Settings)
  extends StarRocksReader[StarrocksRow](partition, settings) {

  override def decode(values: Array[Any]) : StarrocksRow = {
    new StarrocksRow(values)
  }
}

// For v2.0 Reader based on catalog
class StarrocksInternalRowValueReader(partition: PartitionDefinition, settings: Settings)
  extends StarRocksReader[StarrocksInternalRow](partition, settings) {

  override def decode(values: Array[Any]) : StarrocksInternalRow = {
    new StarrocksInternalRow(values)
  }
}
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

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TIMEZONE
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.serialization.BaseRowBatch
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.sql.connector.read.InputPartition

import java.time.ZoneId
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * read data from Starrocks BE to array.
 *
 * @param partition Starrocks RDD partition
 * @param settings  request configuration
 */
class BaseValueReader(partition: InputPartition, settings: Settings, schema: StarRocksSchema) {
  protected val srTimeZone =
    ZoneId.of(settings.getProperty(STARROCKS_TIMEZONE, ZoneId.systemDefault().getId))
  protected val sparkTimeZone =
    ZoneId.of(settings.getProperty("spark.sql.session.timeZone", ZoneId.systemDefault().getId))

  var rowBatch: BaseRowBatch = _
  var rowHasNext: Boolean = true
  var rowBatchHasNext: Boolean = false

  private var currentRecord: List[Object] = _

  def init(): Unit = {}

  def hasNext: Boolean = {
    false
  }

  def getNextRecord: AnyRef = {
    currentRecord
  }

  def getRow: List[Object] = {
    if (!hasNext) {
      return null
    }
    currentRecord = rowBatch.next.asScala.toList
    currentRecord
  }

  def close(): Unit = {}
}

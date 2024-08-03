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
import com.starrocks.connector.spark.rdd.{BaseValueReader, RpcValueReader}
import com.starrocks.connector.spark.rest.RpcPartition
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}

abstract class AbstractStarRocksDecoder[Record <: StarRocksGenericRow](partition: InputPartition,
                                                                       settings: Settings,
                                                                       schema: StarRocksSchema)
  extends PartitionReader[Record]
    with Logging {
  // the reader obtain data from StarRocks BE
  lazy val reader: BaseValueReader = {
    val baseValueReader = new RpcValueReader(partition.asInstanceOf[RpcPartition], settings)
    baseValueReader.init
    baseValueReader
  }
  private val rowOrder: Seq[String] = settings.getProperty(STARROCKS_READ_FIELD).split(",")
  private val currentRow: Array[Any] = Array.fill(rowOrder.size)(null)

  def next(): Boolean = {
    reader.fillRow(currentRow)
    reader.notFinal
  }

  def get: Record = decode(currentRow)

  def decode(values: Array[Any]): Record

  override def close: Unit = {
    reader.close
  }
}
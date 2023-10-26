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
import com.starrocks.connector.spark.rest.{PartitionDefinition, RestService}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class StarRocksScanBuilder(schema: StructType,
                           config: Settings) extends ScanBuilder
                           with SupportsPushDownRequiredColumns{

  private var readSchema: StructType = schema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this.readSchema = StructType(readSchema.filter(field => requiredCols.contains(field.name)))

    // pass read column to BE
    config.setProperty(STARROCKS_READ_FIELD, readSchema.fieldNames.mkString(","))
  }

  override def build(): Scan = new StarRocksScan(readSchema, config)
}

class StarRocksScan(schema: StructType, config: Settings) extends Scan
  with Batch
  with Logging
  with SupportsReportPartitioning
  with PartitionReaderFactory {
  lazy val inputPartitions: Array[PartitionDefinition] = RestService.findPartitions(config, log).asScala.toArray

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def outputPartitioning(): Partitioning = new UnknownPartitioning(inputPartitions.length)

  override def planInputPartitions(): Array[InputPartition] = inputPartitions.toArray

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val reader = new StarrocksInternalRowValueReader(partition.asInstanceOf[PartitionDefinition], config)
    reader.asInstanceOf[PartitionReader[InternalRow]]
  }
}

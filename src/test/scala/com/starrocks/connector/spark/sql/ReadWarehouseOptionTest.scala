// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_WAREHOUSE
import com.starrocks.connector.spark.rdd.{StarRocksPartition, StarRocksSpark}
import com.starrocks.connector.spark.rest.{FakeFeServer, RpcPartition}
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig
import com.starrocks.connector.spark.sql.schema.{StarRocksField, StarRocksSchema}
import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.slf4j.LoggerFactory

import java.util.Collections
import scala.collection.JavaConverters._

/**
 * Every read API must hand `starrocks.request.warehouse` to the FE `_query_plan` request, so that the
 * Spark partitions read from the compute nodes of that warehouse.
 */
class ReadWarehouseOptionTest {

  private var fe: FakeFeServer = _

  @BeforeEach
  def startFe(): Unit = {
    fe = FakeFeServer.startWithTwoWarehouses()
  }

  @AfterEach
  def stopFe(): Unit = {
    fe.close()
  }

  // Options of a DataFrame read or a Spark SQL temporary view.
  private def sqlOptions: Map[String, String] = Map(
    "starrocks.fe.http.url" -> fe.address(),
    "starrocks.table.identifier" -> "db.tbl",
    "starrocks.user" -> "root",
    "starrocks.password" -> "")

  private def assertReadOnParity(partitions: Seq[RpcPartition]): Unit = {
    assertEquals(FakeFeServer.PARITY_WAREHOUSE, fe.lastRequest().header("warehouse"))
    assertEquals(FakeFeServer.PARITY_WAREHOUSE_NODES.asScala.toSet, partitions.map(_.getBeAddress).toSet)
  }

  private def rpcPartitions(partitions: Array[Partition]): Seq[RpcPartition] =
    partitions.map(_.asInstanceOf[StarRocksPartition].starrocksPartition).toSeq

  // Plans a DataSource V2 scan of a table whose config holds tableOptions, with readOptions for this read.
  private def planV2Scan(tableOptions: Map[String, String], readOptions: Map[String, String]): Seq[RpcPartition] = {
    val schema = new StructType().add("id", IntegerType)
    val starRocksSchema = new StarRocksSchema(
      Collections.singletonList(new StarRocksField("id", "int", 1, null, null, null)))
    val table = new StarRocksTable(schema, starRocksSchema, new SimpleStarRocksConfig(tableOptions.asJava),
      Identifier.of(Array("db"), "tbl"))
    table.newScanBuilder(new CaseInsensitiveStringMap(readOptions.asJava)).build().toBatch
      .planInputPartitions().map(_.asInstanceOf[RpcPartition]).toSeq
  }

  @Test
  def dataFrameRead(): Unit = {
    // spark.read.format("starrocks").option("starrocks.request.warehouse", "parity"): Spark hands the same
    // options to StarRocksDataSourceProvider#getTable and to StarRocksTable#newScanBuilder.
    val options = sqlOptions + (STARROCKS_REQUEST_WAREHOUSE -> FakeFeServer.PARITY_WAREHOUSE)
    assertReadOnParity(planV2Scan(options, options))
  }

  @Test
  def dataFrameReadWithoutWarehouse(): Unit = {
    val partitions = planV2Scan(sqlOptions, sqlOptions)
    assertNull(fe.lastRequest().header("warehouse"))
    assertEquals(FakeFeServer.DEFAULT_WAREHOUSE_NODES.asScala.toSet, partitions.map(_.getBeAddress).toSet)
  }

  @Test
  def catalogRead(): Unit = {
    // spark.sql.catalog.<name>.starrocks.request.warehouse=parity: the option is in the table config,
    // not in the options of the read.
    assertReadOnParity(planV2Scan(sqlOptions + (STARROCKS_REQUEST_WAREHOUSE -> FakeFeServer.PARITY_WAREHOUSE),
      Map.empty))
  }

  @Test
  def rddAndSparkSqlRead(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(classOf[ReadWarehouseOptionTest].getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    try {
      // RDD API.
      val rddOptions = Map(
        "starrocks.fenodes" -> fe.address(),
        "starrocks.request.auth.user" -> "root",
        "starrocks.request.auth.password" -> "",
        STARROCKS_REQUEST_WAREHOUSE -> FakeFeServer.PARITY_WAREHOUSE)
      val rdd = StarRocksSpark.starrocksRDD(spark.sparkContext, Some("db.tbl"), None, Some(rddOptions))
      assertReadOnParity(rpcPartitions(rdd.partitions))

      // Spark SQL temporary view (DataSource V1): StarRocksDataSourceProvider#createRelation builds the
      // relation from these params; it also loads the schema over JDBC, which this test skips.
      val log = LoggerFactory.getLogger(classOf[ReadWarehouseOptionTest])
      def scanV1(warehouse: String): Seq[RpcPartition] = {
        val params = DialectUtils.params(sqlOptions + (STARROCKS_REQUEST_WAREHOUSE -> warehouse), log)
        val relation = new StarRocksRelation(spark.sqlContext, params)
        rpcPartitions(relation.buildScan(Array("id"), Array.empty[Filter]).partitions)
      }
      assertReadOnParity(scanV1(FakeFeServer.PARITY_WAREHOUSE))
      // DialectUtils.params turns '_' into '.' in option keys; a warehouse name with '_' must reach FE unchanged.
      val defaultPartitions = scanV1(FakeFeServer.DEFAULT_WAREHOUSE)
      assertEquals(FakeFeServer.DEFAULT_WAREHOUSE, fe.lastRequest().header("warehouse"))
      assertEquals(FakeFeServer.DEFAULT_WAREHOUSE_NODES.asScala.toSet, defaultPartitions.map(_.getBeAddress).toSet)
    } finally {
      spark.stop()
    }
  }
}

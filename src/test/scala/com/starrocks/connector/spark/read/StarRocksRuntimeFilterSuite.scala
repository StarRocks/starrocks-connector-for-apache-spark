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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.spark.read

import com.starrocks.connector.spark.cfg.{ConfigurationOptions, PropertiesSettings}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysTrue, And}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.Assertions.{assertTrue, assertFalse, assertEquals}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

class StarRocksRuntimeFilterSuite {

  private def newScan(config: PropertiesSettings): StarRocksScan = {
    val schema = new StructType()
      .add("inv_date_sk", DataTypes.IntegerType, true)
      .add("inv_item_sk", DataTypes.IntegerType, true)
      .add("inv_warehouse_sk", DataTypes.IntegerType, true)
    new StarRocksScan("inventory", schema, null, Array.empty[org.apache.spark.sql.connector.expressions.filter.Predicate], config)
  }

  @Test
  def testFilterAttributesMatchesReadSchema(): Unit = {
    val scan = newScan(new PropertiesSettings())
    val names = scan.filterAttributes().map(_.fieldNames().mkString(".")).toSeq
    assertEquals(Seq("inv_date_sk", "inv_item_sk", "inv_warehouse_sk"), names)
  }

  @Test
  def testRuntimePredicateMergedIntoExistingStaticQuery(): Unit = {
    val config = new PropertiesSettings()
    config.setProperty(ConfigurationOptions.STARROCKS_FILTER_QUERY, "(inv_warehouse_sk = 10)")
    val scan = newScan(config)

    // Spark passes the runtime predicates as a set to be ANDed together.
    scan.filter(Array(new And(new AlwaysTrue(), new AlwaysTrue())))

    val query = config.getProperty(ConfigurationOptions.STARROCKS_FILTER_QUERY)
    // The runtime predicate is ANDed with the static one and written back into the same config
    // key, so RestService.findPartitions posts a _query_plan that contains both.
    assertTrue(query.startsWith("(inv_warehouse_sk = 10) and ("), query)
  }

  @Test
  def testRuntimePredicateWithoutStaticSetsTheQuery(): Unit = {
    val config = new PropertiesSettings()
    val scan = newScan(config)
    scan.filter(Array(new And(new AlwaysTrue(), new AlwaysTrue())))
    val query = config.getProperty(ConfigurationOptions.STARROCKS_FILTER_QUERY)
    // No static predicate yet: the runtime one alone becomes starrocks.filter.query.
    assertTrue(query.contains("(true) and (true)"), query)
    assertFalse(query.contains("inv_warehouse_sk"), query)
  }

  @Test
  def testEstimateStatisticsFromConfig(): Unit = {
    val config = new PropertiesSettings()
    config.setProperty(ConfigurationOptions.STARROCKS_READ_STATS_ROW_COUNT, "73049")
    config.setProperty(ConfigurationOptions.STARROCKS_READ_STATS_SIZE_IN_BYTES, "123456789")
    val stats = newScan(config).estimateStatistics()
    assertTrue(stats.numRows().isPresent && stats.numRows().getAsLong == 73049L)
    assertTrue(stats.sizeInBytes().isPresent && stats.sizeInBytes().getAsLong == 123456789L)
  }

  @Test
  def testEstimateStatisticsEmptyByDefault(): Unit = {
    val stats = newScan(new PropertiesSettings()).estimateStatistics()
    assertFalse(stats.numRows().isPresent)
    assertFalse(stats.sizeInBytes().isPresent)
  }
}
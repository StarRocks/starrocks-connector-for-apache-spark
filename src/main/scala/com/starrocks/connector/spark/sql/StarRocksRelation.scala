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

import com.starrocks.connector.spark.cfg.ConfigurationOptions
import com.starrocks.connector.spark.sql.conf.StarRocksConfig
import com.starrocks.connector.spark.sql.schema.InferSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import scala.collection.JavaConverters._
import scala.collection.mutable


private[sql] class StarRocksRelation(
    val sqlContext: SQLContext, parameters: Map[String, String])
    extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with InsertableRelation {

  private lazy val conf = {
    val conf = StarRocksConfig.createConfig(parameters.asJava)
    conf
  }

  private lazy val inValueLengthLimit = conf.toReadConfig.getQueryFilterInMaxCount

  private lazy val lazySchema = SchemaUtils.discoverSchema(conf)

  private lazy val dialect = JdbcDialects.get("")

  override def schema: StructType = InferSchema.inferSchema(parameters.asJava)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(Utils.compileFilter(_, dialect, inValueLengthLimit).isEmpty)
  }

  // TableScan
  override def buildScan(): RDD[Row] = buildScan(Array.empty)

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  // PrunedFilteredScan
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val paramWithScan = mutable.LinkedHashMap[String, String]() ++ parameters

    // filter where clause can be handled by StarRocks BE
    val filterWhereClause: String = {
      filters.flatMap(Utils.compileFilter(_, dialect, inValueLengthLimit))
          .map(filter => s"($filter)").mkString(" and ")
    }

    // required columns for column pruner
    if (requiredColumns != null && requiredColumns.length > 0) {
      paramWithScan += (ConfigurationOptions.STARROCKS_READ_FIELD ->
          requiredColumns.map(Utils.quote).mkString(","))
    } else {
      paramWithScan += (ConfigurationOptions.STARROCKS_READ_FIELD ->
          lazySchema.fields.map(f => f.name).mkString(","))
    }

    if (filters != null && filters.length > 0) {
      paramWithScan += (ConfigurationOptions.STARROCKS_FILTER_QUERY -> filterWhereClause)
    }

    new ScalaStarRocksRowRDD(sqlContext.sparkContext, paramWithScan.toMap)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .format("starrocks_writer")
      .options(conf.getOriginOptions)
      .mode(SaveMode.Append)
      .save()
  }
}

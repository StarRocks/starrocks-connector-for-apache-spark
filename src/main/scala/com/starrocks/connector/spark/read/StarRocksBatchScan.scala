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

import com.starrocks.connector.spark.cfg.ConfigurationOptions.{STARROCKS_FILTER_QUERY, STARROCKS_READ_FIELD}
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rest.RestService
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class StarRocksScanBuilder(tableName: String,
                           schema: StructType,
                           starRocksSchema: StarRocksSchema,
                           config: Settings) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  // use filter pushdown v2
  with SupportsPushDownV2Filters {

  import StarRocksScanBuilder._

  private var readSchema: StructType = schema

  private lazy val dialect = JdbcDialects.get("")
  private lazy val sqlBuilder: V2ExpressionSQLBuilder = new V2ExpressionSQLBuilder

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this.readSchema = StructType(readSchema.filter(field => requiredCols.contains(field.name)))

    // pass read column to BE
    config.setProperty(STARROCKS_READ_FIELD, readSchema.fieldNames.mkString(","))
  }

  private var supportedPredicates = Array.empty[Predicate]

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (supported, _) = predicates.partition(dialect.compileExpression(_).isDefined)

    val predicateWhereClause = supported
      .flatMap(compilePredicate)
      .map(p => s"($p)")
      .mkString(" and ")
    // only for test
    predicateWhereClauseForTest = predicateWhereClause
    val mySqlDialect = JdbcDialects.get("jdbc:mysql")
    val filterWhereClause: String = {
      predicates.flatMap(mySqlDialect.compileExpression(_)).map(p => s"($p)").mkString(" AND ")
    }
    // pass filter column to BE
    config.setProperty(STARROCKS_FILTER_QUERY, filterWhereClause)

    supportedPredicates = supported
    supported
  }

  override def pushedPredicates(): Array[Predicate] = supportedPredicates

  override def build(): Scan = new StarRocksScan(tableName, readSchema, starRocksSchema, pushedPredicates(), config)

  private def compilePredicate(predicate: Predicate): Option[String] = {
    Option(predicate match {
      case and: And =>
        val elems = Seq(and.left(), and.right()).flatMap(compilePredicate)
        if (elems.size == 2) elems.map(p => s"($p)").mkString(" and ") else null
      case or: Or =>
        val elems = Seq(or.left(), or.right()).flatMap(compilePredicate)
        if (elems.size == 2) elems.map(p => s"($p)").mkString(" or ") else null
      case not: Not =>
        compilePredicate(not.child()) match {
          case Some(p) => s"not $p"
          case None => null
        }
      case _: AlwaysTrue => "true"
      case _: AlwaysFalse => "false"
      case _ =>
        try {
          sqlBuilder.build(predicate)
        }
        catch {
          case _: Throwable => null
        }
    })
  }
}

object StarRocksScanBuilder {
  var predicateWhereClauseForTest: String = ""
}

class StarRocksScan(tableName: String,
                    schema: StructType,
                    starRocksSchema: StarRocksSchema,
                    pushedPredicates: Array[Predicate],
                    config: Settings) extends Scan
  with Batch
  with Logging
  with SupportsReportPartitioning
  with PartitionReaderFactory {

  private lazy val inputPartitions: Array[InputPartition] = {
      RestService.findPartitions(config, log).asScala.toArray
  }

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def description(): String = {
    super.description() +
      s", table: ${tableName}" +
      s", prune columns: ${schema.fields.map(f => f.name).mkString(", ")}" +
      s", pushed predicates: ${pushedPredicates.map(p => s"(${p.toString})").mkString(" and ")}"
  }

  override def outputPartitioning(): Partitioning = new UnknownPartitioning(inputPartitions.length)

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val reader = new StarRocksCatalogDecoder(partition, config, starRocksSchema)
    reader.asInstanceOf[PartitionReader[InternalRow]]
  }
}

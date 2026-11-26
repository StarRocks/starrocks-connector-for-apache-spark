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

import com.starrocks.connector.spark.cfg.ConfigurationOptions._
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rest.RestService
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util.OptionalLong

import scala.collection.JavaConverters._
import scala.util.Try

class StarRocksScanBuilder(tableName: String,
                           schema: StructType,
                           starRocksSchema: StarRocksSchema,
                           config: Settings) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  // use filter pushdown v2
  with SupportsPushDownV2Filters {

  import StarRocksScanBuilder._

  private var readSchema: StructType = schema

  private lazy val dialect = JdbcDialects.get("jdbc:mysql")

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

    // pass filter column to BE
    config.setProperty(STARROCKS_FILTER_QUERY, predicateWhereClause)

    supportedPredicates = supported
    supported
  }

  override def pushedPredicates(): Array[Predicate] = supportedPredicates

  override def build(): Scan = new StarRocksScan(tableName, readSchema, starRocksSchema, pushedPredicates(), config)

  private def compilePredicate(predicate: Predicate): Option[String] = StarRocksScanBuilder.compilePredicate(predicate)
}

object StarRocksScanBuilder {
  var predicateWhereClauseForTest: String = ""

  private lazy val dialect = JdbcDialects.get("jdbc:mysql")

  /**
   * Compiles a data source V2 [[Predicate]] to a MySQL where-clause clause. Shared by static
   * pushdown ([[StarRocksScanBuilder.pushPredicates]]) and runtime filtering
   * ([[StarRocksScan.filter]]) so the runtime IN list travels in the same starrocks.filter.query
   * that the FE turns into the opaque tablet scan plan. Returns None when it cannot be pushed.
   */
  def compilePredicate(predicate: Predicate): Option[String] = {
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
          dialect.compileExpression(predicate).get
        }
        catch {
          case _: Throwable => null
        }
    })
  }
}

/**
 * StarRocksScan can be filtered at runtime by Spark's dynamic pruning (SupportsRuntimeV2Filtering,
 * honored by Spark 4).
 *
 * Spark calls [[filter]] once the build side of an equi-join completes and re-runs
 * [[planInputPartitions]]. The compiled runtime predicate is written back into the same
 * starrocks.filter.query that static pushdown populated, so [[RestService.findPartitions]] posts a
 * _query_plan whose SQL carries the runtime IN list. The opaque plan inside every RpcPartition then
 * applies that predicate on the BE while reading each tablet, so the driver transfers only matching
 * rows even when the table is hash-distributed on a different column.
 */
class StarRocksScan(tableName: String,
                    schema: StructType,
                    starRocksSchema: StarRocksSchema,
                    pushedPredicates: Array[Predicate],
                    config: Settings) extends Scan
  with Batch
  with SupportsReportPartitioning
  with SupportsRuntimeV2Filtering
  with SupportsReportStatistics
  with PartitionReaderFactory {

  @transient private lazy val log = LoggerFactory.getLogger(classOf[StarRocksScan])

  // Runtime predicates received via filter(), kept for EXPLAIN output.
  @volatile private var runtimePredicates: Array[Predicate] = Array.empty

  // Tablet plan cache. It is populated during planning (outputPartitioning forces it) before any
  // runtime predicate exists, so filter() drops it to make the next planInputPartitions() post a
  // fresh _query_plan to the FE.
  @volatile private var partitionsCache: Array[InputPartition] = _

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def description(): String = {
    val runtime = if (runtimePredicates.nonEmpty) {
      s", runtime predicates: ${runtimePredicates.map(p => s"(${p.toString})").mkString(" and ")}"
    } else {
      ""
    }
    super.description() +
      s", table: ${tableName}" +
      s", prune columns: ${schema.fields.map(f => f.name).mkString(", ")}" +
      s", pushed predicates: ${pushedPredicates.map(p => s"(${p.toString})").mkString(" and ")}" +
      runtime
  }

  override def filterAttributes(): Array[NamedReference] =
    schema.fields.map(f => StarRocksFieldReference(f.name): NamedReference)

  override def filter(predicates: Array[Predicate]): Unit = {
    // The supplied predicates are ANDed together; compile each with the dialect pushdown already
    // uses, then concatenate with the static filter so both reach the FE in one _query_plan SQL.
    val runtime = predicates.flatMap(StarRocksScanBuilder.compilePredicate).map(p => s"($p)")
    if (runtime.nonEmpty) {
      val static = Option(config.getProperty(STARROCKS_FILTER_QUERY)).map(_.trim).filter(_.nonEmpty)
      config.setProperty(STARROCKS_FILTER_QUERY, (static.toSeq ++ runtime).mkString(" and "))
      // The cached partitions were planned with the static predicates only. Drop them so the next
      // planInputPartitions() asks the FE for a plan that contains the runtime predicate.
      partitionsCache = null
    }
    runtimePredicates = predicates
  }

  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = longStat(STARROCKS_READ_STATS_SIZE_IN_BYTES)
    override def numRows(): OptionalLong = longStat(STARROCKS_READ_STATS_ROW_COUNT)
  }

  private def longStat(key: String): OptionalLong =
    Option(config.getProperty(key)).flatMap(v => Try(v.trim.toLong).toOption).map(OptionalLong.of).getOrElse(OptionalLong.empty())

  override def outputPartitioning(): Partitioning = new UnknownPartitioning(planInputPartitions().length)

  override def planInputPartitions(): Array[InputPartition] = {
    if (partitionsCache == null) {
      partitionsCache = RestService.findPartitions(config, log).asScala.toArray
    }
    partitionsCache
  }

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val reader = new StarRocksCatalogDecoder(partition, config, starRocksSchema)
    reader.asInstanceOf[PartitionReader[InternalRow]]
  }
}

/** NamedReference whose equality collapses to the column name, so Spark can match runtime
 * predicates derived for `inv_date_sk` back to [[StarRocksScan.filterAttributes]]. */
final case class StarRocksFieldReference(name: String) extends NamedReference {
  override def fieldNames(): Array[String] = Array(name)
  override def toString: String = name
}

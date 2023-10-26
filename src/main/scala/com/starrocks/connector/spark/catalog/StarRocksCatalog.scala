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

package com.starrocks.connector.spark.catalog

import com.starrocks.connector.spark.sql.StarRocksDataSourceProvider.{addPrefixInStarRockConfig, getStarRocksSchema}
import com.starrocks.connector.spark.sql.StarRocksTable
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig
import com.starrocks.connector.spark.sql.connect.StarRocksConnector
import com.starrocks.connector.spark.sql.schema.InferSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `map AsScala`}

class StarRocksCatalog extends TableCatalog
  with SupportsNamespaces
  with FunctionCatalog
  with Logging {

  private var catalogName: String = _
  private var properties: util.Map[String, String] = _
  private var simpleStarRocksConfig: SimpleStarRocksConfig = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.properties = addPrefixInStarRockConfig(options.asCaseSensitiveMap())
    this.simpleStarRocksConfig = new SimpleStarRocksConfig(properties)

    log.info(s"StarRocks clusters' detail: $options")
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val table2Db = StarRocksConnector.getTables(simpleStarRocksConfig, namespace.toList.asJava)
    table2Db.map { case (tb, db) => Identifier.of(Array(db), tb) }.toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val starRocksSchema = getStarRocksSchema(simpleStarRocksConfig, ident)
    val schema = InferSchema.inferSchema(starRocksSchema, simpleStarRocksConfig)
    new StarRocksTable(schema, starRocksSchema, simpleStarRocksConfig, ident)
  }

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = throw new UnsupportedOperationException

  override def alterTable(ident: Identifier, changes: TableChange*): Table = throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = throw new UnsupportedOperationException

  override def listNamespaces(): Array[Array[String]] = {
    val dbNames = StarRocksConnector.getDatabases(simpleStarRocksConfig)
    dbNames.map(dbName => Array(dbName)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = throw new UnsupportedOperationException

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    StarRocksConnector.loadDatabase(simpleStarRocksConfig, namespace.toList.asJava)
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    throw new UnsupportedOperationException

  override def listFunctions(namespace: Array[String]): Array[Identifier] = throw new UnsupportedOperationException

  override def loadFunction(ident: Identifier): UnboundFunction = throw new UnsupportedOperationException
}

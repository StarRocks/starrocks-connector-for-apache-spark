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

import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.exception.StarRocksException
import com.starrocks.connector.spark.rest.RestService
import com.starrocks.connector.spark.rest.models.{Field, Schema}
import com.starrocks.connector.spark.sql.schema.{StarRocksField, StarRocksSchema}
import com.starrocks.thrift.TScanColumnDesc
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.util
import java.util.Optional
import scala.collection.JavaConverters._

private[spark] object SchemaUtils {
  private val logger = LoggerFactory.getLogger(SchemaUtils.getClass.getSimpleName.stripSuffix("$"))

  /**
   * discover StarRocks table schema from StarRocks FE.
   *
   * @param cfg configuration
   * @return Spark Catalyst StructType
   */
  def discoverDataType(cfg: Settings): StructType = {
    val schema = discoverTableSchema(cfg)
    convertToStruct(schema)
  }

  /**
   * discover StarRocks table schema from StarRocks FE.
   *
   * @param cfg configuration
   * @return inner schema struct
   */
  def discoverTableSchema(cfg: Settings): Schema = RestService.getSchema(cfg, logger)

  /**
   * convert inner schema struct to Spark Catalyst StructType
   *
   * @param schema inner schema
   * @return Spark Catalyst StructType
   */
  def convertToStruct(schema: Schema): StructType = {
    var fields: List[StructField] = List()
    schema.getProperties.asScala.foreach(f =>
      fields :+= DataTypes.createStructField(f.getName, getCatalystType(f.getType.get(), f.getPrecision, f.getScale), true))
    DataTypes.createStructType(fields.asJava)
  }

  /**
   * translate StarRocks Type to Spark Catalyst type
   *
   * @param starrocksType StarRocks type
   * @param precision     decimal precision
   * @param scale         decimal scale
   * @return Spark Catalyst type
   */
  def getCatalystType(starrocksType: String, precision: Int, scale: Int): DataType = {
    starrocksType match {
      case "NULL_TYPE" => DataTypes.NullType
      case "BOOLEAN" => DataTypes.BooleanType
      case "TINYINT" => DataTypes.ByteType
      case "SMALLINT" => DataTypes.ShortType
      case "INT" => DataTypes.IntegerType
      case "BIGINT" => DataTypes.LongType
      case "FLOAT" => DataTypes.FloatType
      case "DOUBLE" => DataTypes.DoubleType
      case "DATE" => DataTypes.StringType
      case "DATETIME" => DataTypes.StringType
      case "BINARY" => DataTypes.BinaryType
      case "DECIMAL" => DecimalType(precision, scale)
      case "CHAR" => DataTypes.StringType
      case "LARGEINT" => DataTypes.StringType
      case "VARCHAR" => DataTypes.StringType
      case "DECIMALV2" => DecimalType(precision, scale)
      case "DECIMAL32" => DecimalType(precision, scale)
      case "DECIMAL64" => DecimalType(precision, scale)
      case "DECIMAL128" => DecimalType(precision, scale)
      case "TIME" => DataTypes.DoubleType
      case "JSON" => DataTypes.StringType
      // ARRAY HLL BITMAP
      case _ =>
        throw new StarRocksException("Unsupported type " + starrocksType)
    }
  }

  /**
   * convert StarRocks return schema to inner schema struct.
   *
   * @param tscanColumnDescs StarRocks BE return schema
   * @return inner schema struct
   */
  def convert(tscanColumnDescs: Seq[TScanColumnDesc]): StarRocksSchema = {
    val fields = tscanColumnDescs.toStream
      .map(desc => {
        new StarRocksField(desc.getName,
          if (Optional.ofNullable(desc.getType).isPresent) desc.getType.name() else null, 0, 0, 0, 0)
      }).toList
    new StarRocksSchema(fields.asJava)
  }

  //TODO schema only use
  def convert(schema: Schema): StarRocksSchema = {
    val columns: util.List[StarRocksField] = new util.ArrayList[StarRocksField]
    val pks: util.List[StarRocksField] = new util.ArrayList[StarRocksField]
    val fields: util.List[Field] = schema.getProperties
    if (CollectionUtils.isNotEmpty(fields)) for (i <- 0 until fields.size) {
      val field: Field = fields.get(i)
      val srField: StarRocksField = new StarRocksField(field.getName, field.getType.get(), i + 1, // start at 1
        field.getColumnSize, field.getPrecision, field.getScale)
      columns.add(srField)
      if (field.getIsKey) pks.add(srField)
    }
    new StarRocksSchema(columns, pks, schema.getTableId)
  }

}

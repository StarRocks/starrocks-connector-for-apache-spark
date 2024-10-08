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

import com.starrocks.connector.spark.exception.StarRocksException
import com.starrocks.connector.spark.rest.models.{Field, Schema}
import com.starrocks.thrift.{TPrimitiveType, TScanColumnDesc}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{Assertions, Test}

import scala.collection.JavaConverters._

class TestSchemaUtils extends ExpectedExceptionTest {
  @Test
  def testConvertToStruct(): Unit = {
    val schema = new Schema
    schema.setStatus(200)
    val k1 = new Field("k1", "TINYINT", 3, "", 0, 0, false)
    val k5 = new Field("k5", "BIGINT", 19, "", 0, 0, false)
    schema.put(k1)
    schema.put(k5)

    var fields = List[StructField]()
    fields :+= DataTypes.createStructField("k1", DataTypes.ByteType, true)
    fields :+= DataTypes.createStructField("k5", DataTypes.LongType, true)
    val expected = DataTypes.createStructType(fields.asJava)
    Assertions.assertEquals(expected, SchemaUtils.convertToStruct(schema))
  }

  @Test
  def testGetCatalystType(): Unit = {
   Assertions.assertEquals(DataTypes.NullType, SchemaUtils.getCatalystType("NULL_TYPE", 0, 0))
   Assertions.assertEquals(DataTypes.BooleanType, SchemaUtils.getCatalystType("BOOLEAN", 0, 0))
   Assertions.assertEquals(DataTypes.ByteType, SchemaUtils.getCatalystType("TINYINT", 0, 0))
   Assertions.assertEquals(DataTypes.ShortType, SchemaUtils.getCatalystType("SMALLINT", 0, 0))
   Assertions.assertEquals(DataTypes.IntegerType, SchemaUtils.getCatalystType("INT", 0, 0))
   Assertions.assertEquals(DataTypes.LongType, SchemaUtils.getCatalystType("BIGINT", 0, 0))
   Assertions.assertEquals(DataTypes.FloatType, SchemaUtils.getCatalystType("FLOAT", 0, 0))
   Assertions.assertEquals(DataTypes.DoubleType, SchemaUtils.getCatalystType("DOUBLE", 0, 0))
   Assertions.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("DATE", 0, 0))
   Assertions.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("DATETIME", 0, 0))
   Assertions.assertEquals(DataTypes.BinaryType, SchemaUtils.getCatalystType("BINARY", 0, 0))
   Assertions.assertEquals(DecimalType(9, 3), SchemaUtils.getCatalystType("DECIMAL", 9, 3))
   Assertions.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("CHAR", 0, 0))
   Assertions.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("LARGEINT", 0, 0))
   Assertions.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("VARCHAR", 0, 0))
   Assertions.assertEquals(DecimalType(10, 5), SchemaUtils.getCatalystType("DECIMALV2", 10, 5))
   Assertions.assertEquals(DecimalType(12, 6), SchemaUtils.getCatalystType("DECIMAL64", 12, 6))
   Assertions.assertEquals(DecimalType(30, 7), SchemaUtils.getCatalystType("DECIMAL128", 30, 7))
   Assertions.assertEquals(DataTypes.DoubleType, SchemaUtils.getCatalystType("TIME", 0, 0))

    var exception = Assertions.assertThrows(classOf[StarRocksException],
      () => {
        SchemaUtils.getCatalystType("HLL", 0, 0)
      })
    Assertions.assertTrue(exception.getMessage().startsWith("Unsupported type"))

    exception = Assertions.assertThrows(classOf[StarRocksException],
      () => {
        SchemaUtils.getCatalystType("UNRECOGNIZED", 0, 0)
      })
    Assertions.assertTrue(exception.getMessage().startsWith("Unsupported type"))
  }

  @Test
  def testConvertToSchema(): Unit = {
    val k1 = new TScanColumnDesc
    k1.setName("k1")
    k1.setType(TPrimitiveType.BOOLEAN)

    val k2 = new TScanColumnDesc
    k2.setName("k2")
    k2.setType(TPrimitiveType.DOUBLE)

    val expected = new Schema
    expected.setStatus(0)
    val ek1 = new Field("k1", "BOOLEAN", 1, "", 0, 0, false)
    val ek2 = new Field("k2", "DOUBLE", 2, "", 0, 0, false)
    expected.put(ek1)
    expected.put(ek2)
  }
}

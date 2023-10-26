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
import com.starrocks.connector.spark.exception.StarRocksException
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.junit.jupiter.api.{Assertions, Test}
import org.slf4j.LoggerFactory

class TestUtils extends ExpectedExceptionTest {
  private lazy val logger = LoggerFactory.getLogger(classOf[TestUtils])

  @Test
  def testCompileFilter(): Unit = {
    val dialect = JdbcDialects.get("")
    val inValueLengthLimit = 5

    val equalFilter = EqualTo("left", 5)
    val greaterThanFilter = GreaterThan("left", 5)
    val greaterThanOrEqualFilter = GreaterThanOrEqual("left", 5)
    val lessThanFilter = LessThan("left", 5)
    val lessThanOrEqualFilter = LessThanOrEqual("left", 5)
    val validInFilter = In("left", Array(1, 2, 3, 4))
    val emptyInFilter = In("left", Array.empty)
    val invalidInFilter = In("left", Array(1, 2, 3, 4, 5))
    val isNullFilter = IsNull("left")
    val isNotNullFilter = IsNotNull("left")
    val notSupportFilter = StringContains("left", "right")
    val validAndFilter = And(equalFilter, greaterThanFilter)
    val invalidAndFilter = And(equalFilter, notSupportFilter)
    val validOrFilter = Or(equalFilter, greaterThanFilter)
    val invalidOrFilter = Or(equalFilter, notSupportFilter)

    Assertions.assertEquals("`left` = 5", DialectUtils.compileFilter(equalFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("`left` > 5", DialectUtils.compileFilter(greaterThanFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("`left` >= 5", DialectUtils.compileFilter(greaterThanOrEqualFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("`left` < 5", DialectUtils.compileFilter(lessThanFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("`left` <= 5", DialectUtils.compileFilter(lessThanOrEqualFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("`left` in (1, 2, 3, 4)", DialectUtils.compileFilter(validInFilter, dialect, inValueLengthLimit).get)
    Assertions.assertTrue(DialectUtils.compileFilter(emptyInFilter, dialect, inValueLengthLimit).isEmpty)
    Assertions.assertTrue(DialectUtils.compileFilter(invalidInFilter, dialect, inValueLengthLimit).isEmpty)
    Assertions.assertEquals("`left` is null", DialectUtils.compileFilter(isNullFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("`left` is not null", DialectUtils.compileFilter(isNotNullFilter, dialect, inValueLengthLimit).get)
    Assertions.assertEquals("(`left` = 5) and (`left` > 5)",
      DialectUtils.compileFilter(validAndFilter, dialect, inValueLengthLimit).get)
    Assertions.assertTrue(DialectUtils.compileFilter(invalidAndFilter, dialect, inValueLengthLimit).isEmpty)
    Assertions.assertEquals("(`left` = 5) or (`left` > 5)",
      DialectUtils.compileFilter(validOrFilter, dialect, inValueLengthLimit).get)
    Assertions.assertTrue(DialectUtils.compileFilter(invalidOrFilter, dialect, inValueLengthLimit).isEmpty)
  }

  @Test
  def testParams(): Unit = {
    val parameters1 = Map(
      ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER -> "a.b",
      "test_underline" -> "x_y",
      "user" -> "user",
      "password" -> "password"
    )
    val result1 = DialectUtils.params(parameters1, logger)
    Assertions.assertEquals("a.b", result1(ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER))
    Assertions.assertEquals("x_y", result1("starrocks.test.underline"))
    Assertions.assertEquals("user", result1("starrocks.request.auth.user"))
    Assertions.assertEquals("password", result1("starrocks.request.auth.password"))


    val parameters2 = Map(
      ConfigurationOptions.TABLE_IDENTIFIER -> "a.b"
    )
    val result2 = DialectUtils.params(parameters2, logger)
    Assertions.assertEquals("a.b", result2(ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER))

    val parameters3 = Map(
      ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD -> "a.b"
    )
    var exception = Assertions.assertThrows(classOf[StarRocksException],
      () => DialectUtils.params(parameters3, logger))
    Assertions.assertTrue(exception.getMessage().startsWith(s"${ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD} " +
      s"cannot use in StarRocks Datasource,"))

    val parameters4 = Map(
      ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER -> "a.b"
    )
    exception = Assertions.assertThrows(classOf[StarRocksException],
      () => DialectUtils.params(parameters4, logger))
    Assertions.assertTrue(exception.getMessage().startsWith(s"${ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER} " +
      s"cannot use in StarRocks Datasource,"))
  }

}

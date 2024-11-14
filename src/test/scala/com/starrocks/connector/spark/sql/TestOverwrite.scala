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

import com.starrocks.connector.spark.exception.NotSupportedOperationException
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig
import com.starrocks.connector.spark.sql.connect.StarRocksConnector
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{Assertions, Test}
import org.slf4j.LoggerFactory

import java.sql.{Connection, ResultSet, Statement}

/**
 * test on local docker environment, spark version 3.3, 3.4, 3.5
 * docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --name quickstart-3.1.14 -itd starrocks/allin1-ubuntu:3.1.14
 * docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --name quickstart-3.2.12 -itd starrocks/allin1-ubuntu:3.2.12
 * docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --name quickstart-3.3.2 -itd starrocks/allin1-ubuntu:3.3.2
 */
class TestOverwrite extends ExpectedExceptionTest {
  private lazy val logger = LoggerFactory.getLogger(classOf[TestOverwrite])

  private val STARROCKS_FE_HTTP_URL = "127.0.0.1:8030"
  private val STARROCKS_FE_JDBC_URL = "jdbc:mysql://127.0.0.1:9030"
  private val STARROCKS_FE_JDBC_USER = "root"
  private val STARROCKS_FE_JDBC_PASSWORD = ""

  @Test
  def testFullTableOverwrite(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`score_board`")
      val createTableDDL =
        """
          |CREATE TABLE IF NOT EXISTS `test`.`score_board`
          |(
          |    `id` int(11) NOT NULL COMMENT "",
          |    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
          |    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
          |)
          |ENGINE=OLAP
          |PRIMARY KEY(`id`)
          |COMMENT "OLAP"
          |DISTRIBUTED BY HASH(`id`)
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`score_board` values (1, 'spark', 100)")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      val data = Seq((5, "starrocks", 103), (6, "spark", 103))
      val df = data.toDF("id", "name", "score")

      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
        .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
        .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
        .option("starrocks.user", STARROCKS_FE_JDBC_USER)
        .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
        .option("starrocks.table.identifier", "test.score_board")
        .mode("overwrite")
        .save()

      rs = statement.executeQuery("select id, name, score from test.score_board order by id asc")
      if (rs.next()) {
        Assertions.assertEquals(5, rs.getInt("id"))
        Assertions.assertEquals("starrocks", rs.getString("name"))
        Assertions.assertEquals(103, rs.getInt("score"))
      }

      if (rs.next()) {
        Assertions.assertEquals(6, rs.getInt("id"))
        Assertions.assertEquals("spark", rs.getString("name"))
        Assertions.assertEquals(103, rs.getInt("score"))
      }
    } finally {
      try {
        dropTable(statement, "`test`.`score_board`")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  def dropTable(statement: Statement, table: String): Unit = {
    if (statement != null) {
      statement.execute(s"drop table if exists $table")
    }
  }
  def releaseConn(conn: Connection, statement: Statement, rs: ResultSet): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: Throwable => logger.error("close connection error", e)
      }
    }

    if (statement != null) {
      try {
        statement.close()
      } catch {
        case e: Throwable => logger.error("close statement error", e)
      }
    }

    if (rs != null) {
      try {
        rs.close()
      } catch {
        case e: Throwable => logger.error("close resultSet error", e)
      }
    }
  }

  @Test
  def testFullTableOverwriteWithEx(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`score_board`")
      val createTableDDL =
        """
          |CREATE TABLE IF NOT EXISTS `test`.`score_board`
          |(
          |    `id` int(11) NOT NULL COMMENT "",
          |    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
          |    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
          |)
          |ENGINE=OLAP
          |PRIMARY KEY(`id`)
          |COMMENT "OLAP"
          |DISTRIBUTED BY HASH(`id`)
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`score_board` values (1, 'spark', 100)")
      try {
        val spark = SparkSession.builder().master("local[2]").getOrCreate()
        import spark.implicits._
        // 1. Create a DataFrame from a sequence.
        val data = Seq((5, "starrocks", 103), (6, "spark", 103))
        val frame = data.toDF("id", "name", "score")
        val encoder = frame.encoder
        val df = frame.map(x => {
          if (x.getInt(0) == 6) {
            throw new RuntimeException()
          }
          x
        })(encoder)

        // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
        // You need to modify the options according your own environment.
        df.write.format("starrocks")
          .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
          .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
          .option("starrocks.user", STARROCKS_FE_JDBC_USER)
          .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
          .option("starrocks.table.identifier", "test.score_board")
          .mode("overwrite")
          .save()
      } catch {
        case e: Throwable => logger.error("error occurs", e)
      }

      rs = statement.executeQuery("select id, name, score from test.score_board order by id asc")
      if (rs.next()) {
        Assertions.assertEquals(1, rs.getInt("id"))
        Assertions.assertEquals("spark", rs.getString("name"))
        Assertions.assertEquals(100, rs.getInt("score"))
      }
    } finally {
      try {
        dropTable(statement, "`test`.`score_board`")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  @Test
  def testPartitionOverwrite(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`t_recharge_detail1`")
      val createTableDDL =
        """
          |CREATE TABLE `test`.`t_recharge_detail1` (
          |    id bigint,
          |    user_id bigint,
          |    city varchar(20) not null,
          |    dt varchar(20) not null
          |)
          |DUPLICATE KEY(id)
          |PARTITION BY LIST (city) (
          |   PARTITION pLos_Angeles VALUES IN ("Los Angeles"),
          |   PARTITION pSan_Francisco VALUES IN ("San Francisco")
          |)
          |DISTRIBUTED BY HASH(`id`);
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`t_recharge_detail1` values (1, 1, 'Los Angeles', '20241107')," +
        " (2, 2, 'San Francisco', '20241101')")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      //
      val data = Seq((3, 3, "Los Angeles", "20241107"), (2, 2, "Los Angeles", "20241106"))
      val df = data.toDF("id", "user_id", "city", "dt")

      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
        .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
        .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
        .option("starrocks.user", STARROCKS_FE_JDBC_USER)
        .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
        .option("starrocks.table.identifier", "test.t_recharge_detail1")
        .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
        .mode("overwrite")
        .save()

      rs = statement.executeQuery(
        "select id, user_id, city, dt from test.t_recharge_detail1 where city = 'Los Angeles' order by id asc")
      if (rs.next()) {
        Assertions.assertEquals(2, rs.getInt("id"))
        Assertions.assertEquals(2, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241106", rs.getString("dt"))
      }

      if (rs.next()) {
        Assertions.assertEquals(3, rs.getInt("id"))
        Assertions.assertEquals(3, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241107", rs.getString("dt"))
      }
    } finally {
      try {
        dropTable(statement, "test.t_recharge_detail1")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  @Test
  def testPartitionOverwriteMultiPartitions(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`t_recharge_detail1`")
      val createTableDDL =
        """
          |CREATE TABLE `test`.`t_recharge_detail1` (
          |    id bigint,
          |    user_id bigint,
          |    city varchar(20) not null,
          |    dt varchar(20) not null
          |)
          |DUPLICATE KEY(id)
          |PARTITION BY LIST (city) (
          |   PARTITION pLos_Angeles VALUES IN ("Los Angeles"),
          |   PARTITION pSan_Francisco VALUES IN ("San Francisco")
          |)
          |DISTRIBUTED BY HASH(`id`);
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`t_recharge_detail1` values (1, 1, 'Los Angeles', '20241107')," +
        " (2, 2, 'San Francisco', '20241101')")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      //
      val data = Seq(
        (3, 3, "Los Angeles", "20241107"),
        (2, 2, "Los Angeles", "20241106"),
        (5, 5, "San Francisco", "20241108"))
      val df = data.toDF("id", "user_id", "city", "dt")

      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
        .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
        .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
        .option("starrocks.user", STARROCKS_FE_JDBC_USER)
        .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
        .option("starrocks.table.identifier", "test.t_recharge_detail1")
        .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
        .option("starrocks.write.overwrite.partitions.pSan_Francisco", "(\"San Francisco\")")
        .mode("overwrite")
        .save()

      rs = statement.executeQuery(
        "select id, user_id, city, dt from test.t_recharge_detail1 where city = 'Los Angeles' order by id asc")
      if (rs.next()) {
        Assertions.assertEquals(2, rs.getInt("id"))
        Assertions.assertEquals(2, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241106", rs.getString("dt"))
      }

      if (rs.next()) {
        Assertions.assertEquals(3, rs.getInt("id"))
        Assertions.assertEquals(3, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241107", rs.getString("dt"))
      }

      if (rs.next()) {
        Assertions.assertEquals(5, rs.getInt("id"))
        Assertions.assertEquals(5, rs.getInt("user_id"))
        Assertions.assertEquals("San Francisco", rs.getString("city"))
        Assertions.assertEquals("20241108", rs.getString("dt"))
      }
    } finally {
      try {
        dropTable(statement, "test.t_recharge_detail1")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  @Test
  def testPartitionOverwriteWithEx(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`t_recharge_detail1`")
      val createTableDDL =
        """
          |CREATE TABLE `test`.`t_recharge_detail1` (
          |    id bigint,
          |    user_id bigint,
          |    city varchar(20) not null,
          |    dt varchar(20) not null
          |)
          |DUPLICATE KEY(id)
          |PARTITION BY LIST (city) (
          |   PARTITION pLos_Angeles VALUES IN ("Los Angeles"),
          |   PARTITION pSan_Francisco VALUES IN ("San Francisco")
          |)
          |DISTRIBUTED BY HASH(`id`);
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`t_recharge_detail1` values (1, 1, 'Los Angeles', '20241107')," +
        " (2, 2, 'San Francisco', '20241101')")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      //
      try {
        val data = Seq((3, 3, "Los Angeles", "20241107"), (2, 2, "Los Angeles", "20241106"))
        val frame = data.toDF("id", "user_id", "city", "dt")
        val encoder = frame.encoder
        val df = frame.map(x => {
          if (x.getInt(0) == 2) {
            throw new RuntimeException()
          }
          x
        })(encoder)

        // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
        // You need to modify the options according your own environment.
        df.write.format("starrocks")
          .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
          .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
          .option("starrocks.user", STARROCKS_FE_JDBC_USER)
          .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
          .option("starrocks.table.identifier", "test.t_recharge_detail1")
          .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
          .mode("overwrite")
          .save()
      } catch {
        case e: Throwable => logger.error("error occurs", e)
      }

      rs = statement.executeQuery(
        "select id, user_id, city, dt from test.t_recharge_detail1 where city = 'Los Angeles' order by id asc")
      if (rs.next()) {
        Assertions.assertEquals(1, rs.getInt("id"))
        Assertions.assertEquals(1, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241107", rs.getString("dt"))
      }
    } finally {
      try {
        dropTable(statement, "test.t_recharge_detail1")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  @Test
  def testPartitionOverwriteWithExistsTemporaryPartition(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`t_recharge_detail1`")
      val createTableDDL =
        """
          |CREATE TABLE `test`.`t_recharge_detail1` (
          |    id bigint,
          |    user_id bigint,
          |    city varchar(20) not null,
          |    dt varchar(20) not null
          |)
          |DUPLICATE KEY(id)
          |PARTITION BY LIST (city) (
          |   PARTITION pLos_Angeles VALUES IN ("Los Angeles"),
          |   PARTITION pSan_Francisco VALUES IN ("San Francisco")
          |)
          |DISTRIBUTED BY HASH(`id`);
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`t_recharge_detail1` values (1, 1, 'Los Angeles', '20241107')," +
        " (2, 2, 'San Francisco', '20241101')")
      statement.execute("ALTER TABLE `test`.`t_recharge_detail1` ADD TEMPORARY PARTITION pLos_Angeles" + WriteStarRocksConfig.TEMPORARY_PARTITION_SUFFIX
        + System.currentTimeMillis() + " VALUES IN (\"Los Angeles\")")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      //
      val data = Seq((3, 3, "Los Angeles", "20241107"), (2, 2, "Los Angeles", "20241106"))
      val df = data.toDF("id", "user_id", "city", "dt")
      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
        .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
        .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
        .option("starrocks.user", STARROCKS_FE_JDBC_USER)
        .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
        .option("starrocks.table.identifier", "test.t_recharge_detail1")
        .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
        .mode("overwrite")
        .save()

      rs = statement.executeQuery(
        "select id, user_id, city, dt from test.t_recharge_detail1 where city = 'Los Angeles' order by id asc")
      if (rs.next()) {
        Assertions.assertEquals(2, rs.getInt("id"))
        Assertions.assertEquals(2, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241106", rs.getString("dt"))
      }

      if (rs.next()) {
        Assertions.assertEquals(3, rs.getInt("id"))
        Assertions.assertEquals(3, rs.getInt("user_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city"))
        Assertions.assertEquals("20241107", rs.getString("dt"))
      }
    } finally {
      try {
        dropTable(statement, "test.t_recharge_detail1")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  @Test
  def testPartitionOverwriteWithDynamicPartition(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`site_access`")
      val createTableDDL =
        """
          |CREATE TABLE `test`.`site_access`(
          |    event_day DATE,
          |    site_id INT DEFAULT '10',
          |    city_code VARCHAR(100),
          |    user_name VARCHAR(32) DEFAULT '',
          |    pv BIGINT DEFAULT '0'
          |)
          |DUPLICATE KEY(event_day, site_id, city_code, user_name)
          |PARTITION BY RANGE(event_day)(
          |    PARTITION p20241111 VALUES LESS THAN ("2024-11-12"),
          |    PARTITION p20241112 VALUES LESS THAN ("2024-11-13"),
          |    PARTITION p20241113 VALUES LESS THAN ("2024-11-14"),
          |    PARTITION p20241114 VALUES LESS THAN ("2024-11-15"),
          |    PARTITION p20241115 VALUES LESS THAN ("2024-11-16")
          |)
          |DISTRIBUTED BY HASH(event_day, site_id)
          |PROPERTIES(
          |    "dynamic_partition.enable" = "true",
          |    "dynamic_partition.time_unit" = "DAY",
          |    "dynamic_partition.start" = "-3",
          |    "dynamic_partition.end" = "3",
          |    "dynamic_partition.prefix" = "p",
          |    "dynamic_partition.buckets" = "32",
          |    "dynamic_partition.history_partition_num" = "0"
          |);
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`site_access`(event_day, site_id, city_code, user_name, pv) values ('2024-11-15', 1, 'Los Angeles', 'jack', 10)")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      val data = Seq(("2024-11-15 12:12:23", 10, "Los Angeles", "jack", 30), ("2024-11-15 08:12:23", 20, "Los Angeles", "jack", 20))
      var df = data.toDF("event_day", "site_id", "city_code", "user_name", "pv")
      df.createOrReplaceTempView("test_view")
      df = spark.sql("select cast(event_day as date) as event_day, site_id, city_code, user_name, pv from test_view")
      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
        .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
        .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
        .option("starrocks.user", STARROCKS_FE_JDBC_USER)
        .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
        .option("starrocks.table.identifier", "test.site_access")
        .option("starrocks.write.overwrite.partitions.p20241115", "[(\"2024-11-15\"),(\"2024-11-16\"))")
        .mode("overwrite")
        .save()

      rs = statement.executeQuery(
        "select event_day, site_id, city_code, user_name, pv from test.site_access order by site_id asc")
      if (rs.next()) {
        Assertions.assertEquals("2024-11-15", rs.getDate("event_day").toString)
        Assertions.assertEquals(10, rs.getInt("site_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city_code"))
        Assertions.assertEquals("jack", rs.getString("user_name"))
        Assertions.assertEquals(30, rs.getInt("pv"))
      }

      if (rs.next()) {
        Assertions.assertEquals("2024-11-15", rs.getDate("event_day").toString)
        Assertions.assertEquals(20, rs.getInt("site_id"))
        Assertions.assertEquals("Los Angeles", rs.getString("city_code"))
        Assertions.assertEquals("jack", rs.getString("user_name"))
        Assertions.assertEquals(20, rs.getInt("pv"))
      }
    } finally {
      try {
        dropTable(statement, "`test`.`site_access`")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }

  @Test
  def testPartitionOverwriteWithExpressionPartitioning(): Unit = {
    val conn = StarRocksConnector.createJdbcConnection(STARROCKS_FE_JDBC_URL,
      STARROCKS_FE_JDBC_USER, STARROCKS_FE_JDBC_PASSWORD)
    val statement = conn.createStatement()
    var rs: ResultSet = null
    try {
      statement.execute("CREATE DATABASE IF NOT EXISTS `test`")
      statement.execute("DROP TABLE IF EXISTS `test`.`site_access1`")
      val createTableDDL =
        """
          |CREATE TABLE `test`.`site_access1` (
          |    event_day DATETIME NOT NULL,
          |    site_id INT DEFAULT '10',
          |    city_code VARCHAR(100),
          |    user_name VARCHAR(32) DEFAULT '',
          |    pv BIGINT DEFAULT '0'
          |)
          |DUPLICATE KEY(event_day, site_id, city_code, user_name)
          |PARTITION BY date_trunc('day', event_day)
          |DISTRIBUTED BY HASH(event_day, site_id);
          |""".stripMargin
      statement.execute(createTableDDL)
      statement.execute("insert into `test`.`site_access1`(event_day, site_id, city_code, user_name, pv)" +
        " values ('2023-02-26 20:12:04',2,'New York','Sam Smith',1)," +
        " ('2023-02-27 21:06:54',1,'Los Angeles','Taylor Swift',1)")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      val data = Seq(("2023-02-26 12:12:23", 10, "Los Angeles", "jack", 30), ("2023-02-26 08:12:23", 20, "Los Angeles", "jack", 20))
      var df = data.toDF("event_day", "site_id", "city_code", "user_name", "pv")
      df.createOrReplaceTempView("test_view1")
      df = spark.sql("select cast(event_day as timestamp) as event_day, site_id, city_code, user_name, pv from test_view1")
      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      try {
        df.write.format("starrocks")
          .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
          .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
          .option("starrocks.user", STARROCKS_FE_JDBC_USER)
          .option("starrocks.password", STARROCKS_FE_JDBC_PASSWORD)
          .option("starrocks.table.identifier", "test.site_access1")
          .option("starrocks.write.overwrite.partitions.p20230226", "[(\"2022-02-26 00:00:00\"),(\"2024-02-27 00:00:00\"))")
          .mode("overwrite")
          .save()
      } catch {
        case e: Throwable => Assertions.assertTrue(e.isInstanceOf[NotSupportedOperationException]
          && e.getMessage.equals(
          "Overwriting partition only supports list/range partitioning, not support expression/automatic partitioning !!!"))
      }
    } finally {
      try {
        dropTable(statement, "`test`.`site_access1`")
      } finally {
        releaseConn(conn, statement, rs)
      }
    }
  }
}

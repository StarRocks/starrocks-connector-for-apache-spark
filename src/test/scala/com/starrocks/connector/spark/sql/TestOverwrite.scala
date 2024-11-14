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
import com.starrocks.connector.spark.sql.ITTestBase.genRandomUuid
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{Assertions, Test}
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import java.util

class TestOverwrite extends ITTestBase {

  private lazy val logger = LoggerFactory.getLogger(classOf[TestOverwrite])

  @Test
  def testFullTableOverwrite(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    var rs: ResultSet = null
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE IF NOT EXISTS `$database`.`$tableName`
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
      statement.execute(s"insert into `$database`.`$tableName` values (1, 'spark', 100)")
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      import spark.implicits._
      // 1. Create a DataFrame from a sequence.
      val data = Seq((5, "starrocks", 103), (6, "spark", 103))
      val df = data.toDF("id", "name", "score")

      // 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
      // You need to modify the options according your own environment.
      df.write.format("starrocks")
        .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
        .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
        .option("starrocks.user", ITTestBase.USER)
        .option("starrocks.password", ITTestBase.PASSWORD)
        .option("starrocks.table.identifier", s"$database.$tableName")
        .mode("overwrite")
        .save()

      val expectedData = new util.ArrayList[util.List[AnyRef]]()
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(5), "starrocks", Integer.valueOf(103)))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(6), "spark", Integer.valueOf(103)))
      val actualWriteData = ITTestBase.scanTable(ITTestBase.DB_CONNECTION, database, tableName);
      ITTestBase.verifyResult(expectedData, actualWriteData);
    }
  }

  @Test
  def testFullTableOverwriteWithEx(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    var rs: ResultSet = null
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE IF NOT EXISTS `$database`.`$tableName`
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
      statement.execute(s"insert into `$database`.`$tableName` values (1, 'spark', 100)")
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
          .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
          .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
          .option("starrocks.user", ITTestBase.USER)
          .option("starrocks.password", ITTestBase.PASSWORD)
          .option("starrocks.table.identifier", s"$database.$tableName")
          .mode("overwrite")
          .save()
      } catch {
        case e: Throwable => logger.error("error occurs", e)
      }

      val expectedData = new util.ArrayList[util.List[AnyRef]]()
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(1), "spark", Integer.valueOf(100)))
      val actualWriteData = ITTestBase.scanTable(ITTestBase.DB_CONNECTION, database, tableName)
      ITTestBase.verifyResult(expectedData, actualWriteData)
    }
  }

  @Test
  def testPartitionOverwrite(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    var rs: ResultSet = null
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE `$database`.`$tableName` (
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
      statement.execute(s"insert into `$database`.`$tableName` values (1, 1, 'Los Angeles', '20241107')," +
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
        .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
        .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
        .option("starrocks.user", ITTestBase.USER)
        .option("starrocks.password", ITTestBase.PASSWORD)
        .option("starrocks.table.identifier", s"$database.$tableName")
        .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
        .mode("overwrite")
        .save()

      rs = statement.executeQuery(
        s"select id, user_id, city, dt from `$database`.`$tableName` where city = 'Los Angeles' order by id asc")

      val expectedData = new util.ArrayList[util.List[AnyRef]]()
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(2), Integer.valueOf(2), "San Francisco", "20241101"))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(2), Integer.valueOf(2), "Los Angeles", "20241106"))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(3), Integer.valueOf(3), "Los Angeles", "20241107"))
      val actualWriteData = ITTestBase.scanTable(ITTestBase.DB_CONNECTION, database, tableName)
      ITTestBase.verifyResult(expectedData, actualWriteData)

    }
  }

  @Test
  def testPartitionOverwriteMultiPartitions(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    var rs: ResultSet = null
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE `$database`.`$tableName` (
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
      statement.execute(s"insert into `$database`.`$tableName` values (1, 1, 'Los Angeles', '20241107')," +
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
        .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
        .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
        .option("starrocks.user", ITTestBase.USER)
        .option("starrocks.password", ITTestBase.PASSWORD)
        .option("starrocks.table.identifier", s"$database.$tableName")
        .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
        .option("starrocks.write.overwrite.partitions.pSan_Francisco", "(\"San Francisco\")")
        .mode("overwrite")
        .save()

      val expectedData = new util.ArrayList[util.List[AnyRef]]()
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(2), Integer.valueOf(2), "Los Angeles", "20241106"))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(3), Integer.valueOf(3), "Los Angeles", "20241107"))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(5), Integer.valueOf(5), "San Francisco", "20241108"))
      val actualWriteData = ITTestBase.scanTable(ITTestBase.DB_CONNECTION, database, tableName)
      ITTestBase.verifyResult(expectedData, actualWriteData)
    }
  }

  @Test
  def testPartitionOverwriteWithEx(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    var rs: ResultSet = null
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE `$database`.`$tableName` (
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
      statement.execute(s"insert into `$database`.`$tableName` values (1, 1, 'Los Angeles', '20241107')," +
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
          .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
          .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
          .option("starrocks.user", ITTestBase.USER)
          .option("starrocks.password", ITTestBase.PASSWORD)
          .option("starrocks.table.identifier", s"$database.$tableName")
          .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
          .mode("overwrite")
          .save()
      } catch {
        case e: Throwable => logger.error("error occurs", e)
      }
    }
  }

  @Test
  def testPartitionOverwriteWithExistsTemporaryPartition(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    var rs: ResultSet = null
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE `$database`.`$tableName` (
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
      statement.execute(s"insert into `$database`.`$tableName` values (1, 1, 'Los Angeles', '20241107')," +
        " (2, 2, 'San Francisco', '20241101')")
      statement.execute(s"ALTER TABLE `$database`.`$tableName` ADD TEMPORARY PARTITION pLos_Angeles" + WriteStarRocksConfig.TEMPORARY_PARTITION_SUFFIX
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
        .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
        .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
        .option("starrocks.user", ITTestBase.USER)
        .option("starrocks.password", ITTestBase.PASSWORD)
        .option("starrocks.table.identifier", s"$database.$tableName")
        .option("starrocks.write.overwrite.partitions.pLos_Angeles", "(\"Los Angeles\")")
        .mode("overwrite")
        .save()

      val expectedData = new util.ArrayList[util.List[AnyRef]]()
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(2), Integer.valueOf(2), "San Francisco", "20241101"))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(2), Integer.valueOf(2), "Los Angeles", "20241106"))
      expectedData.add(util.Arrays.asList[AnyRef](Integer.valueOf(3), Integer.valueOf(3), "Los Angeles", "20241107"))
      val actualWriteData = ITTestBase.scanTable(ITTestBase.DB_CONNECTION, database, tableName)
      ITTestBase.verifyResult(expectedData, actualWriteData)
    }
  }

  @Test
  def testPartitionOverwriteWithExpressionPartitioning(): Unit = {
    val statement =  ITTestBase.DB_CONNECTION.createStatement()
    val database = ITTestBase.DB_NAME
    val tableName = "testOverwrite" + genRandomUuid
    try {
      val createTableDDL =
        s"""
          |CREATE TABLE `$database`.`$tableName` (
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
      statement.execute(s"insert into `$database`.`$tableName`(event_day, site_id, city_code, user_name, pv)" +
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
          .option("starrocks.fe.http.url", ITTestBase.FE_HTTP)
          .option("starrocks.fe.jdbc.url", ITTestBase.FE_JDBC)
          .option("starrocks.user", ITTestBase.USER)
          .option("starrocks.password", ITTestBase.PASSWORD)
          .option("starrocks.table.identifier", s"$database.$tableName")
          .option("starrocks.write.overwrite.partitions.p20230226", "[(\"2022-02-26 00:00:00\"),(\"2024-02-27 00:00:00\"))")
          .mode("overwrite")
          .save()
      } catch {
        case e: Throwable => Assertions.assertTrue(e.isInstanceOf[NotSupportedOperationException]
          && e.getMessage.equals(
          "Overwriting partition only supports list/range partitioning, not support expression/automatic partitioning !!!"))
      }
    }
  }
}

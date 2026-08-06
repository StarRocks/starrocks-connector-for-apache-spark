/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.starrocks.connector.spark.ci

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager}

import scala.collection.mutable.ArrayBuffer

import com.starrocks.connector.spark._
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Runs the core examples from connector-write.md and connector-read.md against
 * a packaged connector JAR on a real Spark standalone worker.
 */
object ConnectorDocsSmoke {

  private case class Config(
      database: String,
      sharedWorkDirectory: String,
      feHttp: String,
      feJdbc: String,
      expectedConnectorName: String) {
    val user = "root"
    val password = ""
  }

  private def requireCheck(condition: Boolean, message: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(message)
    }
  }

  private def withJdbc[T](config: Config)(body: Connection => T): T = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val separator = if (config.feJdbc.contains("?")) "&" else "?"
    val connection = DriverManager.getConnection(
      config.feJdbc + separator + "useSSL=false&allowPublicKeyRetrieval=true",
      config.user,
      config.password)
    try body(connection) finally connection.close()
  }

  private def execute(connection: Connection, sql: String): Unit = {
    val statement = connection.createStatement()
    try statement.execute(sql) finally statement.close()
  }

  private def prepareStarRocks(config: Config): Unit = withJdbc(config) { connection =>
    execute(connection, s"DROP DATABASE IF EXISTS `${config.database}`")
    execute(connection, s"CREATE DATABASE `${config.database}`")

    val tableDdl =
      """(
        |  `id` INT NOT NULL,
        |  `name` VARCHAR(65533) NULL DEFAULT "",
        |  `score` INT NOT NULL DEFAULT "0"
        |)
        |ENGINE=OLAP
        |PRIMARY KEY(`id`)
        |DISTRIBUTED BY HASH(`id`) BUCKETS 1
        |PROPERTIES ("replication_num" = "1")
        |""".stripMargin

    execute(connection, s"CREATE TABLE `${config.database}`.`score_board_write` $tableDdl")
    execute(connection, s"CREATE TABLE `${config.database}`.`score_board_read` $tableDdl")
    execute(connection,
      s"""INSERT INTO `${config.database}`.`score_board_read` VALUES
         |(1, 'Bob', 21), (2, 'Stan', 21), (3, 'Sam', 22),
         |(4, 'Tony', 22), (5, 'Alice', 22), (6, 'Lucy', 23),
         |(7, 'Polly', 23), (8, 'Tom', 23), (9, 'Rose', 24),
         |(10, 'Jerry', 24), (11, 'Jason', 24), (12, 'Lily', 25),
         |(13, 'Stephen', 25), (14, 'David', 25), (15, 'Eddie', 26),
         |(16, 'Kate', 27), (17, 'Cathy', 27), (18, 'Judy', 27),
         |(19, 'Julia', 28), (20, 'Robert', 28), (21, 'Jack', 29)
         |""".stripMargin)
  }

  private def options(config: Config, table: String): Map[String, String] = Map(
    "starrocks.fe.http.url" -> config.feHttp,
    "starrocks.fe.jdbc.url" -> config.feJdbc,
    "starrocks.table.identifier" -> s"${config.database}.$table",
    "starrocks.user" -> config.user,
    "starrocks.password" -> config.password)

  private def sqlOptions(config: Config, table: String): String =
    s"""(
       |  "starrocks.fe.http.url"="${config.feHttp}",
       |  "starrocks.fe.jdbc.url"="${config.feJdbc}",
       |  "starrocks.table.identifier"="${config.database}.$table",
       |  "starrocks.user"="${config.user}",
       |  "starrocks.password"="${config.password}"
       |)""".stripMargin

  private def starRocksDataFrame(
      spark: SparkSession,
      config: Config,
      table: String): DataFrame = {
    spark.read.format("starrocks").options(options(config, table)).load()
  }

  private def jdbcRows(config: Config, table: String): Seq[(Int, String, Int)] =
    withJdbc(config) { connection =>
      val statement = connection.createStatement()
      val result = statement.executeQuery(
        s"SELECT id, name, score FROM `${config.database}`.`$table` ORDER BY id")
      val rows = ArrayBuffer.empty[(Int, String, Int)]
      try {
        while (result.next()) {
          rows += ((result.getInt(1), result.getString(2), result.getInt(3)))
        }
      } finally {
        result.close()
        statement.close()
      }
      rows.toSeq
    }

  private def awaitJdbcIds(
      config: Config,
      table: String,
      expectedIds: Seq[Int],
      timeoutMs: Long = 120000L): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    var actualIds = Seq.empty[Int]
    while (System.currentTimeMillis() < deadline) {
      actualIds = jdbcRows(config, table).map(_._1)
      if (actualIds == expectedIds) {
        return
      }
      Thread.sleep(1000L)
    }
    throw new IllegalStateException(
      s"Timed out waiting for $table IDs ${expectedIds.mkString(",")}; " +
        s"last result was ${actualIds.mkString(",")}")
  }

  def main(args: Array[String]): Unit = {
    requireCheck(
      args.length == 5,
      "Expected arguments: <database> <shared-work-directory> <fe-http> <fe-jdbc> " +
        "<expected-connector-name>")
    val config = Config(args(0), args(1), args(2), args(3), args(4))
    val workDirectory = Paths.get(
      config.sharedWorkDirectory,
      config.database + "-" + System.currentTimeMillis())
    val inputDirectory = workDirectory.resolve("csv-data")
    val checkpointDirectory = workDirectory.resolve("checkpoint")
    Files.createDirectories(inputDirectory)

    prepareStarRocks(config)

    val spark = SparkSession.builder()
      .appName("StarRocks connector docs smoke " + config.database)
      .getOrCreate()
    try {
      spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._

      val connectorLocation = classOf[com.starrocks.connector.spark.sql.StarRocksDataSourceProvider]
        .getProtectionDomain.getCodeSource.getLocation.toString
      println(
        s"VALIDATION_ENV spark=${spark.version} scala=${scala.util.Properties.versionNumberString} " +
          s"master=${spark.sparkContext.master} connector=$connectorLocation")
      requireCheck(
        spark.sparkContext.master.startsWith("spark://"),
        "The smoke test must run on a standalone Spark cluster")
      requireCheck(
        connectorLocation.contains(config.expectedConnectorName),
        s"Driver loaded connector from unexpected location: $connectorLocation")

      val executorEvidence = spark.sparkContext.parallelize(1 to 40, 4).mapPartitions { values =>
        val executorId = SparkEnv.get.executorId
        val host = InetAddress.getLocalHost.getHostName
        val location = Class.forName("com.starrocks.connector.spark.sql.StarRocksDataSourceProvider")
          .getProtectionDomain.getCodeSource.getLocation.toString
        Iterator(s"$executorId@$host:${values.sum}:$location")
      }.collect()
      requireCheck(
        executorEvidence.exists(value => !value.startsWith("driver@")),
        "No standalone executor executed the distributed probe")
      requireCheck(
        executorEvidence.forall(_.contains(config.expectedConnectorName)),
        "An executor loaded the connector from an unexpected location: " +
          executorEvidence.mkString(","))
      println("CHECK distributed_execution PASS " + executorEvidence.mkString(","))

      val batch = Seq((1, "starrocks", 100), (2, "spark", 100)).toDF("id", "name", "score")
      batch.repartition(2).write.format("starrocks")
        .options(options(config, "score_board_write"))
        .mode("append")
        .save()
      awaitJdbcIds(config, "score_board_write", Seq(1, 2))
      println("CHECK dataframe_batch_write PASS ids=1,2")

      val streamSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("score", IntegerType, nullable = false)))
      val streamData = spark.readStream
        .option("sep", ",")
        .schema(streamSchema)
        .format("csv")
        .load(inputDirectory.toString)
      Files.write(
        inputDirectory.resolve("test.csv"),
        "3,starrocks,100\n4,spark,100\n".getBytes(StandardCharsets.UTF_8))
      val streamQuery = streamData.writeStream.format("starrocks")
        .options(options(config, "score_board_write"))
        .option("checkpointLocation", checkpointDirectory.toString)
        .outputMode("append")
        .trigger(Trigger.Once())
        .start()
      requireCheck(
        streamQuery.awaitTermination(120000L),
        "Structured Streaming write did not terminate within 120 seconds")
      awaitJdbcIds(config, "score_board_write", Seq(1, 2, 3, 4))
      println("CHECK structured_streaming_write PASS ids=3,4")

      spark.sql("DROP TABLE IF EXISTS score_board_sql")
      spark.sql(
        s"CREATE TABLE score_board_sql USING starrocks " +
          s"OPTIONS ${sqlOptions(config, "score_board_write")}")
      spark.sql("INSERT INTO score_board_sql VALUES (5, 'starrocks', 100), (6, 'spark', 100)")
      awaitJdbcIds(config, "score_board_write", Seq(1, 2, 3, 4, 5, 6))
      println("CHECK spark_sql_write PASS ids=5,6")

      spark.sql(
        s"CREATE OR REPLACE TEMPORARY VIEW spark_starrocks USING starrocks " +
          s"OPTIONS ${sqlOptions(config, "score_board_read")}")
      val sqlReadRows = spark.sql(
        "SELECT id, name, score FROM spark_starrocks ORDER BY id").collect()
      requireCheck(
        sqlReadRows.length == 21 && sqlReadRows.head.getInt(0) == 1 &&
          sqlReadRows.last.getInt(0) == 21,
        "Spark SQL read did not return the expected 21 rows")
      println("CHECK spark_sql_read PASS rows=21 first=Bob last=Jack")

      val dataFrameRows = starRocksDataFrame(spark, config, "score_board_read")
        .orderBy("id").limit(10).collect()
      requireCheck(
        dataFrameRows.length == 10 && dataFrameRows.head.getString(1) == "Bob" &&
          dataFrameRows.last.getString(1) == "Jerry",
        "Spark DataFrame read did not return the expected first 10 rows")
      println("CHECK dataframe_read PASS rows=10 first=Bob tenth=Jerry")

      val starrocksSparkRDD = spark.sparkContext.starrocksRDD(
        tableIdentifier = Some(s"${config.database}.score_board_read"),
        cfg = Some(Map(
          "starrocks.fenodes" -> config.feHttp,
          "starrocks.request.auth.user" -> config.user,
          "starrocks.request.auth.password" -> config.password)))
      val rddRows = starrocksSparkRDD.collect()
      requireCheck(rddRows.length == 21, "Spark RDD read did not return the expected 21 rows")
      println("CHECK rdd_read PASS rows=21 sample=" + rddRows.head)

      val writtenRows = starRocksDataFrame(spark, config, "score_board_write")
        .orderBy("id").collect()
      requireCheck(
        writtenRows.map(_.getInt(0)).toSeq == Seq(1, 2, 3, 4, 5, 6),
        "Connector readback did not return all six written rows")
      requireCheck(
        jdbcRows(config, "score_board_write").map(_._1) == Seq(1, 2, 3, 4, 5, 6),
        "JDBC readback did not return all six written rows")
      println("CHECK connector_and_jdbc_readback PASS ids=1,2,3,4,5,6")

      println(s"VALIDATION_RESULT PASS spark=${spark.version} database=${config.database}")
    } finally {
      spark.stop()
    }
  }
}

/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.starrocks.connector.spark.examples

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object KafkaToStarRocksStreaming {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("KafkaToStarRocksStreaming")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(100))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "group.id" -> "kafka_to_sr_streaming_example",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false")

    val topics = Array("pk-test")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

//    kafkaStream.foreachRDD(rdd =>
//      spark.createDataFrame(rdd.map(record => Row(record.value())), SchemalessConverter.SCHEMA)
//        .write.format("starrocks")
//          .option("starrocks.fe.http.url", "127.0.0.1:8030")
//          .option("starrocks.fe.jdbc.url", "jdbc://mysql/127.0.0.1:9030")
//          .option("starrocks.table.identifier", "test.score_board")
//          .option("starrocks.user", "root")
//          .option("starrocks.password", "")
//          .option("starrocks.schemaless", "true")
//          .option("starrocks.write.properties.format", "json")
//          .mode("append")
//          .save()
//    )

    kafkaStream.foreachRDD(rdd =>
      spark.read.json(rdd.map(record => record.value()))
        .toDF()
        .write.format("starrocks")
        .option("starrocks.fe.http.url", "127.0.0.1:8030")
        .option("starrocks.fe.jdbc.url", "jdbc://mysql/127.0.0.1:9030")
        .option("starrocks.table.identifier", "test.score_board")
        .option("starrocks.user", "root")
        .option("starrocks.password", "")
//        .option("starrocks.schemaless", "true")
//        .option("starrocks.write.properties.format", "json")
        .mode("append")
        .save()
    )


//    import com.starrocks.connector.spark.sql.schema.SchemalessConverter
//
//    dStream.foreachRDD(rdd =>
//      spark.createDataFrame(
//          rdd.map(_.value().toString)
//            .map(JSON.parseObject(_).getString("after")),
//          SchemalessConverter.SCHEMA
//        )
//        .write.format("starrocks")
//        .option("starrocks.fe.http.url", "10.200.1.91:8030")
//        .option("starrocks.fe.jdbc.url", "jdbc://mysql/10.200.1.91:9030")
//        .option("starrocks.table.identifier", "spark.s3")
//        .option("starrocks.user", "root")
//        .option("starrocks.password", "")
//        .option("starrocks.schemaless", "true")
//        .option("starrocks.write.properties.format", "json")
//        .mode("append")
//        .save()
//    )

    ssc.start()
    ssc.awaitTermination()
  }
}
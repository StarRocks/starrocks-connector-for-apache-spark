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

package com.starrocks.connector.spark.sql;

import com.starrocks.connector.spark.sql.schema.SchemalessConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assume.assumeTrue;

public class StreamingITTest extends ITTestBase {

    private static String KAFKA_BROKER_SERVER;
    private static final String KAFKA_TOPIC = "pk-test";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String tableName;
    private String tableId;

    @Before
    public void prepare() throws Exception {
        this.tableName = "testConfig_" + genRandomUuid();
        this.tableId = String.join(".", DB_NAME, tableName);
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "id INT," +
                                "name STRING," +
                                "score INT" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`id`) " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);

        // TODO start kafka cluster, and produce messages
        // {"id":1,"name":"starrocks","score":100}
        // {"id":2,"name":"spark","score":100}
        KAFKA_BROKER_SERVER = DEBUG_MODE ? "127.0.0.1:9092" : System.getProperty("kafka_broker_server");
    }

    @Test
    public void testStructuredStreaming() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testStructuredStreaming")
                .getOrCreate();

        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.readStream()
                .option("sep", ",")
                .schema(schema)
                .format("csv")
                .load("src/test/resources/data/");

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", tableId);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        String checkpointDir = temporaryFolder.newFolder().toURI().toString();
        StreamingQuery query = df.writeStream()
                .format("starrocks")
                .outputMode(OutputMode.Append())
                .options(options)
                .option("checkpointLocation", checkpointDir)
                .trigger(Trigger.Continuous(100))
                .start();

        query.awaitTermination(5000);
        spark.stop();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(3, "starrocks", 100));
        expectedData.add(Arrays.asList(4, "spark", 100));
        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);
    }

    @Test
    public void testKafkaStructuredStreaming() throws Exception {
        assumeTrue(KAFKA_BROKER_SERVER != null);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testKafkaStructuredStreaming")
                .getOrCreate();

        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER_SERVER)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)");

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", tableId);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.schemaless", "true");
        options.put("starrocks.write.properties.format", "json");

        String checkpointDir = temporaryFolder.newFolder().toURI().toString();
        StreamingQuery query = df.writeStream()
                .format("starrocks")
                .outputMode(OutputMode.Append())
                .options(options)
                .option("checkpointLocation", checkpointDir)
                .trigger(Trigger.ProcessingTime(100))
                .start();

        query.awaitTermination(10000);
        spark.stop();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "starrocks", 100));
        expectedData.add(Arrays.asList(2, "spark", 100));
        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);
    }

    @Test
    public void testKafkaStreaming() throws Exception {
        assumeTrue(KAFKA_BROKER_SERVER != null);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testKafkaStreaming")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext, Durations.milliseconds(100));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", KAFKA_BROKER_SERVER);
        kafkaParams.put("group.id", "test_kafka_streaming");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList(KAFKA_TOPIC);
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", tableId);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
//        options.put("starrocks.schemaless", "true");
//        options.put("starrocks.write.properties.format", "json");
//        kafkaStream.foreachRDD(rdd ->
//                spark.createDataFrame(rdd.map(record -> RowFactory.create(record.value())), SchemalessConverter.SCHEMA)
//                    .write()
//                    .format("starrocks")
//                    .options(options)
//                    .mode(SaveMode.Append)
//                    .save()
//        );

        kafkaStream.foreachRDD(rdd ->
                spark.read().json(rdd.map(ConsumerRecord::value))
                        .write()
                        .format("starrocks")
                        .options(options)
                        .mode(SaveMode.Append)
                        .save()
        );

        jssc.start();
        jssc.awaitTerminationOrTimeout(5000);
        spark.stop();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "starrocks", 100));
        expectedData.add(Arrays.asList(2, "spark", 100));
        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);
    }
}

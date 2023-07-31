// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.connector.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReadWriteITTest extends ITTestBase {

    @Test
    public void testDataFrame() throws Exception {
        String tableName = "testDataFrame_" + genRandomUuid();
        prepareScoreBoardTable(tableName);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "2", 3));
        expectedData.add(Arrays.asList(2, "3", 4));
        List<Row> data = expectedData.stream().map(list -> list.toArray(new Object[0]))
                .map(RowFactory::create).collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", String.join(".", DB_NAME, tableName));
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        Dataset<Row> readDf = spark.read().format("starrocks")
                .option("starrocks.table.identifier", String.join(".", DB_NAME, tableName))
                .option("starrocks.fenodes", FE_HTTP)
                .option("starrocks.fe.jdbc.url", FE_JDBC)
                .option("user", USER)
                .option("password", PASSWORD)
                .load();
        List<Row> readRows = readDf.collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    @Test
    public void testSql() throws Exception {
        String tableName = "testSql_" + genRandomUuid();
        prepareScoreBoardTable(tableName);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testSql")
                .getOrCreate();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "2", 3));
        expectedData.add(Arrays.asList(2, "3", 4));

        String ddl = String.format("CREATE TABLE sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES (1, \"2\", 3), (2, \"3\", 4)");

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        List<Row> readRows = spark.sql("SELECT * FROM sr_table").collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    @Test
    public void testDataFramePartition() throws Exception {
        testDataFramePartitionBase("5", null);
    }

    @Test
    public void testDataFramePartitionColumns() throws Exception {
        testDataFramePartitionBase("10", "name,score");
    }

    private void testDataFramePartitionBase(String numPartitions, String partitionColumns) throws Exception {
        String tableName = "testDataFramePartitionBase_" + genRandomUuid();
        prepareScoreBoardTable(tableName);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFramePartitionBase")
                .getOrCreate();

        List<List<Object>> expectedData = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            expectedData.add(Arrays.asList(i, String.valueOf(i + 1), i + 2));
        }
        List<Row> data = expectedData.stream().map(list -> list.toArray(new Object[0]))
                .map(RowFactory::create).collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", String.join(".", DB_NAME, tableName));
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.write.num.partitions", numPartitions);
        if (partitionColumns != null) {
            options.put("starrocks.write.partition.columns", partitionColumns);
        }

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }

    private void prepareScoreBoardTable(String tableName) throws Exception {
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
        executeSRDDLSQL(createStarRocksTable);
    }

    @Test
    public void testWriteInCsvFormatContainJsonColumn() throws Exception {
        testWriteContainJsonColumnBase(true);
    }

    @Test
    public void testWriteInJsonFormatContainJsonColumn() throws Exception {
        testWriteContainJsonColumnBase(false);
    }

    private void testWriteContainJsonColumnBase(boolean csvFormat) throws Exception {
        String tableName = "testWriteContainJsonColumnBase_" + genRandomUuid();
        String createStarRocksTable =
            String.format("CREATE TABLE `%s`.`%s` (" +
                    "c0 BOOLEAN," +
                    "c1 TINYINT," +
                    "c2 SMALLINT," +
                    "c3 INT," +
                    "c4 BIGINT," +
                    "c5 LARGEINT," +
                    "c6 FLOAT," +
                    "c7 DOUBLE," +
                    "c8 DECIMAL(20, 0)," +
                    "c9 CHAR(10)," +
                    "c10 VARCHAR(100)," +
                    "c11 STRING," +
                    "c12 DATE," +
                    "c13 DATETIME," +
                    "c14 JSON" +
                ") ENGINE=OLAP " +
                    "PRIMARY KEY(`c0`, `c1`) " +
                    "DISTRIBUTED BY HASH(`c0`) BUCKETS 2 " +
                    "PROPERTIES (" +
                    "\"replication_num\" = \"1\"" +
                ")",
                DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testWriteContainJsonColumnBase")
                .getOrCreate();

        List<Row> data = new ArrayList<>();
        Row row = RowFactory.create(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                "5",
                6.0f,
                7.0,
                BigDecimal.valueOf(8.0),
                "9",
                "10",
                "11",
                Date.valueOf("2022-01-01"),
                Timestamp.valueOf("2023-01-01 00:00:00"),
                "{\"key\": 1, \"value\": 2}"
        );
        data.add(row);

        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("c1", DataTypes.ByteType, false, Metadata.empty()),
                new StructField("c2", DataTypes.ShortType, false, Metadata.empty()),
                new StructField("c3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c4", DataTypes.LongType, false, Metadata.empty()),
                new StructField("c5", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c6", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c7", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("c8", new DecimalType(20, 0), false, Metadata.empty()),
                new StructField("c9", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c10", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c11", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c12", DataTypes.DateType, false, Metadata.empty()),
                new StructField("c13", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("c14", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", String.join(".", DB_NAME, tableName));
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.write.properties.format", csvFormat ? "csv" : "json");

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        // TODO verify the result after read supports json

        spark.stop();
    }

    @Test
    public void testReadWriteInCsvFormatNotContainsJsonColumnBase() throws Exception {
        testReadWriteNotContainsJsonColumnBase(true);
    }

    @Test
    public void testReadWriteInJsonFormatNotContainsJsonColumnBase() throws Exception {
        testReadWriteNotContainsJsonColumnBase(false);
    }

    private void testReadWriteNotContainsJsonColumnBase(boolean csvFormat) throws Exception {
        String tableName = "testWriteContainJsonColumnBase_" + genRandomUuid();
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "c0 BOOLEAN," +
                                "c1 TINYINT," +
                                "c2 SMALLINT," +
                                "c3 INT," +
                                "c4 BIGINT," +
                                "c5 LARGEINT," +
                                "c6 FLOAT," +
                                "c7 DOUBLE," +
                                "c8 DECIMAL(20, 1)," +
                                "c9 CHAR(10)," +
                                "c10 VARCHAR(100)," +
                                "c11 STRING," +
                                "c12 DATE," +
                                "c13 DATETIME" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`c0`, `c1`) " +
                                "DISTRIBUTED BY HASH(`c0`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testReadWriteNotContainsJsonColumnBase")
                .getOrCreate();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                "5",
                6.0f,
                7.0,
                BigDecimal.valueOf(8.1),
                "9",
                "10",
                "11",
                Date.valueOf("2022-01-01"),
                Timestamp.valueOf("2023-01-01 00:00:00")
            ));

        List<Row> data = expectedData.stream().map(list -> list.toArray(new Object[0]))
                .map(RowFactory::create).collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("c1", DataTypes.ByteType, false, Metadata.empty()),
                new StructField("c2", DataTypes.ShortType, false, Metadata.empty()),
                new StructField("c3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c4", DataTypes.LongType, false, Metadata.empty()),
                new StructField("c5", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c6", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c7", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("c8", new DecimalType(20, 1), false, Metadata.empty()),
                new StructField("c9", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c10", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c11", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c12", DataTypes.DateType, false, Metadata.empty()),
                new StructField("c13", DataTypes.TimestampType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", String.join(".", DB_NAME, tableName));
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.write.properties.format", csvFormat ? "csv" : "json");

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        Dataset<Row> readDf = spark.read().format("starrocks")
                .option("starrocks.table.identifier", String.join(".", DB_NAME, tableName))
                .option("starrocks.fe.http.url", FE_HTTP)
                .option("starrocks.fe.jdbc.url", FE_JDBC)
                .option("user", USER)
                .option("password", PASSWORD)
                .load();
        List<Row> readRows = readDf.collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    @Test
    public void testDateTimeJava8API() throws Exception {
        String tableName = "testDateTimeJava8API" + genRandomUuid();
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "id INT," +
                                "dt DATE," +
                                "dtt DATETIME" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`id`) " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);

        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.datetime.java8API.enabled", "true")
                .master("local[1]")
                .appName("testReadWriteNotContainsJsonColumnBase")
                .getOrCreate();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "2023-07-16", "2023-07-16 12:00:00"));

        String ddl = String.format("CREATE TABLE sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES (1, CAST(\"2023-07-16\" as DATE), " +
                "CAST(\"2023-07-16 12:00:00\" AS TIMESTAMP))");

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }

    @Test
    public void testTimeZone() throws Exception {
        String tableName = "testTimeZone" + genRandomUuid();
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "id INT," +
                                "dt DATE," +
                                "dtt DATETIME" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`id`) " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);

        String sparkTimeZone = "+08:00";
        String starrocksTimeZone = "+00:00";
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.session.timeZone", sparkTimeZone)
                .master("local[1]")
                .appName("testReadWriteNotContainsJsonColumnBase")
                .getOrCreate();

        String ddl = String.format("CREATE TABLE sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\",\n" +
                "  \"starrocks.timezone\"=\"%s\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER, PASSWORD, starrocksTimeZone);
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES (1, CAST(\"2023-07-16\" as DATE), " +
                "CAST(\"2023-07-16 06:00:00\" AS TIMESTAMP))");

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(Collections.singletonList(Arrays.asList(1, "2023-07-16", "2023-07-15 22:00:00")), actualWriteData);

        List<Row> readRows = spark.sql("SELECT * FROM sr_table").collectAsList();
        verifyRows(Collections.singletonList(Arrays.asList(1, "2023-07-16", "2023-07-16 06:00:00")), readRows);

        spark.stop();
    }

    @Test
    public void testWritePkBitmapWitCsv() throws Exception {
        testWritePkBitmapBase(false);
    }

    @Test
    public void testWritePkBitmapWitJson() throws Exception {
        testWritePkBitmapBase(true);
    }

    private void testWritePkBitmapBase(boolean useJson) throws Exception {
        String tableName = "testWritePkBitmap_" + genRandomUuid();
        prepareBitmapTable(tableName, true);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testWritePkBitmap")
                .getOrCreate();

        String columnTypes = "userid BIGINT";
        String ddl = String.format("CREATE TABLE sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\",\n" +
                "  \"starrocks.column.types\"=\"%s\",\n" +
                "  \"starrocks.write.properties.format\"=\"%s\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER,
                    PASSWORD, columnTypes, (useJson ? "json" : "csv"));
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES ('age', '18', 3), ('gender', 'male', 5)");

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList("age", "18", "3"));
        expectedData.add(Arrays.asList("gender", "male", "5"));

        String query = String.format("SELECT tagname, tagvalue, bitmap_to_string(userid) FROM `%s`.`%s`", DB_NAME, tableName);;
        List<List<Object>> actualWriteData = queryTable(DB_CONNECTION, query);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }

    @Test
    public void testWriteAggBitmapWithCsv() throws Exception {
        testWriteAggBitmapBase(false);
    }

    @Test
    public void testWriteAggBitmapWithJson() throws Exception {
        testWriteAggBitmapBase(true);
    }

    private void testWriteAggBitmapBase(boolean useJson) throws Exception {
        String tableName = "testWriteAggBitmap_" + genRandomUuid();
        prepareBitmapTable(tableName, false);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testWriteAggBitmap")
                .getOrCreate();

        String columnTypes = "userid BIGINT";
        String ddl = String.format("CREATE TABLE sr_table \n" +
                        " USING starrocks\n" +
                        "OPTIONS(\n" +
                        "  \"starrocks.table.identifier\"=\"%s\",\n" +
                        "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                        "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                        "  \"starrocks.user\"=\"%s\",\n" +
                        "  \"starrocks.password\"=\"%s\",\n" +
                        "  \"starrocks.column.types\"=\"%s\",\n" +
                        "  \"starrocks.write.properties.format\"=\"%s\"\n" +
                    ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER,
                            PASSWORD, columnTypes, (useJson ? "json" : "csv"));
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES ('age', '18', 3), ('gender', 'male', 3)");
        spark.sql("INSERT INTO sr_table VALUES ('age', '18', 5), ('gender', 'female', 5)");

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList("age", "18", "3,5"));
        expectedData.add(Arrays.asList("gender", "female", "5"));
        expectedData.add(Arrays.asList("gender", "male", "3"));

        String query = String.format("SELECT tagname, tagvalue, bitmap_to_string(userid) FROM `%s`.`%s`", DB_NAME, tableName);;
        List<List<Object>> actualWriteData = queryTable(DB_CONNECTION, query);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }

    private void prepareBitmapTable(String tableName, boolean isPk) throws Exception {
        String pkTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "tagname STRING," +
                                "tagvalue STRING," +
                                "userid BITMAP" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`tagname`, `tagvalue`) " +
                                "DISTRIBUTED BY HASH(`tagname`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        String aggTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "tagname STRING," +
                                "tagvalue STRING," +
                                "userid BITMAP BITMAP_UNION" +
                                ") ENGINE=OLAP " +
                                "AGGREGATE KEY(`tagname`, `tagvalue`) " +
                                "DISTRIBUTED BY HASH(`tagname`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(isPk ? pkTable : aggTable);
    }

    @Test
    public void testWriteHllWithCsv() throws Exception {
        testWriteHllBase(false);
    }

    @Test
    public void testWriteHllWithJson() throws Exception {
        testWriteHllBase(true);
    }

    private void testWriteHllBase(boolean useJson) throws Exception {
        String tableName = "testWriteHll_" + genRandomUuid();
        prepareHllTable(tableName);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testWriteHll")
                .getOrCreate();

        String columnTypes = "userid BIGINT";
        String ddl = String.format("CREATE TABLE sr_table \n" +
                        " USING starrocks\n" +
                        "OPTIONS(\n" +
                        "  \"starrocks.table.identifier\"=\"%s\",\n" +
                        "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                        "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                        "  \"starrocks.user\"=\"%s\",\n" +
                        "  \"starrocks.password\"=\"%s\",\n" +
                        "  \"starrocks.column.types\"=\"%s\",\n" +
                        "  \"starrocks.write.properties.format\"=\"%s\"\n" +
                    ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER,
                        PASSWORD, columnTypes, (useJson ? "json" : "csv"));
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES ('age', '18', 3), ('gender', 'male', 3)");
        spark.sql("INSERT INTO sr_table VALUES ('age', '18', 5), ('gender', 'female', 5)");

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList("age", "18", "2"));
        expectedData.add(Arrays.asList("gender", "female", "1"));
        expectedData.add(Arrays.asList("gender", "male", "1"));

        String query = String.format("SELECT tagname, tagvalue, HLL_CARDINALITY(userid) FROM `%s`.`%s`", DB_NAME, tableName);;
        List<List<Object>> actualWriteData = queryTable(DB_CONNECTION, query);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }

    private void prepareHllTable(String tableName) throws Exception {
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "tagname STRING," +
                                "tagvalue STRING," +
                                "userid HLL HLL_UNION" +
                                ") ENGINE=OLAP " +
                                "AGGREGATE KEY(`tagname`, `tagvalue`) " +
                                "DISTRIBUTED BY HASH(`tagname`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);
    }
}

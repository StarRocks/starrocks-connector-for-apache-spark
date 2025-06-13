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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReadWriteITTest extends ITTestBase {
    @Test
    public void testOutputColNames() throws Exception {
        String tableName = "testOutputColNames_" + genRandomUuid();
        prepareScoreBoardTable(tableName);

        try (Statement statement = DB_CONNECTION.createStatement()) {
            statement.execute("insert into " + DB_NAME + "." + tableName + " VALUES (1, '2', 3), (2, '3', 4)");
        }

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();
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


        List<Row> rows = spark.sql("select id, id, upper(name) as upper_name from sr_table where score = 4")
                .collectAsList();
        Assertions.assertEquals(1, rows.size());

        GenericRowWithSchema row = (GenericRowWithSchema) rows.get(0);
        Assertions.assertEquals(1, row.schema().fieldIndex("id"));
        Assertions.assertEquals(2, row.schema().fieldIndex("upper_name"));

        spark.stop();
    }

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

        StructType schema = new StructType(new StructField[] {
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
    public void testMultiTableJoinSql() throws Exception {
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

        String ddl = String.format("CREATE TEMPORARY VIEW sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.filter.query\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\"\n" +
                ")", String.join(".", DB_NAME, tableName), "id=1", FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES (1, \"2\", 3), (2, \"3\", 4)");

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        List<List<Object>> joinExpectedData = new ArrayList<>();
        joinExpectedData.add(Arrays.asList(1, "2", 3, 1, "2", 3));
        joinExpectedData.add(Arrays.asList(2, "3", 4, 2, "3", 4));
        Dataset<Row> df = spark.sql("SELECT a.*, b.* FROM sr_table a join sr_table b on a.id=b.id");
        verifyRows(joinExpectedData, df.collectAsList());

        spark.stop();
    }

    @Test
    public void testPartialUpdates() throws Exception {
        String tableName = "testPartialUpdates_" + genRandomUuid();
        prepareScoreBoardTable(tableName);
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (1, '1', 100), (2, '2', 200)", DB_NAME, tableName));
        verifyResult(Arrays.asList(Arrays.asList(1, "1", 100), Arrays.asList(2, "2", 200)),
                scanTable(DB_CONNECTION, DB_NAME, tableName));

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testSql")
                .getOrCreate();

        String ddl = String.format("CREATE TABLE sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\",\n" +
                "  \"starrocks.columns\"=\"id,score\",\n" +
                "  \"starrocks.write.properties.partial_update\"=\"true\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES (1, 101), (2, 199)");

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "1", 101));
        expectedData.add(Arrays.asList(2, "2", 199));
        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        expectedData.clear();
        expectedData.add(Arrays.asList(1, 101));
        expectedData.add(Arrays.asList(2, 199));
        List<Row> readRows = spark.sql("SELECT * FROM sr_table").collectAsList();
        verifyRows(expectedData, readRows);

        String dropTable = String.format("DROP TABLE sr_table");
        spark.sql(dropTable);

        spark.stop();
    }

    @Test
    public void testConditionalUpdates() throws Exception {
        String tableName = "testConditionalUpdates_" + genRandomUuid();
        prepareScoreBoardTable(tableName);
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (1, '1', 100), (2, '2', 200)", DB_NAME, tableName));
        verifyResult(Arrays.asList(Arrays.asList(1, "1", 100), Arrays.asList(2, "2", 200)),
                scanTable(DB_CONNECTION, DB_NAME, tableName));

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testSql")
                .getOrCreate();

        String ddl = String.format("CREATE TABLE sr_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\",\n" +
                "  \"starrocks.write.properties.merge_condition\"=\"score\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("INSERT INTO sr_table VALUES (1, '2', 101), (2, '3', 199)");

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "2", 101));
        expectedData.add(Arrays.asList(2, "2", 200));
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

        StructType schema = new StructType(new StructField[] {
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

    public static void prepareScoreBoardTable(String tableName) throws Exception {
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
                    "c8 DECIMAL(20, 1)," +
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
        executeSrSQL(createStarRocksTable);

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
                BigDecimal.valueOf(8.1),
                "9",
                "10",
                "11",
                Date.valueOf("2022-01-01"),
                Timestamp.valueOf("2023-01-01 00:00:00"),
                "{\"key\": 1, \"value\": 2}"
        );
        data.add(row);

        StructType schema = new StructType(new StructField[] {
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
                Timestamp.valueOf("2023-01-01 00:00:00"),
                "{\"key\": 1, \"value\": 2}"
        ));


        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

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
        executeSrSQL(createStarRocksTable);

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

        StructType schema = new StructType(new StructField[] {
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
        executeSrSQL(createStarRocksTable);

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
        executeSrSQL(createStarRocksTable);

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

        String dropTable = String.format("DROP TABLE sr_table");
        spark.sql(dropTable);

        spark.stop();
    }

    @Test
    public void testWritePkBitmapWitCsv() throws Exception {
        testWritePkBitmapBase(false);
    }

    @Test
    public void testDate() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("+00:00"));
        String value = "2023-07-15 22:00:00";
        LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
        ZonedDateTime inputZone = ZonedDateTime.of(localDateTime, ZoneId.of("+00:00"));
        ZonedDateTime outZone = ZonedDateTime.of(localDateTime, ZoneId.of("+08:00"));

        System.out.println(formatter.format(inputZone));
        System.out.println(formatter.format(outZone));
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

        String query = String.format("SELECT tagname, tagvalue, bitmap_to_string(userid) FROM `%s`.`%s`", DB_NAME, tableName);

        List<List<Object>> actualWriteData = queryTable(DB_CONNECTION, query);
        verifyResult(expectedData, actualWriteData);

        String dropTable = String.format("DROP TABLE sr_table");
        spark.sql(dropTable);

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

        String query = String.format("SELECT tagname, tagvalue, bitmap_to_string(userid) FROM `%s`.`%s`", DB_NAME, tableName);
        ;
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
        executeSrSQL(isPk ? pkTable : aggTable);
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

        String query = String.format("SELECT tagname, tagvalue, HLL_CARDINALITY(userid) FROM `%s`.`%s`", DB_NAME, tableName);

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
        executeSrSQL(createStarRocksTable);
    }

    @Test
    public void testJsonType() throws Exception {
        String tableName = "testJsonType_" + genRandomUuid();
        prepareJsonTable(tableName);
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testJsonType")
                .getOrCreate();

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
        spark.sql("INSERT INTO sr_table VALUES (1, 1.1, '{\"a\": 1, \"b\": true}'), (2, 2.2, '{\"a\": 2, \"b\": false}')");

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, 1.1, "{\"a\": 1, \"b\": true}"));
        expectedData.add(Arrays.asList(2, 2.2, "{\"a\": 2, \"b\": false}"));

        List<Row> readRows = spark.sql("SELECT * FROM sr_table").collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    private void prepareJsonTable(String tableName) throws Exception {
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 DOUBLE," +
                                "c2 JSON" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`c0`) " +
                                "DISTRIBUTED BY HASH(`c0`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
    }

    @Test
    public void testArrayWithCsv() throws Exception {
        testArrayBase(false);
    }

    @Test
    public void testArrayWithJson() throws Exception {
        testArrayBase(true);
    }

    private void testArrayBase(boolean useJson) throws Exception {
        String tableName = prepareArrayTable("testArray");

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testArray")
                .getOrCreate();

        List<List<Object>> expectedData = new ArrayList<>();
        // TODO not support null nested array, and " in string
//        expectedData.add(Arrays.asList(
//                1,
//                Arrays.asList(true, false, null),
//                Arrays.asList(1, 2, null),
//                Arrays.asList(1.1, 2.2, null),
//                Arrays.asList(BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), null),
//                Arrays.asList("1[]',\"", "2[]',\"", null),
//                Arrays.asList(Date.valueOf("2022-01-01"), Date.valueOf("2022-02-02"), null),
//                Arrays.asList(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-02-02 00:00:00"), null),
//                Arrays.asList(Arrays.asList(true, false, null), Arrays.asList(true, false, null), null),
//                Arrays.asList(Arrays.asList(1, 2, null), Arrays.asList(1, 2, null), null),
//                Arrays.asList(Arrays.asList(1.1, 2.2, null), Arrays.asList(1.1, 2.2, null), null),
//                Arrays.asList(Arrays.asList(BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), null),
//                        Arrays.asList(BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), null), null),
//                Arrays.asList(Arrays.asList("1[]',\"", "2[]',\"", null), Arrays.asList("1[]',\"", "2[]',\"", null),
//                Arrays.asList(Arrays.asList(Date.valueOf("2022-01-01"), Date.valueOf("2022-02-02"), null),
//                        Arrays.asList(Date.valueOf("2022-01-01"), Date.valueOf("2022-02-02"), null), null),
//                Arrays.asList(Arrays.asList(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-02-02 00:00:00"), null),
//                        Arrays.asList(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-02-02 00:00:00"), null), null)
//        ));

        expectedData.add(Arrays.asList(
                1,
                Arrays.asList(true, false, null),
                Arrays.asList(1, 2, null),
                Arrays.asList(1.1, 2.2, null),
                Arrays.asList(BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), null),
                Arrays.asList("1[]',", "2[]',", null),
                Arrays.asList(Date.valueOf("2022-01-01"), Date.valueOf("2022-02-02"), null),
                Arrays.asList(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-02-02 00:00:00"), null),
                Arrays.asList(Arrays.asList(true, false, null), Arrays.asList(true, false, null)),
                Arrays.asList(Arrays.asList(1, 2, null), Arrays.asList(1, 2, null)),
                Arrays.asList(Arrays.asList(1.1, 2.2, null), Arrays.asList(1.1, 2.2, null)),
                Arrays.asList(Arrays.asList(BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), null),
                        Arrays.asList(BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), null)),
                Arrays.asList(Arrays.asList("1", "2", null), Arrays.asList("1", "2", null)),
                Arrays.asList(Arrays.asList(Date.valueOf("2022-01-01"), Date.valueOf("2022-02-02"), null),
                        Arrays.asList(Date.valueOf("2022-01-01"), Date.valueOf("2022-02-02"), null)),
                Arrays.asList(
                        Arrays.asList(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-02-02 00:00:00"), null),
                        Arrays.asList(Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("2023-02-02 00:00:00"), null))
        ));

        List<Row> data = expectedData.stream().map(list -> list.toArray(new Object[0]))
                .map(RowFactory::create).collect(Collectors.toList());

        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("a0_bool", new ArrayType(DataTypes.BooleanType, true), false, Metadata.empty()),
                new StructField("a0_int", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty()),
                new StructField("a0_double", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField("a0_decimal", new ArrayType(new DecimalType(20, 1), true), false, Metadata.empty()),
                new StructField("a0_string", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()),
                new StructField("a0_date", new ArrayType(DataTypes.DateType, true), false, Metadata.empty()),
                new StructField("a0_datetime", new ArrayType(DataTypes.TimestampType, true), false, Metadata.empty()),
                new StructField("a1_bool", new ArrayType(new ArrayType(DataTypes.BooleanType, true), true), false,
                        Metadata.empty()),
                new StructField("a1_int", new ArrayType(new ArrayType(DataTypes.IntegerType, true), true), false,
                        Metadata.empty()),
                new StructField("a1_double", new ArrayType(new ArrayType(DataTypes.DoubleType, true), true), false,
                        Metadata.empty()),
                new StructField("a1_decimal", new ArrayType(new ArrayType(new DecimalType(20, 1), true), true), false,
                        Metadata.empty()),
                new StructField("a1_string", new ArrayType(new ArrayType(DataTypes.StringType, true), true), false,
                        Metadata.empty()),
                new StructField("a1_date", new ArrayType(new ArrayType(DataTypes.DateType, true), true), false, Metadata.empty()),
                new StructField("a1_datetime", new ArrayType(new ArrayType(DataTypes.TimestampType, true), true), false,
                        Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        String columnTypes = "a0_bool ARRAY<BOOLEAN>," +
                "a0_int ARRAY<INT>," +
                "a0_double ARRAY<DOUBLE>," +
                "a0_decimal ARRAY<DECIMAL(10, 1)>," +
                "a0_string ARRAY<STRING>," +
                "a0_date ARRAY<DATE>," +
                "a0_datetime ARRAY<TIMESTAMP>," +
                "a1_bool ARRAY<ARRAY<BOOLEAN>>," +
                "a1_int ARRAY<ARRAY<INT>>," +
                "a1_double ARRAY<ARRAY<DOUBLE>>," +
                "a1_decimal ARRAY<ARRAY<DECIMAL(10, 1)>>," +
                "a1_string ARRAY<ARRAY<STRING>>," +
                "a1_date ARRAY<ARRAY<DATE>>," +
                "a1_datetime ARRAY<ARRAY<TIMESTAMP>>";

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", String.join(".", DB_NAME, tableName));
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.column.types", columnTypes);
        options.put("starrocks.write.properties.format", useJson ? "json" : "csv");
        options.put("starrocks.write.properties.strict_mode", "true");

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        // replace the boolean with int because starrocks return true as 1, and false as 0
        expectedData.get(0).set(1, Arrays.asList(1, 0, null));
        expectedData.get(0).set(8, Arrays.asList(Arrays.asList(1, 0, null), Arrays.asList(1, 0, null)));
        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }

    private String prepareArrayTable(String tableNamePrefix) throws Exception {
        String tableName = tableNamePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "id INT," +
                                "a0_bool ARRAY<BOOLEAN>," +
                                "a0_int ARRAY<INT>," +
                                "a0_double ARRAY<DOUBLE>," +
                                "a0_decimal ARRAY<DECIMAL(10, 1)>," +
                                "a0_string ARRAY<STRING>," +
                                "a0_date ARRAY<DATE>," +
                                "a0_datetime ARRAY<DATETIME>," +
                                "a1_bool ARRAY<ARRAY<BOOLEAN>>," +
                                "a1_int ARRAY<ARRAY<INT>>," +
                                "a1_double ARRAY<ARRAY<DOUBLE>>," +
                                "a1_decimal ARRAY<ARRAY<DECIMAL(10, 1)>>," +
                                "a1_string ARRAY<ARRAY<STRING>>," +
                                "a1_date ARRAY<ARRAY<DATE>>," +
                                "a1_datetime ARRAY<ARRAY<DATETIME>>" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`id`) " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

    @Test
    public void testJsonLz4Compression() throws Exception {
        String tableName = "testJsonLz4Compression_" + genRandomUuid();
        prepareScoreBoardTable(tableName);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testJsonLz4Compression")
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
                "  \"starrocks.write.properties.format\"=\"json\",\n" +
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
    public void testArrowVectorNameIsEmptyForTimestampType() throws Exception {
        String tableName = "testArrowVectorNameIsEmptyForTimestampType_" + genRandomUuid();
        prepareDateAndDateTimeTable(tableName);
        executeSrSQL(
                String.format("INSERT INTO `%s`.`%s` VALUES (1, '1', '2024-04-20', '2024-04-20 19:00:00')",
                    DB_NAME, tableName));

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testArrowVectorNameIsEmptyForTimestampType")
                .getOrCreate();

        String ddl = String.format("CREATE TABLE src \n" +
                "USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"starrocks.user\"=\"%s\",\n" +
                "  \"starrocks.password\"=\"%s\"\n" +
                ")", String.join(".", DB_NAME, tableName), FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        List<Row> readRows = spark.sql("SELECT c1, c2, c3 FROM src WHERE c0 = 1;").collectAsList();

        List<List<Object>> expectedData = Collections.singletonList(
                Arrays.asList(
                        "1",
                        Date.valueOf("2024-04-20"),
                        Timestamp.valueOf("2024-04-20 19:00:00")
                )
        );
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    private void prepareDateAndDateTimeTable(String tableName) throws Exception {
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 STRING," +
                                "c2 DATE," +
                                "c3 DATETIME" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`c0`) " +
                                "DISTRIBUTED BY HASH(`c0`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
    }

    @Test
    public void testTimestampTypeWithMilliseconds() throws Exception {
        String tableName = "testTimestampTypeWithMilliseconds" + genRandomUuid();
        prepareTimestampTypeWithMilliseconds(tableName);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testTimestampTypeWithMilliseconds")
                .getOrCreate();

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
        spark.sql("INSERT INTO sr_table VALUES " +
                "(0, CAST(\"2024-09-19 14:00:00\" AS TIMESTAMP))," +
                "(1, CAST(\"2024-09-19 14:01:00.123\" AS TIMESTAMP))," +
                "(2, CAST(\"2024-09-19 14:02:00.123456\" AS TIMESTAMP))," +
                "(3, CAST(\"2024-09-19 14:03:00.123456789\" AS TIMESTAMP))"
            );

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(0, Timestamp.valueOf("2024-09-19 14:00:00")));
        expectedData.add(Arrays.asList(1, Timestamp.valueOf("2024-09-19 14:01:00.123")));
        expectedData.add(Arrays.asList(2, Timestamp.valueOf("2024-09-19 14:02:00.123456")));
        expectedData.add(Arrays.asList(3, Timestamp.valueOf("2024-09-19 14:03:00.123456")));

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        List<Row> readRows = spark.sql("SELECT * FROM sr_table").collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    private void prepareTimestampTypeWithMilliseconds(String tableName) throws Exception {
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 DATETIME" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`c0`) " +
                                "DISTRIBUTED BY HASH(`c0`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
    }
    @Test
    public void TestStageWrite() throws Exception {
        String tableName = "testStageWrite" + genRandomUuid();
        prepareStageTest(tableName);
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("TestStageWrite")
                .getOrCreate();
        //test always mode
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c3_double", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("c4_float", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("c5_int", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("c6_largeint", DataTypes.LongType, true, Metadata.empty()), // 对应 LARGEINT，使用 LongType
                new StructField("c7_smallint", DataTypes.ShortType, true, Metadata.empty()),
                new StructField("c8_tinyint", DataTypes.ByteType, true, Metadata.empty()),
                new StructField("c9_date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("c10_datetime", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("c11_char", DataTypes.StringType, true, Metadata.empty()),
        });
        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(1, 12345.12, 12345.12f, 123456, 987654321012345671L, (short) 1234, (byte) 123, Date.valueOf("2024-01-01"), Timestamp.valueOf("2024-01-01 12:34:56"), "char_value_11"));
        data.add(RowFactory.create(2, 23456.23, 23456.23f, 234567, 887654321012345672L, (short) 2345, (byte) 234, Date.valueOf("2024-02-02"), Timestamp.valueOf("2024-02-02 12:34:56"), "char_value_22"));
        data.add(RowFactory.create(3, 34567.34, 34567.34f, 345678, 787654321012345673L, (short) 3456, (byte) 345, Date.valueOf("2024-03-03"), Timestamp.valueOf("2024-03-03 12:34:56"), "char_value_33"));
        Dataset<Row> df = spark.createDataFrame(data, schema);
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", String.join(".", DB_NAME, tableName));
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.write.stage.mode", "true");
        options.put("starrocks.write.properties.partial_update_mode", "column");
        options.put("starrocks.write.properties.partial_update", "true");
        options.put("starrocks.write.stage.session.query.timeout", "309");
        options.put("starrocks.write.stage.session.query.mem.limit", "100000000");
        options.put("starrocks.write.stage.session.exec.mem.limit", "8589934592");
        options.put("starrocks.write.stage.columns.update.ratio", "20");

        options.put("starrocks.write.stage.use", "always");
        options.put("starrocks.columns", "id,c3_double,c4_float,c5_int,c6_largeint,c7_smallint,c8_tinyint,c9_date,c10_datetime,c11_char");
        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 12345.12, 12345.12f, 123456, 987654321012345671L, (short) 1234, (byte) 123, Date.valueOf("2024-01-01"), Timestamp.valueOf("2024-01-01 12:34:56"), "char_value_11"),
                Arrays.asList(2, 23456.23, 23456.23f, 234567, 887654321012345672L, (short) 2345, (byte) 234, Date.valueOf("2024-02-02"), Timestamp.valueOf("2024-02-02 12:34:56"), "char_value_22"),
                Arrays.asList(3, 34567.34, 34567.34f, 345678, 787654321012345673L, (short) 3456, (byte) 345, Date.valueOf("2024-03-03"), Timestamp.valueOf("2024-03-03 12:34:56"), "char_value_33")
        );
        String selectSql = String.format("SELECT id,c3_double,c4_float,c5_int,c6_largeint,c7_smallint,c8_tinyint,c9_date,c10_datetime,c11_char FROM %s.%s where id between 1 and 3 ", DB_NAME, tableName);
        List<List<Object>> actual = queryTable(DB_CONNECTION, selectSql);
        verifyResult(expectedData, actual);
        //test auto mode
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, 101L, false, new BigDecimal("111.1")),
                RowFactory.create(2, 102L, true, new BigDecimal("111.2")),
                RowFactory.create(3, 103L, false, new BigDecimal("111.3")),
                RowFactory.create(4, 104L, true, new BigDecimal("111.4"))
        );
        StructType schema1 = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c0_bigint", DataTypes.LongType, true, Metadata.empty()),
                new StructField("c1_boolean", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("c2_decimal", DataTypes.createDecimalType(10, 1), true, Metadata.empty()),

        });
        Dataset<Row> df1 = spark.createDataFrame(data1, schema1);
        options.put("starrocks.write.stage.use", "auto");
        options.put("starrocks.columns", "id,c0_bigint,c1_boolean,c2_decimal");
        options.put("starrocks.write.stage.columns.update.ratio", "60");
        df1.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();
        String selectSql1 = String.format("SELECT id,c0_bigint,c1_boolean,c2_decimal FROM %s.%s where id between 1 and 4 ", DB_NAME, tableName);
        List<List<Object>> actual1 = queryTable(DB_CONNECTION, selectSql1);
        List<List<Object>> expectedData1 = Arrays.asList(
                Arrays.asList(1, 101L, false, new BigDecimal("111.1")),
                Arrays.asList(2, 102L, true, new BigDecimal("111.2")),
                Arrays.asList(3, 103L, false, new BigDecimal("111.3")),
                Arrays.asList(4, 104L, true, new BigDecimal("111.4"))
        );
        verifyResult(expectedData1, actual1);

//test never mode
        List<Row> data2 = Arrays.asList(
                RowFactory.create(1, 102L, false, new BigDecimal("111.1")),
                RowFactory.create(2, 103L, true, new BigDecimal("111.2")),
                RowFactory.create(3, 104L, false, new BigDecimal("111.3")),
                RowFactory.create(4, 105L, true, new BigDecimal("111.4"))
        );
        List<List<Object>> expectedData2 = Arrays.asList(
                Arrays.asList(1, 102L, false, new BigDecimal("111.1")),
                Arrays.asList(2, 103L, true, new BigDecimal("111.2")),
                Arrays.asList(3, 104L, false, new BigDecimal("111.3")),
                Arrays.asList(4, 105L, true, new BigDecimal("111.4"))
        );
        options.put("starrocks.write.stage.use", "never");
        Dataset<Row> df2 = spark.createDataFrame(data2, schema1);
        df2.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();
        List<List<Object>> actual2 = queryTable(DB_CONNECTION, selectSql1);
        verifyResult(expectedData2,actual2);
        spark.stop();

    }





    private void prepareStageTest(String tableName) throws Exception {
        String createStarRocksTable =
                String.format("CREATE TABLE %s.%s (" +
                                "id INT," +                        // 主键
                                "c0_bigint BIGINT," +
                                "c1_boolean BOOLEAN," +
                                "c2_decimal DECIMAL(10, 1)," +
                                "c3_double DOUBLE," +
                                "c4_float FLOAT," +
                                "c5_int INT," +
                                "c6_largeint LARGEINT," +
                                "c7_smallint SMALLINT," +
                                "c8_tinyint TINYINT," +
                                "c9_date DATE," +
                                "c10_datetime DATETIME," +
                                "c11_char CHAR(50)," +
                                "c12_string STRING," +
                                "c13_varchar VARCHAR(255)" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(id) " +
                                "DISTRIBUTED BY HASH(id) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);

        executeSrSQL(createStarRocksTable);

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("prepareTestStage")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c0_bigint", DataTypes.LongType, true, Metadata.empty()),
                new StructField("c1_boolean", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("c2_decimal", DataTypes.createDecimalType(10, 1), true, Metadata.empty()),
                new StructField("c3_double", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("c4_float", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("c5_int", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("c6_largeint", DataTypes.LongType, true, Metadata.empty()), // 对应 LARGEINT，使用 LongType
                new StructField("c7_smallint", DataTypes.ShortType, true, Metadata.empty()),
                new StructField("c8_tinyint", DataTypes.ByteType, true, Metadata.empty()),
                new StructField("c9_date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("c10_datetime", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("c11_char", DataTypes.StringType, true, Metadata.empty()),
                new StructField("c12_string", DataTypes.StringType, true, Metadata.empty()),
                new StructField("c13_varchar", DataTypes.StringType, true, Metadata.empty())
        });


        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(1, 1234567890123L, true, new BigDecimal("12345.6"), 12345.67, 12345.67f, 12345, 987654321012345678L, (short) 123, (byte) 12, Date.valueOf("2023-01-01"), Timestamp.valueOf("2023-01-01 12:34:56"), "char_value_1", "string_value_1", "varchar_value_1"));
        data.add(RowFactory.create(2, 2234567890123L, false, new BigDecimal("23456.7"), 23456.78, 23456.78f, 23456, 887654321012345678L, (short) 234, (byte) 23, Date.valueOf("2023-02-02"), Timestamp.valueOf("2023-02-02 12:34:56"), "char_value_2", "string_value_2", "varchar_value_2"));
        data.add(RowFactory.create(3, 3234567890123L, true, new BigDecimal("34567.8"), 34567.89, 34567.89f, 34567, 787654321012345678L, (short) 345, (byte) 34, Date.valueOf("2023-03-03"), Timestamp.valueOf("2023-03-03 12:34:56"), "char_value_3", "string_value_3", "varchar_value_3"));
        data.add(RowFactory.create(4, 4234567890123L, false, new BigDecimal("45678.9"), 45678.90, 45678.90f, 45678, 687654321012345678L, (short) 456, (byte) 45, Date.valueOf("2023-04-04"), Timestamp.valueOf("2023-04-04 12:34:56"), "char_value_4", "string_value_4", "varchar_value_4"));
        data.add(RowFactory.create(5, 5234567890123L, true, new BigDecimal("56789.0"), 56789.01, 56789.01f, 56789, 587654321012345678L, (short) 567, (byte) 56, Date.valueOf("2023-05-05"), Timestamp.valueOf("2023-05-05 12:34:56"), "char_value_5", "string_value_5", "varchar_value_5"));
        data.add(RowFactory.create(6, 6234567890123L, false, new BigDecimal("67890.1"), 67890.12, 67890.12f, 67890, 487654321012345678L, (short) 678, (byte) 67, Date.valueOf("2023-06-06"), Timestamp.valueOf("2023-06-06 12:34:56"), "char_value_6", "string_value_6", "varchar_value_6"));
        data.add(RowFactory.create(7, 7234567890123L, true, new BigDecimal("78901.2"), 78901.23, 78901.23f, 78901, 387654321012345678L, (short) 789, (byte) 78, Date.valueOf("2023-07-07"), Timestamp.valueOf("2023-07-07 12:34:56"), "char_value_7", "string_value_7", "varchar_value_7"));
        data.add(RowFactory.create(8, 8234567890123L, false, new BigDecimal("89012.3"), 89012.34, 89012.34f, 89012, 287654321012345678L, (short) 890, (byte) 89, Date.valueOf("2023-08-08"), Timestamp.valueOf("2023-08-08 12:34:56"), "char_value_8", "string_value_8", "varchar_value_8"));
        data.add(RowFactory.create(9, 9234567890123L, true, new BigDecimal("90123.4"), 90123.45, 90123.45f, 90123, 187654321012345678L, (short) 901, (byte) 90, Date.valueOf("2023-09-09"), Timestamp.valueOf("2023-09-09 12:34:56"), "char_value_9", "string_value_9", "varchar_value_9"));
        data.add(RowFactory.create(10, 10234567890123L, false, new BigDecimal("01234.5"), 01234.56, 01234.56f, 1234, 98765432101234567L, (short) 12, (byte) 1, Date.valueOf("2023-10-10"), Timestamp.valueOf("2023-10-10 12:34:56"), "char_value_10", "string_value_10", "varchar_value_10"));

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("starrocks")
                .option("starrocks.fe.http.url", FE_HTTP)
                .option("starrocks.fe.jdbc.url", FE_JDBC)
                .option("starrocks.user", USER)
                .option("starrocks.password", PASSWORD)
                .option("starrocks.table.identifier", DB_NAME + "." + tableName)
                .mode(SaveMode.Append)
                .save();

        String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, tableName);

        List<List<Object>> actualRows = queryTable(DB_CONNECTION, selectSql);

        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 1234567890123L, true, new BigDecimal("12345.6"), 12345.67, 12345.67f, 12345, 987654321012345678L, (short) 123, (byte) 12, Date.valueOf("2023-01-01"), Timestamp.valueOf("2023-01-01 12:34:56"), "char_value_1", "string_value_1", "varchar_value_1"),
                Arrays.asList(2, 2234567890123L, false, new BigDecimal("23456.7"), 23456.78, 23456.78f, 23456, 887654321012345678L, (short) 234, (byte) 23, Date.valueOf("2023-02-02"), Timestamp.valueOf("2023-02-02 12:34:56"), "char_value_2", "string_value_2", "varchar_value_2"),
                Arrays.asList(3, 3234567890123L, true, new BigDecimal("34567.8"), 34567.89, 34567.89f, 34567, 787654321012345678L, (short) 345, (byte) 34, Date.valueOf("2023-03-03"), Timestamp.valueOf("2023-03-03 12:34:56"), "char_value_3", "string_value_3", "varchar_value_3"),
                Arrays.asList(4, 4234567890123L, false, new BigDecimal("45678.9"), 45678.90, 45678.90f, 45678, 687654321012345678L, (short) 456, (byte) 45, Date.valueOf("2023-04-04"), Timestamp.valueOf("2023-04-04 12:34:56"), "char_value_4", "string_value_4", "varchar_value_4"),
                Arrays.asList(5, 5234567890123L, true, new BigDecimal("56789.0"), 56789.01, 56789.01f, 56789, 587654321012345678L, (short) 567, (byte) 56, Date.valueOf("2023-05-05"), Timestamp.valueOf("2023-05-05 12:34:56"), "char_value_5", "string_value_5", "varchar_value_5"),
                Arrays.asList(6, 6234567890123L, false, new BigDecimal("67890.1"), 67890.12, 67890.12f, 67890, 487654321012345678L, (short) 678, (byte) 67, Date.valueOf("2023-06-06"), Timestamp.valueOf("2023-06-06 12:34:56"), "char_value_6", "string_value_6", "varchar_value_6"),
                Arrays.asList(7, 7234567890123L, true, new BigDecimal("78901.2"), 78901.23, 78901.23f, 78901, 387654321012345678L, (short) 789, (byte) 78, Date.valueOf("2023-07-07"), Timestamp.valueOf("2023-07-07 12:34:56"), "char_value_7", "string_value_7", "varchar_value_7"),
                Arrays.asList(8, 8234567890123L, false, new BigDecimal("89012.3"), 89012.34, 89012.34f, 89012, 287654321012345678L, (short) 890, (byte) 89, Date.valueOf("2023-08-08"), Timestamp.valueOf("2023-08-08 12:34:56"), "char_value_8", "string_value_8", "varchar_value_8"),
                Arrays.asList(9, 9234567890123L, true, new BigDecimal("90123.4"), 90123.45, 90123.45f, 90123, 187654321012345678L, (short) 901, (byte) 90, Date.valueOf("2023-09-09"), Timestamp.valueOf("2023-09-09 12:34:56"), "char_value_9", "string_value_9", "varchar_value_9"),
                Arrays.asList(10, 10234567890123L, false, new BigDecimal("01234.5"), 01234.56, 01234.56f, 1234, 98765432101234567L, (short) 12, (byte) 1, Date.valueOf("2023-10-10"), Timestamp.valueOf("2023-10-10 12:34:56"), "char_value_10", "string_value_10", "varchar_value_10")
        );
        verifyResult(expectedData, actualRows);

    }
}

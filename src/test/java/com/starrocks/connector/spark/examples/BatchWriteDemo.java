package com.starrocks.connector.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// StarRocks Table
//    CREATE TABLE `test` (
//        `c0` int(11) NULL COMMENT "",
//        `c1` varchar(20) NULL COMMENT ""
//    ) ENGINE=OLAP
//    DUPLICATE KEY(`c0`)
//    COMMENT "OLAP"
//    DISTRIBUTED BY HASH(`c0`) BUCKETS 2
//    PROPERTIES (
//        "replication_num" = "1",
//        "in_memory" = "false",
//        "storage_format" = "DEFAULT",
//        "enable_persistent_index" = "false",
//        "compression" = "LZ4"
//    );

public class BatchWriteDemo {

    private static void test_pufa_sql() throws Exception {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.codegen.wholeStage", "false");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .master("local[1]")
                .appName("StarRocksSqlDemo")
                .getOrCreate();

//        StarRocks Table
//        CREATE TABLE `bowen_kyuubi_out` (
//            `id` int(11) NULL COMMENT "",
//            `name` varchar(65533) NULL COMMENT "",
//            `age` int(11) NULL COMMENT ""
//        ) ENGINE=OLAP
//        DUPLICATE KEY(`id`)
//        COMMENT "OLAP"
//        DISTRIBUTED BY HASH(`id`) BUCKETS 2
//        PROPERTIES (
//            "replication_num" = "1",
//            "in_memory" = "false",
//            "storage_format" = "DEFAULT",
//            "enable_persistent_index" = "false",
//            "replicated_storage" = "true",
//            "compression" = "LZ4"
//        );

        spark.sql("CREATE TEMPORARY VIEW sr_kyuubi_out (\n" +
                "id int,\n" +
                "name string,\n" +
                "age int\n" +
                ") USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"spark.starrocks.fe.urls.http\"=\"127.0.0.1:11901\",\n" +
                "  \"spark.starrocks.fe.urls.jdbc\"=\"jdbc:mysql://127.0.0.1:11903\",\n" +
                "  \"spark.starrocks.database\"=\"starrocks\",\n" +
                "  \"spark.starrocks.table\"=\"bowen_kyuubi_out\",\n" +
                "  \"spark.starrocks.username\"=\"root\",\n" +
                "  \"spark.starrocks.password\"=\"\"\n" +
                ");");
        spark.sql("SELECT * FROM sr_kyuubi_out").show();
        spark.sql("INSERT INTO sr_kyuubi_select * from view");
    }



    private static void test_starrocks() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("StarRocksBatchWriteDemo")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(4, "1"),
                RowFactory.create(5, "2"),
                RowFactory.create(6, "3")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("t0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("t1", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

//        Dataset<Row> csvDf = spark.read().schema(schema)
//                .format("csv")
//                .option("sep", ",")
//                .schema(schema)
//                .option("inferSchema", "true")
//                .load("/Users/lpf/Downloads/test.csv");


        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "write");
        options.put("spark.starrocks.fe.urls.http", "127.0.0.1:11901");
        options.put("spark.starrocks.fe.urls.jdbc", "jdbc:mysql://127.0.0.1:11903");
        options.put("spark.starrocks.database", "starrocks");
        options.put("spark.starrocks.table", "test");
        options.put("spark.starrocks.username", "root");
        options.put("spark.starrocks.password", "");

        StructType schema1 = new StructType(new StructField[]{
                new StructField("c0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c1", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df1 = spark.createDataFrame(df.rdd(), schema1);

        df.write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

        spark.stop();
    }

    @Test
    public void testDataFrameSchema() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("StarRocksBatchWriteDemo")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1.23f, 70),
                RowFactory.create(2.34f, 80)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("c2", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c1", DataTypes.IntegerType, false, Metadata.empty())
        });

        StructType schema1 = new StructType(new StructField[]{
                new StructField("c0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c1", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c2", DataTypes.DateType, false, Metadata.empty())
        });
        System.out.println(schema1.toDDL());

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "write");
        options.put("spark.starrocks.fe.urls.http", "127.0.0.1:11901");
        options.put("spark.starrocks.fe.urls.jdbc", "jdbc:mysql://127.0.0.1:11903");
        options.put("spark.starrocks.database", "starrocks");
        options.put("spark.starrocks.table", "test");
        options.put("spark.starrocks.username", "root");
        options.put("spark.starrocks.password", "");

        df.write().format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testSqlInferSchema() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("StarRocksSqlDemo")
                .getOrCreate();

        spark.sql("CREATE TABLE spark_sink \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"spark.starrocks.conf\"=\"write\",\n" +
                "  \"spark.starrocks.fe.urls.http\"=\"127.0.0.1:11901\",\n" +
                "  \"spark.starrocks.fe.urls.jdbc\"=\"jdbc:mysql://127.0.0.1:11903\",\n" +
                "  \"spark.starrocks.database\"=\"starrocks\",\n" +
                "  \"spark.starrocks.table\"=\"test\",\n" +
                "  \"spark.starrocks.username\"=\"root\",\n" +
                "  \"spark.starrocks.password\"=\"\"\n" +
                ");");

        List<Row> data = Arrays.asList(
                RowFactory.create(10, "1"),
                RowFactory.create(11, "2"),
                RowFactory.create(12, "3")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("t0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("t1", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);
        df.createTempView("test");

        spark.sql("INSERT INTO spark_sink SELECT * FROM test;\n");
    }

    @Test
    public void testSqlUserSchema() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("StarRocksSqlDemo")
                .getOrCreate();

        spark.sql("CREATE TABLE spark_sink (\n" +
                "c0 int,\n" +
                "c1 string\n" +
                ") USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"spark.starrocks.conf\"=\"write\",\n" +
                "  \"spark.starrocks.fe.urls.http\"=\"127.0.0.1:11901\",\n" +
                "  \"spark.starrocks.fe.urls.jdbc\"=\"jdbc:mysql://127.0.0.1:11903\",\n" +
                "  \"spark.starrocks.database\"=\"starrocks\",\n" +
                "  \"spark.starrocks.table\"=\"test\",\n" +
                "  \"spark.starrocks.username\"=\"root\",\n" +
                "  \"spark.starrocks.password\"=\"\"\n" +
                ");");

        List<Row> data = Arrays.asList(
                RowFactory.create(10, "1"),
                RowFactory.create(11, "2"),
                RowFactory.create(12, "3")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("t0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("t1", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);
        df.createTempView("test");

        spark.sql("INSERT INTO spark_sink SELECT * FROM test;\n");
    }

    @Test
    public void testStructuredStreaming() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("StarRocksBatchWriteDemo")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "70"),
                RowFactory.create(2, "80")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c1", DataTypes.StringType, false, Metadata.empty())
        });
        spark.createDataFrame(data, schema);

        MemoryStream<Row> memoryStream = new MemoryStream<>(0, spark.sqlContext(), Option.empty(), Encoders.javaSerialization(Row.class));
        Dataset<Row> df = memoryStream.toDF();
        memoryStream.addData(JavaConverters.asScalaIteratorConverter(data.iterator()).asScala().toSeq());

        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "write");
        options.put("spark.starrocks.fe.urls.http", "127.0.0.1:11901");
        options.put("spark.starrocks.fe.urls.jdbc", "jdbc:mysql://127.0.0.1:11903");
        options.put("spark.starrocks.database", "starrocks");
        options.put("spark.starrocks.table", "test");
        options.put("spark.starrocks.username", "root");
        options.put("spark.starrocks.password", "");
        options.put("checkpointLocation", "/Users/lpf/Downloads/spark-cpt");

        StreamingQuery query = df.writeStream().format("starrocks_writer")
                .options(options)
                .start();

        query.awaitTermination();
        spark.stop();
    }

//    @Test
//    public void test() throws Exception {
//        SparkSession spark = SparkSession.builder()
//                .appName("MemoryStreamExample")
//                .getOrCreate();
//
//        // 定义模式（Schema）用于流式DataFrame
//        StructType schema = new StructType()
//                .add("name", "string")
//                .add("age", "integer");
//
//        // 创建内存流式数据源
//        MemoryStream<Row> memoryStream = new MemoryStream<>(1, spark.sqlContext(), Option.empty(), Encoders.bean(Row.class));
//
//        // 将数据添加到内存流式数据源
//        memoryStream.addData(RowFactory.create("Alice", 25));
//        memoryStream.addData(new Row[]{RowFactory.create("Bob", 30)});
//        memoryStream.addData(new Row[]{RowFactory.create("Charlie", 35)});
//
//        // 将内存流式数据源转换为流式DataFrame
//        Dataset<Row> streamingDF = spark.readStream()
//                .schema(schema)
//                .format("memory")
//                .option("queryName", "people")
//                .load(memoryStream.toDF(), memoryStream.getEncoder());
//
//        // 执行流式查询
//        StreamingQuery query = streamingDF.writeStream()
//                .outputMode("append")
//                .format("console")
//                .start();
//
//        query.awaitTermination();
//    }
}

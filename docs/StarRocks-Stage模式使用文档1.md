## 列模式部分更新-暂存区模式

在列模式部分更新的场景中，一般涉及少数列大量行，暂存区模式适用于某一列或多列更新的数据行占比很大，甚至近似于全列更新的场景。

配置参数starrocks.write.stage.use用于控制暂存区模式的使用，支持取值为

always

指定使用部分列更新时采用暂存区模式，适用于更新时行占比很大的场景

auto

系统会根据更新数据涉及的列以及目标表的列数，行占比ratio决定是否使用暂存区模式

更新数据的行占比超过60，且更新列数少于4个

更新列数占所有列数的百分比小于25%

反之，系统不会使用暂存区模式

starrocks.write.stage.columns.update.ratio：被更新数据列的行占比，可指定占比，默认20

never（默认值）

指定不使用暂存区模式，涉及行占比不大时采用



**sql配置**

暂存区模式采用在StarRocks中创建临时表实现，使用暂存区模式更新导入数据时需要执行update语句，可以使用StarRocks系统变量配置sql执行的参数,对应参数为starrocks.write.stage.session.*

参数映射：exec_mem_limit->exec.mem.limit  //todo

比如：.option ("starrocks.write.stage.session.exec.mem.limit","8589934592") ,可以实现update执行时的StarRocks内存限制。

**暂存表清理**

正常流程中，在执行完update后会将暂存表清楚，完成整个部分列更新流程。一些特殊情况下暂存表未被正确删除，会占用数据库空间。可以定期执行SparkDrop作业，删除存在时长超过一天的暂存表实现对冗余暂存表的清理。

参数：

1. FE MySQL Server 端口(默认127.0.0.1:9030)

2.用户名(username)

3.密码(password)

4.Spark作业执行环境(默认local[*])    用--master指定也可（优先级更高）

用例 

```
spark-submit \
  --class com.TableDrop.SparkDrop \
  --master local[*] \
/opt/SparkTask/original-SparkDrop-1.0-SNAPSHOT.jar  192.168.181.1:9030 root "password" local[*]
```

**使用示例**

建表语句：

```
CREATE TABLE `test`.`stage_test`
(
    `id` BIGINT NOT NULL COMMENT "主键",
    `name` VARCHAR(65533) NULL ,
    `score` INT NOT NULL ,
    `is_active` BOOLEAN NULL  ,
    `decimal_score` DECIMAL(10, 2) NULL ,
    `double_value` DOUBLE NULL ,
    `float_value` FLOAT NULL ,
    `largeint_value` LARGEINT NULL ,
    `smallint_value` SMALLINT NULL  ,
    `tinyint_value` TINYINT NULL ,
    `char_value` CHAR(10) NULL  ,
    `string_value` STRING NULL  ,
    `create_date` DATE NULL 
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
BUCKETS 4;

```

插入数据

```
INSERT INTO `test`.`stage_test` (`id`, `name`, `score`, `is_active`, `decimal_score`, `double_value`, `float_value`, `largeint_value`, `smallint_value`, `tinyint_value`, `char_value`, `string_value`, `create_date`)
VALUES
    (1, 'Alice', 95, true, 95.50, 95.5, 95.5, 10000000000, 100, 10, 'A', 'Alice String', '2024-01-01'),
    (2, 'Bob', 88, true, 88.00, 88.0, 88.0, 20000000000, 90, 9, 'B', 'Bob String', '2024-01-02'),
    (3, 'Charlie', 76, false, 76.75, 76.75, 76.75, 30000000000, 80, 8, 'C', 'Charlie String', '2024-01-03'),
    (4, 'David', 82, true, 82.25, 82.25, 82.25, 40000000000, 70, 7, 'D', 'David String', '2024-01-04'),
    (5, 'Eva', 90, true, 90.00, 90.0, 90.0, 50000000000, 60, 6, 'E', 'Eva String', '2024-01-05'),
    (6, 'Frank', 65, false, 65.50, 65.5, 65.5, 60000000000, 50, 5, 'F', 'Frank String', '2024-01-06'),
    (7, 'Grace', 70, true, 70.00, 70.0, 70.0, 70000000000, 40, 4, 'G', 'Grace String', '2024-01-07'),
    (8, 'Heidi', 80, true, 80.00, 80.0, 80.0, 80000000000, 30, 3, 'H', 'Heidi String', '2024-01-08'),
    (9, 'Ivan', 92, true, 92.00, 92.0, 92.0, 90000000000, 20, 2, 'I', 'Ivan String', '2024-01-09'),
    (10, 'Judy', 85, false, 85.00, 85.0, 85.0, 10000000000, 10, 1, 'J', 'Judy String', '2024-01-10');

```

以Scala语言为例，在Spark-Shell中运行

```
import org.apache.spark.sql.SparkSession

val data = Seq((1, "starrocks", 124578), (2, "spark", 235689))
val df = data.toDF("id", "name", "score")

df.write.format("starrocks")
  .option("starrocks.fe.http.url", "http://127.0.0.1:8030")  
  .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030") 
  .option("starrocks.table.identifier", "test.stage_test")
  .option("starrocks.user", "root")
  .option("starrocks.password", "")
  .option("starrocks.write.stage.use","always")
  .option("starrocks.write.properties.partial_update_mode","column")
  .option("starrocks.write.properties.partial_update","true")
  .option("starrocks.columns", "id,name,score")
  .option("starrocks.write.stage.session.query.timeout","309")
  .option("starrocks.write.stage.session.query.mem.limit","100000000")
  .option("starrocks.write.stage.session.exec.mem.limit","8589934592")
  .option("starrocks.write.stage.columns.update.ratio","80")
  .mode("append")
  .save()
```

运行后会在temp_db下创建具有"id", "name", "score"字段的临时表，并更新test.stage_test，更新完成后删除临时表。






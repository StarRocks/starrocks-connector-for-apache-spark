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

package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.catalog.SRCatalog;
import com.starrocks.connector.spark.catalog.SRColumn;
import com.starrocks.connector.spark.catalog.SRTable;
import com.starrocks.connector.spark.exception.CatalogException;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TEMP_DBNAME;

public class StarRocksWrite implements BatchWrite, StreamingWrite {

    private static final Logger log = LoggerFactory.getLogger(StarRocksWrite.class);

    private final LogicalWriteInfo logicalInfo;
    private final WriteStarRocksConfig config;
    private final StarRocksSchema starRocksSchema;
    private final boolean isBatch;
    private boolean useStage;
    @Nullable
    private transient String tempTableName;
    @Nullable
    private transient SRTable srTable;
    @Nullable
    private transient Map<String, String> TableConfig;
    //todo

    public StarRocksWrite(LogicalWriteInfo logicalInfo, WriteStarRocksConfig config, StarRocksSchema starRocksSchema,boolean isBatch) {
        this.logicalInfo = logicalInfo;
        this.config = config;
        this.isBatch = isBatch;
        this.starRocksSchema = starRocksSchema;

    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {

        WriteStarRocksConfig writeConfig = doPrepare();

        return new StarRocksWriterFactory(logicalInfo.schema(), writeConfig);

    }



    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        log.info("batch query `{}` commit", logicalInfo.queryId());
        doCommit(messages);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("batch query `{}` abort", logicalInfo.queryId());
        doAbort(messages);
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(logicalInfo.schema(), config);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` commit", logicalInfo.queryId());
    }


    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` abort", logicalInfo.queryId());
    }

    private void doAbort(WriterCommitMessage[] messages) {
if(!useStage)
{
    return;
}
dropTempTable();

}
    public boolean useStageMode() {
        String database = config.getDatabase();
        String table = config.getTable();
        if (this.TableConfig==null)
        {
            this.TableConfig = StarRocksConnector.getTableConfig(config, database, table);
        }

        String tableModel = TableConfig.get("TABLE_MODEL");


        if(isBatch&&config.isPartialUpdate()&&config.isPartialUpdateColumnMode()&&tableModel.equals("PRIMARY_KEYS"))
        {

            this.srTable = StarRocksConnector.getSRTable(config, config.getDatabase(), config.getTable());
         //always
            if (config.getStageUse()== WriteStarRocksConfig.StageUse.ALWAYS)
            {
                return true;
            }
            //todo auto when to use stageMode need a better strategy
            //auto
            else if (config.getStageUse()== WriteStarRocksConfig.StageUse.AUTO)
            {
                Map<String, String> stageConfig = config.getStageConfig();
                int ratio = Integer.parseInt(stageConfig.getOrDefault("columns.update.ratio","60"));

                StructType schema = config.getSparkSchema();
                Set<String> fieldNames = new HashSet<>();
                for (StructField field : schema.fields()) {
                    fieldNames.add(field.name());
                }

                int srColumnSize = srTable.getColumns().size();

                return  ratio >= 60 && fieldNames.size() * 4 >= srColumnSize;

            }
            //never
            else
            {
                return false;
            }
        }

        else
        {
            return false;
        }
    }

    private WriteStarRocksConfig doPrepare() {
        this.useStage=useStageMode();
        if (!useStage)
        {
            return config;
        }
        SRTable tempTable = toSRTable(starRocksSchema);
        SRCatalog catalog = getSRCatalog();
        if (!StarRocksConnector.dbExists(config, STARROCKS_TEMP_DBNAME))
        {
            catalog.createDatabase(STARROCKS_TEMP_DBNAME,true);
        }
        try {
            catalog.createTable(tempTable, false);
        } catch (Exception e) {
            log.error("Failed to create temp table {}.{},already exists", STARROCKS_TEMP_DBNAME, tempTable.getTableName(), e);
            throw e;
        }
       this.tempTableName = tempTable.getTableName();
        return config.copy(tempTable.getDatabaseName(),tempTable.getTableName(), Arrays.asList("partial_update", "partial_update_mode"));
    }

    private void doCommit(WriterCommitMessage[] messages) {
        if (!useStage) {
            return;
        }

        String srcTableId = String.format("`%s`.`%s`", STARROCKS_TEMP_DBNAME, tempTableName);
        String targetTableId = String.format("`%s`.`%s`", config.getDatabase(), config.getTable());
        List<String> primaryKeys = starRocksSchema.getPrimaryKeys().stream()
                .map(StarRocksField::getName)
                .collect(Collectors.toList());
        String joinedKeys = primaryKeys.stream()
                .map(key -> String.format("%s.`%s` = t2.`%s`", targetTableId, key, key))
                .collect(Collectors.joining(" AND "));

        List<SRColumn> columns = srTable.getColumns();

        SRTable srTable1 = StarRocksConnector.getSRTable(config, STARROCKS_TEMP_DBNAME, tempTableName);
        List<String> tempColumns = srTable1.getColumns().stream()
                .map(SRColumn::getColumnName)
                .collect(Collectors.toList());

        String joinedColumns = columns.stream()
                .map(SRColumn::getColumnName)
                .filter(tempColumns::contains)
                .filter(col -> !primaryKeys.contains(col))
                .map(col -> String.format("`%s` = `t2`.`%s`", col, col))
                .collect(Collectors.joining(", "));

        Map<String, String> stageConfig = config.getStageConfig();

        Map<String, String> sessionMap = stageConfig.entrySet().stream().filter(entry -> entry.getKey().startsWith("session.")).collect(
                Collectors.toMap(
                        entry -> entry.getKey().substring(8).replace('.', '_'),
                        Map.Entry::getValue
                )
        );
        StringBuilder updateHeader = new StringBuilder("UPDATE /* + SET_VAR (");
        for (Map.Entry<String, String> sessionSet : sessionMap.entrySet()) {
            if(!updateHeader.toString().endsWith("("))
            {
                updateHeader.append(",");
            }
            updateHeader.append(sessionSet.getKey()).append("=").append(sessionSet.getValue());
        }
        updateHeader.append(") */");

        String updateSql = String.format(updateHeader + " %s SET %s FROM %s AS `t2` WHERE %s;",
                targetTableId, joinedColumns, srcTableId, joinedKeys);

        String setVar = "SET partial_update_mode = 'column';";

        log.info("Update sql: {}", updateSql);

        try {
            getSRCatalog().executeDDLBySql(setVar+updateSql);

        }
        catch (Exception e) {
            log.error("Failed to execute update, temp table: {}, target table: {}", srcTableId, targetTableId, e);
            throw new CatalogException("Failed to execute update, temp table: " + srcTableId, e);
        }
        dropTempTable();
        log.info("Success to execute update, temp table: {}, target table: {}", srcTableId, targetTableId);
    }


private void dropTempTable() {
  if (tempTableName!=null)
  {
     try {
         getSRCatalog().dropTable(STARROCKS_TEMP_DBNAME, tempTableName);
     }
     catch (Exception e) {
         log.error("Failed to drop temp table {}.{}", STARROCKS_TEMP_DBNAME, tempTableName, e);
     }
  }
}

private SRTable toSRTable(StarRocksSchema starRocksSchema)
{
    String uuid = UUID.randomUUID().toString().replace("-", "_");
    return new SRTable.Builder()
            .setDatabaseName(STARROCKS_TEMP_DBNAME)
            .setTableName(srTable.getDatabaseName() + "_" + srTable.getTableName() + "_temp_"+uuid)
            .setTableType(SRTable.TableType.DUPLICATE_KEYS)
            .setTableKeys(
                    starRocksSchema.getPrimaryKeys().stream()
                            .map(StarRocksField::getName)
                            .collect(Collectors.toList())
            )
            .setColumns(
                    Arrays.stream(config.getSparkSchema().fields())
                            .map(f -> starRocksSchema.getField(f.name()))
                            .map(this::toStarRocksColumn)
                            .collect(Collectors.toList())
            )
            .setComment(
                    String.format("Spark partial update with column mode, table: %s.%s, query: %s",
                            config.getDatabase(), config.getTable(), logicalInfo.queryId())
            )
            .setTableProperties(srTable.getProperties())
            .setDistributionKeys(srTable.getDistributionKeys().get())
            .setNumBuckets(srTable.getNumBuckets().get())
            .build();
}
    private SRColumn toStarRocksColumn(StarRocksField field) {
        return new SRColumn.Builder()
                .setColumnName(field.getName())
                .setDataType(toStarRocksType(field))
                .setColumnSize(field.getSize() == null ? null : field.getSize())
                .setDecimalDigits(field.getScale() == null ? null : String.valueOf(field.getScale()))
                .setNullable("YES")
                .build();
    }
    private String toStarRocksType(StarRocksField field) {
        String type = field.getType().toLowerCase(Locale.ROOT);
        switch (type) {
            case "tinyint":
                // mysql does not have boolean type, and starrocks `information_schema`.`COLUMNS` will return
                // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type, We distinguish them by
                // column size, and the size of BOOLEAN is null
                return field.getSize() == null ? "BOOLEAN" : "TINYINT";
            case "bigint unsigned":
                return "LARGEINT";
            case "smallint":
            case "int":
            case "bigint":
            case "float":
            case "double":
            case "decimal":
            case "char":
            case "varchar":
            case "json":
            case "date":
            case "datetime":
                return type.toUpperCase();
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported starrocks type, column name: %s, data type: %s", field.getName(), field.getType()));
        }
    }

private SRCatalog getSRCatalog()
{
    return new SRCatalog(config.getFeJdbcUrl(), config.getUsername(), config.getPassword());
}


}

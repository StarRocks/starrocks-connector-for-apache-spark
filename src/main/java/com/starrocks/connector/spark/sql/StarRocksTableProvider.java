package com.starrocks.connector.spark.sql;

import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.InferSchema;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class StarRocksTableProvider implements TableProvider, DataSourceRegister {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return InferSchema.inferSchema(options);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new StarRocksTable(schema, partitioning, new SimpleStarRocksConfig(properties));
    }

    @Override
    public String shortName() {
        return "starrocks_writer";
    }
}

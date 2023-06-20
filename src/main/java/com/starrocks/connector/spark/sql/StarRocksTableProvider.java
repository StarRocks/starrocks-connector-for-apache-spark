package com.starrocks.connector.spark.sql;

import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.InferSchema;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_USER;
import static com.starrocks.connector.spark.sql.conf.StarRocksConfigBase.KEY_FE_HTTP;

public class StarRocksTableProvider implements RelationProvider, TableProvider, DataSourceRegister {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksTableProvider.class);

    @Override
    public BaseRelation createRelation(SQLContext sqlContext,
                                       scala.collection.immutable.Map<String, String> parameters) {
        return new StarrocksRelation(sqlContext, Utils.params(parameters, LOG));
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return InferSchema.inferSchema(makeWriteCompatibleWithRead(options));
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new StarRocksTable(schema, partitioning, new SimpleStarRocksConfig(makeWriteCompatibleWithRead(properties)));
    }

    @Override
    public String shortName() {
        return "starrocks";
    }

    private static Map<String, String> makeWriteCompatibleWithRead(Map<String, String> options) {
        Map<String, String> compatibleOptions = new HashMap<>(options);

        // user and password compatible
        String user = options.get("user");
        if (user != null && !options.containsKey(STARROCKS_USER)) {
            compatibleOptions.put(STARROCKS_USER, user);
        }
        String password = options.get("password");
        if (password != null && !options.containsKey(STARROCKS_PASSWORD)) {
            compatibleOptions.put(STARROCKS_PASSWORD, password);
        }

        // FE http url compatible
        String feNodes = options.get(STARROCKS_FENODES);
        String feHttp = options.get(KEY_FE_HTTP);
        if (feNodes == null && feHttp != null) {
            compatibleOptions.put(STARROCKS_FENODES, feHttp);
        }
        if (feNodes != null && feHttp == null) {
            compatibleOptions.put(KEY_FE_HTTP, feNodes);
        }

        return compatibleOptions;
    }
}

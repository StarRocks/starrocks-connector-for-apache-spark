package com.starrocks.connector.spark.sql.connect;

import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StarRocksConnector {

    public static StarRocksSchema getSchema(StarRocksConfig config) {

        List<Map<String, String>> columnValues = extractColumnValuesBySql(
                config.getFeJdbcUrl(),
                config.getUsername(),
                config.getPassword(),
                "SELECT `COLUMN_NAME`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?;",
                config.getDatabase(),
                config.getTable()
        );

        List<StarRocksField> pks = new LinkedList<>();

        LinkedHashMap<String, StarRocksField> columns = new LinkedHashMap<>();

        for (Map<String, String> columnValue : columnValues) {
            StarRocksField field = new StarRocksField();

            field.setName(columnValue.get("COLUMN_NAME"));
            field.setType(columnValue.get("DATA_TYPE"));
            field.setSize(columnValue.get("COLUMN_SIZE"));
            field.setScale(columnValue.get("DECIMAL_DIGITS"));

            columns.put(field.getName(), field);

            if ("PRI".equals(columnValue.get("COLUMN_KEY"))) {
                pks.add(field);
            }
        }

        StarRocksSchema schema = new StarRocksSchema();
        schema.setFieldMap(columns);
        schema.setPks(pks);

        return schema;
    }

    private static List<Map<String, String>> extractColumnValuesBySql(String url,
                                                                      String username,
                                                                      String password,
                                                                      String sql,
                                                                      Object... args) {
        List<Map<String, String>> columnValues = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Objects.nonNull(args) && args.length > 0) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

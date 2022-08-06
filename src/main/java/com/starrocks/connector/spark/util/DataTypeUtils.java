package com.starrocks.connector.spark.util;

import org.apache.arrow.vector.types.Types;

public class DataTypeUtils {

    public static String map(Types.MinorType minorType) {
        switch (minorType) {
            case NULL:
                return "NULL_TYPE";
            case BIT:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT4:
                return "FLOAT";
            case FLOAT8:
                return "DOUBLE";
            case VARBINARY:
                return "BINARY";
            case VARCHAR:
                return "VARCHAR";
            case DECIMAL:
                return "DECIMAL128";
            default:
                return "";
        }
    }
}

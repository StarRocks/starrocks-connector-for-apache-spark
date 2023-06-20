package com.starrocks.connector.spark.sql.schema;

public class StarRocksField {

    public static final StarRocksField __OP = new StarRocksField("__op", "tinyint", Integer.MAX_VALUE, null, null);

    private String name;
    private String type;
    private int ordinalPosition;
    private String size;
    private String scale;

    public StarRocksField(String name, String type, int ordinalPosition, String size, String scale) {
        this.name = name;
        this.type = type;
        this.ordinalPosition = ordinalPosition;
        this.size = size;
        this.scale = scale;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getSize() {
        return size;
    }

    public String getScale() {
        return scale;
    }
}

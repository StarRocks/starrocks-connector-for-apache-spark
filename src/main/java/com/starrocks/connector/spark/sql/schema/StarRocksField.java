package com.starrocks.connector.spark.sql.schema;

public class StarRocksField {

    public static final StarRocksField __OP = new StarRocksField("__op", "tinyint");

    private String name;
    private String type;
    private String size;
    private String scale;

    public StarRocksField() {
    }

    private StarRocksField(String name, String type) {
        this(name, type, null, null);
    }

    public StarRocksField(String name, String type, String size, String scale) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.scale = scale;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getScale() {
        return scale;
    }

    public void setScale(String scale) {
        this.scale = scale;
    }
}

package com.starrocks.connector.spark.sql.schema;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class StarRocksSchema {
    private LinkedHashMap<String, StarRocksField> fieldMap;
    private List<StarRocksField> pks;

    public LinkedHashMap<String, StarRocksField> getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(LinkedHashMap<String, StarRocksField> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public List<StarRocksField> getPks() {
        return pks;
    }

    public void setPks(List<StarRocksField> pks) {
        this.pks = pks;
    }

    public List<StarRocksField> sortAndListField(String[] fieldNames) {
        List<StarRocksField> fields = new LinkedList<>();
        for (String fieldName : fieldNames) {
            if (StarRocksField.__OP.getName().equalsIgnoreCase(fieldName)) {
                fields.add(StarRocksField.__OP);
                continue;
            }
            StarRocksField field = fieldMap.get(fieldName);
            if (field != null) {
                fields.add(field);
            }
        }
        return fields;
    }
}

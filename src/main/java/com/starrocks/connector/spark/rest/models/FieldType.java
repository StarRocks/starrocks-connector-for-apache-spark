// Modifications Copyright 2021 StarRocks Limited.
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

package com.starrocks.connector.spark.rest.models;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum FieldType {
    NULL("NULL_TYPE"),
    BOOLEAN("BOOLEAN"),
    BIGINT("BIGINT"),
    INT("INT"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    LARGEINT("LARGEINT"),
    VARCHAR("VARCHAR"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DATE("DATE"),
    DECIMAL("DECIMAL"),
    DECIMAL32("DECIMAL32"),
    DECIMAL64("DECIMAL64"),
    DECIMAL128("DECIMAL128"),
    DATETIME("DATETIME"),
    TIME("TIME"),
    CHAR("CHAR"),
    BINARY("BINARY"),
    DECIMALV2("DECIMALV2"),
    STRUCT("STRUCT"),
    ARRAY("ARRAY"),
    MAP("MAP");

    private static final Map<String, FieldType> REVERSE_MAPPING = new HashMap<>(values().length);

    static {
        for (FieldType fieldType : values()) {
            if (REVERSE_MAPPING.containsKey(fieldType.getTypeName().toUpperCase())) {
                throw new IllegalStateException("duplicated type: " + fieldType.getTypeName());
            }

            REVERSE_MAPPING.put(fieldType.getTypeName().toUpperCase(), fieldType);
        }
    }


    public static FieldType of(String typeName) {
        return Arrays.stream(values())
                .filter(type ->
                        type.getTypeName().equalsIgnoreCase(typeName)
                                || type.name().equalsIgnoreCase(typeName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown field type: " + typeName));
    }

    private final String typeName;

    FieldType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public static Optional<FieldType> elegantOf(String typeName) {
        return Optional.ofNullable(typeName)
                .map(String::toUpperCase)
                .map(REVERSE_MAPPING::get);
    }

    public static boolean isSupported(String typeName) {
        return elegantOf(typeName).isPresent();
    }

    public static String[] extractStructTypes(String structType) {
        String inner = extractInnerContent(structType, "STRUCT<");
        if (inner == null) {
            return new String[0];
        }

        java.util.List<String> types = new java.util.ArrayList<>();
        java.util.List<String> fields = splitByComma(inner);

        for (String field : fields) {
            String type = extractTypeFromField(field.trim());
            if (type != null) {
                types.add(type);
            }
        }

        return types.toArray(new String[0]);
    }

    public static String[] extractArrayTypes(String arrayType) {
        String inner = extractInnerContent(arrayType, "ARRAY<");
        if (inner == null) {
            return new String[0];
        }

        String type = extractBaseType(inner.trim());
        return type != null ? new String[] { type } : new String[0];
    }

    public static String[] extractMapTypes(String mapType) {
        String inner = extractInnerContent(mapType, "MAP<");
        if (inner == null) {
            return new String[0];
        }

        java.util.List<String> parts = splitByComma(inner);
        if (parts.size() != 2) {
            return new String[0];
        }

        String keyType = extractBaseType(parts.get(0).trim());
        String valueType = extractBaseType(parts.get(1).trim());

        if (keyType == null || valueType == null) {
            return new String[0];
        }

        return new String[] { keyType, valueType };
    }

    private static String extractInnerContent(String type, String prefix) {
        if (!type.endsWith(">")) {
            return null;
        }
        return type.substring(prefix.length(), type.length() - 1).trim();
    }

    private static java.util.List<String> splitByComma(String content) {
        java.util.List<String> result = new java.util.ArrayList<>();
        int angleDepth = 0;
        int parenDepth = 0;
        StringBuilder current = new StringBuilder();

        for (char c : content.toCharArray()) {
            if (c == '<') {
                angleDepth++;current.append(c);
            } else if (c == '>') {
                angleDepth--;
                current.append(c);
            } else if (c == '(') {
                parenDepth++;
                current.append(c);
            } else if (c == ')') {
                parenDepth--;
                current.append(c);
            } else if (c == ',' && angleDepth == 0 && parenDepth == 0) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            result.add(current.toString());
        }

        return result;
    }


    private static String extractTypeFromField(String field) {
        int spaceIdx = field.indexOf(' ');
        if (spaceIdx <= 0) {
            return null;
        }

        String type = field.substring(spaceIdx + 1).trim();
        return extractBaseType(type);
    }

    private static String extractBaseType(String type) {
        String upperType = type.toUpperCase();

        // If it is a complex type (STRUCT, ARRAY, MAP), return complete without modification
        if (upperType.startsWith("STRUCT<") ||
                upperType.startsWith("ARRAY<") ||
                upperType.startsWith("MAP<")) {
            return upperType;
        }

        // For simple types, remove parameters
        int parenIdx = type.indexOf('(');
        if (parenIdx > 0) {
            return type.substring(0, parenIdx).toUpperCase().replace("DECIMAL", "DECIMALV2");
        }

        return type.toUpperCase().replace("DECIMAL", "DECIMALV2");
    }




}

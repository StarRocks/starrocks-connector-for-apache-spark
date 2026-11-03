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
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql.schema;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Parses the recursive type syntax returned by
 * information_schema.COLUMNS.COLUMN_TYPE.
 */
final class StarRocksTypeParser {

    /** Type text being parsed. */
    private final String input;
    /** Current offset in {@link #input}. */
    private int position;

    private StarRocksTypeParser(final String source) {
        this.input = source;
    }

    static DataType parse(final String source) {
        StarRocksTypeParser parser = new StarRocksTypeParser(source);
        DataType dataType = parser.parseType();
        parser.skipWhitespace();
        if (!parser.isAtEnd()) {
            throw parser.parseError("Unexpected trailing content");
        }
        return dataType;
    }

    private DataType parseType() {
        skipWhitespace();
        String typeName = readIdentifier().toLowerCase(Locale.ROOT);
        switch (typeName) {
            case "array":
                expect('<');
                DataType elementType = parseType();
                expect('>');
                return DataTypes.createArrayType(elementType, true);
            case "map":
                expect('<');
                DataType keyType = parseType();
                expect(',');
                DataType valueType = parseType();
                expect('>');
                return DataTypes.createMapType(keyType, valueType, true);
            case "struct":
                return parseStructType();
            default:
                return parseScalarType(typeName);
        }
    }

    private DataType parseStructType() {
        expect('<');
        List<StructField> fields = new ArrayList<>();
        skipWhitespace();
        if (consumeIf('>')) {
            return DataTypes.createStructType(fields);
        }

        while (true) {
            String fieldName = readFieldName();
            skipWhitespace();
            consumeIf(':');
            DataType fieldType = parseType();
            fields.add(new StructField(
                    fieldName, fieldType, true, Metadata.empty()));
            skipWhitespace();
            if (consumeIf('>')) {
                return DataTypes.createStructType(fields);
            }
            expect(',');
        }
    }

    private DataType parseScalarType(final String typeName) {
        List<Integer> parameters = parseIntegerParameters();
        skipWhitespace();
        boolean unsigned = consumeKeyword("unsigned");
        if (unsigned && !"bigint".equals(typeName)) {
            throw parseError("Unsigned modifier is only supported for bigint");
        }

        switch (typeName) {
            case "boolean":
                return DataTypes.BooleanType;
            case "tinyint":
                return DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "bigint":
                return unsigned ? DataTypes.StringType : DataTypes.LongType;
            case "largeint":
                return DataTypes.StringType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "decimal":
            case "decimalv2":
            case "decimal32":
            case "decimal64":
            case "decimal128":
                if (parameters.size() != 2) {
                    throw parseError(
                            "Decimal type must contain precision and scale");
                }
                return DataTypes.createDecimalType(
                        parameters.get(0), parameters.get(1));
            case "char":
            case "varchar":
            case "string":
            case "json":
                return DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "datetime":
                return DataTypes.TimestampType;
            default:
                throw parseError("Unsupported type " + typeName);
        }
    }

    private List<Integer> parseIntegerParameters() {
        List<Integer> parameters = new ArrayList<>();
        skipWhitespace();
        if (!consumeIf('(')) {
            return parameters;
        }

        parameters.add(readInteger());
        skipWhitespace();
        if (consumeIf(',')) {
            parameters.add(readInteger());
        }
        expect(')');
        return parameters;
    }

    private int readInteger() {
        skipWhitespace();
        int start = position;
        while (!isAtEnd() && Character.isDigit(input.charAt(position))) {
            position++;
        }
        if (start == position) {
            throw parseError("Expected integer");
        }
        return Integer.parseInt(input.substring(start, position));
    }

    private String readFieldName() {
        skipWhitespace();
        if (!isAtEnd() && input.charAt(position) == '`') {
            position++;
            StringBuilder fieldName = new StringBuilder();
            while (!isAtEnd()) {
                char current = input.charAt(position++);
                if (current != '`') {
                    fieldName.append(current);
                } else if (!isAtEnd() && input.charAt(position) == '`') {
                    fieldName.append('`');
                    position++;
                } else {
                    return fieldName.toString();
                }
            }
            throw parseError("Unterminated quoted field name");
        }
        return readIdentifier();
    }

    private String readIdentifier() {
        skipWhitespace();
        int start = position;
        while (!isAtEnd()) {
            char current = input.charAt(position);
            if (!Character.isLetterOrDigit(current) && current != '_') {
                break;
            }
            position++;
        }
        if (start == position) {
            throw parseError("Expected identifier");
        }
        return input.substring(start, position);
    }

    private boolean consumeKeyword(final String keyword) {
        skipWhitespace();
        int end = position + keyword.length();
        if (end > input.length()
                || !input.regionMatches(
                        true, position, keyword, 0, keyword.length())) {
            return false;
        }
        if (end < input.length()) {
            char next = input.charAt(end);
            if (Character.isLetterOrDigit(next) || next == '_') {
                return false;
            }
        }
        position = end;
        return true;
    }

    private boolean consumeIf(final char expected) {
        skipWhitespace();
        if (!isAtEnd() && input.charAt(position) == expected) {
            position++;
            return true;
        }
        return false;
    }

    private void expect(final char expected) {
        if (!consumeIf(expected)) {
            throw parseError("Expected '" + expected + "'");
        }
    }

    private void skipWhitespace() {
        while (!isAtEnd() && Character.isWhitespace(input.charAt(position))) {
            position++;
        }
    }

    private boolean isAtEnd() {
        return position >= input.length();
    }

    private UnsupportedOperationException parseError(final String message) {
        return new UnsupportedOperationException(
                String.format(
                        "%s at position %d in StarRocks type '%s'",
                        message, position, input));
    }
}

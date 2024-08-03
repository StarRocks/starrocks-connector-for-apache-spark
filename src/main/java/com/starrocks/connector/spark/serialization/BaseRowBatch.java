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

package com.starrocks.connector.spark.serialization;

import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * row batch data container.
 */
public class BaseRowBatch implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(BaseRowBatch.class);

    public static class Row {
        private List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }

    // offset for iterate the rowBatch
    protected long rowCountInOneBatch = 0;
    private long offsetInRowBatch = 0;
    protected long readRowCount = 0;
    protected List<Row> rowBatch = new ArrayList<>();
    protected final StarRocksSchema schema;
    protected final Map<String, StarRocksField> fieldMap;
    protected DateTimeFormatter dateTimeFormatter;
    protected DateTimeFormatter dateFormatter;
    protected ZoneId zoneId;

    public BaseRowBatch(StarRocksSchema schema) throws StarRocksException {
        this(schema, ZoneId.systemDefault());
    }

    public BaseRowBatch(StarRocksSchema schema, ZoneId timeZone) throws StarRocksException {
        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(timeZone);
        this.dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(timeZone);
        this.schema = schema;
        this.zoneId = timeZone;
        this.fieldMap = schema.getColumns().stream()
                .collect(
                        Collectors.toMap(
                                StarRocksField::getName,
                                Function.identity()
                        )
                );
    }

    public boolean hasNext() {
        return offsetInRowBatch < readRowCount;
    }

    public List<Object> next() throws StarRocksException {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(new Long(offsetInRowBatch++).intValue()).getCols();
    }

    protected String typeMismatchMessage(final String sparkType, final Types.MinorType arrowType) {
        final String messageTemplate = "Spark type is %1$s, but mapped type is %2$s.";
        return String.format(messageTemplate, sparkType, arrowType.name());
    }

    public long getReadRowCount() {
        return readRowCount;
    }

    protected void addValueToRow(long rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(new Long(readRowCount + rowIndex).intValue()).put(obj);
    }

    @Override
    public void close() throws Exception {

    }
}

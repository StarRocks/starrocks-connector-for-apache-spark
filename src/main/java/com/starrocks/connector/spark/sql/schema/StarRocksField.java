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
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql.schema;

import java.io.Serializable;

public class StarRocksField implements Serializable {

    public static final StarRocksField OP = new StarRocksField("__op", "tinyint", Integer.MAX_VALUE, 3, null,  null);

    private String name;
    private String type;
    private int ordinalPosition;
    private Integer size;

    private Integer precision;
    private Integer scale;

    public StarRocksField(String name, String type, int ordinalPosition, Integer size, Integer precision, Integer scale) {
        this.name = name;
        this.type = type;
        this.ordinalPosition = ordinalPosition;
        this.size = size;
        this.precision = precision;
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

    public Integer getSize() {
        return size;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }

    public boolean isJson() {
        return "json".equalsIgnoreCase(type);
    }

    public boolean isBitmap() {
        return "bitmap".equalsIgnoreCase(type);
    }

    public boolean isHll() {
        return "hll".equalsIgnoreCase(type);
    }
}

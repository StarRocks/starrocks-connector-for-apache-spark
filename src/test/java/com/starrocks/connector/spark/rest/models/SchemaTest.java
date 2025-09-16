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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaTest {

    @Test
    public void testPutGet() {
        Schema ts = new Schema(1);
        Field f = new Field();
        ts.put(f);
        Assertions.assertEquals(f, ts.get(0));

        IndexOutOfBoundsException exception = Assertions.assertThrows(IndexOutOfBoundsException.class, () -> ts.get(1));
        Assertions.assertEquals(exception.getMessage(), "Index: 1, Fields size: 1");
    }
}

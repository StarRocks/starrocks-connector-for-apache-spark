/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.starrocks.connector.spark.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class JsonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

    private static final boolean SUPPORT_MAX_STRING_LENGTH = checkSupportMaxStringLength();

    public static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        setMaxStringLength(mapper.getFactory(), 1024 * 1024 * 1024);
        return mapper;
    }

    private static boolean checkSupportMaxStringLength() {
        try {
            // only for jackson-core 2.15+
            Class.forName("com.fasterxml.jackson.core.StreamReadConstraints");
            LOG.info("jackson-core supports to set max string length");
            return true;
        } catch (ClassNotFoundException e) {
            // ignore
        }
        LOG.info("jackson-core does not support to set max string length");
        return false;
    }

    private static void setMaxStringLength(JsonFactory jsonFactory, int maxStringLen) {
        if (!SUPPORT_MAX_STRING_LENGTH) {
            return;
        }
        try {
            Field constraintsField = JsonFactory.class.getDeclaredField("_streamReadConstraints");
            constraintsField.setAccessible(true);
            Object constraints = constraintsField.get(jsonFactory);

            Field maxStringLenField = constraints.getClass().getDeclaredField("_maxStringLen");
            maxStringLenField.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(maxStringLenField, maxStringLenField.getModifiers() & ~Modifier.FINAL);
            maxStringLenField.setInt(constraints, maxStringLen);
        } catch (Exception e) {
            LOG.warn("Failed to set max string length {}", maxStringLen, e);
        }
    }
}

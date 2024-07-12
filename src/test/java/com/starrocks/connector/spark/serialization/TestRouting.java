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

import static org.hamcrest.core.StringStartsWith.startsWith;

import com.starrocks.connector.spark.cfg.ConfigurationOptions;
import com.starrocks.connector.spark.cfg.PropertiesSettings;
import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.exception.IllegalArgumentException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestRouting {
    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void testRoutingNoBeMappingList() throws Exception {
        Settings settings = new PropertiesSettings();
        Routing r1 = new Routing("10.11.12.13:1234", settings);
        Assert.assertEquals("10.11.12.13", r1.getHost());
        Assert.assertEquals(1234, r1.getPort());

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(startsWith("argument "));
        new Routing("10.11.12.13:wxyz", settings);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(startsWith("Parse "));
        new Routing("10.11.12.13", settings);
    }

    @Test
    public void testRoutingBeMappingList() throws Exception {
        Settings settings = new PropertiesSettings();
        String mappingList = "20.11.12.13:6666,10.11.12.13:1234;21.11.12.13:5555,11.11.12.13:1234";
        settings.setProperty(ConfigurationOptions.STARROCKS_BE_HOST_MAPPING_LIST, mappingList);

        Routing r1 = new Routing("10.11.12.13:1234", settings);
        Assert.assertEquals("20.11.12.13", r1.getHost());
        Assert.assertEquals(6666, r1.getPort());

        Routing r2 = new Routing("11.11.12.13:1234", settings);
        Assert.assertEquals("21.11.12.13", r2.getHost());
        Assert.assertEquals(5555, r2.getPort());

    }
}

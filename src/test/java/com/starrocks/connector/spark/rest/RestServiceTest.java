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

package com.starrocks.connector.spark.rest;

import com.starrocks.connector.spark.cfg.PropertiesSettings;
import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.exception.IllegalArgumentException;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.rest.models.Field;
import com.starrocks.connector.spark.rest.models.QueryPlan;
import com.starrocks.connector.spark.rest.models.Schema;
import com.starrocks.connector.spark.rest.models.Tablet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_DEFAULT;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_MIN;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;

public class RestServiceTest {
    private static Logger logger = LoggerFactory.getLogger(RestServiceTest.class);

    @Test public void testParseIdentifier() {
        String validIdentifier = "a.b";
        String[] names = RestService.parseIdentifier(validIdentifier, logger);
        Assertions.assertEquals(2, names.length);
        Assertions.assertEquals("a", names[0]);
        Assertions.assertEquals("b", names[1]);

        String invalidIdentifier1 = "a";
        StarRocksException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> RestService.parseIdentifier(invalidIdentifier1, logger));
        Assertions.assertEquals(exception.getMessage(),
                "argument 'table.identifier' is illegal, value is '" + invalidIdentifier1 + "'.");

        String invalidIdentifier3 = "a.b.c";
        exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> RestService.parseIdentifier(invalidIdentifier3, logger));
        Assertions.assertEquals(exception.getMessage(),
                "argument 'table.identifier' is illegal, value is '" + invalidIdentifier3 + "'.");

        String emptyIdentifier = "";
        exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> RestService.parseIdentifier(emptyIdentifier, logger));
        Assertions.assertEquals(exception.getMessage(),
                "argument 'table.identifier' is illegal, value is '" + emptyIdentifier + "'.");

        String nullIdentifier = null;
        exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> RestService.parseIdentifier(nullIdentifier, logger));
        Assertions.assertEquals(exception.getMessage(),
                "argument 'table.identifier' is illegal, value is '" + nullIdentifier + "'.");
    }

    @Test public void testChoiceFe() {
        String validFes = "1,2 , 3";
        String fe = RestService.randomEndpoint(validFes, logger);
        List<String> feNodes = new ArrayList<>(3);
        feNodes.add("1");
        feNodes.add("2");
        feNodes.add("3");
        Assertions.assertTrue(feNodes.contains(fe));

        String emptyFes = "";
        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> RestService.randomEndpoint(emptyFes, logger));
        Assertions.assertEquals(exception.getMessage(), "argument 'fenodes' is illegal, value is '" + emptyFes + "'.");

        String nullFes = null;
        exception = Assertions.assertThrows(IllegalArgumentException.class, () -> RestService.randomEndpoint(nullFes, logger));
        Assertions.assertEquals(exception.getMessage(), "argument 'fenodes' is illegal, value is '" + nullFes + "'.");
    }

    @Test public void testGetUriStr() throws Exception {
        Settings settings = new PropertiesSettings();
        settings.setProperty(STARROCKS_TABLE_IDENTIFIER, "a.b");
        settings.setProperty(STARROCKS_FENODES, "fe");

        String expected = "http://fe/api/a/b/";
        Assertions.assertEquals(expected, RestService.getSchemaUriStr(settings));
    }

    @Test public void testFeResponseToSchema() throws Exception {
        String res = "{\"properties\":[{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},{\"name\":\"k5\","
                + "\"scale\":\"0\",\"comment\":\"\",\"type\":\"DECIMALV2\",\"precision\":\"9\"},{\"name\":\"k6\","
                + "\"scale\":\"9\",\"comment\":\"\",\"type\":\"DECIMAL128\",\"precision\":\"30\"}],\"status\":200}";
        Schema expected = new Schema();
        expected.setStatus(200);
        Field k1 = new Field("k1", "TINYINT", null, "", null, null, null);
        Field k5 = new Field("k5", "DECIMALV2", null, "", 9, 0, null);
        Field k6 = new Field("k6", "DECIMAL128", null, "", 30, 9, null);
        expected.put(k1);
        expected.put(k5);
        expected.put(k6);
        Assertions.assertEquals(expected, RestService.parseSchema(res, logger));

        String notJsonRes = "not json";
        StarRocksException exception =
                Assertions.assertThrows(StarRocksException.class, () -> RestService.parseSchema(notJsonRes, logger));
        Assertions.assertTrue(exception.getMessage().startsWith("StarRocks FE's response is not a json. res:"));

        String notOkRes = "{\"properties\":[{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},{\"name\":\"k5\","
                + "\"scale\":\"0\",\"comment\":\"\",\"type\":\"DECIMALV2\",\"precision\":\"9\"}],\"status\":20}";
        exception = Assertions.assertThrows(StarRocksException.class, () -> RestService.parseSchema(notOkRes, logger));
        Assertions.assertTrue(exception.getMessage().startsWith("StarRocks FE's response is not OK, status is "));
    }

    @Test public void testFeResponseToQueryPlan() throws Exception {
        String res = "{\"partitions\":{"
                + "\"11017\":{\"routings\":[\"be1\",\"be2\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                + "\"11019\":{\"routings\":[\"be3\",\"be4\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";

        List<String> routings11017 = new ArrayList<>(2);
        routings11017.add("be1");
        routings11017.add("be2");

        Tablet tablet11017 = new Tablet();
        tablet11017.setSchemaHash(1L);
        tablet11017.setVersionHash(1L);
        tablet11017.setVersion(3);
        tablet11017.setRoutings(routings11017);

        List<String> routings11019 = new ArrayList<>(2);
        routings11019.add("be3");
        routings11019.add("be4");

        Tablet tablet11019 = new Tablet();
        tablet11019.setSchemaHash(1L);
        tablet11019.setVersionHash(1L);
        tablet11019.setVersion(3);
        tablet11019.setRoutings(routings11019);

        Map<String, Tablet> partitions = new LinkedHashMap<>();
        partitions.put("11017", tablet11017);
        partitions.put("11019", tablet11019);

        QueryPlan expected = new QueryPlan();
        expected.setPartitions(partitions);
        expected.setStatus(200);
        expected.setOpaquedQueryPlan("query_plan");

        QueryPlan actual = RestService.parseQueryPlan(res, logger);
        Assertions.assertEquals(expected, actual);

        String notJsonRes = "not json";
        StarRocksException exception =
                Assertions.assertThrows(StarRocksException.class, () -> RestService.parseSchema(notJsonRes, logger));
        Assertions.assertTrue(exception.getMessage().startsWith("StarRocks FE's response is not a json. res:"));

        String notOkRes = "{\"partitions\":{\"11017\":{\"routings\":[\"be1\",\"be2\"],\"version\":3,"
                + "\"versionHash\":1,\"schemaHash\":1}},\"opaqued_query_plan\":\"queryPlan\",\"status\":20}";
        exception = Assertions.assertThrows(StarRocksException.class, () -> RestService.parseSchema(notOkRes, logger));
        Assertions.assertTrue(exception.getMessage().startsWith("StarRocks FE's response is not OK, status is "));
    }

    @Test public void testSelectTabletBe() throws Exception {
        String res = "{\"partitions\":{"
                + "\"11017\":{\"routings\":[\"be1\",\"be2\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                + "\"11019\":{\"routings\":[\"be3\",\"be4\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                + "\"11021\":{\"routings\":[\"be3\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";

        QueryPlan queryPlan = RestService.parseQueryPlan(res, logger);

        List<Long> be1Tablet = new ArrayList<>();
        be1Tablet.add(11017L);
        List<Long> be3Tablet = new ArrayList<>();
        be3Tablet.add(11019L);
        be3Tablet.add(11021L);
        Map<String, List<Long>> expected = new HashMap<>();
        expected.put("be1", be1Tablet);
        expected.put("be3", be3Tablet);

        Assertions.assertEquals(expected, RestService.selectBeForTablet(queryPlan, logger));

        String noBeRes = "{\"partitions\":{" + "\"11021\":{\"routings\":[],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> RestService.selectBeForTablet(RestService.parseQueryPlan(noBeRes, logger), logger));
        Assertions.assertTrue(exception.getMessage().startsWith("Cannot choice StarRocks BE for tablet"));

        String notNumberRes =
                "{\"partitions\":{" + "\"11021xxx\":{\"routings\":[\"be1\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                        + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";
        exception = Assertions.assertThrows(StarRocksException.class,
                () -> RestService.selectBeForTablet(RestService.parseQueryPlan(noBeRes, logger), logger));
        Assertions.assertTrue(exception.getMessage().startsWith("Cannot choice StarRocks BE for tablet"));
    }

    @Test public void testGetTabletSize() {
        Settings settings = new PropertiesSettings();
        Assertions.assertEquals(STARROCKS_TABLET_SIZE_DEFAULT, RestService.tabletCountLimitForOnePartition(settings, logger));

        settings.setProperty(STARROCKS_TABLET_SIZE, "xx");
        Assertions.assertEquals(STARROCKS_TABLET_SIZE_DEFAULT, RestService.tabletCountLimitForOnePartition(settings, logger));

        settings.setProperty(STARROCKS_TABLET_SIZE, "10");
        Assertions.assertEquals(10, RestService.tabletCountLimitForOnePartition(settings, logger));

        settings.setProperty(STARROCKS_TABLET_SIZE, "1");
        Assertions.assertEquals(STARROCKS_TABLET_SIZE_MIN, RestService.tabletCountLimitForOnePartition(settings, logger));
    }

    @Test public void testTabletsMapToPartition() throws Exception {
        List<Long> tablets1 = new ArrayList<>();
        tablets1.add(1L);
        tablets1.add(2L);
        List<Long> tablets2 = new ArrayList<>();
        tablets2.add(3L);
        tablets2.add(4L);
        Map<String, List<Long>> beToTablets = new HashMap<>();
        beToTablets.put("be1", tablets1);
        beToTablets.put("be2", tablets2);

        Settings settings = new PropertiesSettings();
        String opaquedQueryPlan = "query_plan";
        String cluster = "c";
        String database = "d";
        String table = "t";

        Set<Long> be1Tablet = new HashSet<>();
        be1Tablet.add(1L);
        be1Tablet.add(2L);
        RpcPartition pd1 = new RpcPartition(database, table, settings, "be1", be1Tablet, opaquedQueryPlan);

        Set<Long> be2Tablet = new HashSet<>();
        be2Tablet.add(3L);
        be2Tablet.add(4L);
        RpcPartition pd2 = new RpcPartition(database, table, settings, "be2", be2Tablet, opaquedQueryPlan);

        List<RpcPartition> expected = new ArrayList<>();
        expected.add(pd1);
        expected.add(pd2);
        Collections.sort(expected);

        List<RpcPartition> actual =
                RestService.tabletsMapToPartition(settings, beToTablets, opaquedQueryPlan, database, table, logger);
        Collections.sort(actual);

        Assertions.assertEquals(expected, actual);
    }

    @Test public void testNewSchemaResp() {
        String resp = "{\"tableId\":\"16025\",\"etlTable\":{\"indexes\":[{\"indexId\":16026,"
                + "\"columns\":[{\"columnName\":\"id\",\"columnType\":\"INT\",\"isAllowNull\":false,\"isKey\":true,"
                + "\"aggregationType\":null,\"defaultValue\":null,\"stringLength\":0,\"precision\":0,\"scale\":0,"
                + "\"defineExpr\":null},{\"columnName\":\"name\",\"columnType\":\"VARCHAR\",\"isAllowNull\":true,"
                + "\"isKey\":false,\"aggregationType\":\"NONE\",\"defaultValue\":\"\",\"stringLength\":65533,"
                + "\"precision\":0,\"scale\":0,\"defineExpr\":null},{\"columnName\":\"score\",\"columnType\":\"INT\","
                + "\"isAllowNull\":false,\"isKey\":false,\"aggregationType\":\"NONE\",\"defaultValue\":\"0\","
                + "\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}],\"schemaHash\":610340242,"
                + "\"indexType\":\"DUPLICATE\",\"isBaseIndex\":true}],\"partitionInfo\":{\"partitionType\":"
                + "\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[\"id\"],\"partitions\":"
                + "[{\"partitionId\":16024,\"startKeys\":[],\"endKeys\":[],\"isMinPartition\":true,\"isMaxPartition"
                + "\":true,\"bucketNum\":2}]},\"fileGroups\":[]},\"properties\":[{\"name\":\"id\",\"isKey\":\"true\","
                + "\"comment\":\"\",\"type\":\"INT\"},{\"name\":\"name\",\"isKey\":\"false\",\"comment\":\"\",\"type\":"
                + "\"VARCHAR\"},{\"name\":\"score\",\"isKey\":\"false\",\"comment\":\"\",\"type\":\"INT\"}],\"status\":200}";

        Schema schema = RestService.parseSchema(resp, logger);
        Assertions.assertEquals(schema.getTableId().longValue(), 16025L);

    }
}

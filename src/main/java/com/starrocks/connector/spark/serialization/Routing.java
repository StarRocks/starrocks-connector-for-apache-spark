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

import static com.starrocks.connector.spark.util.ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE;

import com.starrocks.connector.spark.cfg.ConfigurationOptions;
import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.exception.IllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * present an StarRocks BE address.
 */
public class Routing {
    private static Logger logger = LoggerFactory.getLogger(Routing.class);

    private String host;
    private int port;

    public Routing(String routing, Settings settings) throws IllegalArgumentException {
        parseRouting(routing, settings);
    }

    private void parseRouting(String routing, Settings settings) throws IllegalArgumentException {
        logger.debug("Parse StarRocks BE address: '{}'.", routing);
        String beHostMappingList = settings.getProperty(ConfigurationOptions.STARROCKS_BE_HOST_MAPPING_LIST, "");
        if (beHostMappingList.length() > 0) {
            String list = beHostMappingList;
            Map<String, String> mappingMap = new HashMap<>();
            String[] beHostMappingInfos = list.split(";");
            for (String beHostMappingInfo : beHostMappingInfos) {
                String[] mapping = beHostMappingInfo.split(",");
                mappingMap.put(mapping[1].trim(), mapping[0].trim());
            }
            if (!mappingMap.containsKey(routing)) {
                throw new RuntimeException("Not find be node info from the be port mappping list");
            }
            routing = mappingMap.get(routing);
            logger.info("query data from be by using be-hostname {}", routing);
        } else {
            logger.info("query data from be by using be-ip {}", routing);
        }

        String[] hostPort = routing.split(":");
        if (hostPort.length != 2) {
            logger.error("Format of StarRocks BE address '{}' is illegal.", routing);
            throw new IllegalArgumentException("StarRocks BE", routing);
        }
        this.host = hostPort[0];
        try {
            this.port = Integer.parseInt(hostPort[1]);
        } catch (NumberFormatException e) {
            logger.error(PARSE_NUMBER_FAILED_MESSAGE, "StarRocks BE's port", hostPort[1]);
            throw new IllegalArgumentException("StarRocks BE", routing);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "StarRocks BE{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}

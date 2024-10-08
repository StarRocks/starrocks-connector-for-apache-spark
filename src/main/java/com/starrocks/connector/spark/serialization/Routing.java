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


import com.starrocks.connector.spark.exception.IllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.starrocks.connector.spark.util.ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE;

/**
 * present an StarRocks BE address.
 */
public class Routing {
    private static Logger logger = LoggerFactory.getLogger(Routing.class);

    private String host;
    private int port;

    public Routing(String routing) throws IllegalArgumentException {
        parseRouting(routing);
    }

    private void parseRouting(String routing) throws IllegalArgumentException {
        logger.debug("Parse StarRocks BE address: '{}'.", routing);
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

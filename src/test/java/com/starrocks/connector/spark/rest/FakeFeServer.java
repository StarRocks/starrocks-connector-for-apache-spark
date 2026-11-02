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

package com.starrocks.connector.spark.rest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A fake StarRocks FE that answers {@code POST /api/{db}/{table}/_query_plan} the way
 * {@code TableQueryPlanAction} does: it plans the scan on the warehouse named by the {@code warehouse}
 * request header ({@code default_warehouse} when the header is absent), routes every tablet to a compute
 * node of that warehouse, and answers an unknown warehouse with HTTP 400.
 */
public class FakeFeServer implements AutoCloseable {

    public static final String DEFAULT_WAREHOUSE = "default_warehouse";
    public static final String PARITY_WAREHOUSE = "parity";
    public static final List<String> DEFAULT_WAREHOUSE_NODES =
            Collections.unmodifiableList(Arrays.asList("cn-default-0:9060", "cn-default-1:9060"));
    public static final List<String> PARITY_WAREHOUSE_NODES =
            Collections.unmodifiableList(Arrays.asList("cn-parity-0:9060", "cn-parity-1:9060"));

    /** One request the fake FE received. */
    public static final class Request {
        public final String method;
        public final String path;
        public final String body;
        private final Map<String, String> headers;

        private Request(String method, String path, String body, Map<String, String> headers) {
            this.method = method;
            this.path = path;
            this.body = body;
            this.headers = headers;
        }

        /** The value of a request header (case-insensitive name), or null if the request has none. */
        public String header(String name) {
            return headers.get(name);
        }
    }

    private final Map<String, List<String>> computeNodesByWarehouse;
    private final List<Long> tabletIds;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final HttpServer server;

    /**
     * @param computeNodesByWarehouse the warehouses this FE knows, each with its compute nodes ("host:be_port")
     * @param tabletIds               the tablets of the table the requests read
     */
    public FakeFeServer(Map<String, List<String>> computeNodesByWarehouse, Long... tabletIds) throws IOException {
        this.computeNodesByWarehouse = computeNodesByWarehouse;
        this.tabletIds = Arrays.asList(tabletIds);
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        this.server.createContext("/api/", this::handle);
        this.server.start();
    }

    /**
     * Starts an FE with two warehouses, {@code default_warehouse} and {@code parity}, of two compute nodes
     * each, serving a table of four tablets.
     */
    public static FakeFeServer startWithTwoWarehouses() throws IOException {
        Map<String, List<String>> warehouses = new TreeMap<>();
        warehouses.put(DEFAULT_WAREHOUSE, DEFAULT_WAREHOUSE_NODES);
        warehouses.put(PARITY_WAREHOUSE, PARITY_WAREHOUSE_NODES);
        return new FakeFeServer(warehouses, 11001L, 11002L, 11003L, 11004L);
    }

    /** The FE HTTP address in the {@code host:port} form that {@code starrocks.fenodes} takes. */
    public String address() {
        return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }

    public List<Request> requests() {
        return Collections.unmodifiableList(new ArrayList<>(requests));
    }

    public Request lastRequest() {
        if (requests.isEmpty()) {
            throw new IllegalStateException("The fake FE has not received any request");
        }
        return requests.get(requests.size() - 1);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        exchange.getRequestHeaders().forEach((name, values) -> headers.put(name, values.get(0)));
        String path = exchange.getRequestURI().getPath();
        requests.add(new Request(exchange.getRequestMethod(), path, readFully(exchange.getRequestBody()), headers));

        if (!path.endsWith("/_query_plan")) {
            respond(exchange, 404, "{\"status\":404}");
            return;
        }
        String warehouse = headers.getOrDefault("warehouse", DEFAULT_WAREHOUSE);
        List<String> computeNodes = computeNodesByWarehouse.get(warehouse);
        if (computeNodes == null) {
            respond(exchange, 400,
                    "{\"exception\":\"The warehouse parameter [" + warehouse + "] is invalid\",\"status\":400}");
            return;
        }
        StringBuilder partitions = new StringBuilder();
        for (int i = 0; i < tabletIds.size(); i++) {
            if (i > 0) {
                partitions.append(',');
            }
            partitions.append('"').append(tabletIds.get(i)).append("\":{\"routings\":[\"")
                    .append(computeNodes.get(i % computeNodes.size()))
                    .append("\"],\"version\":2,\"versionHash\":0,\"schemaHash\":1}");
        }
        respond(exchange, 200,
                "{\"partitions\":{" + partitions + "},\"opaqued_query_plan\":\"plan\",\"status\":200}");
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String readFully(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n;
        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}

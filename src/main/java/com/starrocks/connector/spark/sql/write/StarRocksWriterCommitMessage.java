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

package com.starrocks.connector.spark.sql.write;

import com.starrocks.data.load.stream.StreamLoadSnapshot;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;

public class StarRocksWriterCommitMessage implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final StreamLoadSnapshot snapshot;

    public StarRocksWriterCommitMessage(int partitionId,
                                        long taskId,
                                        long epochId,
                                        StreamLoadSnapshot snapshot) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.snapshot = snapshot;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getTaskId() {
        return taskId;
    }

    public long getEpochId() {
        return epochId;
    }

    public StreamLoadSnapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarRocksWriterCommitMessage that = (StarRocksWriterCommitMessage) o;
        return partitionId == that.partitionId && taskId == that.taskId && epochId == that.epochId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, taskId, epochId);
    }

    @Override
    public String toString() {
        return "StarRocksWriterCommitMessage{" +
                "partitionId=" + partitionId +
                ", taskId=" + taskId +
                ", epochId=" + epochId +
                '}';
    }
}

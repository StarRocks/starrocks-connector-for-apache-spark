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

package com.ververica.cdc.connectors.mysql.debezium.model;

import org.apache.flink.table.types.logical.RowType;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * @program: flink-cdc-connectors
 * @description: SourceRecordWithRowType
 * @author: zns
 * @create: 2022-01-13 15:15
 */
public class SourceRecordWithRowType extends SourceRecord {
    private RowType rowType;

    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    public SourceRecordWithRowType(SourceRecord sourceRecord) {
        super(
                sourceRecord.sourcePartition(),
                sourceRecord.sourceOffset(),
                sourceRecord.topic(),
                sourceRecord.kafkaPartition(),
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema(),
                sourceRecord.value(),
                sourceRecord.timestamp(),
                sourceRecord.headers());
    }

    public SourceRecordWithRowType(SourceRecord sourceRecord, RowType rowType) {
        super(
                sourceRecord.sourcePartition(),
                sourceRecord.sourceOffset(),
                sourceRecord.topic(),
                sourceRecord.kafkaPartition(),
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema(),
                sourceRecord.value(),
                sourceRecord.timestamp(),
                sourceRecord.headers());
        this.rowType = rowType;
    }

    @Override
    public String toString() {
        return "SourceRecordWithRowType{" + "rowType=" + rowType + "} " + super.toString();
    }
}

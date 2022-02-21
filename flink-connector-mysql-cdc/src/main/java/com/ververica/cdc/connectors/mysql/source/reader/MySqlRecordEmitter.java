/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.reader;

import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import io.debezium.relational.TableId;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.*;

/**
 * The {@link RecordEmitter} implementation for {@link MySqlSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 */
public final class MySqlRecordEmitter<T>
        implements RecordEmitter<SourceRecord, T, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    private final MySqlSourceReaderMetrics sourceReaderMetrics;
    private final boolean includeSchemaChanges;
    private final boolean generateSchemaRowTypes;
    private final boolean includeRowTypesWithData;
    private final OutputCollector<T> outputCollector;

    private final Set<TableId> tableIdSet;

    public MySqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges) {
        this(debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges, false);
    }

    public MySqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            boolean generateSchemaRowTypes) {
        this(debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges, false, generateSchemaRowTypes);
    }

    public MySqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            boolean includeRowTypesWithData,
            boolean generateSchemaRowTypes) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.includeSchemaChanges = includeSchemaChanges;
        this.includeRowTypesWithData = includeRowTypesWithData;
        this.outputCollector = new OutputCollector<>();
        this.generateSchemaRowTypes = generateSchemaRowTypes;
        this.tableIdSet = new HashSet<>();
    }

    @Override
    public void emitRecord(SourceRecord element, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            BinlogOffset watermark = getWatermark(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } //else if (isSchemaChangeEvent(element)) {
        else if (isSchemaChangeEvent(element) && splitState.isBinlogSplitState()) {
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asBinlogSplitState().recordSchema(tableChange.getId(), tableChange);
//                if (generateSchemaRowTypes) {
//                    emitElement(getSourceRecordWithRowType(tableChange), output);
//                }
            }
            if (includeSchemaChanges) {
                if (includeRowTypesWithData) {
                    element = getSourceRecordWithRowType(element, splitState.toMySqlSplit().asBinlogSplit().getTableSchemas());
                }
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element)) {
            if (splitState.isBinlogSplitState()) {
                BinlogOffset position = getBinlogPosition(element);
                splitState.asBinlogSplitState().setStartingOffset(position);
                if (includeRowTypesWithData) {
                    element = getSourceRecordWithRowType(element, splitState.toMySqlSplit().asBinlogSplit().getTableSchemas());
                }

            } else if (splitState.isSnapshotSplitState()) {
                if (includeRowTypesWithData) {
                    element = getSourceRecordWithRowType(element, splitState.toMySqlSplit().asSnapshotSplit().getTableSchemas());
                }
                if (generateSchemaRowTypes && !tableIdSet.contains(splitState.toMySqlSplit().asSnapshotSplit().getTableId())) {
                    MySqlSnapshotSplit mySqlSnapshotSplit = splitState.toMySqlSplit().asSnapshotSplit();
                    TableId tableId = mySqlSnapshotSplit.getTableId();
                    tableIdSet.add(tableId);
                    TableChanges.TableChange tableChange = mySqlSnapshotSplit.getTableSchemas().get(tableId);
                    emitElement(getSourceRecordWithRowType(tableChange), output);
                }
            }
            reportMetrics(element);
            emitElement(element, output);
        } else {
            // unknown element
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

    private void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        outputCollector.output = output;
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }

    private void reportMetrics(SourceRecord element) {
        long now = System.currentTimeMillis();
        // record the latest process time
        sourceReaderMetrics.recordProcessTime(now);
        Long messageTimestamp = getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
            // report emit delay
            sourceReaderMetrics.recordEmitDelay(now - messageTimestamp);
        }
    }

    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}

package com.ververica.cdc.connectors.mysql.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.mysql.debezium.model.SourceRecordWithRowType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @program: econ-analysis
 * @description: 获取SourceRecord
 * @author: zns
 * @create: 2021-12-16 18:03
 */
public class SourceRecordWithRowTypeSchema
        implements DebeziumDeserializationSchema<SourceRecordWithRowType> {
    private static final long serialVersionUID = 5260331829313411586L;

    @Override
    public void deserialize(SourceRecord record, Collector<SourceRecordWithRowType> out)
            throws Exception {
        out.collect((SourceRecordWithRowType) record);
    }

    @Override
    public TypeInformation<SourceRecordWithRowType> getProducedType() {
        return TypeInformation.of(SourceRecordWithRowType.class);
    }
}

package com.example.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class DateSubtractTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "subtract 32400000 milliseconds from date field";

    @Override
    public R apply(R record) {
        Struct value = (Struct) record.value();
        Schema schema = record.valueSchema();

        long date = value.getInt64("date");
        long updatedDate = date - 32400000;

        Struct updatedValue = new Struct(schema)
            .put("date_id", value.getInt32("date_id"))
            .put("date", updatedDate)
            .put("timeframe", value.getString("timeframe"));

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            updatedValue,
            record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op
    }
}

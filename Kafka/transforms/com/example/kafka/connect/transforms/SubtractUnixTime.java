package com.example.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class SubtractUnixTime<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Subtracts a specified amount of time in milliseconds from a Unix timestamp field.";
    private static final String PURPOSE = "subtract time from Unix timestamp";

    private interface ConfigName {
        String FIELD = "field";
        String AMOUNT = "amount";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The field to subtract time from.")
            .define(ConfigName.AMOUNT, ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "The amount of time in milliseconds to subtract.");

    private String field;
    private long amount;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        field = config.getString(ConfigName.FIELD);
        amount = config.getLong(ConfigName.AMOUNT);
        System.out.println("Configured with field: " + field + " and amount: " + amount);
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        System.out.println("Record class: " + value.getClass().getName());
        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            if (map.containsKey(field)) {
                long timestamp = (long) map.get(field);
                long newTimestamp = timestamp - amount;
                map.put(field, newTimestamp);
                // Log to ensure this code path is executed
                System.out.println("Subtracted " + amount + " from field '" + field + "'. New value: " + newTimestamp);
                // Create and return a new record with the modified value
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), map, record.timestamp());
            } else {
                System.out.println("Field '" + field + "' not found in the record.");
            }
        } else {
            System.out.println("Record value is not a Map instance.");
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No-op
    }
}

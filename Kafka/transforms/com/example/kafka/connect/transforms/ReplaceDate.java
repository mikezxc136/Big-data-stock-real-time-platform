package com.example.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class ReplaceDate<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Replaces the specified field's value with 9999999999999.";
    private static final String PURPOSE = "replace field value";

    private interface ConfigName {
        String FIELD = "field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The field to replace value of.");

    private String field;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        field = config.getString(ConfigName.FIELD);
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            if (map.containsKey(field)) {
                map.put(field, 9999999999999L);
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), map, record.timestamp());
            }
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

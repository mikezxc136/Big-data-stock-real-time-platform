package com.example.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class PrintData<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_NAME = "date";
    private static final Long SUBTRACT_VALUE = 32400000L;

    @Override
    public R apply(R record) {
        Object value = record.value();
        String abcd = "=======================================";
        System.out.println(abcd);
        System.out.println("Record class: " + value.getClass().getName());
        System.out.println("Record value: " + value);

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Object dateField = struct.get(FIELD_NAME);
            if (dateField != null) {
                System.out.println("Date field class: " + dateField.getClass().getName());
                System.out.println("Date field value: " + dateField);
                System.out.println("Updated Date SUBTRACT_VALUE value: " + SUBTRACT_VALUE);

                if (dateField instanceof Long) {
                    Long dateValue = (Long) dateField;
                    Long newValue = dateValue - SUBTRACT_VALUE;
                    struct.put(FIELD_NAME, newValue);
                    System.out.println("Updated Date field value: " + newValue);
                } else {
                    System.out.println("Date field is not of type Long");
                }
            } else {
                System.out.println("Date field is null");
            }
        } else {
            System.out.println("Record value is not an instance of Struct");
        }

        return record;
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

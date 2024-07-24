package com.example.kafka.connect.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

public class SubtractUnixTimeTest {
    public static void main(String[] args) {
        // Create an instance of SubtractUnixTime
        SubtractUnixTime<SinkRecord> transform = new SubtractUnixTime<>();

        // Configure the transform
        Map<String, String> config = new HashMap<>();
        config.put("field", "date");
        config.put("amount", "32400000"); // 9 hours in milliseconds
        transform.configure(config);

        // Create a sample record with a timestamp
        Map<String, Object> value = new HashMap<>();
        value.put("date", 1704067200000L); // Unix timestamp for 2024-01-01T00:00:00.000Z
        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, value, 0);

        // Apply the transform
        SinkRecord transformedRecord = transform.apply(record);

        // Check the result
        Map<String, Object> transformedValue = (Map<String, Object>) transformedRecord.value();
        System.out.println("Original timestamp in record: " + value.get("date"));
        System.out.println("Transformed timestamp in record: " + transformedValue.get("date"));
    }
}

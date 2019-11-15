package io.confluent.connect.transforms.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class TimestampToDateConverterTest {

    @org.junit.jupiter.api.Test
    void apply() {
        SchemaBuilder valueSchema = new SchemaBuilder(Schema.Type.STRUCT);
        valueSchema.field("id", Schema.INT32_SCHEMA);
        Struct value = new Struct(valueSchema);
        value.put("id", 1);
        SinkRecord testRecord = new SinkRecord(
                "topic1",
                0,
                null,
                null,
                valueSchema,
                value,
                0L);
        TimestampToDateConverter<SinkRecord> timestampToDateConverter = new TimestampToDateConverter.Value<>();
        Map<String, String> configurations = new HashMap<>();
        configurations.put("target_field", "day");
        configurations.put("source_field", "timestamp");
        timestampToDateConverter.configure(configurations);
        SinkRecord result = timestampToDateConverter.apply(testRecord);
        assertEquals(1, ((Struct) result.value()).getInt32("id"));
    }
}
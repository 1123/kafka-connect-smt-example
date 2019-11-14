package io.confluent.connect.transforms.examples;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public abstract class TimestampToDateConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private String fieldName;

    public R apply(R record) {
        return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private static final String FIELD_CONFIG = "field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Field name to extract.");

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_CONFIG);
    }

    public static class Value<R extends ConnectRecord<R>> extends TimestampToDateConverter<R> {
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        protected Object operatingValue(R record) {
            return record.value();
        }

        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
package io.confluent.connect.transforms.examples;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

@Slf4j
public abstract class TimestampToDateConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private String sourceField;
    private static final String SOURCE_FIELD_CONFIG = "source_field";
    private String targetField;
    private static final String TARGET_FIELD_CONFIG = "target_field";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SOURCE_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.MEDIUM,
                    "Input field name. This must be of type timestamp.")
            .define(TARGET_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Target field name to hold the day."
            );

    private Schema createNewValueSchema(R record) {
        SchemaBuilder newValueSchema = new SchemaBuilder(Schema.Type.STRUCT);
        for (Field field: record.valueSchema().fields()) {
            newValueSchema.field(field.name(), field.schema());
        }
        newValueSchema.field(targetField, new SchemaBuilder(Schema.Type.STRING).defaultValue("9999-99-99"));
        return newValueSchema.build();
    }

    private Struct createNewValueStruct(R record, Schema newValueSchema) {
        Struct newValueStruct = new Struct(newValueSchema);
        for (Field field: record.valueSchema().fields()) {
            newValueStruct.put(field.name(), ((Struct) record.value()).get(field));
        }
        newValueStruct.put(targetField, "2019-11-11");
        return newValueStruct;
    }

    public R apply(R record) {
        log.info(record.toString());
        log.info(record.value().toString());
        log.info(record.valueSchema().toString());
        log.info(record.valueSchema().fields().toString());
        Schema newValueSchema = createNewValueSchema(record);
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newValueSchema,
                createNewValueStruct(record, newValueSchema),
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }


    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        sourceField = config.getString(SOURCE_FIELD_CONFIG);
        targetField = config.getString(TARGET_FIELD_CONFIG);
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
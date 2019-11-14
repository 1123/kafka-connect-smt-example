package io.confluent.connect.transforms.examples;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class TimestampToDateConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public R apply(R record) {
        return record;
    }

}
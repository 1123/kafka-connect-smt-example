curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
        "name": "jdbc_sink_postgres_08",
        "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "connection.url": "jdbc:postgresql://postgres:5432/postgres",
                "connection.user": "connect_user",
                "connection.password": "asgard",
                "key.converter":"org.apache.kafka.connect.json.JsonConverter",
                "value.converter":"org.apache.kafka.connect.json.JsonConverter",
		"topics": "postgres-02-accounts",
		"table.name.format": "demo.kafka_${topic}_08",
		"auto.create": "true",
		"insert.mode" : "upsert", 
		"pk.mode": "record_value",
		"pk.fields" : "id",
		"errors.tolerance": "all",
                "transforms": "AddDayFromTimestampConverter",
                "transforms.AddDayFromTimestampConverter.type":
                   "io.confluent.connect.transforms.examples.TimestampToDateConverter$Value",
                "transforms.AddDayFromTimestampConverter.source_field": "update_ts",
                "transforms.AddDayFromTimestampConverter.target_field": "day"
        }
}'

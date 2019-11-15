curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
        "name": "jdbc_source_postgres_02",
        "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                  "connection.url": "jdbc:postgresql://postgres:5432/postgres",
                "connection.user": "connect_user",
                "connection.password": "asgard",
                "topic.prefix": "postgres-02-",
                "mode":"bulk",
                "poll.interval.ms" : 3600000,
                "key.converter":"org.apache.kafka.connect.json.JsonConverter",
                "value.converter":"org.apache.kafka.connect.json.JsonConverter"
        }
}'

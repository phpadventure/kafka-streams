package com.stream.reactive_kafka.dto.kafka_connector;

public record KafkaConnectMessage<P> (Schema schema, P payload) {
    public static final String SCHEMA = "schema";
    public static final String PAYLOAD = "payload";
}

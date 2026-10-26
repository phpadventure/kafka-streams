package com.stream.reactive_kafka.dto.kafka_connector;

import java.util.UUID;

public record PaymentEvent(UUID orderId, PaymentEventStatusEnum status) {
    public static String CONNECTOR_CLASS_NAME = "com.kafka.connector.models.PaymentEvent";
}

package com.kafka.connector.models;

import java.util.UUID;

public record PaymentEvent(UUID orderId, PaymentStatus status) {
   
}

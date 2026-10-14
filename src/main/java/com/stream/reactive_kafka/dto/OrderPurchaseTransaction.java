package com.stream.reactive_kafka.dto;

import java.util.UUID;

public record OrderPurchaseTransaction(UUID orderId, Integer totalPrice) {
}

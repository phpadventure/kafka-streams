package com.stream.reactive_kafka.dto;

import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

import java.util.UUID;

@JsonNaming(PropertyNamingStrategies.LowerCamelCaseStrategy.class)
public record ItemAdded(UUID orderId, UUID itemId, Integer price, Integer amount) {
}

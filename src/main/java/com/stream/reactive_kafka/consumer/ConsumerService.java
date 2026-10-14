package com.stream.reactive_kafka.consumer;

import com.stream.reactive_kafka.dto.ItemAdded;
import com.stream.reactive_kafka.dto.OrderCreated;
import com.stream.reactive_kafka.dto.OrderPurchaseTransaction;
import jakarta.annotation.PostConstruct;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;


@Configuration
@Slf4j
@KafkaListener(topics = "${kafka.my.default.topic}", ackMode = "MANUAL_IMMEDIATE")
public class ConsumerService {

    @Value("${kafka.my.default.topic}")
    private String topicName;

//    @KafkaListener(topics = "${kafka.my.default.topic}", ackMode = "MANUAL_IMMEDIATE")
//    public void listenWithHeaders(
//            @Payload String message,
//            @Header(KafkaHeaders.RECEIVED_KEY) String key,
//            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
//            Acknowledgment ack
//    ) {
//
//        try {
//            log.info("[consumer] ({}) Received Message: {} from partition: {} for key : {}",
//                    Thread.currentThread().getName(), message, partition, key);
//            ack.acknowledge();
//        } catch (Exception e) {
//            log.error("Failed to proceed msg: {} add to dead letter queue", message);
//        }
//    }

    private ObjectMapper mapper;

    @PostConstruct
    public void setMapper() {
        mapper = JsonMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
                // .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)   // для snake_case
                // .propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)   // для kebab-case
                .build();
    }


    @KafkaHandler
    public void handleOrderCreated(OrderCreated orderCreated, @Header(KafkaHeaders.RECEIVED_KEY) String key,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               Acknowledgment ack) {
        var msg = convertMsg(orderCreated);
        try {
            log.info("[consumer] ({}) Received Message: {} from partition: {} for key : {}",
                    Thread.currentThread().getName(), msg, partition, key);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to proceed msg: {} add to dead letter queue", msg);
        }
    }

    @KafkaHandler
    public void handleItemAdded(ItemAdded itemAdded, @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                Acknowledgment ack) {
        var msg = convertMsg(itemAdded);
        try {
            log.info("[consumer] ({}) Received Message: {} from partition: {} for key : {}",
                    Thread.currentThread().getName(), msg, partition, key);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to proceed msg: {} add to dead letter queue", msg);
        }
    }

    @KafkaHandler
    public void handleOrderPurchase(OrderPurchaseTransaction orderTransaction,
                                    @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                    Acknowledgment ack) {
        var msg = convertMsg(orderTransaction);
        try {
            log.info("[consumer] ({}) Received Message: {} from partition: {} for key : {}",
                    Thread.currentThread().getName(), msg, partition, key);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to proceed msg: {} add to dead letter queue", msg);
        }
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object message,  @Header(KafkaHeaders.RECEIVED_KEY) String key,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        Acknowledgment ack) {
        try {
            log.info("[consumer] ({}) Received Message: {} from partition: {} for key : {}",
                    Thread.currentThread().getName(), message, partition, key);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to proceed msg: {} add to dead letter queue", message);
        }
    }

    @SneakyThrows
    private String convertMsg(Object object) {
        return mapper.writeValueAsString(object);
    }
}
package com.stream.reactive_kafka.consumer;

import com.stream.reactive_kafka.dto.ItemAdded;
import com.stream.reactive_kafka.dto.OrderCreated;
import com.stream.reactive_kafka.dto.OrderPurchaseTransaction;
import com.stream.reactive_kafka.dto.kafka_connector.KafkaConnectMessage;
import com.stream.reactive_kafka.dto.kafka_connector.PaymentEvent;
import com.stream.reactive_kafka.dto.kafka_connector.Schema;
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
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.TypeFactory;

import java.util.LinkedHashMap;
import java.util.Map;


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

    @KafkaHandler
    public void handleUnknowJsonObject(Map<String, Object> message,
                                       @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                       Acknowledgment ack){
        if (!message.containsKey(KafkaConnectMessage.SCHEMA)) {
            unknown(convertMsg(message), key, partition, ack);
        }

        if (!message.containsKey(KafkaConnectMessage.PAYLOAD)) {
            unknown(convertMsg(message), key, partition, ack);
        }

        KafkaConnectMessage<PaymentEvent> messageObj = mapper.convertValue(message, TypeFactory.createDefaultInstance()
                        .constructParametricType(KafkaConnectMessage.class, PaymentEvent.class));

        // should convert separately and verify first class name only then convert map to obj
        if (!PaymentEvent.CONNECTOR_CLASS_NAME.equals(messageObj.schema().getName())) {
            unknown(convertMsg(message), key, partition, ack);
        }

        try {
            log.info("[consumer] [connector] ({}) Received Message: {} from partition: {} for key : {}",
                    Thread.currentThread().getName(), convertMsg(messageObj.payload()), partition, key);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to proceed msg from connector: {} add to dead letter queue", convertMsg(message));
        }

        ack.acknowledge();
    }

    @KafkaHandler(isDefault = true)
    public void unknown(String message,  @Header(KafkaHeaders.RECEIVED_KEY) String key,
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
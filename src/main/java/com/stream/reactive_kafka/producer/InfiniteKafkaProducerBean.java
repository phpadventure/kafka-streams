package com.stream.reactive_kafka.producer;

import com.stream.reactive_kafka.dto.OrderCreated;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class InfiniteKafkaProducerBean {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper mapper;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    
    @Value("${kafka.my.default.topic}")
    private String topicName;
    private int messageCounter = 0;

    private final AtomicBoolean shutDown = new AtomicBoolean(false);
    private final CountDownLatch countDownLatch = new CountDownLatch(2);

    public InfiniteKafkaProducerBean(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        mapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    private static final int partition_number = 3;

    @PostConstruct
    @SneakyThrows
    public void startSendingMessages() {
        // send message per partition key
        // use avro or json as serializer
        // set up two replicas cluster
        //executorService.submit(sendStringMessages());
        CompletableFuture.runAsync(sendStringMessages(), executorService);
        CompletableFuture.runAsync(sendDtoMessage(), executorService);
    }

    @PreDestroy
    @SneakyThrows
    public void shutdown() {
        shutDown.set(true);
        log.info("===shutdown waiting...");
        countDownLatch.await();
        log.info("===shutdown graceful after count down latch");
        executorService.shutdownNow();
    }

    private Runnable sendStringMessages() {
        return () -> {
            try {
                while (!shutDown.get()) {
                    String messagePayload = "Message " + (++messageCounter) + " key " + messageCounter % partition_number;

                    CompletableFuture<SendResult<String, Object>> result = kafkaTemplate.send(topicName,
                            String.valueOf(messageCounter % partition_number), messagePayload);

                    result.whenComplete((messageResult, ex) -> {
                        if (Objects.isNull(ex)) {
                            log.info("[producer] Send message:\"{}\" successfully", messagePayload);
                        } else {
                            log.error("[producer] Failed to send message: {}", messagePayload);
                        }
                    });

                    Thread.sleep(1000);
                }

                countDownLatch.countDown();
            } catch (InterruptedException e) {
                log.error("Exception during shutdown interruption");
                countDownLatch.countDown();
                Thread.currentThread().interrupt();
            }
        };
    }

    private static Integer keyCounter = 0;

    private Runnable sendDtoMessage() {

        return () -> {
            try {
                while (!shutDown.get()) {

                    OrderCreated orderCreated = new OrderCreated(UUID.randomUUID());

                    String messagePayload = "Key " + (++keyCounter) + " for object " + mapper.writeValueAsString(orderCreated);

                    CompletableFuture<SendResult<String, Object>> result = kafkaTemplate.send(topicName,
                            String.valueOf(keyCounter), orderCreated);

                    result.whenComplete((messageResult, ex) -> {
                        if (Objects.isNull(ex)) {
                            log.info("[producer] Send message:\"{}\" successfully", messagePayload);
                        } else {
                            log.error("[producer] Failed to send message: {}", messagePayload);
                        }
                    });


                    log.info("Send DTO");
                    Thread.sleep(1000);
                }

                countDownLatch.countDown();
            } catch (InterruptedException e) {
                log.error("Exception during shutdown interruption");
                countDownLatch.countDown();
                Thread.currentThread().interrupt();
            } finally {
                // do not throw here because I want message in catch
            }
        };
    }
}

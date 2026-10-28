package com.kafka.connector;

import com.kafka.connector.models.PaymentEvent;
import com.kafka.connector.models.PaymentStatus;
import com.kafka.connector.models.SchemaFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MySourceTask.class);

    private Map<String, String> configProps;

    private Integer keyCounter = 0;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;

        if (!configProps.containsKey(MyConnectorConfig.TOPIC_WRITE_TO)) {
            throw new RuntimeException("Missing topic configuration");
        }

// THIS PART OF CODE CAN BE USED WHEN WE NEED TO READ FROM PLACE WHERE PREVIOUS WORKER STOPPED
//        // Отримуємо доступ до сховища збережених офсетів через контекст таски
//        OffsetStorageReader offsetReader = this.context.offsetStorageReader();
//
//        // Питаємо, який офсет було збережено для таблиці "orders"
//        Map<String, String> partition = Collections.singletonMap("table", "orders");
//        Map<String, Object> lastOffset = offsetReader.offset(partition);
//
//        long lastReadId = 0;
//        if (lastOffset != null) {
//            // Якщо ми вже працювали раніше, відновлюємо наш ID
//            lastReadId = (Long) lastOffset.get("last_id");
//        }
//
//        // Тепер ви знаєте, що наступний SQL-запит до вашої БД має починатися з:
//        // "SELECT * FROM orders WHERE id > " + lastReadId
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        var statusList = List.of(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
        List<SourceRecord> events = new ArrayList<>();
        try {
            var paymentEvent = new PaymentEvent(UUID.randomUUID(), statusList.get(getRandomStatus()));
            // create payment event
            // order id: <random uuid>;
            // status: success, failed
            // add schema source also
            events.add(new SourceRecord(
                    new HashMap<>(), // used to let you know on start last table you stopped fetching data from you db, example table "orders"
                    new HashMap<>(), // used to let you know on start last record you stopped fetching data from you db,
                    // example record order id "100", retrieve this on start when you fetch data
                    configProps.get(MyConnectorConfig.TOPIC_WRITE_TO),
                    null,
                    null,
                    ++keyCounter,
                    SchemaFactory.paymentEventSchema,
                    SchemaFactory.buildPaymentEventStruct(paymentEvent)
            ));
            Thread.sleep(5000);
        } catch (InterruptedException exception) {
            log.error("Failed to execute task due to thread interrupt error");
            throw exception;
        }

        return events;
    }

    private int getRandomStatus() {
        Random random = new Random();
        return (random.nextInt(100 - 1) + 1) % 2;
    }


    @Override
    public void stop() {
        // Cleanup resources if needed when the task stops
    }
}

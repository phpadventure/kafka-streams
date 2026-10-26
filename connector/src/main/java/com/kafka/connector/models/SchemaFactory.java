package com.kafka.connector.models;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class SchemaFactory {
    public static final Schema paymentEventSchema = SchemaBuilder.struct()
            .name("com.kafka.connector.models.PaymentEvent")
            .field("orderId", Schema.STRING_SCHEMA)
            .field("status", Schema.STRING_SCHEMA)
            .build();

    public static Struct buildPaymentEventStruct(PaymentEvent paymentEvent) {
        return new Struct(paymentEventSchema)
                .put("orderId", paymentEvent.orderId().toString())
                .put("status", paymentEvent.status().name());

    }
}

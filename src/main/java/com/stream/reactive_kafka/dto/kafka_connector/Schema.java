package com.stream.reactive_kafka.dto.kafka_connector;

import lombok.*;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Schema {
    private String type;
    private List<SchemaField> fields;
    private Boolean optional;
    private String name;
}

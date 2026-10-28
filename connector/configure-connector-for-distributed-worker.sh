#!/bin/bash

echo "Sending POST request to Kafka Connect..."
curl -i -X POST http://kafka-connect-distributed-worker:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
       "name": "my-source-connector-distributed",
       "config": {
         "connector.class": "com.kafka.connector.MySourceConnector",
         "topic.write.to": "reactive-kafka-test",
         "tasks.max": "3"
       }
     }'
echo ""
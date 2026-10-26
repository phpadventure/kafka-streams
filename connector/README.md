1. We need to create fatJar using com.gradleup.shadow. We need all dependecies for connector except kafaka. They should be only for complication
2. Create a source or sink connector. => SourceConnector, SourceTask abstractions, etc.
3. We need to have connector config with DB url, tasks.max, topics. For distributed mod passed as REST call, for standalone as config file
4. ConfigDef - is a schema of custom values for config.

## How to start as standalone worker:
1. Create connector configuration file, example `my-source-connector.configuration`
2. put tasks.max for distributed task.
3. put name for a connector
4. put connector class name that worker can find later in jar plugin you upload. Jar can have multiple connectors inside
5. put other properties you need, topic name, db url, etc
6. create a fat jar `./gradlew shadowJar`
7. Worker configuration for stand alone. 
- bootstrap.servers - brokers ip   
- serialization configuration of objects for key, value, schema included for key,value
- plugin.path to the jar
- offset.storage.file.filename - path where stored offset data on local machine if worker crashes that it can gracefully restart
8. Quick start with running docker container of kafka
    - copy jar `docker cp D:\development\lohika\kafka_course\reactive-kafka\connector\build\libs\my-kafka-connector-1.0.jar  kind_elbakyan:/opt/kafka/connectors`
    - create copy of worker config and put plugin path `/opt/kafka/connectors`
    - copy configuration `docker cp D:\development\lohika\kafka_course\reactive-kafka\connector\my-source-connector.configuration  kind_elbakyan:/opt/kafka/config`
    - run in new terminal in docker container ` ./connect-standalone.sh ../config/my-connector-standalone.properties ../config/my-source-connector.configuration`
    -  before create a new topic `reactive-kafka-test`
    - in other terminal connect to consume messages for test `./kafka-console-consumer.sh --topic reactive-kafka-test --from-beginning --bootstrap-server localhost:9092 --group concole_group_2 --property print.key=true --property print.headers=true`


## How to start as distributed workers:
- Replication factor for offset and topic - is used to store worker state between replicas and topics.
- port
- 
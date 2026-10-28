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
8. Dockerize
   1. MANUAL - Quick start with running docker container of kafka
             - copy jar `docker cp D:\development\lohika\kafka_course\reactive-kafka\connector\build\libs\my-kafka-connector-1.0.jar  kind_elbakyan:/opt/kafka/connectors`
             - create copy of worker config and put plugin path `/opt/kafka/connectors`
             - copy configuration `docker cp D:\development\lohika\kafka_course\reactive-kafka\connector\my-source-connector.configuration  kind_elbakyan:/opt/kafka/config`
             - run in new terminal in docker container ` ./connect-standalone.sh ../config/my-connector-standalone.properties ../config/my-source-connector.configuration`
             -  before create a new topic `reactive-kafka-test`
             - in other terminal connect to consume messages for test `./kafka-console-consumer.sh --topic reactive-kafka-test --from-beginning --bootstrap-server localhost:9092 --group concole_group_2 --property print.key=true --property print.headers=true`
   2. Docker images
      - `docker build -t local-connector .`
      - use network `kafka-net` and broker name `my-kafka-broker`
      - `docker run -p 8083:8038 --network kafka-net local-connector`
      - in Java app fix to call 127.0.0.1:9094 as should be changed listeners for kafka brokers to work in external and internal network
   3. Docker compose
      - Docker file or simple service in docker-compose which start kafka and uses volume to persist(no mount)
      - Other service with workers for docker connect. We will need script which build JAVA(multistage) image, copy configs for worker and connector
      - Docker-compose at root level in reactive-kafka project 
      - `docker-compose up -d`


## How to start as distributed workers:
Additional settings for workers:
- Replication factor for offset and topic - is used to store worker state between replicas and topics.
- port
- group.id - also need to be set
- use DistributedWorker.Dockerfile. We will use Rest call to setup connectors for a cluster. Port of workers should go from 8083-8085 for example and should differ if it's on one machine. \
In our case it different replicas, so docker will give random ports. 
- Use docker-compose for this and separate service to configure workers after workers is up
- distributed.docker.config simply has overridden params for my docker-compose variables from default distributed.config(default)
- `configure-connector-for-distributed-worker.sh` execute rest configuration call
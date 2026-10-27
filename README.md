Purpose of this project is to host kafka application with minimal features including next tech stack:

### Phase 1:
- kafka [DONE]
- kafka connect [DONE]
- kafka streams + play with windows
- multi threads [POSTPONE]
- istio
- k8s + helm
- AWS
- terraform
- docker/docker-compose [DONE]
- monitoring prometheus/graphana
- open telemetry cloud sleuth(spring cloud) + jaeger


### Phase 2:
- kafka SQL
- ELK(filebeat) 
- refactor to use WebFlux, Java Reactor [POSTPONE]
- GCP? [POSTPONE]
- github actions [POSTPONE]
- tests [POSTPONE]


# Windows prerequisites
- winget installed
- Java 21 installed
- WSL installed
- rancher installed

# Kafka steps
## Install via docker process (no connectors)
- `docker network create kafka-net`
- `docker pull apache/kafka:4.3.1`
- if run java locally and no need for docker kafka connector inside container run this `docker run -p 9092:9092 apache/kafka:4.3.1`
- if you want to run connector within docker imgs use docker compose. Go to instruction to update Java application

## new cluster if run manually under UNIX
-  KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
- $ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
- $ bin/kafka-server-start.sh config/server.properties

## Install via docker-compose + connector
Will be used to run connector worker in standalone and distributed mode.
Components: kafka + kafka workers with connectors. We use own network here to distibute trafic on host and inside docker network \
    so different configration for Java application will be.
- `docker-compose up -d` // used docker compose because was complicated to pass all env vars, or should be inside image
- and change listener in Java app to be 127.0.0.1:9094 from :9092 for kafka brokers to work in external and internal network

## DEFAULT commands
- enter container `docker exec -it <container_id> bash`
- `/opt/kafka/` inside container to check kafka folders
- `./kafka-topics.sh --create --topic reactive-kafka-test --bootstrap-server localhost:9092` to create topic
- `./kafka-console-producer.sh --topic reactive-kafka-test --bootstrap-server localhost:9092` to produce events
- `./kafka-console-consumer.sh --topic reactive-kafka-test --from-beginning --bootstrap-server localhost:9092` consumer to read events
- `./kafka-console-consumer.sh --topic reactive-kafka-test --from-beginning --bootstrap-server localhost:9092 --group concole_group_2 --property print.key=true --property print.headers=true`


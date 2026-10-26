Purpose of this project is to host kafka application with minimal features including next tech stack:

### Phase 1:
- kafka
- kafka connect
- kafka streams + play with windows
- multi threads
- istio
- k8s + helm
- AWS
- terraform
- docker
- monitoring prometheus/graphana
- open telemetry cloud sleuth(spring cloud) + jaeger


### Phase 2:
- kafka SQL
- ELK(filebeat)
- refactor to use WebFlux, Java Reactor
- GCP?
- github actions
- tests


# Windows prerequisites
- winget installed
- Java 21 installed
- WSL installed
- rancher installed

# Kafka steps
## Install via docker process
- docker pull apache/kafka:4.3.1
- docker run -p 9092:9092 apache/kafka:4.3.1
- enter container `docker exec -it <container_id> bash`
- `/opt/kafka/` inside container to check kafka folders
- ` ./kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092` to create topic
- `./kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092` to produce events
- `./kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092` consumer to read events
- `./kafka-console-consumer.sh --topic reactive-kafka-test --from-beginning --bootstrap-server localhost:9092 --group concole_group_2 --property print.key=true --property print.headers=true`

## new cluster if run manually under UNIX
-  KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
- $ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
- $ bin/kafka-server-start.sh config/server.properties

## Install via docker-compose.
Will be used to run connector worker in standalone and distributed mode.
Components: kafka + kafka workers with connectors


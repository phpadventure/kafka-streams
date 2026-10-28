FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

COPY . .

RUN ./gradlew shadowJar --no-daemon

FROM apache/kafka:4.3.1

WORKDIR /opt/kafka

RUN mkdir -p /opt/kafka/connectors

# copy connector JAR
COPY --from=builder /build/build/libs/my-kafka-connector-*.jar connectors/my-kafka-connector.jar

# copy standalone worker config
COPY --from=builder /build/distributed.docker.config connectors/distributed.docker.config

WORKDIR /opt/kafka/bin

EXPOSE 8083

ENTRYPOINT ["./connect-distributed.sh",  "../connectors/distributed.docker.config"]



FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

COPY . .

RUN ./gradlew shadowJar --no-daemon

FROM apache/kafka:4.3.1

WORKDIR /opt/kafka

RUN mkdir -p /opt/kafka/connectors

# copy connector JAR
COPY --from=builder /build/build/libs/my-kafka-connector-*.jar connectors/my-kafka-connector.jar

# copy connector config
COPY --from=builder /build/my-source-connector.configuration connectors/my-source-connector.configuration

# copy standalone worker config
COPY --from=builder /build/standalone.docker.config connectors/standalone.docker.config

WORKDIR /opt/kafka/bin

EXPOSE 8083

ENTRYPOINT ["./connect-standalone.sh",  "../connectors/standalone.docker.config", "../connectors/my-source-connector.configuration"]



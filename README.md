# Kafka Streams Project

Spring Boot application demonstrating usage of the Kafka Streams.

This repo accompanies the following series of articles on Kafka Streams:

- [Kafka Streams: Introduction](https://medium.com/lydtech-consulting/kafka-streams-introduction-d7e5421feb1b)
- [Kafka Streams: Spring Boot Demo](https://medium.com/lydtech-consulting/kafka-streams-spring-boot-demo-ff0e74e08c9c)
- [Kafka Streams: Testing](https://medium.com/lydtech-consulting/kafka-streams-testing-f263f216808f)

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate streaming payments events that are filtered and transformed, and results emitted to outbound topics.

The account balances are tracked in a state store, which is exposed via REST endpoint allowing querying the current values.

## Component Tests

Build Spring Boot application jar:

```
mvn clean install
```

Build Docker container:

```
docker build -t ct/kafka-streams-demo:latest .
```

Assumes `ct` is used as the container prefix for the component tests (which is the default but can be overridden).

Run tests:

```
mvn test -Pcomponent
```

Run tests leaving containers running (for further test runs):

```
mvn test -Pcomponent -Dcontainers.stayup
```

### Inspecting Kafka Topics

View consumer groups:
`docker exec -it ct-kafka  /bin/sh /usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --list`

Inspect consumer group:
`docker exec -it ct-kafka  /bin/sh /usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kafka-streams-demo`

View topics:
`docker exec -it ct-kafka  /bin/sh /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list`

Inspect topic:
`docker exec -it ct-kafka  /bin/sh /usr/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic payment-event`

View messages on topic:
`docker exec -it ct-kafka  /bin/sh /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-event --from-beginning`

### Docker Commands

Manual clean up (if left containers up):
`docker rm -f $(docker ps -aq)`

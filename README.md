# Kafka Sample Project

## Setup

1. Start Kafka:
```bash
docker-compose up -d
```

2. Build the project:
```bash
mvn clean package
```

## Running

### Start Producer (generates random JSON events every second):
```bash
mvn exec:java -Dexec.mainClass="com.kafka.producer.Producer"
```

### Start Consumer (reads events from topic):
```bash
mvn exec:java -Dexec.mainClass="com.kafka.consumer.Consumer"
```

## Stop Kafka

```bash
docker-compose down
```

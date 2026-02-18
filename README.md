# Kafka Sample Project

A streaming application that generates random order data and calculates total prices by product category using Kafka.

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

**Producer Output:**
```
Starting order stream...
Sent Order: a1b2c3d4-5678-90ab-cdef-1234567890ab | Electronics | Qty: 3 | $45.67
Sent Order: e5f6g7h8-9012-34ij-klmn-5678901234op | Books | Qty: 2 | $12.99
Sent Order: q9r0s1t2-3456-78uv-wxyz-9012345678qr | Toys | Qty: 5 | $8.50
Sent Order: a3b4c5d6-7890-12ef-ghij-3456789012kl | Clothing | Qty: 1 | $29.99
```

### Start Consumer (reads events from topic):
```bash
mvn exec:java -Dexec.mainClass="com.kafka.consumer.Consumer"
```

**Consumer Output:**
```
Streaming orders and calculating totals by category...

Order: a1b2c3d4-5678-90ab-cdef-1234567890ab | Electronics | Qty: 3 × $45.67 = $137.01

=== CUMULATIVE TOTALS BY CATEGORY ===
  Electronics: $137.01
=====================================

Order: e5f6g7h8-9012-34ij-klmn-5678901234op | Books | Qty: 2 × $12.99 = $25.98

=== CUMULATIVE TOTALS BY CATEGORY ===
  Electronics: $137.01
  Books: $25.98
=====================================

Order: q9r0s1t2-3456-78uv-wxyz-9012345678qr | Toys | Qty: 5 × $8.50 = $42.50

=== CUMULATIVE TOTALS BY CATEGORY ===
  Electronics: $137.01
  Books: $25.98
  Toys: $42.50
=====================================

Order: a3b4c5d6-7890-12ef-ghij-3456789012kl | Clothing | Qty: 1 × $29.99 = $29.99

=== CUMULATIVE TOTALS BY CATEGORY ===
  Electronics: $137.01
  Books: $25.98
  Toys: $42.50
  Clothing: $29.99
=====================================
```

## Features

- **Streaming Data**: Producer generates orders every second
- **Real-time Processing**: Consumer calculates running totals by category
- **Product Categories**: Electronics, Books, Toys, Clothing

## Stop Kafka

```bash
docker-compose down
```

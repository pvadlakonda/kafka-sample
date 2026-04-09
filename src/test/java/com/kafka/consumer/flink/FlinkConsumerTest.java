package com.kafka.consumer.flink;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class FlinkConsumerTest {

    @Test
    void buildKafkaSourceReturnsNonNull() {
        KafkaSource<String> source = FlinkConsumer.buildKafkaSource();
        assertNotNull(source);
    }
}
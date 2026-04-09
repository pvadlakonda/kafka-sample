package com.kafka.consumer.flink;

import com.google.gson.Gson;
import com.kafka.config.KafkaConfig;
import com.kafka.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkConsumer {

    private static final Gson gson = new Gson();

    static KafkaSource<String> buildKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_NAME)
                .setGroupId(KafkaConfig.CONSUMER_GROUP_ID + "-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    void run(StreamExecutionEnvironment env) throws Exception {
        env.fromSource(buildKafkaSource(), WatermarkStrategy.noWatermarks(), "Kafka Source")
           .map(json -> gson.fromJson(json, Order.class))
           .process(new OrderProcessor());

        System.out.println("Streaming orders and calculating totals by category...\n");
        env.execute("Flink Kafka Consumer");
    }

    public static void main(String[] args) throws Exception {
        new FlinkConsumer().run(StreamExecutionEnvironment.getExecutionEnvironment());
    }
}
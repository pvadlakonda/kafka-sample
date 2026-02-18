package com.kafka.consumer;

import com.google.gson.Gson;
import com.kafka.config.KafkaConfig;
import com.kafka.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class FlinkConsumer {
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.TOPIC_NAME)
                .setGroupId(KafkaConfig.CONSUMER_GROUP_ID + "-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream.map(json -> gson.fromJson(json, Order.class))
              .process(new OrderProcessor());

        System.out.println("Streaming orders and calculating totals by category...\n");
        env.execute("Flink Kafka Consumer");
    }

    static class OrderProcessor extends ProcessFunction<Order, Order> {
        private final Map<String, Double> categoryTotals = new HashMap<>();

        @Override
        public void processElement(Order order, Context ctx, Collector<Order> out) {
            double totalPrice = order.getOrderQuantity() * order.getPricePerEach();
            categoryTotals.merge(order.getProductCategory(), totalPrice, Double::sum);

            System.out.printf("Order: %s | %s | Qty: %d × $%.2f = $%.2f%n",
                order.getOrderId(), order.getProductCategory(),
                order.getOrderQuantity(), order.getPricePerEach(), totalPrice);

            System.out.println("\n=== CUMULATIVE TOTALS BY CATEGORY ===");
            categoryTotals.forEach((category, total) ->
                System.out.printf("  %s: $%.2f%n", category, total));
            System.out.println("=====================================\n");

            out.collect(order);
        }
    }
}

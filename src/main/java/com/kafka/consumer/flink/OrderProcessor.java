package com.kafka.consumer.flink;

import com.kafka.model.Order;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class OrderProcessor extends ProcessFunction<Order, Order> {

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
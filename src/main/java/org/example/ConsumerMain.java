package org.example;

public class ConsumerMain {
    public static void main(String[] args) {

        String topicName = "prices";
        String bootstrapServers = "localhost:9092";

        PriceConsumer priceConsumer = new PriceConsumer(topicName,bootstrapServers, "price-group");
        priceConsumer.startSubscribing();
        priceConsumer.handlePrices();
    }
}
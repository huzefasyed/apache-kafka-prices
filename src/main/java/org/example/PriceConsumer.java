package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PriceConsumer {

    String topicName = "prices";
    String bootstrapServers = "localhost:9092";
    String groupId = "price-consumer-group";
    Consumer<String, String> consumer;

    PriceConsumer(String topicName, String bootstrapServers, String groupId) {
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the beginning of the topic

        consumer = new KafkaConsumer<>(properties);
    }

    public void startSubscribing() {
        consumer.subscribe(Arrays.asList(topicName));
    }

    public void handlePrices() {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                System.out.printf("Consumed record with key %s and value %s%n", record.key(), record.value());
            });
        }
    }
}

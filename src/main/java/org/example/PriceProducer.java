package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class PriceProducer {

    private final String topicName;
    private Producer<String, String> producer;

    PriceProducer(String topicName, String bootstrapServers) {

        this.topicName = topicName;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(properties);
    }

    public void send(String ticker, double price, String dateTime) {

        String tickerJson = "\"ticker\": " + "\"" + ticker + "\",";
        String priceJson = "\"price\": " + "\"" + price + "\",";
        String dateTimeJson = "\"dateTime\": " + "\"" + dateTime + "\"";
        String json = "{" + tickerJson + priceJson + dateTimeJson + "}";

        producer.send(new ProducerRecord<>(topicName,ticker + dateTime, json));

        System.out.println("Message sent: " + json);
    }

    public void closeProducer() {
        producer.close();
    }
}


package org.example;


import java.time.LocalDateTime;
import java.util.Random;

public class ProducerMain {
    public static void main(String[] args) {

        String topicName = "prices";
        String bootstrapServers = "localhost:9092";
        String ticker = "GME";

        PriceProducer priceProducer = new PriceProducer(topicName, bootstrapServers);

        double currPrice = 100.0;
        Random random = new Random();

        while (true) {

            try {
                double multiplier = -0.15 + (0.15 + 0.15) * random.nextDouble();
                double latestPrice = currPrice + (currPrice * multiplier);
                priceProducer.send(ticker, latestPrice, LocalDateTime.now().toString());

                currPrice = latestPrice;
                Thread.sleep(1000);

            } catch (Exception e) {

                e.printStackTrace();
                priceProducer.closeProducer();
                break;
            }
        }
    }

}
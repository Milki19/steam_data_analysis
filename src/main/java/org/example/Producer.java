package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "real_data_topic";
        String serverUrl = "http://localhost:5000/stream/type3";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        AtomicInteger sentMessagesCount = new AtomicInteger(0); // Brojač poslatih poruka

        try {
            URL url = new URL(serverUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("data: ")) {
                    String jsonData = line.substring(6);
                    sendToKafka(producer, topic, jsonData, sentMessagesCount);
                }
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Total messages sent: " + sentMessagesCount.get());
            producer.close();
        }
    }

    private static void sendToKafka(KafkaProducer<String, String> producer, String topic, String data, AtomicInteger counter) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                int count = counter.incrementAndGet();
                System.out.println("Sent (" + count + "): " + data);
            } else {
                exception.printStackTrace();
            }
        });

        try {
            Thread.sleep(1); // Pauza od 1ms između poruka
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}

package org.example;

import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class ConsumerF {

    private static final long MAX_FILE_SIZE = 450 * 1024 * 1024; // 450 MB
    private static int fileCounter = 3;

    private static CSVWriter createNewCSVWriter() throws Exception {
        String fileName = "game_data_part_" + fileCounter++ + ".csv";
        CSVWriter writer = new CSVWriter(new FileWriter(fileName, true));
        String[] header = {"AppID", "Name", "ReleaseDate", "EstimatedOwners", "PeakCCU", "RequiredAge", "Price",
                "Discount", "DLCCount", "AboutTheGame", "SupportedLanguages", "FullAudioLanguages", "Reviews",
                "HeaderImage", "Website", "SupportURL", "SupportEmail", "Windows", "Mac", "Linux", "MetacriticScore",
                "MetacriticURL", "UserScore", "Positive", "Negative", "ScoreRank", "Achievements", "Recommendations",
                "Notes", "AvgPlaytimeForever", "AvgPlaytimeTwoWeeks", "MedianPlaytimeForever", "MedianPlaytimeTwoWeeks",
                "Developers", "Publishers", "Categories", "Genres", "Tags", "Screenshots", "Movies"};
        writer.writeNext(header);
        return writer;
    }

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "game-data-consumer";
        String topic = "real_data_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        int emptyPolls = 0;
        final int maxEmptyPolls = 5;
        final Duration timeout = Duration.ofSeconds(3);

        AtomicInteger receivedMessagesCount = new AtomicInteger(0);
        AtomicInteger writtenToCsv = new AtomicInteger(0);
        AtomicInteger notWrittenToCsv = new AtomicInteger(0);

        CSVWriter writer = null;
        File currentFile = null;

        try {
            writer = createNewCSVWriter();
            currentFile = new File("game_data_part_" + (fileCounter - 1) + ".csv");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);

                if (records.isEmpty()) {
                    emptyPolls++;
                    System.out.println("No new messages... (" + emptyPolls + "/" + maxEmptyPolls + ")");
                    if (emptyPolls >= maxEmptyPolls) {
                        System.out.println("Total messages received: " + receivedMessagesCount.get());
                        System.out.println("Total messages written to CSV: " + writtenToCsv.get());
                        System.out.println("Total messages not written to CSV: " + notWrittenToCsv.get());
                        break;
                    }
                } else {
                    emptyPolls = 0;

                    for (ConsumerRecord<String, String> record : records) {
                        receivedMessagesCount.incrementAndGet();
                        System.out.println("Received (" + receivedMessagesCount.get() + "): " + record.value());
                        try {
                            JSONObject jsonData = new JSONObject(record.value());

                            String[] data = {
                                    jsonData.optString("AppID", ""),
                                    jsonData.optString("Name", ""),
                                    jsonData.optString("Release date", ""),
                                    jsonData.optString("Estimated owners", ""),
                                    jsonData.optString("Peak CCU", ""),
                                    jsonData.optString("Required age", ""),
                                    parsePrice(jsonData.optString("Price", "0")),
                                    jsonData.optString("Discount", ""),
                                    jsonData.optString("DLC count", ""),
                                    jsonData.optString("About the game", "").replace("\n", " "),
                                    getJsonArrayAsString(jsonData, "Supported languages"),
                                    getJsonArrayAsString(jsonData, "Full audio languages"),
                                    jsonData.optString("Reviews", ""),
                                    jsonData.optString("Header image", ""),
                                    jsonData.optString("Website", ""),
                                    jsonData.optString("Support url", ""),
                                    jsonData.optString("Support email", ""),
                                    jsonData.optString("Windows", ""),
                                    jsonData.optString("Mac", ""),
                                    jsonData.optString("Linux", ""),
                                    jsonData.optString("Metacritic score", ""),
                                    jsonData.optString("Metacritic url", ""),
                                    jsonData.optString("User score", ""),
                                    jsonData.optString("Positive", ""),
                                    jsonData.optString("Negative", ""),
                                    jsonData.optString("Score rank", ""),
                                    jsonData.optString("Achievements", ""),
                                    jsonData.optString("Recommendations", ""),
                                    jsonData.optString("Notes", ""),
                                    jsonData.optString("Average playtime forever", ""),
                                    jsonData.optString("Average playtime two weeks", ""),
                                    jsonData.optString("Median playtime forever", ""),
                                    jsonData.optString("Median playtime two weeks", ""),
                                    getJsonArrayAsString(jsonData, "Developers"),
                                    getJsonArrayAsString(jsonData, "Publishers"),
                                    getJsonArrayAsString(jsonData, "Categories"),
                                    getJsonArrayAsString(jsonData, "Genres"),
                                    getJsonArrayAsString(jsonData, "Tags"),
                                    getJsonArrayAsString(jsonData, "Screenshots"),
                                    getJsonArrayAsString(jsonData, "Movies")
                            };

                            writer.writeNext(data);
                            writtenToCsv.incrementAndGet();

                            // Provera veličine fajla nakon svakog reda
                            writer.flush();
                            if (currentFile.length() >= MAX_FILE_SIZE) {
                                writer.close();
                                System.out.println("File size limit reached. Creating new file.");
                                writer = createNewCSVWriter();
                                currentFile = new File("game_data_part_" + (fileCounter - 1) + ".csv");
                            }

                            System.out.println("Written to CSV");

                        } catch (Exception e) {
                            notWrittenToCsv.incrementAndGet();
                            System.err.println("Error processing record: " + record.value());
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null)
                    writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            consumer.close();
            System.out.println("Consumer closed.");
        }
    }


    private static String parsePrice(String priceStr) {
        try {
            return String.format("%.2f", Double.parseDouble(priceStr.replace(",", ".")));
        } catch (NumberFormatException e) {
            return "0.00";
        }
    }

    private static String getJsonArrayAsString(JSONObject json, String key) {
        if (json.has(key) && !json.isNull(key)) {
            String rawData = json.optString(key, "[]").replace("'", "\"").trim();
            if (rawData.startsWith("[") && rawData.endsWith("]")) {
                JSONArray jsonArray = new JSONArray(rawData);
                List<String> list = IntStream.range(0, jsonArray.length())
                        .mapToObj(jsonArray::optString)
                        .collect(Collectors.toList());
                return String.join(";", list);
            }
            return rawData.replace(",", ";");
        }
        return "";
    }
}
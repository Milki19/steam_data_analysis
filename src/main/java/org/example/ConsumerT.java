package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumerT {

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

        String csvFile = "game_data.csv";

        // Brojač za praćenje koliko puta Consumer ne primi nijednu poruku
        int emptyPolls = 0;
        final int maxEmptyPolls = 5; // Nakon 5 praznih poll poziva prekidamo Consumer
        final Duration timeout = Duration.ofSeconds(3); // 3 sekunde čekanja između iteracija

        AtomicInteger receivedMessagesCount = new AtomicInteger(0);
        AtomicInteger writtenToCsv = new AtomicInteger(0);
        AtomicInteger notWrittenToCsv = new AtomicInteger(0);


        try (FileWriter writer = new FileWriter(csvFile, true)) {
//            writer.append("AppID,Name,ReleaseDate,EstimatedOwners,PeakCCU,RequiredAge,Price,Discount,DLCCount,AboutTheGame,SupportedLanguages,FullAudioLanguages,Reviews,HeaderImage,Website,SupportURL,SupportEmail,Windows,Mac,Linux,MetacriticScore,MetacriticURL,UserScore,Positive,Negative,ScoreRank,Achievements,Recommendations,Notes,AvgPlaytimeForever,AvgPlaytimeTwoWeeks,MedianPlaytimeForever,MedianPlaytimeTwoWeeks,Developers,Publishers,Categories,Genres,Tags,Screenshots,Movies\n");
            writer.append("AppID,Name,ReleaseDate,EstimatedOwners," +
                    "PeakCCU,RequiredAge,Price,Discount," +
                    "DLCCount,SupportedLanguages,FullAudioLanguages,Reviews," +
                    "Windows,Mac,Linux,MetacriticScore,U" +
                    "serScore,Positive,Negative,ScoreRank," +
                    "Achievements,Recommendations,AvgPlaytimeForever,AvgPlaytimeTwoWeeks," +
                    "MedianPlaytimeForever,MedianPlaytimeTwoWeeks,Developers,Publishers," +
                    "Categories,Genres,Tags\n");

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
                    emptyPolls = 0; // Resetujemo brojač ako su stigle nove poruke

                    for (ConsumerRecord<String, String> record : records) {
                        receivedMessagesCount.incrementAndGet();
                        System.out.println("Received (" + receivedMessagesCount.get() + "): " + record.value());
                        try {
                            JSONObject jsonData = new JSONObject(record.value());

                            String appId = escapeCsv(jsonData.optString("AppID", ""));
                            String name = escapeCsv(jsonData.optString("Name", ""));
                            String releaseDate = escapeCsv(jsonData.optString("Release date", ""));
                            String estimatedOwners = escapeCsv(jsonData.optString("Estimated owners", ""));
                            String peakCCU = escapeCsv(jsonData.optString("Peak CCU", ""));
                            String requiredAge = escapeCsv(jsonData.optString("Required age", ""));
                            String price = jsonData.optString("Price", "0").replace(",", ".");
                            try {
                                price = String.format("%.2f", Double.parseDouble(price));
                            } catch (NumberFormatException e) {
                                price = "0.00"; // Ako ne uspe, postavi na 0.00
                            }                  String discount = escapeCsv(jsonData.optString("Discount", ""));
                            String dlcCount = escapeCsv(jsonData.optString("DLC count", ""));
                            String aboutTheGame = escapeCsv(jsonData.optString("About the game", "").replace("\n", " "));
                            String reviews = escapeCsv(jsonData.optString("Reviews", ""));
                            String headerImage = escapeCsv(jsonData.optString("Header image", ""));
                            String website = escapeCsv(jsonData.optString("Website", ""));
                            String supportUrl = escapeCsv(jsonData.optString("Support url", ""));
                            String supportEmail = escapeCsv(jsonData.optString("Support email", ""));
                            String windows = escapeCsv(jsonData.optString("Windows", ""));
                            String mac = escapeCsv(jsonData.optString("Mac", ""));
                            String linux = escapeCsv(jsonData.optString("Linux", ""));
                            String metacriticScore = escapeCsv(jsonData.optString("Metacritic score", ""));
                            String metacriticUrl = escapeCsv(jsonData.optString("Metacritic url", ""));
                            String userScore = escapeCsv(jsonData.optString("User score", ""));
                            String positive = escapeCsv(jsonData.optString("Positive", ""));
                            String negative = escapeCsv(jsonData.optString("Negative", ""));
                            String scoreRank = escapeCsv(jsonData.optString("Score rank", ""));
                            String achievements = escapeCsv(jsonData.optString("Achievements", ""));
                            String recommendations = escapeCsv(jsonData.optString("Recommendations", ""));
                            String notes = escapeCsv(jsonData.optString("Notes", ""));
                            String avgPlaytimeForever = escapeCsv(jsonData.optString("Average playtime forever", ""));
                            String avgPlaytimeTwoWeeks = escapeCsv(jsonData.optString("Average playtime two weeks", ""));
                            String medianPlaytimeForever = escapeCsv(jsonData.optString("Median playtime forever", ""));
                            String medianPlaytimeTwoWeeks = escapeCsv(jsonData.optString("Median playtime two weeks", ""));

                            // Parsiranje lista
                            String supportedLanguages = getJsonArrayAsString(jsonData, "Supported languages");
                            String fullAudioLanguages = getJsonArrayAsString(jsonData, "Full audio languages");
                            String developers = getJsonArrayAsString(jsonData, "Developers");
                            String publishers = getJsonArrayAsString(jsonData, "Publishers");
                            String categories = getJsonArrayAsString(jsonData, "Categories");
                            String genres = getJsonArrayAsString(jsonData, "Genres");
                            String tags = getJsonArrayAsString(jsonData, "Tags");
                            String screenshots = getJsonArrayAsString(jsonData, "Screenshots");
                            String movies = getJsonArrayAsString(jsonData, "Movies");

                            //sve kolone
                            String csvLine = String.format(
                                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s\n",
                                    appId, name, releaseDate, estimatedOwners, peakCCU, requiredAge, price, discount, dlcCount, aboutTheGame,
                                    supportedLanguages, fullAudioLanguages, reviews, headerImage, website, supportUrl, supportEmail,
                                    windows, mac, linux, metacriticScore, metacriticUrl, userScore, positive, negative, scoreRank,
                                    achievements, recommendations, notes, avgPlaytimeForever, avgPlaytimeTwoWeeks, medianPlaytimeForever, medianPlaytimeTwoWeeks,
                                    developers, publishers, categories, genres, tags, screenshots, movies
                            );

                            writtenToCsv.incrementAndGet();
                            writer.append(csvLine).append("\n");
                            writer.flush();

                            System.out.println("Written to CSV: " + csvLine);
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + record.value());
                            notWrittenToCsv.incrementAndGet();
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("Consumer closed.");
        }
    }

    private static String getJsonArrayAsString(JSONObject json, String key) {
        if (json.has(key) && !json.isNull(key)) {
            try {
                String rawData = json.optString(key, "[]").trim();

                // Ako string koristi apostrofe umesto pravih JSON navodnika, konvertuj ih
                rawData = rawData.replace("'", "\"");

                // Ako izgleda kao JSON niz, parsiraj ga
                if (rawData.startsWith("[") && rawData.endsWith("]")) {
                    JSONArray jsonArray = new JSONArray(rawData);
                    List<String> list = IntStream.range(0, jsonArray.length())
                            .mapToObj(i -> jsonArray.optString(i, "").trim())
                            .collect(Collectors.toList());

                    return String.join(";", list); // Koristi ";" za CSV format
                }

                // Ako nije validan JSON niz, vraćamo sirovi string sa zarezima kao separatorom
                return rawData.replace(",", ";");

            } catch (Exception e) {
                return ""; // Ako dođe do greške, vraćamo prazan string
            }
        }
        return "";
    }


    private static String escapeCsv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\""; // Escape dvostruke navodnike i obavij ih sa još jednim parom navodnika
    }
}

# Steam Games Data Analysis

A small end-to-end big data project: a stream of Steam game records (about 91,000 unique games) is ingested through Apache Kafka, written to CSV, loaded into Databricks and analysed with PySpark.

Built as a project for a university Big Data course (2025).

## How it works

```
HTTP event stream ──> Java producer ──> Kafka topic ──> Java consumer ──> CSV files ──> Databricks (PySpark)
 (Docker container)                  real_data_topic                    game_data_part_*.csv     notebooks/
```

1. The course provided a Docker image that serves the dataset as a server-sent event stream (`/stream/type3`).
2. `Producer` reads the stream line by line and publishes every record to the Kafka topic `real_data_topic`.
3. `ConsumerF` reads the topic, flattens each JSON record into 40 CSV columns (list fields such as genres and tags are joined with `;`) and writes them to `game_data_part_N.csv`, starting a new file every 450 MB so the files stay small enough to upload to Databricks. It stops after 5 empty polls in a row.
4. The CSV files are uploaded to Databricks as the table `default.game_data` and analysed in [`notebooks/steam_analysis.ipynb`](notebooks/steam_analysis.ipynb).

## Repository layout

```
kafka-ingestion/            Java + Gradle project
  src/main/java/org/example/
    Producer.java           stream -> Kafka
    ConsumerF.java          Kafka -> CSV (opencsv, splits files at 450 MB)
    ConsumerT.java          earlier, simpler consumer that builds the CSV by hand into one file
notebooks/
  steam_analysis.ipynb      data cleaning and analyses (Databricks, PySpark)
```

## Analysis highlights

Cleaning:

- The raw data had almost exactly twice the expected rows (183,038, of which 91,521 were duplicates), so the first step is `dropDuplicates`.
- `UserScore` was empty, so it is rebuilt from positive and negative review counts on a 0–10 scale.
- Games with fewer reviews than the median (7) are left out of the score-based analyses, since one positive review would otherwise give a perfect 10. That leaves 47,780 games.

Findings:

| Question | Result |
|---|---|
| Are expensive games rated better? | No. The Pearson correlation between price and user score is 0.08. |
| When did Linux support take off? | 2015 had the biggest jump (+351 Linux games over 2014). The share of releases supporting Linux peaked at 38% in 2013 and has been falling as total releases grew. |
| How did genres change over time? | Single-player is the top category in every decade. Action led the genres in the 2000s; indie has led since the 2010s. |
| Does price affect the number of owners the same way in every genre? | No. The log-log correlation between price and estimated owners is positive overall (0.35) and strongest for action, strategy and RPG (about 0.23). Only creative software and utilities show a weak negative relationship. |
| Which genres are rated highest? | Casual, indie and adventure (about 7.65–7.7 out of 10), but most genres are within 0.7 points of each other. |

A K-means segmentation of games is also in the notebook, but it could not run on Databricks Community Edition because `VectorAssembler` is blocked there, so it has no results.

## Running it

### 1. Start the data stream

The stream comes from a Docker image (`bigdata-g3`) that was handed out during the course. It isn't published on a registry, so you need the `.tar` file:

```bash
docker load -i bigdata-g3.tar
docker run -d -p 5000:5000 --name bigdata-g3 bigdata-g3:latest
curl http://localhost:5000/health-check     # should return "Success"
```

### 2. Start Kafka

Any Kafka broker on `127.0.0.1:9092` works. For example, with the official image:

```bash
docker run -d -p 9092:9092 --name kafka apache/kafka:3.7.0
```

### 3. Run the producer and consumer

Requires a JDK (11 or newer). From `kafka-ingestion/`, in two terminals:

```bash
./gradlew runConsumer     # Kafka -> game_data_part_N.csv
./gradlew runProducer     # stream -> Kafka
```

`./gradlew runConsumerSimple` runs the older single-file consumer instead.

### 4. Analyse in Databricks

Upload the CSV files as a table named `default.game_data`, then import `notebooks/steam_analysis.ipynb` and run it top to bottom.

## Tech stack

Java, Gradle, Apache Kafka (kafka-clients 3.3), opencsv, org.json · Databricks, Apache Spark (PySpark, Spark SQL, MLlib) · Docker

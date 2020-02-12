package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * What we'll learn here:
 * How to set up a non subscribing consumer that recovers it's state from a compaction log.
 * <p>
 * Run this consumer to see results of the scoreboard. You can stop and restart it and it will be consistent.
 */
public class DcScoreConsumer {


    private static Logger LOGGER = LoggerFactory.getLogger(DcScoreConsumer.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "scoreboard.topic";

        // consumer configuration
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /*
        The client.id allows the cluster to correlate producers/consumers using something other than ip/port.
         */
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "Scoreboard Viewer " + UUID.randomUUID());


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

        /*
        We will be maintaining the score board in memory so we won't be setting up a subscriber.
         */
            Collection<TopicPartition> topicPartitions = consumer.partitionsFor(topic)
                    .stream()
                    .map(pInfo -> new TopicPartition(pInfo.topic(), pInfo.partition()))
                    .collect(Collectors.toList());

            consumer.assign(topicPartitions);

        /*
        This will move us to the beginning of the topic.
         */
            consumer.seekToBeginning(topicPartitions);

            AtomicBoolean running = new AtomicBoolean(true);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);

                //causes a blocking poll to throw a WakeupException
                consumer.wakeup();
                LOGGER.info("shutting down");
            }));


            Map<String, String> scoreboard = new TreeMap<>();
            long start = System.currentTimeMillis();
            boolean upToDate = false;

            long lastPrint = 0;

            // poll non stop. Must kill with sigterm
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(1000));

                    if (!upToDate && records.isEmpty()) {
                        LOGGER.info("all caught up");
                        upToDate = true;
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.value() == null) {
                            scoreboard.remove(record.key());
                        } else {
                            scoreboard.put(record.key(), record.value());
                        }
                        if (!upToDate && record.timestamp() <= start) {
                            LOGGER.info("all caught up");
                            upToDate = true;
                        }
                    }

                    // only print every 5 seconds if we are up to date
                    long now = System.currentTimeMillis();
                    if (upToDate && (now - lastPrint) >= 3_000L) {
                        print(scoreboard);
                        lastPrint = now;
                    }
                } catch (WakeupException ex) {
                    LOGGER.info("consumer wakeup");
                }
            }
        }
    }

    private static void print(Map<String, String> scoreboard) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            sb.append("\n");
        }
        sb.append(new Date().toString()).append("\n");
        for (Map.Entry<String, String> entry : scoreboard.entrySet()) {
            sb.append(entry.getValue()).append("\t\t").append(entry.getKey()).append("\n");
        }

        LOGGER.info(sb.toString());
    }

}

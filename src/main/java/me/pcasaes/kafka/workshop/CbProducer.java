package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Please start with {@link CaTopicCreator
 * <p>
 * Running this will add 1000 messages to java.created.topic
 * <p>
 * run this and wait a minute then run again. After each attempt check it out in the console
 * <p>
 * kafka-console-consumer.sh  \
 * --bootstrap-server localhost:9092 \
 * --topic java.created.topic \
 * --from-beginning
 * <p>
 * Wait for it to scroll through then ctrl-c. It'll tell you how many messages where processed.
 * As you wait about a minute to fill it up more and recheck the consumer, you will notice the number of
 * messages processed raise and fall between each attempt.
 */
public class CbProducer {

    private static Logger LOGGER = LoggerFactory.getLogger(CbProducer.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "java.created.topic";

        // Producer configuration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
        Will only receive ACK after the message is in the disk IO buffer of all replicas configured for the topic.
        Kafka does not wait for FSYNC. Guarantee is provided by having a replica factor of at least 3.
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        /*
        This producer will not wait for acknowledgement from the cluster.
         */
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, i + "-" + System.currentTimeMillis());

            /*
            sending is always asynchronous. You can making it blocking by calling its returned Future's get.
            Or you can pass a callback.

            Here every 128 messages will be sent blocking.

            Blocking will reduce throughput since kafka sends in batches.

             */
            if (i % 127 == 0) {
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    LOGGER.info("\n\nMessage sync sent, partition:\t" + metadata.partition() + ", offset:\t" + metadata.offset() + "\n\n");
                } catch (InterruptedException | ExecutionException ex) {
                    LOGGER.error("Could not send messages", ex);
                }
            } else {
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        LOGGER.error("Could not send messages", ex);
                    } else {
                        LOGGER.info("Message async sent, partition:\t" + metadata.partition() + ", offset:\t" + metadata.offset());
                    }
                });
            }

        }


        producer.flush();
        producer.close();

    }

}

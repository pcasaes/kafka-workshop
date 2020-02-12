package me.pcasaes.kafka.workshop;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Leave DcScoreConsumer running and the run this producer
 * <p>
 * You can view the in the command line
 * <p>
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic scoreboard.topic \
  --from-beginning \
  --property print.key=true
 */
public class DbScoreProducer {

    private static Logger LOGGER = LoggerFactory.getLogger(DbScoreProducer.class.getName());


    private static final String[] FirstNames = new String[]{
            "Pal",
            "Cla",
    };

    private static final String[] LastNames = new String[]{
            " Tsu",
            " Hyp",
    };

    private static final Random RNG = new SecureRandom();

    private static String generateName() {
        return FirstNames[RNG.nextInt(FirstNames.length)].concat(LastNames[RNG.nextInt(LastNames.length)]);
    }


    static class Player {

        private static final Map<String, Player> CACHE = new ConcurrentHashMap<>();

        private final String name;

        private int score;

        public Player(String name) {
            this.name = name;
            this.score = 0;
        }

        public static Player withName(String name) {
            return CACHE.computeIfAbsent(name, Player::new);
        }

        public void updateScore(int points) {
            this.score += points;
        }

        public String getName() {
            return name;
        }

        public int getScore() {
            return score;
        }

        public static List<String> removeAll() {
            List<String> names = CACHE
                    .values()
                    .stream()
                    .map(Player::getName)
                    .collect(Collectors.toList());

            CACHE.clear();
            return names;
        }
    }

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "scoreboard.topic";
        int debounce = 5_000;

        // Producer configuration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
        The client.id allows the cluster to correlate producers/consumers using something other than ip/port.
         */
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "Scoreboard fill Run instance " + UUID.randomUUID());


        /*
        This producer will not wait for acknowledgement from the cluster.
         */
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean done = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("shutting down");
            running.set(false);

            for (int i = 0; i < 24 && !done.get(); i++) {
                try {
                    Thread.sleep(250L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            LOGGER.info("clearing scoreboard");
            Player
                    .removeAll()
                    .stream()
                    .map(name -> new ProducerRecord<String, String>(topic, name, null))
                    .forEach(producer::send);


            producer.flush();
            producer.close();
        }));


        for (int i = 0; i < 100 && running.get(); i++) {
            ProducerRecord<String, String> record;

            Player player = Player.withName(generateName());
            player.updateScore(RNG.nextInt(100) + 1);
            record = new ProducerRecord<>(topic, player.getName(), String.valueOf(player.getScore()));


            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    LOGGER.error("Could not send messages", ex);
                } else {
                    LOGGER.info("Message async sent, partition:\t" + metadata.partition() + ", offset:\t" + metadata.offset());
                }
            });


            try {
                Thread.sleep(RNG.nextInt(debounce));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        done.set(true);

    }
}

package br.com.claudiodornelles.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaReceiver {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaReceiver.class.getSimpleName());

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configs)) {

            kafkaConsumer.subscribe(Collections.singleton(INPUT_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    LOGGER.info("Found {} new message(s)", records.count());
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        LOGGER.info(
                                "Message at offset {}: Topic: {} | Key: {} | Value: {} | Partition {}",
                                consumerRecord.offset(),
                                consumerRecord.topic(),
                                consumerRecord.key(),
                                consumerRecord.value(),
                                consumerRecord.partition()
                        );
                    }
                }
            }
        }
    }

}

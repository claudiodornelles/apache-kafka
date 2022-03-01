package br.com.claudiodornelles.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "input-topic";

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDispatcher.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configs)) {

            String key = "key";
            String value = "value";

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);
            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.debug("Could not send the message", exception);
                    return;
                }

                LOGGER.info(
                        "Message sent successfully! Topic = {} | Partition = {} | Offset = {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            };

            Future<RecordMetadata> future = kafkaProducer.send(producerRecord, callback);
            future.get(); // Waits the computation to complete (Blocking call)
        }
    }

}

package com.github.claudiodornelles;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.github.claudiodornelles.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAvroProducer {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  private static final String TOPIC = "customer-input-topic";
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroProducer.class);

  public static void main(String[] args) {
    final var properties = getKafkaProperties();
    try (final var kafkaProducer = new KafkaProducer<String, Customer>(properties)) {
      final var customerId = UUID.randomUUID().toString();
      final var customer = Customer.newBuilder()
          .setId(customerId)
          .setFirstName("Tobe")
          .setMiddleName("Pacuba")
          .setLastName("Poista")
          .setAge(36)
          .setWeight(83.9F)
          .setHeight(180.0F)
          .setAutomatedEmail(false)
          .build();

      final var producerRecord = new ProducerRecord<>(TOPIC, customerId, customer);
      final Callback callback = getCallback(customerId, customer);

      kafkaProducer.send(producerRecord, callback);
      kafkaProducer.flush();
    }
  }

  private static Properties getKafkaProperties() {
    final var properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
    return properties;
  }

  private static Callback getCallback(String customerId, Customer customer) {
    return (metadata, exception) -> {
      if (exception != null) {
        LOGGER.warn("Could not send message to topic({}) | key({}) | value({})", TOPIC, customerId, customer);
        return;
      }
      final var destinationTopic = metadata.topic();
      final var destinationPartition = metadata.partition();
      final var messageOffset = metadata.offset();
      LOGGER.info("Message sent successfully! Topic = {} | Partition = {} | Offset = {}", destinationTopic, destinationPartition, messageOffset);
    };
  }

}

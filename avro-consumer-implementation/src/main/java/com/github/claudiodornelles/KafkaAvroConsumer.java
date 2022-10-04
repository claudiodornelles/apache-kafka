package com.github.claudiodornelles;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.github.claudiodornelles.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAvroConsumer {

  private static final String BOOTSTRAP_SERVER = "localhost:9092";
  private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  private static final String INPUT_TOPIC = "customer-input-topic";
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroConsumer.class);

  public static void main(String[] args) {
    final var kafkaProperties = getKafkaProperties();

    try (final var kafkaConsumer = new KafkaConsumer<String, Customer>(kafkaProperties)) {
      kafkaConsumer.subscribe(Collections.singleton(INPUT_TOPIC));

      while (true) {
        final var records = kafkaConsumer.poll(Duration.ofMillis(100));
        final var amountOfRecordsFound = records.count();
        if (amountOfRecordsFound > 0) {
          LOGGER.info("Found {} new message(s).", amountOfRecordsFound);
          for (final var consumerRecord : records) {
            final var offset = consumerRecord.offset();
            final var topic = consumerRecord.topic();
            final var key = consumerRecord.key();
            final var customer = consumerRecord.value();
            final var partition = consumerRecord.partition();
            LOGGER.info("Reading message at offset {} from topic({}) | partition({}): key={} | value={}", offset, topic, partition, key, customer);
            LOGGER.info("Customer id = {}", customer.getId());
            LOGGER.info("Customer first name = {}",customer.getFirstName());
            LOGGER.info("Customer middle name = {}",customer.getMiddleName());
            LOGGER.info("Customer last name = {}",customer.getLastName());
            LOGGER.info("Customer age = {}",customer.getAge());
            LOGGER.info("Customer height = {}",customer.getHeight());
            LOGGER.info("Customer weight = {}",customer.getWeight());
            LOGGER.info("Customer automatedEmail = {}",customer.getAutomatedEmail());
          }
        }
      }
    }
  }

  private static Properties getKafkaProperties() {
    final var properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(GROUP_ID_CONFIG, KafkaAvroConsumer.class.getSimpleName());
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
    properties.setProperty(SPECIFIC_AVRO_READER_CONFIG, "true");
    return properties;
  }

}

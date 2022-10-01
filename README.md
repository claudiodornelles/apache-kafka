# apache-kafka

In order to run the consumer/producer implementation, using Apache Kafka, please run the docker containers defined in `docker-compose.yml` file.
To do so, open a new Terminal window within this project folder and run the following command:
```
docker-compose up -d
```

To create a new topic named 'new-topic' use:
```
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic new-topic
```

To stop the Kafka broker use:
```
docker-compose down
```

You can refer to https://developer.confluent.io/quickstart/kafka-docker/ in order to get more information.
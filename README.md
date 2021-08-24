[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Hub-of-Hubs-Kafka-Transport

[![Go Report Card](https://goreportcard.com/badge/github.com/open-cluster-management/hub-of-hubs-kafka-transport)](https://goreportcard.com/report/github.com/open-cluster-management/hub-of-hubs-kafka-transport)

The kafka-transport component of [Hub-of-Hubs](https://github.com/open-cluster-management/hub-of-hubs).

This repo is a **work in progress** that serves as a general base for the ongoing hub-of-hubs integration of Kafka as transport.

To use the producer/consumer wrappers provided, the following environment variables must be set:

1. Set the `KAFKA_HOSTS` environment variable to hold the kafka bootstrap servers host.
    ```
    $ export KAFKA_HOSTS=...
    ```
   
2. Set the `KAFKA_SSL_CA` environment variable to hold the PEM formatted CA encoded in base64 if connecting securely
   is required. Otherwise, leave unset.
     ```
    $ export KAFKA_SSL_CA=$(cat PATH_TO_CA | base64 -w 0)
    ```

### When using kafka-producer:
1. Set the `KAFKA_PRODUCER_ID` environment variable to hold the name of the producer.
    ```
    $ export KAFKA_PRODUCER_ID=...
    ```

1. Set the `KAFKA_PRODUCER_TOPIC` environment variable to hold the name of the topic to send to.
    ```
    $ export KAFKA_PRODUCER_TOPIC=...
    ```
   
1. Set the `KAFKA_MESSAGE_SIZE_LIMIT_KB` environment variable to hold the maximum allowed message size in kilobytes 
(not above ~900).
    ```
    $ export KAFKA_MESSAGE_SIZE_LIMIT_KB=...
    ```
    
### When using kafka-consumer:
1.  Set the `KAFKA_CONSUMER_ID` environment variable to hold the name of the consumer's group.
    ```
    $ export KAFKA_CONSUMER_ID=...
    ```

1.  Set the `KAFKA_CONSUMER_TOPIC` environment variable to hold the name(s) of the topic(s) to subscribe to.
    ```
    $ export KAFKA_CONSUMER_TOPIC=...
    ```

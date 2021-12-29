[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Hub-of-Hubs-Kafka-Transport

[![Go Report Card](https://goreportcard.com/badge/github.com/open-cluster-management/hub-of-hubs-kafka-transport)](https://goreportcard.com/report/github.com/open-cluster-management/hub-of-hubs-kafka-transport)
[![Go Reference](https://pkg.go.dev/badge/github.com/open-cluster-management/hub-of-hubs-kafka-transport.svg)](https://pkg.go.dev/github.com/open-cluster-management/hub-of-hubs-kafka-transport)
[![License](https://img.shields.io/github/license/open-cluster-management/hub-of-hubs-kafka-transport)](/LICENSE)

The kafka-transport component of [Hub-of-Hubs](https://github.com/open-cluster-management/hub-of-hubs).

This repo holds the common logic of kafka producer/consumer and is used by the different Hub-of-Hubs components as part 
of using kafka as transport.

Go to the [Contributing guide](CONTRIBUTING.md) to learn how to get involved.

## Getting Started

To use the producer/consumer wrappers provided, a kafka.ConfigMap must be passed to the initialization 
functions to configure the clients.

If SSL connection is required, you must call SetCertificate with a Base64-encoded PEM formatted certificate, 
and set the following keys in the ConfigMap mentioned above:
```
security.protocol:  ssl
ssl.ca.location:    the string returned from SetCertificate() call
```
The certificate would be written to /opt/kafka.

Helpful links:
* Confluent - [Consumer Configurations](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
* Confluent - [Producer Configurations](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
* librdkafka - [Configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

## Deploy Kafka in cluster

To set up a Kafka cluster we use the Red Hat Integration - AMQ Streams operator (v1.7.2) to install Kafka v2.7.0
on the ACM cluster.

1.  Create *kafka* namespace:
    ```
    kubectl create namespace kafka
    ```

1.  Deploy the AMQ streams operator to your cluster (subscription watches kafka namespace):
    ```
    kubectl apply -f deploy/amq_streams_operator.yaml
    ```

1.  Deploy Kafka Cluster CR
    ```
    kubectl apply -f deploy/kafka_cluster.yaml
    ```
    
Result:
* AMQ Streams operator running and watching kafka namespace
* Kafka instance "kafka-brokers-cluster" deployed:
  * 3 broker pods
  * 3 zookeeper pods
  * an internal plaintext listener (port 9092)
  * an external TLS secure listener (port 9093)
  * status Topic deployed
  * spec Topic deployed
  
#### Get servers/certificates for other components to connect
Run the following command to fetch the required information (if it doesn't appear than cluster is still being created, need to retry again in several seconds):
```
kubectl -n kafka get Kafka kafka-brokers-cluster -o json | jq -r '.status.listeners[] | {bootstrapServers, certificates}' | sed 's/\\n/\n/g'
```
Output:
```
{
  "bootstrapServers": "kafka-brokers-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092",
  "certificates": null
}
{
  "bootstrapServers": "kafka-brokers-cluster-kafka-external-bootstrap-kafka.apps.veisenbe-hoh2.dev10.red-chesterfield.com:443",
  "certificates": [
  "-----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
  "
  ]
}
```
  
The first entry should be used for clients deployed in the cluster (unsecure connection).

The second entry should be used for clients deployed outside the cluster (TLS protected).

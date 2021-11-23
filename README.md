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
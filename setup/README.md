[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Hub-of-Hubs-Kafka-Transport

[![Go Report Card](https://goreportcard.com/badge/github.com/open-cluster-management/hub-of-hubs-kafka-transport)](https://goreportcard.com/report/github.com/open-cluster-management/hub-of-hubs-kafka-transport)
[![Go Reference](https://pkg.go.dev/badge/github.com/open-cluster-management/hub-of-hubs-kafka-transport.svg)](https://pkg.go.dev/github.com/open-cluster-management/hub-of-hubs-kafka-transport)
[![License](https://img.shields.io/github/license/open-cluster-management/hub-of-hubs-kafka-transport)](/LICENSE)

## Setup
To set up a Kafka cluster we use the Red Hat Integration - AMQ Streams operator (v1.7.2) to install Kafka v2.7.0
on the ACM cluster (easily done via Red Hat OpenShift Container Platform - Administrator view).

### Step 1
Once the operator is installed, head to "Kafka" tab and create a new instance.

### Step 2
Name your cluster (e.g. with "kafka-brokers-cluster") and expand the Kafka -> Listeners tab:
* Keep the first default listener for inter-cluster connections.
  * { name = plain, port = 9092, TLS disabled, type = internal }
* Change the second default listener's type to "route" to be used for external connections.
  * { name = tls, port = 9093, TLS enabled, type = route }

You may change any of the above / further configure the cluster following 
[Using AMQ Streams on OpenShift](https://access.redhat.com/documentation/en-us/red_hat_amq/7.7/html-single/using_amq_streams_on_openshift/index).

### Step 3
Create the cluster and wait for the status to be updated, fetch (cluster YAML):
* status -> listeners -> bootstrapServers:
    ```
    listeners:
    - addresses:
        - host: kafka-brokers-cluster-kafka-bootstrap.kafka.svc
          port: 9092
      bootstrapServers: 'kafka-brokers-cluster-kafka-bootstrap.kafka.svc:9092'
      type: plain
    - addresses:
        - host: >-
            kafka-brokers-cluster-kafka-tls-bootstrap-kafka.apps.veisenbe-hoh2.dev10.red-chesterfield.com
          port: 443
      bootstrapServers: >-
        kafka-brokers-cluster-kafka-tls-bootstrap-kafka.apps.veisenbe-hoh2.dev10.red-chesterfield.com:443
      certificates:
        - |
          -----BEGIN CERTIFICATE-----
          ...
          -----END CERTIFICATE-----
      type: tls
    ```
  
the bootstrapServers associated with the plain listener should be used for internal connections, 
such as producers/consumers that are deployed on the same ACM (hub-of-hubs-...-transport-bridge), 
while the TLS listener should be used (with the certificate) for connections external to the 
cluster (leaf-hub-...-sync)
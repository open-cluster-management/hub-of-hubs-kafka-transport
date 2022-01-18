// Copyright (c) 2021 Red Hat, Inc.
// Copyright Contributors to the Open Cluster Management project

package v1beta2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KafkaTopicSpec defines the desired state of KafkaTopic.
type KafkaTopicSpec struct {
	// +kubebuilder:validation:Minimum=1
	Partitions int `json:"partitions"`
	// +kubebuilder:validation:Maximum=32767
	// +kubebuilder:validation:Minimum=1
	Replicas  int                         `json:"replicas"`
	TopicName string                      `json:"topicName"`
	Config    map[interface{}]interface{} `json:"config,omitempty"`
}

// KafkaTopicStatus defines the observed state of KafkaTopic.
type KafkaTopicStatus struct{}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// KafkaTopic is the Schema for the KafkaTopic API.
type KafkaTopic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaTopicSpec   `json:"spec,omitempty"`
	Status KafkaTopicStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// KafkaTopicList contains a list of KafkaTopic.
type KafkaTopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaTopic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaTopic{}, &KafkaTopicList{})
}

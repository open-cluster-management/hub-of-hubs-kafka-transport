// Copyright (c) 2021 Red Hat, Inc.
// Copyright Contributors to the Open Cluster Management project

package v1beta2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KafkaUserAuthentication specifies the auth type required from clients identifying as the relevant KafkaUser.
type KafkaUserAuthentication struct {
	// only type is needed for now.
	// +kubebuilder:validation:Enum=tls;tls-external;scram-sha-512
	Type string `json:"type"`
}

// KafkaUserResource specifies the resource part in an ACL entry.
type KafkaUserResource struct {
	Name string `json:"name,omitempty"`
	// +kubebuilder:validation:literal;prefix
	PatternType string `json:"patternType,omitempty"`
	// +kubebuilder:validation:topic;group;cluster;transactionalId
	Type string `json:"type"`
}

// KafkaUserACL specifies an ACL entry.
type KafkaUserACL struct {
	Host     string            `json:"host,omitempty"`
	Resource KafkaUserResource `json:"resource"`
	// +kubebuilder:validation:Enum=Read;Write;Create;Delete;Alter;Describe;ClusterAction;AlterConfigs;DescribeConfigs;IdempotentWrite;All
	Operation string `json:"operation"`
}

// KafkaUserAuthorization specifies the authorization for the relevant KafkaUser.
type KafkaUserAuthorization struct {
	// only Type is needed for now.
	// +kubebuilder:validation:Enum=simple
	Type string         `json:"type"`
	ACLs []KafkaUserACL `json:"acls"`
}

// KafkaUserSpec defines the desired state of KafkaUser.
type KafkaUserSpec struct {
	// only authentication and authorization are needed for now.
	Authentication KafkaUserAuthentication `json:"authentication"`
	Authorization  KafkaUserAuthorization  `json:"authorization"`
}

// KafkaUserStatus defines the observed state of KafkaUser.
type KafkaUserStatus struct{}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// KafkaUser is the Schema for the KafkaUser API.
type KafkaUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaUserSpec   `json:"spec,omitempty"`
	Status KafkaUserStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// KafkaUserList contains a list of KafkaUser.
type KafkaUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaUser{}, &KafkaUserList{})
}

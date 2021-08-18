package kafkaclient

import (
	"crypto/x509"
	"encoding/base64"
	"io/ioutil"
	"log"
)

const (
	ownerOnlyRW = 0o600
)

// GetCertificate creates a file with the certificate (PEM) received and registers it in root certification authority.
func GetCertificate(cert *string) string {
	localCertFile := "/usr/local/bin/ca.crt"

	certBytes, err := base64.StdEncoding.DecodeString(*cert)
	if err != nil {
		log.Fatalf("kafka-certificate-manager: %v", err)
	}

	if err = ioutil.WriteFile(localCertFile, certBytes, ownerOnlyRW); err != nil {
		log.Fatalf("kafka-certificate-manager: failed write cert file: %v", err)
	}

	// Get the SystemCertPool, continue with an empty pool on error
	rootCertAuth, _ := x509.SystemCertPool()
	if rootCertAuth == nil {
		rootCertAuth = x509.NewCertPool()
	}

	// Append our cert to the system pool
	if ok := rootCertAuth.AppendCertsFromPEM(certBytes); !ok {
		log.Fatalf("kafka-certificate-manager: failed to append certificate")
	}

	return localCertFile
}

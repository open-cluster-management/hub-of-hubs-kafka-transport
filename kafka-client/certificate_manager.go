package kafkaclient

import (
	"crypto/x509"
	"encoding/base64"
	"io/ioutil"
	"log"
)

// GetCertificate creates a file with the CA (PEM) received and registers it in rootCA.
func GetCertificate(CA *string) string {
	localCertFile := "/usr/local/bin/ca.crt"

	decodedCa, err := base64.StdEncoding.DecodeString(*CA)
	if err != nil {
		log.Fatalf("kafka-certificate-manager: %v", err)
	}

	cert := []byte(decodedCa)

	if err = ioutil.WriteFile(localCertFile, cert, 0666); err != nil {
		log.Fatalf("kafka-certificate-manager: failed write cert file: %v", err)
	}

	// Get the SystemCertPool, continue with an empty pool on error
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	// Append our cert to the system pool
	if ok := rootCAs.AppendCertsFromPEM(cert); !ok {
		log.Fatalf("kafka-certificate-manager: failed to append certificate")
	}

	return localCertFile
}

//go:build linux || darwin

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func generateCertificate(certPath, keyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(365 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return err
	}
	secret, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	created := []string{}
	complete := false
	defer func() {
		if !complete {
			for _, path := range created {
				os.Remove(path)
			}
		}
	}()
	for _, entry := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: secret}), 0600},
		{certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644},
	} {
		if err := os.MkdirAll(filepath.Dir(entry.path), 0755); err != nil {
			return err
		}
		file, err := os.OpenFile(entry.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, entry.mode)
		if err != nil {
			return fmt.Errorf("create %s (existing files are preserved): %w", entry.path, err)
		}
		created = append(created, entry.path)
		_, writeErr := file.Write(entry.data)
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	complete = true
	log.Printf("generated demo certificate=%s key=%s", certPath, keyPath)
	return nil
}

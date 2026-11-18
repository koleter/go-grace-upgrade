package tls

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	standard "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

func certificate(t *testing.T) Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: secret}))
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func TestSnapshotRestoresUnreadPlaintextWithoutHandshake(t *testing.T) {
	for _, version := range []uint16{VersionTLS12, VersionTLS13} {
		t.Run(standard.VersionName(version), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			config := &Config{Certificates: []Certificate{certificate(t)}, MinVersion: version, MaxVersion: version, NextProtos: []string{"snapshot-test"}}
			result := make(chan error, 1)
			payload := bytes.Repeat([]byte("data"), 1024)
			go func() {
				raw, err := listener.Accept()
				if err != nil {
					result <- err
					return
				}
				defer raw.Close()
				conn := Server(raw, config)
				first := make([]byte, 5)
				if _, err = io.ReadFull(conn, first); err != nil {
					result <- err
					return
				}
				originalState := conn.ConnectionState()
				exported, err := originalState.ExportKeyingMaterial("migration-test", []byte("context"), 32)
				if err != nil {
					result <- err
					return
				}
				one, err := conn.Snapshot()
				if err != nil {
					result <- err
					return
				}
				two, err := conn.Snapshot()
				if err != nil {
					result <- err
					return
				}
				if !bytes.Equal(one.Input, two.Input) || len(one.Input) == 0 {
					result <- io.ErrUnexpectedEOF
					return
				}
				// Reuse the same transport here to isolate TLS state reconstruction.
				// Cross-process tests separately prove actual FD ownership migration.
				restored, err := Restore(raw, config, two)
				if err != nil {
					result <- err
					return
				}
				restoredState := restored.ConnectionState()
				gotExported, err := restoredState.ExportKeyingMaterial("migration-test", []byte("context"), 32)
				if err != nil || !bytes.Equal(exported, gotExported) {
					result <- fmt.Errorf("exporter changed: %v", err)
					return
				}
				remaining := make([]byte, len(payload)-5)
				if _, err = io.ReadFull(restored, remaining); err != nil {
					result <- err
					return
				}
				if !bytes.Equal(append(first, remaining...), payload) {
					result <- io.ErrUnexpectedEOF
					return
				}
				if restored.ConnectionState().NegotiatedProtocol != "snapshot-test" {
					result <- io.ErrUnexpectedEOF
					return
				}
				_, err = restored.Write([]byte("restored"))
				result <- err
			}()
			client, err := standard.Dial("tcp", listener.Addr().String(), &standard.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version, NextProtos: []string{"snapshot-test"}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := client.Write(payload); err != nil {
				t.Fatal(err)
			}
			response := make([]byte, 8)
			if _, err := io.ReadFull(client, response); err != nil {
				t.Fatal(err)
			}
			if string(response) != "restored" {
				t.Fatal(string(response))
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotAfterTLS13KeyUpdates(t *testing.T) {
	for _, suiteID := range []uint16{TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384, TLS_CHACHA20_POLY1305_SHA256} {
		t.Run(fmt.Sprintf("%x", suiteID), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			config := &Config{Certificates: []Certificate{certificate(t)}, MinVersion: VersionTLS13, MaxVersion: VersionTLS13, CipherSuites: []uint16{suiteID}, PreferServerCipherSuites: true}
			result := make(chan error, 1)
			go func() {
				raw, err := listener.Accept()
				if err != nil {
					result <- err
					return
				}
				defer raw.Close()
				raw.SetDeadline(time.Now().Add(5 * time.Second))
				conn := Server(raw, config)
				input := make([]byte, 4)
				if _, err := io.ReadFull(conn, input); err != nil {
					result <- err
					return
				}
				if string(input) != "ping" {
					result <- fmt.Errorf("unexpected payload %q", input)
					return
				}
				state, err := conn.Snapshot()
				if err != nil {
					result <- err
					return
				}
				if state.CipherSuite != suiteID {
					result <- fmt.Errorf("wrong suite %x", state.CipherSuite)
					return
				}
				restored, err := Restore(raw, config, state)
				if err != nil {
					result <- err
					return
				}
				if _, err := restored.Write([]byte("pong")); err != nil {
					result <- err
					return
				}
				if _, err := io.ReadFull(restored, input); err != nil {
					result <- err
					return
				}
				if string(input) != "more" {
					result <- fmt.Errorf("unexpected payload %q", input)
					return
				}
				_, err = restored.Write([]byte("done"))
				result <- err
			}()
			raw, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			client := Client(raw, &Config{InsecureSkipVerify: true, MinVersion: VersionTLS13, MaxVersion: VersionTLS13})
			defer client.Close()
			client.SetDeadline(time.Now().Add(5 * time.Second))
			if err := client.Handshake(); err != nil {
				t.Fatal(err)
			}
			update := func() error {
				client.out.Lock()
				defer client.out.Unlock()
				suite := cipherSuiteTLS13ByID(client.cipherSuite)
				if _, err := client.writeRecordLocked(recordTypeHandshake, (&keyUpdateMsg{updateRequested: true}).marshal()); err != nil {
					return err
				}
				client.out.setTrafficSecret(suite, suite.nextTrafficSecret(client.out.trafficSecret))
				return nil
			}
			for i, text := range []string{"ping", "more"} {
				if err := update(); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Write([]byte(text)); err != nil {
					t.Fatal(err)
				}
				response := make([]byte, 4)
				if _, err := io.ReadFull(client, response); err != nil {
					t.Fatal(err)
				}
				wanted := []string{"pong", "done"}[i]
				if string(response) != wanted {
					t.Fatal(string(response))
				}
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

//go:build linux || darwin

package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func runClient(o options) error {
	if o.interval <= 0 || o.count < 0 {
		return errors.New("interval must be positive and count must not be negative")
	}
	trusted, err := os.ReadFile(o.ca)
	if err != nil {
		return fmt.Errorf("read trusted certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trusted) {
		return errors.New("no trusted certificate found in -ca")
	}
	// Only one Dial: any broken connection exits the client instead of reconnecting.
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", o.address, &tls.Config{
		RootCAs: roots, ServerName: o.serverName,
		MinVersion: o.tlsVersion, MaxVersion: o.tlsVersion,
		NextProtos: []string{applicationProtocol},
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	state := conn.ConnectionState()
	if state.NegotiatedProtocol != applicationProtocol {
		return errors.New("unexpected negotiated application protocol")
	}
	log.Printf("connected once local=%s remote=%s tls=%s cipher=%s alpn=%s", conn.LocalAddr(), conn.RemoteAddr(), tls.VersionName(state.Version), tls.CipherSuiteName(state.CipherSuite), state.NegotiatedProtocol)
	reader := bufio.NewReader(conn)
	output := json.NewEncoder(os.Stdout)
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for count := uint64(1); o.count == 0 || count <= uint64(o.count); count++ {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		payload := fmt.Sprintf("message-%d", count)
		if _, err := fmt.Fprintln(conn, payload); err != nil {
			return err
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		var reply response
		if err := json.Unmarshal(line, &reply); err != nil {
			return err
		}
		if reply.Count != count || reply.Payload != payload {
			return fmt.Errorf("session state or payload changed: %+v", reply)
		}
		if err := output.Encode(reply); err != nil {
			return err
		}
		if o.count > 0 && count == uint64(o.count) {
			break
		}
		<-ticker.C
	}
	return nil
}

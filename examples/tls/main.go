//go:build linux || darwin

// A standalone TLS hot-upgrade example. See README.md in this directory.
package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"os"
	"time"
)

// Set with go build -ldflags '-X main.version=v2'.
var version = "dev"

type options struct {
	mode, address, cert, key, ca, serverName, upgradeSocket string
	tlsVersion                                              uint16
	interval, readyTimeout                                  time.Duration
	count                                                   int
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	var o options
	flag.StringVar(&o.mode, "mode", "server", "server, client, or cert")
	flag.StringVar(&o.address, "addr", "127.0.0.1:9443", "server address")
	flag.StringVar(&o.cert, "cert", "bin/tls/server.crt", "server certificate PEM")
	flag.StringVar(&o.key, "key", "bin/tls/server.key", "server private key PEM")
	flag.StringVar(&o.ca, "ca", "", "client trusted certificate PEM (defaults to -cert)")
	flag.StringVar(&o.serverName, "server-name", "localhost", "client certificate hostname to verify")
	flag.StringVar(&o.upgradeSocket, "upgrade-socket", "/tmp/graceupgrade-tls.sock", "upgrade UDS path, shared by all generations")
	flag.DurationVar(&o.interval, "interval", 500*time.Millisecond, "client request interval")
	flag.DurationVar(&o.readyTimeout, "ready-timeout", 15*time.Second, "server upgrade timeout")
	flag.IntVar(&o.count, "count", 0, "client requests; 0 keeps the connection running")
	protocol := flag.String("tls-version", "1.3", "TLS 1.2 or 1.3")
	flag.Parse()
	switch *protocol {
	case "1.2":
		o.tlsVersion = tls.VersionTLS12
	case "1.3":
		o.tlsVersion = tls.VersionTLS13
	default:
		return errors.New("tls-version must be 1.2 or 1.3")
	}
	if o.ca == "" {
		o.ca = o.cert
	}
	switch o.mode {
	case "cert":
		return generateCertificate(o.cert, o.key)
	case "server":
		return runServer(o)
	case "client":
		return runClient(o)
	default:
		return errors.New("mode must be server, client, or cert")
	}
}

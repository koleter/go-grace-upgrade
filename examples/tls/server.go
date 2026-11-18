//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	graceupgrade "github.com/koleter/go-grace-upgrade"
	mtls "github.com/koleter/go-grace-upgrade/mtls"
)

const applicationProtocol = "grace-upgrade-demo"

type response struct {
	PID     int    `json:"pid"`
	Version string `json:"version"`
	Count   uint64 `json:"count"`
	Payload string `json:"payload"`
}

func handleMessage(ctx context.Context, session *graceupgrade.Session, message []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(response{PID: os.Getpid(), Version: version, Count: session.Sequence, Payload: string(message)})
}

func runServer(o options) error {
	manager, err := graceupgrade.New(graceupgrade.Options{
		ReadyTimeout: o.readyTimeout, UpgradeSocket: o.upgradeSocket,
		OnUpgradeError: func(err error) { log.Printf("upgrade failed; original connections resumed: %v", err) },
	})
	if err != nil {
		return err
	}
	defer manager.Close()
	certificate, err := mtls.LoadX509KeyPair(o.cert, o.key)
	if err != nil {
		return fmt.Errorf("load TLS certificate (generate with -mode cert): %w", err)
	}
	server, err := manager.NewServer("tls-echo", "tcp", o.address, graceupgrade.ServerOptions{
		Codec:     graceupgrade.LineCodec(),
		OnMessage: handleMessage,
		TLSConfig: &mtls.Config{
			Certificates: []mtls.Certificate{certificate},
			MinVersion:   o.tlsVersion, MaxVersion: o.tlsVersion,
			NextProtos: []string{applicationProtocol},
		},
		OnError: func(err error) { log.Print(err) },
	})
	if err != nil {
		return err
	}
	defer server.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	if err := manager.Ready(ctx); err != nil {
		return err
	}
	log.Printf("TLS ready pid=%d version=%s inherited=%t addr=%s upgrade_socket=%s", os.Getpid(), version, manager.Inherited(), server.Addr(), o.upgradeSocket)
	for {
		select {
		case err := <-serveErr:
			if err != nil {
				return err
			}
			serveErr = nil
		case <-manager.Done():
			log.Printf("TLS connections moved old_pid=%d remaining=%d", os.Getpid(), server.Connections())
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

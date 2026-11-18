//go:build linux || darwin

// A TCP line service. Start a new version with the same -upgrade-socket to
// migrate the listener, existing connections, partial lines and session counts.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	graceupgrade "github.com/koleter/go-grace-upgrade"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "server", "server or client")
	address := flag.String("addr", "127.0.0.1:9090", "TCP address")
	socket := flag.String("upgrade-socket", "/tmp/graceupgrade-tcp.sock", "upgrade UDS path, shared by all generations")
	interval := flag.Duration("interval", 500*time.Millisecond, "client request interval")
	flag.Parse()
	if *mode == "client" {
		return runClient(*address, *interval)
	}
	if *mode != "server" {
		return errors.New("mode must be server or client")
	}
	manager, err := graceupgrade.New(graceupgrade.Options{
		UpgradeSocket:  *socket,
		OnUpgradeError: func(err error) { log.Printf("upgrade failed: %v", err) },
	})
	if err != nil {
		return err
	}
	defer manager.Close()
	server, err := manager.NewServer("tcp", "tcp", *address, graceupgrade.ServerOptions{
		Codec: graceupgrade.LineCodec(),
		OnMessage: func(ctx context.Context, session *graceupgrade.Session, message []byte) ([]byte, error) {
			return json.Marshal(map[string]any{
				"pid": os.Getpid(), "version": version,
				"count": session.Sequence, "payload": string(message),
			})
		},
		OnError: func(err error) { log.Print(err) },
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ctx)
	}()
	if err := manager.Ready(ctx); err != nil {
		return err
	}
	log.Printf("ready pid=%d version=%s addr=%s upgrade_socket=%s", os.Getpid(), version, server.Addr(), *socket)
	for {
		select {
		case err := <-serveErr:
			if err != nil {
				return err
			}
			serveErr = nil
		case <-manager.Done():
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func runClient(address string, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("interval must be positive")
	}
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("connected once local=%s remote=%s; no reconnect logic", conn.LocalAddr(), conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for count := 1; ; count++ {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := fmt.Fprintf(conn, "message-%d\n", count); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		fmt.Print(line)
		<-ticker.C
	}
}

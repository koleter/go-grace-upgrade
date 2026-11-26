//go:build linux || darwin

// A TCP service with #-delimited requests and line-delimited responses. Start a
// new version with the same -upgrade-socket to migrate existing connections,
// partial messages, global application data and per-connection data.
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

type globalData struct {
	StartedAt      time.Time `json:"started_at"`
	InitialVersion string    `json:"initial_version"`
	UpgradeCount   uint64    `json:"upgrade_count"`
}

type connectionData struct {
	InitialVersion string `json:"initial_version"`
	FirstMessage   string `json:"first_message"`
	ReceivedBytes  uint64 `json:"received_bytes"`
}

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
	decoder, err := graceupgrade.DelimiterDecoder("#")
	if err != nil {
		return err
	}
	encoder, err := graceupgrade.DelimiterEncoder("\n")
	if err != nil {
		return err
	}
	// Global data is initialized/restored before serving, then only read by
	// callbacks. Mutable global data would need synchronization across connections.
	global := globalData{StartedAt: time.Now(), InitialVersion: version}
	manager, err := graceupgrade.New(graceupgrade.Options{
		UpgradeSocket: *socket,
		Snapshot: func(ctx context.Context) ([]byte, error) {
			return json.Marshal(global)
		},
		OnUpgradeError: func(err error) { log.Printf("upgrade failed: %v", err) },
	})
	if err != nil {
		return err
	}
	defer manager.Close()
	// State contains the previous process's Snapshot output. Restore it before
	// Ready allows inherited or new connections to run their message callbacks.
	if state := manager.State(); len(state) > 0 {
		if err := json.Unmarshal(state, &global); err != nil {
			return fmt.Errorf("restore global data: %w", err)
		}
		global.UpgradeCount++
	}
	server, err := manager.NewServer("test-tcp", "tcp", *address, graceupgrade.ServerOptions{
		Decoder: decoder,
		Encoder: encoder,
		OnMessage: func(ctx context.Context, session *graceupgrade.Session, message []byte) ([]byte, error) {
			// Values is private to this connection and automatically restored on
			// upgrade. Access it only inside OnMessage; callbacks are serialized
			// per connection, so no lock is needed for connection data.
			connection := connectionData{InitialVersion: version, FirstMessage: string(message)}
			if state, ok := session.Values["connection_data"]; ok {
				if err := json.Unmarshal(state, &connection); err != nil {
					return nil, fmt.Errorf("restore connection data: %w", err)
				}
			}
			connection.ReceivedBytes += uint64(len(message))
			state, err := json.Marshal(connection)
			if err != nil {
				return nil, fmt.Errorf("save connection data: %w", err)
			}
			session.Values["connection_data"] = state
			return json.Marshal(map[string]any{
				"pid": os.Getpid(), "version": version,
				"count": session.Sequence, "payload": string(message),
				"global_data": global, "connection_data": connection,
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
		if _, err := fmt.Fprintf(conn, "message-%d#", count); err != nil {
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

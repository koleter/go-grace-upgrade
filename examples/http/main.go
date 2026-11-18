//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	graceupgrade "github.com/koleter/go-grace-upgrade"
)

// Override with: go build -ldflags '-X main.version=v2'.
var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	readyTimeout := flag.Duration("ready-timeout", 10*time.Second, "replacement startup timeout")
	drainTimeout := flag.Duration("drain-timeout", 30*time.Second, "maximum time to drain active requests")
	upgradeSocket := flag.String("upgrade-socket", "/tmp/graceupgrade-http.sock", "upgrade UDS path, shared by all generations")
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	m, err := graceupgrade.New(graceupgrade.Options{
		ReadyTimeout:   *readyTimeout,
		UpgradeSocket:  *upgradeSocket,
		OnUpgradeError: func(err error) { log.Printf("upgrade failed; continuing pid=%d: %v", os.Getpid(), err) },
	})
	if err != nil {
		return err
	}
	defer m.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/pid" && r.URL.Path != "/slow" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/slow" {
			duration := 5 * time.Second
			if value := r.URL.Query().Get("duration"); value != "" {
				var err error
				duration, err = time.ParseDuration(value)
				if err != nil || duration < 0 || duration > 5*time.Minute {
					http.Error(w, "duration must be between 0 and 5m", http.StatusBadRequest)
					return
				}
			}
			log.Printf("slow request started pid=%d version=%s", os.Getpid(), version)
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"pid": os.Getpid(), "version": version, "inherited": m.Inherited(),
		})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := m.ServeHTTP("http", "tcp", *addr, server, graceupgrade.HTTPOptions{DrainTimeout: *drainTimeout}); err != nil {
		return err
	}
	if err := m.Ready(context.Background()); err != nil {
		return err
	}
	log.Printf("ready pid=%d version=%s inherited=%t addr=%s upgrade_socket=%s", os.Getpid(), version, m.Inherited(), *addr, *upgradeSocket)
	if err := m.Wait(); err != nil {
		return err
	}
	log.Printf("drain complete pid=%d", os.Getpid())
	return nil
}

package graceupgrade

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func newHTTPManager(t *testing.T) *Manager {
	t.Helper()
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close(); m.Wait() })
	return m
}

func managedHTTPAddress(t *testing.T, m *Manager, name string) string {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	return "http://" + m.listeners[name].raw.Addr().String()
}

func managedHTTPForTest(m *Manager, name string) *managedHTTPServer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.httpServers[name]
}

func httpGet(url string) (string, error) {
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return string(data), err
}

func waitHTTPResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP operation did not complete")
		return nil
	}
}

func waitHTTPEvent(t *testing.T, event <-chan struct{}) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP lifecycle event did not happen")
	}
}

// Run an independently started replacement with the same service registration.
func TestHTTPProcess(t *testing.T) {
	mode := os.Getenv("GRACEUPGRADE_HTTP_CHILD")
	if mode == "" {
		return
	}
	m := newHTTPManager(t)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, os.Getpid())
	})}
	if mode == "fail" {
		server.Shutdown(context.Background())
	}
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPHandoffWaitsForInflightRequest(t *testing.T) {
	m := newHTTPManager(t)
	started, release, draining := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRequest)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		fmt.Fprint(w, "old request finished")
	})}
	server.RegisterOnShutdown(func() { close(draining) })
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{DrainTimeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := managedHTTPAddress(t, m, "http")
	request := make(chan error, 1)
	go func() {
		body, err := httpGet(address)
		if err == nil && body != "old request finished" {
			err = fmt.Errorf("unexpected old response %q", body)
		}
		request <- err
	}()
	waitHTTPEvent(t, started)
	waited := make(chan error, 1)
	go func() { waited <- m.Wait() }()
	child, err := replaceProcess(t, context.Background(), m, replacementOptions{
		Args: []string{"-test.run=^TestHTTPProcess$"}, Env: []string{"GRACEUPGRADE_HTTP_CHILD=ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHTTPEvent(t, m.Done())
	waitHTTPEvent(t, draining)
	select {
	case err := <-waited:
		t.Fatalf("Wait returned while old request was running: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if body, err := httpGet(address); err != nil || body != strconv.Itoa(child.PID) {
		t.Fatalf("new request did not reach replacement: %q, %v", body, err)
	}
	releaseRequest()
	if err := waitHTTPResult(t, request); err != nil {
		t.Fatal(err)
	}
	if err := waitHTTPResult(t, waited); err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(); err != nil {
		t.Fatalf("repeated Wait: %v", err)
	}
}

func TestHTTPFailedUpgradeKeepsOldServerRunning(t *testing.T) {
	m := newHTTPManager(t)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "old server")
	})}
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceProcess(t, context.Background(), m, replacementOptions{
		Args: []string{"-test.run=^TestHTTPProcess$"}, Env: []string{"GRACEUPGRADE_HTTP_CHILD=fail"},
	}); err == nil {
		t.Fatal("invalid replacement unexpectedly committed")
	}
	select {
	case <-m.Done():
		t.Fatal("failed upgrade stopped old owner")
	default:
	}
	if body, err := httpGet(managedHTTPAddress(t, m, "http")); err != nil || body != "old server" {
		t.Fatalf("old server did not survive rollback: %q, %v", body, err)
	}
}

func TestHTTPCloseDrainsServersConcurrently(t *testing.T) {
	m := newHTTPManager(t)
	release := make(chan struct{})
	var once sync.Once
	releaseRequests := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseRequests)
	requests := make(chan error, 2)
	started := make(chan struct{}, 2)
	draining := make(chan struct{}, 2)
	for _, name := range []string{"first", "second"} {
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started <- struct{}{}
			<-release
			fmt.Fprint(w, "finished")
		})}
		server.RegisterOnShutdown(func() { draining <- struct{}{} })
		if err := m.ServeHTTP(name, "tcp", "127.0.0.1:0", server, HTTPOptions{}); err != nil {
			t.Fatal(err)
		}
		address := managedHTTPAddress(t, m, name)
		go func() { _, err := httpGet(address); requests <- err }()
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitHTTPEvent(t, started)
	waitHTTPEvent(t, started)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	waitHTTPEvent(t, draining)
	waitHTTPEvent(t, draining)
	waited := make(chan error, 1)
	go func() { waited <- m.Wait() }()
	select {
	case err := <-waited:
		t.Fatalf("Wait returned before requests completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	releaseRequests()
	for i := 0; i < 2; i++ {
		if err := waitHTTPResult(t, requests); err != nil {
			t.Fatal(err)
		}
	}
	if err := waitHTTPResult(t, waited); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPDrainTimeoutClosesConnection(t *testing.T) {
	m := newHTTPManager(t)
	started, canceled := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	})}
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{DrainTimeout: 40 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := make(chan error, 1)
	address := managedHTTPAddress(t, m, "http")
	go func() { _, err := httpGet(address); request <- err }()
	waitHTTPEvent(t, started)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait did not report drain deadline: %v", err)
	}
	waitHTTPEvent(t, canceled)
	if err := waitHTTPResult(t, request); err == nil {
		t.Fatal("timed-out connection was not closed")
	}
}

func TestHTTPUnexpectedServeErrorStopsManager(t *testing.T) {
	m := newHTTPManager(t)
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", &http.Server{}, HTTPOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 2)
	go func() { waited <- m.Wait() }()
	go func() { waited <- m.Wait() }()
	managedHTTPForTest(m, "http").listener.Close()
	for i := 0; i < 2; i++ {
		if err := waitHTTPResult(t, waited); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("serving error not returned: %v", err)
		}
	}
	waitHTTPEvent(t, m.Done())
}

func TestHTTPServeFailureDuringUpgradeWaitsForTransaction(t *testing.T) {
	for _, outcome := range []string{"rollback", "commit"} {
		t.Run(outcome, func(t *testing.T) {
			m := newHTTPManager(t)
			if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", &http.Server{}, HTTPOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := m.Ready(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := m.beginUpgrade(); err != nil {
				t.Fatal(err)
			}
			// Finish the simulated transaction even if an assertion fails.
			var finishOnce sync.Once
			finish := func() { finishOnce.Do(func() { m.finishUpgrade(outcome == "commit") }) }
			t.Cleanup(finish)
			server := managedHTTPForTest(m, "http")
			server.listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stop := wakeOnCancel(ctx, m.changed)
			defer stop()
			m.mu.Lock()
			for !server.served && ctx.Err() == nil {
				m.changed.Wait()
			}
			m.mu.Unlock()
			if err := ctx.Err(); err != nil {
				t.Fatal("HTTP Serve did not finish:", err)
			}
			if err := m.Close(); err != ErrBusy {
				t.Fatalf("Close interrupted an active transaction: %v", err)
			}
			waited := make(chan error, 1)
			go func() { waited <- m.Wait() }()
			select {
			case err := <-waited:
				t.Fatalf("Wait interrupted an active transaction: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			finish()
			if err := waitHTTPResult(t, waited); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("serving error lost after %s: %v", outcome, err)
			}
			if err := m.Close(); err != nil {
				t.Fatalf("Close after %s: %v", outcome, err)
			}
		})
	}
}

func TestHTTPReadyCancellationDuringStartupCanRetry(t *testing.T) {
	m := newHTTPManager(t)
	starting, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	server := &http.Server{
		BaseContext: func(net.Listener) context.Context {
			close(starting)
			<-release
			return context.Background()
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "ready")
		}),
	}
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{}); err != nil {
		t.Fatal(err)
	}
	waitHTTPEvent(t, starting)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err := m.Ready(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready ignored startup cancellation: %v", err)
	}
	unblock()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Ready(ctx); err != nil {
		t.Fatal("Ready could not retry after cancellation:", err)
	}
	if body, err := httpGet(managedHTTPAddress(t, m, "http")); err != nil || body != "ready" {
		t.Fatalf("HTTP did not serve after retry: %q %v", body, err)
	}
}

func TestHTTPRegistrationAndStartupValidation(t *testing.T) {
	m := newHTTPManager(t)
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", nil, HTTPOptions{}); err == nil {
		t.Fatal("nil HTTP server accepted")
	}
	server := &http.Server{}
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{DrainTimeout: -1}); err == nil {
		t.Fatal("negative timeout accepted")
	}
	server.Shutdown(context.Background())
	if err := m.ServeHTTP("http", "tcp", "127.0.0.1:0", server, HTTPOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.ServeHTTP("duplicate", "tcp", "127.0.0.1:0", server, HTTPOptions{}); err == nil {
		t.Fatal("server registered twice")
	}
	if err := m.Ready(context.Background()); !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("already-stopped HTTP server committed: %v", err)
	}
	if err := m.Wait(); err == nil || !strings.Contains(err.Error(), "http") {
		t.Fatalf("startup error not returned by Wait: %v", err)
	}
}

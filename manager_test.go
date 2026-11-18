package graceupgrade

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testPlatformSupported = runtime.GOOS == "linux" || runtime.GOOS == "darwin"

// shortSocketDir avoids macOS filesystem UDS path length limits.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gu-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func testOptions(t *testing.T) Options {
	t.Helper()
	if !testPlatformSupported {
		t.Skip("Unix descriptor passing required")
	}
	path := os.Getenv("GRACEUPGRADE_SOCKET")
	if path == "" {
		path = filepath.Join(shortSocketDir(t), "u.sock")
	}
	return Options{UpgradeSocket: path, ReadyTimeout: 5 * time.Second}
}

type replacementOptions struct{ Args, Env []string }
type replacementProcess struct {
	PID     int
	process *os.Process
	done    chan struct{}
	err     error
	output  bytes.Buffer
}

func (p *replacementProcess) Wait() error { <-p.done; return p.err }

func managerPhaseForTest(m *Manager) managerPhase {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase
}

// Launch a replacement exactly as a deployer does: a separate process with the
// same UDS path, no inherited control descriptor or production launch API.
func replaceProcess(t *testing.T, ctx context.Context, m *Manager, options replacementOptions) (*replacementProcess, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cmd := exec.Command(os.Args[0], options.Args...)
	cmd.Env = append(os.Environ(), append(options.Env, "GRACEUPGRADE_SOCKET="+m.upgradeSocket)...)
	p := &replacementProcess{done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = &p.output, &p.output
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p.process, p.PID = cmd.Process, cmd.Process.Pid
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.process.Kill(); p.Wait() })
	for {
		phase := managerPhaseForTest(m)
		if phase == phaseHandedOff {
			return p, nil
		}
		select {
		case <-p.done:
			if phase == phaseReady {
				return nil, fmt.Errorf("replacement failed: %v: %s", p.err, p.output.String())
			}
		default:
		}
		select {
		case <-ctx.Done():
			p.process.Kill()
			p.Wait()
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// The test binary also acts as a real replacement executable. Its sockets,
// environment, handshake, and startup failures cross an actual process boundary.
func TestListenerProcess(t *testing.T) {
	if os.Getenv("GRACEUPGRADE_TEST_CHILD") == "" {
		return
	}
	m, err := New(testOptions(t))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(21)
	}
	if !m.Inherited() || string(m.State()) != "snapshot" {
		os.Exit(22)
	}
	first, err := m.Listen("first", "tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(24)
	}
	second, err := m.Listen("second", "tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(25)
	}
	for _, ln := range []net.Listener{first, second} {
		go serveLines(ln, "child "+strconv.Itoa(os.Getpid()))
	}
	if err := m.Ready(context.Background()); err != nil {
		os.Exit(30)
	}
	<-m.Done()
}

func testManager(t *testing.T) (*Manager, net.Listener, net.Listener) {
	t.Helper()
	if !testPlatformSupported {
		t.Skip("Unix descriptor passing required")
	}
	options := testOptions(t)
	options.Snapshot = func(context.Context) ([]byte, error) { return []byte("snapshot"), nil }
	m, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	first, err := m.Listen("first", "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Listen("second", "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go serveLines(first, "parent")
	go serveLines(second, "parent")
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m, first, second
}

func serveLines(ln net.Listener, response string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			scanner := bufio.NewScanner(conn)
			for scanner.Scan() {
				if _, err := fmt.Fprintln(conn, response); err != nil {
					return
				}
			}
		}()
	}
}

func dial(t *testing.T, ln net.Listener) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func exchange(t *testing.T, conn net.Conn) string {
	t.Helper()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := fmt.Fprintln(conn, "ping"); err != nil {
		t.Fatal(err)
	}
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(response)
}

func listenerProcessOptions() replacementOptions {
	return replacementOptions{
		Args: []string{"-test.run=^TestListenerProcess$"},
		Env:  []string{"GRACEUPGRADE_TEST_CHILD=1"},
	}
}

func TestUDSTransfersBareListenersAndDrainsExistingConnection(t *testing.T) {
	m, first, second := testManager(t)
	existing := dial(t, first)
	if got := exchange(t, existing); got != "parent" {
		t.Fatal(got)
	}
	child, err := replaceProcess(t, context.Background(), m, listenerProcessOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.process.Kill(); child.Wait() })
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	for _, ln := range []net.Listener{first, second} {
		if got := exchange(t, dial(t, ln)); got != "child "+strconv.Itoa(child.PID) {
			t.Fatal(got)
		}
	}
	if got := exchange(t, existing); got != "parent" {
		t.Fatalf("established connection moved or lost: %q", got)
	}
}

func TestListenerGateAndLifecycle(t *testing.T) {
	if !testPlatformSupported {
		t.Skip("Unix required")
	}
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ln, err := m.Listen("test", "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() { conn, _ := ln.Accept(); accepted <- conn }()
	conn := dial(t, ln)
	select {
	case <-accepted:
		t.Fatal("accepted before readiness")
	case <-time.After(30 * time.Millisecond):
	}
	if err := m.beginUpgrade(); !errors.Is(err, ErrNotReady) {
		t.Fatal(err)
	}
	if _, err := m.Listen("test", "tcp", "127.0.0.1:0"); err == nil {
		t.Fatal("duplicate accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Ready(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled readiness: %v", err)
	}
	if _, err := m.Listen("after-canceled-ready", "tcp", "127.0.0.1:0"); err != nil {
		t.Fatalf("canceled readiness did not restore initialization: %v", err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case acceptedConn := <-accepted:
		if acceptedConn == nil {
			t.Fatal("Accept failed")
		}
		acceptedConn.Close()
	case <-time.After(time.Second):
		t.Fatal("readiness did not release Accept")
	}
	conn.Close()
	if _, err := m.Listen("late", "tcp", "127.0.0.1:0"); err == nil {
		t.Fatal("registered after Ready")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Ready(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestCloseBeforeReadyUnblocksAccept(t *testing.T) {
	for _, owner := range []string{"listener", "manager"} {
		t.Run(owner, func(t *testing.T) {
			m, err := New(testOptions(t))
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			ln, err := m.Listen("test", "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			accepted := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if conn != nil {
					conn.Close()
				}
				accepted <- err
			}()
			if owner == "listener" {
				err = ln.Close()
			} else {
				err = m.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-accepted:
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("Accept after Close: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not release Accept before Ready")
			}
		})
	}
}

func TestConcurrentReadyAndClose(t *testing.T) {
	if !testPlatformSupported {
		t.Skip("Unix descriptor passing required")
	}
	for _, action := range []string{"commit", "close"} {
		t.Run(action, func(t *testing.T) {
			m, err := New(testOptions(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { m.Close() })
			peer, file, err := controlPair()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			control, err := controlFromFile(file)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			m.control = control
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			peer.SetDeadline(time.Now().Add(5 * time.Second))
			done := make(chan error, 1)
			go func() { done <- m.Ready(ctx) }()
			// Receiving readiness proves the first Ready is waiting for the
			// parent's connection batch; concurrent calls cannot race past it.
			var ready message
			if err := readFrame(peer, &ready); err != nil || ready.Kind != "ready" {
				t.Fatalf("receive readiness: %+v, %v", ready, err)
			}
			if err := m.Ready(ctx); !errors.Is(err, ErrBusy) {
				t.Fatalf("concurrent Ready: %v", err)
			}
			if _, err := m.Listen("late", "tcp", "127.0.0.1:0"); err == nil {
				t.Fatal("registered a listener during readiness")
			}
			if err := m.beginUpgrade(); !errors.Is(err, ErrNotReady) {
				t.Fatalf("handoff during readiness: %v", err)
			}
			if action == "close" {
				if err := m.Close(); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err == nil {
					t.Fatal("Ready succeeded after concurrent Close")
				}
				// Ready's deferred rollback must not reopen a closed manager.
				if err := m.Ready(ctx); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed manager became reusable: %v", err)
				}
				if err := m.beginUpgrade(); !errors.Is(err, ErrClosed) {
					t.Fatalf("handoff after Close: %v", err)
				}
				if err := m.Close(); err != nil {
					t.Fatalf("repeated Close: %v", err)
				}
				return
			}
			if err := writeFrame(peer, message{Kind: "connections"}); err != nil {
				t.Fatal(err)
			}
			var prepared message
			if err := readFrame(peer, &prepared); err != nil || prepared.Kind != "prepared" {
				t.Fatalf("receive prepared: %+v, %v", prepared, err)
			}
			if err := writeFrame(peer, message{Kind: "commit"}); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := m.Ready(ctx); err != nil {
				t.Fatalf("committed Ready was not idempotent: %v", err)
			}
		})
	}
}

func TestInputValidation(t *testing.T) {
	if !testPlatformSupported {
		t.Skip("Unix required")
	}
	if _, err := New(Options{ReadyTimeout: -1}); err == nil {
		t.Fatal("negative timeout")
	}
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, network := range []string{"udp", "unix", "invalid"} {
		if _, err := m.Listen("test", network, "127.0.0.1:0"); err == nil {
			t.Fatal(network)
		}
	}
	if _, err := m.Listen("", "tcp", "127.0.0.1:0"); err == nil {
		t.Fatal("empty name")
	}
	if _, err := New(Options{}); err == nil {
		t.Fatal("missing upgrade socket")
	}
}

//go:build linux || darwin

package graceupgrade

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type udsProcessInfo struct {
	Address   string
	PID       int
	Inherited bool
}

// This is an independently launched process: there is no Upgrade call, inherited
// environment marker, ExtraFiles, or upgrade signal in the startup-driven path.
func TestUDSProcess(t *testing.T) {
	path := os.Getenv("GRACEUPGRADE_UDS_PATH")
	if path == "" {
		return
	}
	version, _ := strconv.Atoi(os.Getenv("GRACEUPGRADE_TLS_VERSION"))
	m, err := New(Options{
		UpgradeSocket: path, ReadyTimeout: 3 * time.Second,
		Snapshot:       func(context.Context) ([]byte, error) { return []byte("uds-snapshot"), nil },
		OnUpgradeError: func(err error) { os.WriteFile(path+".rollback", []byte(err.Error()), 0600) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Inherited() && string(m.State()) != "uds-snapshot" {
		t.Fatal("process snapshot was not restored")
	}
	o := ServerOptions{Codec: LineCodec(), TLSConfig: managedTLSConfig(t, uint16(version), 0), OnMessage: func(ctx context.Context, s *Session, data []byte) ([]byte, error) {
		if s.Sequence == 1 {
			s.Values["first"] = bytes.Clone(data)
		}
		return []byte(fmt.Sprintf("%d %d %s/%s", os.Getpid(), s.Sequence, s.Values["first"], data)), nil
	}}
	if os.Getenv("GRACEUPGRADE_UDS_MODE") == "mismatch" {
		o.Codec = renamedCodec{Codec: o.Codec, name: "wrong/v2"}
	}
	s, err := m.NewServer("uds", "tcp", "127.0.0.1:0", o)
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(context.Background())
	info, _ := json.Marshal(udsProcessInfo{Address: s.Addr().String(), PID: os.Getpid(), Inherited: m.Inherited()})
	if stage := os.Getenv("GRACEUPGRADE_UDS_STAGE"); stage != "" {
		if err := os.WriteFile(stage, info, 0600); err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := os.Stat(stage + ".go"); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("GRACEUPGRADE_UDS_READY"), info, 0600); err != nil {
		t.Fatal(err)
	}
	<-m.Done()
}

type udsProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func launchUDS(t *testing.T, path, ready, mode, stage string, version uint16) *udsProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestUDSProcess$")
	cmd.Env = append(os.Environ(), "GRACEUPGRADE_UDS_PATH="+path, "GRACEUPGRADE_UDS_READY="+ready,
		"GRACEUPGRADE_UDS_MODE="+mode, "GRACEUPGRADE_UDS_STAGE="+stage, "GRACEUPGRADE_TLS_VERSION="+strconv.Itoa(int(version)))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &udsProcess{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { cmd.Process.Kill(); <-p.done })
	return p
}

func waitUDSInfo(t *testing.T, path string, p *udsProcess) udsProcessInfo {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil {
			var info udsProcessInfo
			if json.Unmarshal(data, &info) == nil {
				return info
			}
		}
		select {
		case <-p.done:
			t.Fatalf("startup exited: %v", p.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("startup timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitUDSExit(t *testing.T, p *udsProcess, success bool) {
	t.Helper()
	select {
	case <-p.done:
		if (p.err == nil) != success {
			t.Fatalf("exit=%v want success=%t", p.err, success)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old/failed process did not exit")
	}
}

func TestUDSStartupPreservesTCPAndTLSAcrossThreeProcesses(t *testing.T) {
	for _, version := range []uint16{0, tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(strconv.Itoa(int(version)), func(t *testing.T) {
			dir := shortSocketDir(t)
			path, ready := filepath.Join(dir, "u.sock"), filepath.Join(dir, "v1")
			first := launchUDS(t, path, ready, "", "", version)
			info := waitUDSInfo(t, ready, first)
			if info.Inherited {
				t.Fatal("cold start inherited")
			}
			var conn net.Conn
			var err error
			if version == 0 {
				conn, err = net.Dial("tcp", info.Address)
			} else {
				conn, err = tls.Dial("tcp", info.Address, &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(15 * time.Second))
			reader := bufio.NewReader(conn)
			fmt.Fprintln(conn, "before")
			if pid, n, text := readMessage(t, reader, "line"); pid != info.PID || n != 1 || text != "before/before" {
				t.Fatalf("first response %d %d %s", pid, n, text)
			}
			for generation := 2; generation <= 3; generation++ {
				if _, err := fmt.Fprint(conn, "partial"); err != nil {
					t.Fatal(err)
				}
				nextReady := filepath.Join(dir, fmt.Sprint("v", generation))
				next := launchUDS(t, path, nextReady, "", "", version)
				nextInfo := waitUDSInfo(t, nextReady, next)
				waitUDSExit(t, first, true) // Old process really exited before remaining bytes arrive.
				if lease, err := lockUpgradePath(context.Background(), path); !errors.Is(err, ErrBusy) {
					if lease != nil {
						lease.Close()
					}
					t.Fatalf("successor lost endpoint ownership lock: %v", err)
				}
				if !nextInfo.Inherited || nextInfo.Address != info.Address {
					t.Fatal("listener not inherited", nextInfo)
				}
				fmt.Fprintln(conn, "-after")
				if pid, n, text := readMessage(t, reader, "line"); pid != nextInfo.PID || n != generation || text != "before/partial-after" {
					t.Fatalf("handoff %d %d %q", pid, n, text)
				}
				first, info = next, nextInfo
			}
			// A fresh TCP connection still reaches the last generation.
			fresh, err := net.Dial("tcp", info.Address)
			if err != nil {
				t.Fatal(err)
			}
			fresh.Close()
		})
	}
}

func TestUDSRejectsConcurrentUpgradeAndRollsBackFailedStartup(t *testing.T) {
	dir := shortSocketDir(t)
	path, ready := filepath.Join(dir, "u.sock"), filepath.Join(dir, "v1")
	first := launchUDS(t, path, ready, "", "", 0)
	info := waitUDSInfo(t, ready, first)
	conn, err := net.Dial("tcp", info.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	reader := bufio.NewReader(conn)
	fmt.Fprintln(conn, "before")
	readMessage(t, reader, "line")
	stage := filepath.Join(dir, "staged")
	pending := launchUDS(t, path, filepath.Join(dir, "unused"), "", stage, 0)
	waitUDSInfo(t, stage, pending)
	concurrent := launchUDS(t, path, filepath.Join(dir, "concurrent"), "", "", 0)
	waitUDSExit(t, concurrent, false)
	pending.cmd.Process.Kill()
	waitUDSExit(t, pending, false)
	waitUDSRollback(t, path, "wait for readiness")
	os.Remove(path + ".rollback")
	// An incompatible protocol is also rejected after FD and state restoration.
	failed := launchUDS(t, path, filepath.Join(dir, "bad"), "mismatch", "", 0)
	waitUDSExit(t, failed, false)
	waitUDSRollback(t, path, "prepare connection batch")
	fmt.Fprintln(conn, "after")
	if pid, n, text := readMessage(t, reader, "line"); pid != info.PID || n != 2 || text != "before/after" {
		t.Fatalf("rollback %d %d %q", pid, n, text)
	}
	nextReady := filepath.Join(dir, "v2")
	next := launchUDS(t, path, nextReady, "", "", 0)
	nextInfo := waitUDSInfo(t, nextReady, next)
	waitUDSExit(t, first, true)
	fmt.Fprintln(conn, "retry")
	if pid, n, _ := readMessage(t, reader, "line"); pid != nextInfo.PID || n != 3 {
		t.Fatalf("retry %d %d", pid, n)
	}
}

func waitUDSRollback(t *testing.T, path, part string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if data, err := os.ReadFile(path + ".rollback"); err == nil && strings.Contains(string(data), part) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("old process did not complete rollback:", part)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUDSStaleSocketRecoveryAndPathOwnership(t *testing.T) {
	dir := shortSocketDir(t)
	path := filepath.Join(dir, "u.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	stale.Close()
	m, err := New(Options{UpgradeSocket: path})
	if err != nil {
		t.Fatal(err)
	}
	if m.Inherited() {
		t.Fatal("stale socket counted as live process")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("owned socket was not removed", err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if m, err := New(Options{UpgradeSocket: path}); err == nil {
		m.Close()
		t.Fatal("regular file accepted")
	}
	if data, _ := os.ReadFile(path); string(data) != "keep" {
		t.Fatal("regular file was removed")
	}
}

func TestUDSRefusedEndpointWithLiveLeaseIsNeverUnlinked(t *testing.T) {
	dir := shortSocketDir(t)
	path := filepath.Join(dir, "u.sock")
	m, err := New(Options{UpgradeSocket: path})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an unavailable listener while its process is still alive.
	// ECONNREFUSED alone must not authorize deleting its discovery path.
	m.endpoint.listener.Close()
	if next, err := New(Options{UpgradeSocket: path, ReadyTimeout: 50 * time.Millisecond}); err == nil {
		next.Close()
		t.Fatal("live lease was ignored")
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("active owner's endpoint was replaced", err)
	}
}

func TestUDSHandshakeFailureNeverFallsBackToColdStart(t *testing.T) {
	dir := shortSocketDir(t)
	path := filepath.Join(dir, "u.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.AcceptUnix()
		if err != nil {
			return
		}
		defer conn.Close()
		var hello message
		readFrame(conn, &hello)
		writeFrame(conn, message{Kind: "error", Error: "not this service"})
	}()
	if m, err := New(Options{UpgradeSocket: path, ReadyTimeout: time.Second}); err == nil {
		m.Close()
		t.Fatal("handshake failure became a cold start")
	} else if !strings.Contains(err.Error(), "discovery rejected") {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal("live endpoint removed", err)
	}
}

func TestUDSConcurrentColdStartsElectOneListenerOwner(t *testing.T) {
	dir := shortSocketDir(t)
	path := filepath.Join(dir, "u.sock")
	type result struct {
		manager *Manager
		err     error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			m, err := New(Options{UpgradeSocket: path, ReadyTimeout: 3 * time.Second})
			results <- result{m, err}
		}()
	}
	first := <-results
	if first.err != nil {
		t.Fatal(first.err)
	}
	defer first.manager.Close()
	if first.manager.Inherited() {
		t.Fatal("first startup was not cold")
	}
	if _, err := first.manager.Listen("test", "tcp", "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := first.manager.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := <-results
	if second.err != nil {
		t.Fatal(second.err)
	}
	if !second.manager.Inherited() {
		second.manager.Close()
		t.Fatal("two cold starts claimed the endpoint")
	}
	// Abandon startup before Ready: closing the inherited copy must not unlink
	// the old owner's socket, and the owner must become upgradeable again.
	second.manager.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if managerPhaseForTest(first.manager) == phaseReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old owner did not resume")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal("failed startup removed old endpoint:", err)
	}
}

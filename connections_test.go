//go:build linux || darwin

package graceupgrade

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mtls "github.com/koleter/go-grace-upgrade/mtls"
)

// counterCodec demonstrates a business-defined Codec: line-framed requests,
// with response framing supplied by the callback (including raw large replies).
type counterCodec struct{}

func (counterCodec) Name() string { return "test/counter/v1" }
func (counterCodec) Decode(input []byte) (int, []byte, error) {
	return LineCodec().Decode(input)
}
func (counterCodec) Encode(message []byte) ([]byte, error) { return message, nil }

func counterOptions(t *testing.T, version, suite uint16) ServerOptions {
	t.Helper()
	return ServerOptions{
		Codec: counterCodec{}, TLSConfig: managedTLSConfig(t, version, suite),
		OnMessage: func(ctx context.Context, session *Session, data []byte) ([]byte, error) {
			if string(data) == "large" {
				return bytes.Repeat([]byte("Z"), 192<<10), nil
			}
			return []byte(fmt.Sprintf("%d %d %s\n", os.Getpid(), session.Sequence, data)), nil
		},
	}
}

func managedTLSConfig(t *testing.T, version uint16, suite uint16) *mtls.Config {
	t.Helper()
	if version == 0 {
		return nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := mtls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: secret}))
	if err != nil {
		t.Fatal(err)
	}
	config := &mtls.Config{Certificates: []mtls.Certificate{cert}, MinVersion: version, MaxVersion: version}
	if suite != 0 {
		config.CipherSuites = []uint16{suite}
	}
	return config
}

func TestManagedChild(t *testing.T) {
	if os.Getenv("GRACEUPGRADE_MANAGED_CHILD") == "" {
		return
	}
	version, _ := strconv.Atoi(os.Getenv("GRACEUPGRADE_TLS_VERSION"))
	suite, _ := strconv.Atoi(os.Getenv("GRACEUPGRADE_TLS_SUITE"))
	m, err := New(testOptions(t))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(40)
	}
	options := counterOptions(t, uint16(version), uint16(suite))
	if os.Getenv("GRACEUPGRADE_REJECT_RESTORE") == "1" {
		options.Codec = renamedCodec{Codec: options.Codec, name: "incompatible/v2"}
	}
	if os.Getenv("GRACEUPGRADE_REJECT_RESTORE") == "second" {
		options.MaxBuffer = 1024
	}
	s, err := m.NewServer("managed", "tcp", "127.0.0.1:0", options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(41)
	}
	go s.Serve(context.Background())
	if err := m.Ready(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(42)
	}
	select {}
}

func managedOptions(version, suite uint16, reject bool) replacementOptions {
	value := "0"
	if reject {
		value = "1"
	}
	return replacementOptions{Args: []string{"-test.run=^TestManagedChild$"}, Env: []string{
		"GRACEUPGRADE_MANAGED_CHILD=1", "GRACEUPGRADE_TLS_VERSION=" + strconv.Itoa(int(version)),
		"GRACEUPGRADE_TLS_SUITE=" + strconv.Itoa(int(suite)), "GRACEUPGRADE_REJECT_RESTORE=" + value,
	}}
}

func newManaged(t *testing.T, version, suite uint16) (*Manager, *Server, net.Conn) {
	t.Helper()
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	s, err := m.NewServer("managed", "tcp", "127.0.0.1:0", counterOptions(t, version, suite))
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(context.Background())
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	var conn net.Conn
	if version == 0 {
		conn, err = net.Dial("tcp", s.Addr().String())
	} else {
		config := &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version}
		if suite != 0 {
			config.CipherSuites = []uint16{suite}
		}
		conn, err = tls.Dial("tcp", s.Addr().String(), config)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return m, s, conn
}

func managedExchange(t *testing.T, conn net.Conn, text string) (int, int, string) {
	t.Helper()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(conn, text); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
	if len(parts) != 3 {
		t.Fatal(line)
	}
	pid, _ := strconv.Atoi(parts[0])
	count, _ := strconv.Atoi(parts[1])
	return pid, count, parts[2]
}

func TestServerPauseRollbackAndClose(t *testing.T) {
	for _, start := range []string{"before-pause", "after-rollback"} {
		t.Run(start, func(t *testing.T) {
			m, err := New(testOptions(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { m.Close() })
			s, err := m.NewServer("managed", "tcp", "127.0.0.1:0", counterOptions(t, 0, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Ready(context.Background()); err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			serve := func() { go func() { served <- s.Serve(context.Background()) }() }
			if start == "before-pause" {
				serve()
				managedExchange(t, dial(t, s.listener), "before")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			batch := &connectionBatch{}
			err = m.freeze(ctx, batch)
			batch.finish(false)
			if err != nil {
				t.Fatal(err)
			}
			if start == "after-rollback" {
				serve()
			}
			client := dial(t, s.listener)
			if _, count, text := managedExchange(t, client, "after"); count != 1 || text != "after" {
				t.Fatalf("Accept did not resume after rollback: %d %q", count, text)
			}
			batch = &connectionBatch{}
			err = m.freeze(ctx, batch)
			if err != nil {
				batch.finish(false)
				t.Fatal(err)
			}
			// Close must wake the paused Accept and connection loops even before
			// the batch is rolled back.
			if err := s.Close(); err != nil {
				batch.finish(false)
				t.Fatal(err)
			}
			select {
			case err := <-served:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Close did not unblock paused Serve")
			}
			client.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := client.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("Close did not close paused connection: %v", err)
			}
			batch.finish(false)
		})
	}
}

func TestIdleServerAcceptResumesAfterRepeatedRollback(t *testing.T) {
	m, s, first := newManaged(t, 0, 0)
	managedExchange(t, first, "start")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitIdle := func() {
		t.Helper()
		if err := s.waitConnectionsClosed(ctx); err != nil {
			t.Fatal("server did not finish closing its connections:", err)
		}
	}
	first.Close()
	waitIdle()

	for i := 0; i < 20; i++ {
		batch := &connectionBatch{}
		if err := m.freeze(ctx, batch); err != nil {
			batch.finish(false)
			t.Fatalf("pause %d: %v", i, err)
		}
		// Establish a connection while Accept is paused. Rollback must clear
		// the expired deadline and accept this connection without another wakeup.
		client := dial(t, s.listener)
		batch.finish(false)
		if _, count, text := managedExchange(t, client, "after"); count != 1 || text != "after" {
			t.Fatalf("rollback %d did not resume Accept: %d %q", i, count, text)
		}
		client.Close()
		waitIdle()
	}
}

func TestFreezeWaitsForTLSHandshakeAndRollsBack(t *testing.T) {
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	s, err := m.NewServer("managed", "tcp", "127.0.0.1:0", counterOptions(t, tls.VersionTLS13, 0))
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(context.Background())
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Connect without sending ClientHello. Freeze must wait for initialization
	// instead of snapshotting a connection with incomplete TLS state.
	raw := dial(t, s.listener)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for s.Connections() == 0 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal("server did not accept the TLS connection:", err)
	}
	batch := &connectionBatch{}
	freezeCtx, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	err = m.freeze(freezeCtx, batch)
	stop()
	batch.finish(false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("freeze did not wait for the TLS handshake: %v", err)
	}
	client := tls.Client(raw, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	defer client.Close()
	if err := client.HandshakeContext(ctx); err != nil {
		t.Fatal("TLS handshake did not resume after rollback:", err)
	}
	if _, count, text := managedExchange(t, client, "after"); count != 1 || text != "after" {
		t.Fatalf("rollback lost the connection: %d %q", count, text)
	}
	batch = &connectionBatch{}
	err = m.freeze(ctx, batch)
	defer batch.finish(false)
	if err != nil {
		t.Fatal("freeze failed after completing the handshake:", err)
	}
	if len(batch.connections) != 1 || batch.connections[0].state.TLS == nil {
		t.Fatal("completed TLS connection was not snapshotted")
	}
}

func TestEstablishedConnectionsMoveAcrossProcesses(t *testing.T) {
	cases := []struct {
		name           string
		version, suite uint16
	}{
		{"tcp", 0, 0},
		{"tls12-gcm", tls.VersionTLS12, tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		{"tls12-aes256", tls.VersionTLS12, tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384},
		{"tls12-chacha", tls.VersionTLS12, tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256},
		{"tls12-cbc", tls.VersionTLS12, tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256},
		{"tls13", tls.VersionTLS13, 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			m, s, conn := newManaged(t, test.version, test.suite)
			if pid, count, text := managedExchange(t, conn, "before"); pid != os.Getpid() || count != 1 || text != "before" {
				t.Fatalf("%d %d %q", pid, count, text)
			}
			local, remote := conn.LocalAddr().String(), conn.RemoteAddr().String()
			if _, err := conn.Write([]byte("partial")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
			child, err := replaceProcess(t, context.Background(), m, managedOptions(test.version, test.suite, false))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { child.process.Kill(); child.Wait() })
			if s.Connections() != 0 {
				t.Fatal("connections still owned by parent")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			pid, count, text := managedExchange(t, conn, "-after")
			if pid != child.PID || count != 2 || text != "partial-after" {
				t.Fatalf("%d %d %q", pid, count, text)
			}
			if conn.LocalAddr().String() != local || conn.RemoteAddr().String() != remote {
				t.Fatal("connection was replaced")
			}
			if pid, count, text := managedExchange(t, conn, "again"); pid != child.PID || count != 3 || text != "again" {
				t.Fatalf("%d %d %q", pid, count, text)
			}
		})
	}
}

func TestRestoreFailureResumesOriginalTLSAndProtocolState(t *testing.T) {
	m, _, conn := newManaged(t, tls.VersionTLS13, 0)
	managedExchange(t, conn, "before")
	if _, err := conn.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceProcess(t, context.Background(), m, managedOptions(tls.VersionTLS13, 0, true)); err == nil {
		t.Fatal("restore unexpectedly succeeded")
	}
	if pid, count, text := managedExchange(t, conn, "-rollback"); pid != os.Getpid() || count != 2 || text != "partial-rollback" {
		t.Fatalf("%d %d %q", pid, count, text)
	}
	child, err := replaceProcess(t, context.Background(), m, managedOptions(tls.VersionTLS13, 0, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.process.Kill(); child.Wait() })
	if pid, count, _ := managedExchange(t, conn, "retry"); pid != child.PID || count != 3 {
		t.Fatalf("%d %d", pid, count)
	}
}

func TestPendingCiphertextSurvivesHandoff(t *testing.T) {
	for _, version := range []uint16{0, tls.VersionTLS13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			m, s, conn := newManaged(t, version, 0)
			managedExchange(t, conn, "start")
			var clientTCP *net.TCPConn
			if secure, ok := conn.(*tls.Conn); ok {
				clientTCP = secure.NetConn().(*net.TCPConn)
			} else {
				clientTCP = conn.(*net.TCPConn)
			}
			if err := clientTCP.SetReadBuffer(4096); err != nil {
				t.Fatal(err)
			}
			for _, c := range s.sessionSnapshot() {
				if err := c.raw.(*net.TCPConn).SetWriteBuffer(1024); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := conn.Write([]byte("large\n")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
			batch := &connectionBatch{}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := m.freeze(ctx, batch)
			cancel()
			if err != nil {
				batch.finish(false)
				t.Fatal(err)
			}
			pending := 0
			for _, f := range batch.connections {
				if f.state != nil {
					pending += len(f.state.WireBuffer)
				}
			}
			batch.finish(false)
			if pending == 0 {
				t.Fatal("test did not create network backpressure")
			}
			child, err := replaceProcess(t, context.Background(), m, managedOptions(version, 0, false))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { child.process.Kill(); child.Wait() })
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			clientTCP.SetReadBuffer(256 << 10)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			response := make([]byte, 192<<10)
			if _, err := io.ReadFull(conn, response); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(response, bytes.Repeat([]byte("Z"), len(response))) {
				t.Fatal("pending ciphertext lost or duplicated")
			}
			if pid, count, _ := managedExchange(t, conn, "after-backpressure"); pid != child.PID || count != 3 {
				t.Fatalf("%d %d", pid, count)
			}
		})
	}
}

func TestBatchRollbackAfterSomeConnectionsWereRestored(t *testing.T) {
	m, s, first := newManaged(t, tls.VersionTLS13, 0)
	second, err := tls.Dial("tcp", s.Addr().String(), &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	managedExchange(t, first, "first")
	managedExchange(t, second, "second-1")
	managedExchange(t, second, "second-2")
	// Only the second connection exceeds the replacement's smaller read buffer.
	// Wait until the incomplete frame is buffered so the first restore succeeds
	// before the second rejects the batch.
	partial := bytes.Repeat([]byte("x"), 2048)
	if _, err := second.Write(partial); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		batch := &connectionBatch{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := m.freeze(ctx, batch)
		cancel()
		buffered := false
		for _, c := range batch.connections {
			if c.state != nil && bytes.Equal(c.state.ReadBuffer, partial) {
				buffered = true
			}
		}
		batch.finish(false)
		if err != nil {
			t.Fatal(err)
		}
		if buffered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second connection did not buffer the incomplete frame")
		}
	}
	opts := managedOptions(tls.VersionTLS13, 0, false)
	opts.Env = append(opts.Env, "GRACEUPGRADE_REJECT_RESTORE=second")
	if _, err := replaceProcess(t, context.Background(), m, opts); err == nil {
		t.Fatal("batch unexpectedly committed")
	}
	for i, conn := range []net.Conn{first, second} {
		if pid, count, _ := managedExchange(t, conn, "rollback"); pid != os.Getpid() || count != i+2 {
			t.Fatalf("connection %d: %d %d", i, pid, count)
		}
	}
	child, err := replaceProcess(t, context.Background(), m, managedOptions(tls.VersionTLS13, 0, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.process.Kill(); child.Wait() })
	for i, conn := range []net.Conn{first, second} {
		if pid, count, _ := managedExchange(t, conn, "committed"); pid != child.PID || count != i+3 {
			t.Fatalf("connection %d: %d %d", i, pid, count)
		}
	}
}

type fragmentConn struct {
	net.Conn
	enabled atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (c *fragmentConn) Write(data []byte) (int, error) {
	if !c.enabled.CompareAndSwap(true, false) || len(data) < 2 {
		return c.Conn.Write(data)
	}
	n, err := c.Conn.Write(data[:len(data)/2])
	if err != nil {
		return n, err
	}
	close(c.started)
	<-c.release
	rest, err := c.Conn.Write(data[n:])
	return n + rest, err
}

func TestPartialEncryptedRecordSurvivesHandoff(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			m, s, initial := newManaged(t, version, 0)
			initial.Close()
			raw, err := net.Dial("tcp", s.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			transport := &fragmentConn{Conn: raw, started: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(transport.release) }) }
			defer release()
			client := tls.Client(transport, &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version})
			defer client.Close()
			client.SetDeadline(time.Now().Add(10 * time.Second))
			if err := client.Handshake(); err != nil {
				t.Fatal(err)
			}
			transport.enabled.Store(true)
			written := make(chan error, 1)
			go func() { _, err := client.Write([]byte("fragmented\n")); written <- err }()
			select {
			case <-transport.started:
			case <-time.After(2 * time.Second):
				t.Fatal("record was not fragmented")
			}
			time.Sleep(20 * time.Millisecond)
			batch := &connectionBatch{}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = m.freeze(ctx, batch)
			cancel()
			if err != nil {
				batch.finish(false)
				t.Fatal(err)
			}
			partial := false
			for _, f := range batch.connections {
				if f.state != nil && f.state.TLS != nil && len(f.state.TLS.RawInput) > 0 {
					partial = true
				}
			}
			batch.finish(false)
			if !partial {
				t.Fatal("test did not create an incomplete encrypted TLS record")
			}
			child, err := replaceProcess(t, context.Background(), m, managedOptions(version, 0, false))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { child.process.Kill(); child.Wait() })
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			release()
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			line, err := bufio.NewReader(client).ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(line) != fmt.Sprintf("%d 1 fragmented", child.PID) {
				t.Fatal(line)
			}
		})
	}
}

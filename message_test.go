//go:build linux || darwin

package graceupgrade

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type renamedCodec struct {
	Codec
	name string
}

func (c renamedCodec) Name() string { return c.name }

func testMessageCodec(t *testing.T, mode string) Codec {
	t.Helper()
	if mode == "length" {
		codec, err := LengthPrefixCodec(4)
		if err != nil {
			t.Fatal(err)
		}
		return codec
	}
	codec := LineCodec()
	if mode == "changed" {
		codec = renamedCodec{Codec: codec, name: "incompatible/v2"}
	}
	return codec
}

func messageOptions(t *testing.T, version uint16, mode string) ServerOptions {
	t.Helper()
	return ServerOptions{
		Codec: testMessageCodec(t, mode), TLSConfig: managedTLSConfig(t, version, 0),
		OnMessage: func(ctx context.Context, session *Session, input []byte) ([]byte, error) {
			if session.Sequence == 1 {
				session.Values["first"] = bytes.Clone(input)
			}
			return []byte(fmt.Sprintf("%d %d %s/%s", os.Getpid(), session.Sequence, session.Values["first"], input)), nil
		},
	}
}

func TestMessageChild(t *testing.T) {
	if os.Getenv("GRACEUPGRADE_MESSAGE_CHILD") == "" {
		return
	}
	version, _ := strconv.Atoi(os.Getenv("GRACEUPGRADE_TLS_VERSION"))
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	options := messageOptions(t, uint16(version), os.Getenv("GRACEUPGRADE_CODEC"))
	// A replacement must claim the inherited listener without creating one.
	options.Listen = func() (net.Listener, error) { return nil, errors.New("creation called in child") }
	s, err := m.NewServer("messages", "tcp", "127.0.0.1:0", options)
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(context.Background())
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {}
}

func messageProcessOptions(version uint16, mode string) replacementOptions {
	return replacementOptions{Args: []string{"-test.run=^TestMessageChild$"}, Env: []string{
		"GRACEUPGRADE_MESSAGE_CHILD=1", "GRACEUPGRADE_TLS_VERSION=" + strconv.Itoa(int(version)), "GRACEUPGRADE_CODEC=" + mode,
	}}
}

func newMessages(t *testing.T, version uint16, mode string) (*Manager, *Server, net.Conn) {
	t.Helper()
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	options := messageOptions(t, version, mode)
	var calls int
	options.Listen = func() (net.Listener, error) {
		calls++
		return net.Listen("tcp", "127.0.0.1:0")
	}
	s, err := m.NewServer("messages", "tcp", "127.0.0.1:0", options)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("listener created %d times", calls)
	}
	go s.Serve(context.Background())
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	var conn net.Conn
	if version == 0 {
		conn, err = net.Dial("tcp", s.Addr().String())
	} else {
		conn, err = tls.Dial("tcp", s.Addr().String(), &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	return m, s, conn
}

func readMessage(t *testing.T, reader *bufio.Reader, mode string) (int, int, string) {
	t.Helper()
	var data []byte
	var err error
	if mode == "length" {
		var header [4]byte
		if _, err = io.ReadFull(reader, header[:]); err != nil {
			t.Fatal(err)
		}
		size := binary.BigEndian.Uint32(header[:])
		if size > 256<<10 {
			t.Fatal("invalid response length", size)
		}
		data = make([]byte, int(size))
		_, err = io.ReadFull(reader, data)
	} else {
		data, err = reader.ReadBytes('\n')
		data = bytes.TrimSuffix(data, []byte{'\n'})
	}
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(data), " ", 3)
	if len(parts) != 3 {
		t.Fatal(string(data))
	}
	pid, _ := strconv.Atoi(parts[0])
	sequence, _ := strconv.Atoi(parts[1])
	return pid, sequence, parts[2]
}

func TestMessageConnectionsMoveWithoutBusinessSnapshots(t *testing.T) {
	for _, test := range []struct {
		name, mode string
		version    uint16
	}{
		{"tcp-lines", "line", 0}, {"tls12-lines", "line", tls.VersionTLS12},
		{"tls13-lines", "line", tls.VersionTLS13}, {"tcp-length", "length", 0}, {"tls13-length", "length", tls.VersionTLS13},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, s, conn := newMessages(t, test.version, test.mode)
			codec := testMessageCodec(t, test.mode)
			reader := bufio.NewReader(conn)
			first, _ := codec.Encode([]byte("before"))
			if _, err := conn.Write(first); err != nil {
				t.Fatal(err)
			}
			if pid, n, payload := readMessage(t, reader, test.mode); pid != os.Getpid() || n != 1 || payload != "before/before" {
				t.Fatalf("unexpected first response %d %d %q", pid, n, payload)
			}
			frame, _ := codec.Encode([]byte("partial-after"))
			cut := len(frame) / 2
			if _, err := conn.Write(frame[:cut]); err != nil {
				t.Fatal(err)
			}
			// Verify the half frame is actually in user-space framework buffers.
			deadline := time.Now().Add(3 * time.Second)
			for {
				batch := &connectionBatch{}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := m.freeze(ctx, batch)
				cancel()
				found := false
				for _, c := range batch.connections {
					if c.state != nil && bytes.Equal(c.state.ReadBuffer, frame[:cut]) {
						found = true
					}
				}
				batch.finish(false)
				if err != nil {
					t.Fatal(err)
				}
				if found {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("partial message was not buffered")
				}
			}
			child, err := replaceProcess(t, context.Background(), m, messageProcessOptions(test.version, test.mode))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { child.process.Kill(); child.Wait() })
			if s.Connections() != 0 {
				t.Fatal("parent retained migrated connections")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(frame[cut:]); err != nil {
				t.Fatal(err)
			}
			if pid, n, payload := readMessage(t, reader, test.mode); pid != child.PID || n != 2 || payload != "before/partial-after" {
				t.Fatalf("message/state lost: %d %d %q", pid, n, payload)
			}
			// Multiple messages in one write retain their order and sequence.
			a, _ := codec.Encode([]byte("three"))
			b, _ := codec.Encode([]byte("four"))
			if _, err := conn.Write(append(a, b...)); err != nil {
				t.Fatal(err)
			}
			for i, text := range []string{"three", "four"} {
				if pid, n, payload := readMessage(t, reader, test.mode); pid != child.PID || n != i+3 || payload != "before/"+text {
					t.Fatalf("pipelined response %d %d %q", pid, n, payload)
				}
			}
		})
	}
}

func TestMessageCodecChangeRollsBack(t *testing.T) {
	m, _, conn := newMessages(t, tls.VersionTLS13, "line")
	reader := bufio.NewReader(conn)
	fmt.Fprintln(conn, "before")
	readMessage(t, reader, "line")
	if _, err := io.WriteString(conn, "partial"); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"changed", "length"} {
		if _, err := replaceProcess(t, context.Background(), m, messageProcessOptions(tls.VersionTLS13, mode)); err == nil {
			t.Fatal("incompatible framing did not roll back:", mode)
		}
	}
	fmt.Fprintln(conn, "-after")
	if pid, n, text := readMessage(t, reader, "line"); pid != os.Getpid() || n != 2 || text != "before/partial-after" {
		t.Fatalf("rollback lost connection or state: %d %d %q", pid, n, text)
	}
	child, err := replaceProcess(t, context.Background(), m, messageProcessOptions(tls.VersionTLS13, "line"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.process.Kill(); child.Wait() })
	fmt.Fprintln(conn, "retry")
	if pid, n, text := readMessage(t, reader, "line"); pid != child.PID || n != 3 || text != "before/retry" {
		t.Fatalf("retry lost state: %d %d %q", pid, n, text)
	}
}

func TestMessageFreezeWaitsForCallbackAndDoesNotReplay(t *testing.T) {
	m, err := New(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	s, err := m.NewServer("busy", "tcp", "127.0.0.1:0", ServerOptions{
		Codec: LineCodec(), OnMessage: func(ctx context.Context, state *Session, input []byte) ([]byte, error) {
			calls.Add(1)
			close(started)
			<-release
			return []byte("done"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(context.Background())
	if err := m.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintln(client, "work")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not start")
	}
	batch := &connectionBatch{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = m.freeze(ctx, batch)
	cancel()
	batch.finish(false)
	unblock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("freeze ignored in-flight callback: %v", err)
	}
	line, err := bufio.NewReader(client).ReadString('\n')
	if err != nil || line != "done\n" || calls.Load() != 1 {
		t.Fatalf("callback replayed/lost: %q %v %d", line, err, calls.Load())
	}
}

func TestCodecsHandleFragmentsAndEmptyMessages(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8} {
		codec, err := LengthPrefixCodec(width)
		if err != nil {
			t.Fatal(err)
		}
		for _, payload := range [][]byte{[]byte("hello"), {}} {
			frame, err := codec.Encode(payload)
			if err != nil {
				t.Fatal(err)
			}
			for cut := 0; cut < len(frame); cut++ {
				if n, data, err := codec.Decode(frame[:cut]); err != nil || n != 0 || data != nil {
					t.Fatalf("partial frame consumed: %d %v %v", n, data, err)
				}
			}
			if n, data, err := codec.Decode(append(frame, 'x')); err != nil || n != len(frame) || !bytes.Equal(data, payload) {
				t.Fatalf("frame decode: %d %q %v", n, data, err)
			}
		}
	}
	if _, err := LengthPrefixCodec(3); err == nil {
		t.Fatal("invalid width accepted")
	}
	codec, _ := LengthPrefixCodec(8)
	if _, _, err := codec.Decode(bytes.Repeat([]byte{0xff}, 8)); err == nil {
		t.Fatal("overflowing length accepted")
	}
	codec, _ = LengthPrefixCodec(1)
	if _, err := codec.Encode(make([]byte, 256)); err == nil {
		t.Fatal("overflowing response accepted")
	}
	if _, err := LineCodec().Encode([]byte("two\nlines")); err == nil {
		t.Fatal("ambiguous line response accepted")
	}
}

func TestServerRequiresCodecAndOnMessageBeforeListening(t *testing.T) {
	callback := func(context.Context, *Session, []byte) ([]byte, error) { return nil, nil }
	for _, test := range []struct {
		name    string
		options ServerOptions
	}{
		{"missing-both", ServerOptions{}},
		{"missing-callback", ServerOptions{Codec: LineCodec()}},
		{"missing-codec", ServerOptions{OnMessage: callback}},
		{"empty-name", ServerOptions{Codec: renamedCodec{Codec: LineCodec()}, OnMessage: callback}},
		{"long-name", ServerOptions{Codec: renamedCodec{Codec: LineCodec(), name: strings.Repeat("x", 257)}, OnMessage: callback}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, err := New(testOptions(t))
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			called := false
			options := test.options
			options.Listen = func() (net.Listener, error) {
				called = true
				return nil, errors.New("unexpected listener creation")
			}
			if _, err := m.NewServer("invalid", "tcp", "127.0.0.1:0", options); err == nil || called || len(m.listeners) != 0 {
				t.Fatalf("invalid options registered a listener: err=%v called=%v", err, called)
			}
			// A validation failure must leave the same name available for retry.
			if _, err := m.NewServer("invalid", "tcp", "127.0.0.1:0", ServerOptions{Codec: LineCodec(), OnMessage: callback}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestListenWithRejectsNilWithoutRegistering(t *testing.T) {
	for _, listener := range []net.Listener{nil, (*net.TCPListener)(nil)} {
		m, err := New(testOptions(t))
		if err != nil {
			t.Fatal(err)
		}
		_, err = m.ListenWith("nil", "tcp", "127.0.0.1:0", func() (net.Listener, error) { return listener, nil })
		if err == nil || len(m.listeners) != 0 {
			m.Close()
			t.Fatalf("nil creation registered: %v", err)
		}
		m.Close()
	}
}

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

type renamedDecoder struct {
	Decoder
	name string
}

func (d renamedDecoder) Name() string { return d.name }

type renamedEncoder struct {
	Encoder
	name string
}

func (e renamedEncoder) Name() string { return e.name }

const testMessageDelimiter = "\r\n<END>"

func testMessageFraming(t *testing.T, mode string) (Decoder, Encoder) {
	t.Helper()
	if mode == "delimiter" {
		return testDelimiterDecoder(t, testMessageDelimiter), testDelimiterEncoder(t, testMessageDelimiter)
	}
	if mode == "length" {
		decoder, err := LengthPrefixDecoder(4)
		if err != nil {
			t.Fatal(err)
		}
		encoder, err := LengthPrefixEncoder(4)
		if err != nil {
			t.Fatal(err)
		}
		return decoder, encoder
	}
	decoder := testDelimiterDecoder(t, "\n")
	if mode == "changed" {
		decoder = renamedDecoder{Decoder: decoder, name: "incompatible/v2"}
	}
	return decoder, testDelimiterEncoder(t, "\n")
}

func messageOptions(t *testing.T, version uint16, mode string) ServerOptions {
	t.Helper()
	decoder, encoder := testMessageFraming(t, mode)
	options := ServerOptions{
		Decoder: decoder, Encoder: encoder, TLSConfig: managedTLSConfig(t, version, 0),
		OnMessage: func(ctx context.Context, session *Session, input []byte) ([]byte, error) {
			if session.Sequence == 1 {
				session.Values["first"] = bytes.Clone(input)
			}
			return []byte(fmt.Sprintf("%d %d %s/%s", os.Getpid(), session.Sequence, session.Values["first"], input)), nil
		},
	}
	switch mode {
	case "hash-line", "hash-raw", "hash-default", "hash-crlf", "other-line":
		options.Decoder = testDelimiterDecoder(t, "#")
		options.Encoder = nil
		switch mode {
		case "hash-line", "other-line":
			options.Encoder = testDelimiterEncoder(t, "\n")
		case "hash-crlf":
			options.Encoder = testDelimiterEncoder(t, "\r\n")
		case "hash-raw":
			options.Encoder = RawEncoder()
		}
		if mode == "other-line" {
			options.Decoder = testDelimiterDecoder(t, "!")
		}
	}
	return options
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
	options := messageOptions(t, uint16(version), os.Getenv("GRACEUPGRADE_FRAMING"))
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
		"GRACEUPGRADE_MESSAGE_CHILD=1", "GRACEUPGRADE_TLS_VERSION=" + strconv.Itoa(int(version)), "GRACEUPGRADE_FRAMING=" + mode,
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
		delimiter := "\n"
		if mode == "delimiter" {
			delimiter = testMessageDelimiter
		}
		data, err = reader.ReadBytes(delimiter[len(delimiter)-1])
		data = bytes.TrimSuffix(data, []byte(delimiter))
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

func assertMessageReply(t *testing.T, reader *bufio.Reader, mode string, pid, sequence int, payload string) {
	t.Helper()
	if strings.HasPrefix(mode, "hash-") {
		want := fmt.Sprintf("%d %d %s", pid, sequence, payload)
		if mode == "hash-line" {
			want += "\n"
		}
		data := make([]byte, len(want))
		if _, err := io.ReadFull(reader, data); err != nil {
			t.Fatal(err)
		}
		if string(data) != want {
			t.Fatalf("response = %q, want %q", data, want)
		}
		return
	}
	if gotPID, gotSequence, gotPayload := readMessage(t, reader, mode); gotPID != pid || gotSequence != sequence || gotPayload != payload {
		t.Fatalf("response = %d %d %q, want %d %d %q", gotPID, gotSequence, gotPayload, pid, sequence, payload)
	}
}

func TestMessageConnectionsMoveWithoutBusinessSnapshots(t *testing.T) {
	for _, test := range []struct {
		name, mode string
		version    uint16
	}{
		{"tcp-lines", "line", 0}, {"tls12-lines", "line", tls.VersionTLS12},
		{"tls13-lines", "line", tls.VersionTLS13}, {"tcp-length", "length", 0}, {"tls13-length", "length", tls.VersionTLS13},
		{"tcp-delimiter", "delimiter", 0}, {"tls12-delimiter", "delimiter", tls.VersionTLS12},
		{"tls13-delimiter", "delimiter", tls.VersionTLS13},
		{"tcp-hash-line", "hash-line", 0}, {"tls12-hash-line", "hash-line", tls.VersionTLS12},
		{"tls13-hash-line", "hash-line", tls.VersionTLS13}, {"tcp-hash-default", "hash-default", 0},
		{"tcp-hash-raw", "hash-raw", 0}, {"tls12-hash-raw", "hash-raw", tls.VersionTLS12},
		{"tls13-hash-raw", "hash-raw", tls.VersionTLS13},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, s, conn := newMessages(t, test.version, test.mode)
			_, requestEncoder := testMessageFraming(t, test.mode)
			if strings.HasPrefix(test.mode, "hash-") {
				requestEncoder = testDelimiterEncoder(t, "#")
			}
			reader := bufio.NewReader(conn)
			first, _ := requestEncoder.Encode([]byte("before"))
			if _, err := conn.Write(first); err != nil {
				t.Fatal(err)
			}
			assertMessageReply(t, reader, test.mode, os.Getpid(), 1, "before/before")
			frame, _ := requestEncoder.Encode([]byte("partial-after"))
			cut := len(frame) / 2
			if test.mode == "delimiter" {
				cut = len(frame) - 1 // Migrate a partially received delimiter too.
			}
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
			assertMessageReply(t, reader, test.mode, child.PID, 2, "before/partial-after")
			// Multiple messages in one write retain their order and sequence.
			a, _ := requestEncoder.Encode([]byte("three"))
			b, _ := requestEncoder.Encode([]byte("four"))
			if _, err := conn.Write(append(a, b...)); err != nil {
				t.Fatal(err)
			}
			for i, text := range []string{"three", "four"} {
				assertMessageReply(t, reader, test.mode, child.PID, i+3, "before/"+text)
			}
		})
	}
}

func TestMessageFramingChangeRollsBack(t *testing.T) {
	for _, test := range []struct {
		name, mode, retry, delimiter string
		rejected                     []string
	}{
		{"symmetric-framing", "line", "line", "\n", []string{"changed", "length", "delimiter"}},
		{"independent-directions", "hash-line", "hash-line", "#", []string{"other-line", "hash-crlf", "hash-raw"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, _, conn := newMessages(t, tls.VersionTLS13, test.mode)
			reader := bufio.NewReader(conn)
			if _, err := io.WriteString(conn, "before"+test.delimiter); err != nil {
				t.Fatal(err)
			}
			assertMessageReply(t, reader, test.mode, os.Getpid(), 1, "before/before")
			if _, err := io.WriteString(conn, "partial"); err != nil {
				t.Fatal(err)
			}
			for _, mode := range test.rejected {
				if _, err := replaceProcess(t, context.Background(), m, messageProcessOptions(tls.VersionTLS13, mode)); err == nil {
					t.Fatal("incompatible framing did not roll back:", mode)
				}
			}
			if _, err := io.WriteString(conn, "-after"+test.delimiter); err != nil {
				t.Fatal(err)
			}
			assertMessageReply(t, reader, test.mode, os.Getpid(), 2, "before/partial-after")
			child, err := replaceProcess(t, context.Background(), m, messageProcessOptions(tls.VersionTLS13, test.retry))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { child.process.Kill(); child.Wait() })
			if _, err := io.WriteString(conn, "retry"+test.delimiter); err != nil {
				t.Fatal(err)
			}
			assertMessageReply(t, reader, test.mode, child.PID, 3, "before/retry")
		})
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
		Decoder: testDelimiterDecoder(t, "\n"), Encoder: testDelimiterEncoder(t, "\n"),
		OnMessage: func(ctx context.Context, state *Session, input []byte) ([]byte, error) {
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

func TestLengthPrefixFramingHandlesFragmentsAndEmptyMessages(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8} {
		decoder, err := LengthPrefixDecoder(width)
		if err != nil {
			t.Fatal(err)
		}
		encoder, err := LengthPrefixEncoder(width)
		if err != nil {
			t.Fatal(err)
		}
		for _, payload := range [][]byte{[]byte("hello"), {}} {
			frame, err := encoder.Encode(payload)
			if err != nil {
				t.Fatal(err)
			}
			for cut := 0; cut < len(frame); cut++ {
				if n, data, err := decoder.Decode(frame[:cut]); err != nil || n != 0 || data != nil {
					t.Fatalf("partial frame consumed: %d %v %v", n, data, err)
				}
			}
			if n, data, err := decoder.Decode(append(frame, 'x')); err != nil || n != len(frame) || !bytes.Equal(data, payload) {
				t.Fatalf("frame decode: %d %q %v", n, data, err)
			}
		}
	}
	for _, width := range []int{-1, 0, 3, 5, 9} {
		if decoder, err := LengthPrefixDecoder(width); err == nil || decoder != nil {
			t.Fatalf("invalid decoder width accepted: %d", width)
		}
		if encoder, err := LengthPrefixEncoder(width); err == nil || encoder != nil {
			t.Fatalf("invalid encoder width accepted: %d", width)
		}
	}
	decoder, _ := LengthPrefixDecoder(8)
	if _, _, err := decoder.Decode(bytes.Repeat([]byte{0xff}, 8)); err == nil {
		t.Fatal("overflowing length accepted")
	}
	encoder, _ := LengthPrefixEncoder(1)
	if _, err := encoder.Encode(make([]byte, 256)); err == nil {
		t.Fatal("overflowing response accepted")
	}
}

func TestServerRequiresMessageFramingAndCallbackBeforeListening(t *testing.T) {
	callback := func(context.Context, *Session, []byte) ([]byte, error) { return nil, nil }
	for _, test := range []struct {
		name    string
		options ServerOptions
	}{
		{"missing-both", ServerOptions{}},
		{"missing-callback", ServerOptions{Decoder: testDelimiterDecoder(t, "\n"), Encoder: testDelimiterEncoder(t, "\n")}},
		{"missing-framing", ServerOptions{OnMessage: callback}},
		{"missing-decoder", ServerOptions{Encoder: RawEncoder(), OnMessage: callback}},
		{"missing-split-callback", ServerOptions{Decoder: testDelimiterDecoder(t, "#")}},
		{"empty-decoder-name", ServerOptions{Decoder: renamedDecoder{Decoder: testDelimiterDecoder(t, "#")}, OnMessage: callback}},
		{"long-decoder-name", ServerOptions{Decoder: renamedDecoder{Decoder: testDelimiterDecoder(t, "#"), name: strings.Repeat("x", 257)}, OnMessage: callback}},
		{"empty-encoder-name", ServerOptions{Decoder: testDelimiterDecoder(t, "#"), Encoder: renamedEncoder{Encoder: testDelimiterEncoder(t, "\n")}, OnMessage: callback}},
		{"long-encoder-name", ServerOptions{Decoder: testDelimiterDecoder(t, "#"), Encoder: renamedEncoder{Encoder: testDelimiterEncoder(t, "\n"), name: strings.Repeat("x", 257)}, OnMessage: callback}},
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
			if _, err := m.NewServer("invalid", "tcp", "127.0.0.1:0", ServerOptions{Decoder: testDelimiterDecoder(t, "\n"), OnMessage: callback}); err != nil {
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

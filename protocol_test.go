package graceupgrade

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(data []byte) (int, error) {
	if len(data) > 2 {
		data = data[:2]
	}
	return w.Buffer.Write(data)
}

func TestFramingHandlesShortWritesAndInvalidInput(t *testing.T) {
	var w shortWriter
	if err := writeFrame(&w, message{Kind: "ready"}); err != nil {
		t.Fatal(err)
	}
	var value message
	if err := readFrame(&w, &value); err != nil || value.Kind != "ready" {
		t.Fatalf("%+v %v", value, err)
	}
	for _, size := range []uint32{0, maxFrameSize + 1} {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], size)
		if err := readFrame(bytes.NewReader(header[:]), &value); err == nil {
			t.Fatal("invalid length accepted")
		}
	}
	if err := readFrame(bytes.NewReader([]byte{0, 0, 0, 3, '{'}), &value); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

func TestDescriptorTransfer(t *testing.T) {
	if !testPlatformSupported {
		t.Skip("Unix required")
	}
	parent, childFile, err := controlPair()
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	defer childFile.Close()
	child, err := controlFromFile(childFile)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	file, err := listener.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := sendFile(parent, file); err != nil {
		t.Fatal(err)
	}
	received, err := receiveFile(child)
	if err != nil {
		t.Fatal(err)
	}
	defer received.Close()
	restored, err := net.FileListener(received)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.Addr().String() != listener.Addr().String() {
		t.Fatal("bound address changed")
	}
}

func TestManifestRejectsUnrecognizedVersionsAndDuplicateNames(t *testing.T) {
	if !testPlatformSupported {
		t.Skip("Unix required")
	}
	info := listenerInfo{Name: "same", Network: "tcp", Address: "127.0.0.1:0"}
	for _, meta := range []manifest{
		{Version: 999, Listeners: []listenerInfo{info}},
		{Version: 2, UpgradeSocket: "/tmp/invalid.sock", Listeners: []listenerInfo{info}},
		{Version: protocolVersion, Listeners: []listenerInfo{info, info}},
	} {
		parent, childFile, err := controlPair()
		if err != nil {
			t.Fatal(err)
		}
		child, err := controlFromFile(childFile)
		childFile.Close()
		if err != nil {
			parent.Close()
			t.Fatal(err)
		}
		if err := writeFrame(parent, meta); err != nil {
			t.Fatal(err)
		}
		m := &Manager{control: child}
		if err := m.receive(); err == nil {
			t.Fatal("invalid manifest accepted")
		}
		parent.Close()
		child.Close()
	}
}

// A private Unix connection pair is only a wire-test fixture. Production always
// connects through the configured discovery endpoint.
func controlPair() (*net.UnixConn, *os.File, error) {
	dir, err := os.MkdirTemp("/tmp", "gu-wire-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(dir)
	addr := &net.UnixAddr{Name: filepath.Join(dir, "u.sock"), Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, nil, err
	}
	defer listener.Close()
	child, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		return nil, nil, err
	}
	defer child.Close()
	parent, err := listener.AcceptUnix()
	if err != nil {
		return nil, nil, err
	}
	file, err := child.File()
	if err != nil {
		parent.Close()
		return nil, nil, err
	}
	return parent, file, nil
}
func controlFromFile(file *os.File) (*net.UnixConn, error) {
	conn, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, errors.New("wire test expected Unix connection")
	}
	return unix, nil
}

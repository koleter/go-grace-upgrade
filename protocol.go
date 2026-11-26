package graceupgrade

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
)

const (
	protocolVersion = 3
	maxListeners    = 128
	maxStateSize    = 1 << 20
	maxFrameSize    = 4 << 20
)

type listenerInfo struct {
	Name    string `json:"name"`
	Network string `json:"network"`
	Address string `json:"address"`
}

func (i listenerInfo) validate() error {
	if i.Name == "" || len(i.Name) > 256 || len(i.Address) > 4096 {
		return errors.New("graceupgrade: invalid listener name or address")
	}
	switch i.Network {
	case "tcp", "tcp4", "tcp6":
		return nil
	default:
		return fmt.Errorf("graceupgrade: unsupported network %q (TCP only)", i.Network)
	}
}

type manifest struct {
	Version   int            `json:"version"`
	Listeners []listenerInfo `json:"listeners"`
	// State 是旧进程在交接时由 Options.Snapshot 导出的全局快照字节，随 manifest
	// 与监听器 FD 一起传给替换进程。替换进程接收后存入 Manager.state，业务代码
	// 通过 Manager.State() 取回并恢复；冷启动或旧进程未提供 Snapshot 时为空。
	State         []byte `json:"state,omitempty"`
	UpgradeSocket string `json:"upgrade_socket,omitempty"`
}

type message struct {
	Kind  string `json:"kind"`
	Count int    `json:"count,omitempty"`
	Error string `json:"error,omitempty"`
}

func (m *Manager) send(conn *net.UnixConn, state []byte) error {
	names := make([]string, 0, len(m.listeners))
	for name := range m.listeners {
		names = append(names, name)
	}
	sort.Strings(names)
	meta := manifest{Version: protocolVersion, State: state}
	files := make([]*os.File, 0, len(names))
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for _, name := range names {
		r := m.listeners[name]
		f, err := listenerFile(r.raw)
		if err != nil {
			return fmt.Errorf("listener %q: %w", name, err)
		}
		files = append(files, f)
		meta.Listeners = append(meta.Listeners, r.info)
	}
	if m.endpoint != nil {
		f, err := unixListenerFile(m.endpoint.listener)
		if err != nil {
			return err
		}
		files = append(files, f)
		lock, err := duplicateFile(m.endpoint.lease)
		if err != nil {
			return err
		}
		files = append(files, lock)
		meta.UpgradeSocket = m.endpoint.path
	}
	if err := writeFrame(conn, meta); err != nil {
		return err
	}
	for _, f := range files {
		if err := sendFile(conn, f); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) receive() error {
	var meta manifest
	if err := readFrame(m.control, &meta); err != nil {
		return err
	}
	if meta.Version != protocolVersion {
		return fmt.Errorf("unsupported protocol version %d", meta.Version)
	}
	if len(meta.Listeners) == 0 || len(meta.Listeners) > maxListeners || len(meta.State) > maxStateSize {
		return errors.New("invalid manifest size")
	}
	seen := make(map[string]bool)
	for _, info := range meta.Listeners {
		if err := info.validate(); err != nil {
			return err
		}
		if seen[info.Name] {
			return fmt.Errorf("duplicate listener %q", info.Name)
		}
		seen[info.Name] = true
	}
	for _, info := range meta.Listeners {
		f, err := receiveFile(m.control)
		if err != nil {
			return err
		}
		raw, err := net.FileListener(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("restore listener %q: %w", info.Name, err)
		}
		tcp, ok := raw.(*net.TCPListener)
		if !ok {
			raw.Close()
			return fmt.Errorf("listener %q is not TCP", info.Name)
		}
		m.add(info, tcp)
	}
	if meta.UpgradeSocket == "" {
		return errors.New("graceupgrade: missing upgrade socket in manifest")
	}
	if err := m.receiveEndpoint(meta.UpgradeSocket); err != nil {
		return err
	}
	m.state = meta.State
	return nil
}

// Frames use exact reads: a buffered decoder could accidentally consume the
// byte associated with the following SCM_RIGHTS control message.
func writeFrame(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > maxFrameSize {
		return errors.New("frame too large")
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(data)))
	if err := writeAll(w, size[:]); err != nil {
		return err
	}
	return writeAll(w, data)
}

func readFrame(r io.Reader, value any) error {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n == 0 || n > maxFrameSize {
		return errors.New("invalid frame size")
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

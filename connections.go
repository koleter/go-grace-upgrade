package graceupgrade

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	mtls "github.com/koleter/go-grace-upgrade/mtls"
)

const maxConnections = 65536

type connectionSnapshot struct {
	Server                              string
	HandlerMode                         string
	HandlerState                        []byte
	ReadBuffer, WriteBuffer, WireBuffer []byte
	TLS                                 *mtls.Snapshot
	PeerEOF                             bool
}

func (c *session) snapshot() (*connectionSnapshot, error) {
	state, err := c.handler.Snapshot()
	if err != nil {
		return nil, err
	}
	if len(state) > maxStateSize {
		return nil, errors.New("connection handler state exceeds 1 MiB")
	}
	s := &connectionSnapshot{Server: c.server.name, HandlerMode: "messages", HandlerState: state,
		ReadBuffer: c.readBuffer, WriteBuffer: c.writeBuffer, WireBuffer: c.transport.pending, PeerEOF: c.peerEOF}
	if c.tls != nil {
		s.TLS, err = c.tls.Snapshot()
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

type frozenConnection struct {
	session *session
	state   *connectionSnapshot
}
type connectionBatch struct {
	servers     []*Server
	connections []frozenConnection
}

// freeze pauses each server, waits for its current work, then snapshots its
// connections directly. No old FD or handler state is discarded before commit.
func (m *Manager) freeze(ctx context.Context, batch *connectionBatch) error {
	names := make([]string, 0, len(m.servers))
	for name := range m.servers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s := m.servers[name]
		// Rollback also covers a pause interrupted by cancellation or Close.
		batch.servers = append(batch.servers, s)
		if err := s.pauseConnections(ctx); err != nil {
			return err
		}
		conns := s.sessionSnapshot()
		sort.Slice(conns, func(i, j int) bool { return conns[i].id < conns[j].id })
		if len(conns)+len(batch.connections) > maxConnections {
			return errors.New("too many managed connections")
		}
		for _, c := range conns {
			if err := ctx.Err(); err != nil {
				return err
			}
			state, err := c.snapshot()
			if err != nil {
				return fmt.Errorf("snapshot server %q: %w", name, err)
			}
			batch.connections = append(batch.connections, frozenConnection{session: c, state: state})
		}
	}
	return nil
}

func (s *Server) pauseConnections(ctx context.Context) error {
	stop := wakeOnCancel(ctx, s.changed)
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return fmt.Errorf("managed server %q closed", s.name)
	}
	// Existing reads and writes finish concurrently; callbacks return normally.
	// The flag prevents any loop from starting its next step.
	s.paused = true
	if err := s.listener.SetDeadline(time.Now()); err != nil {
		return fmt.Errorf("pause accepting on server %q: %w", s.name, err)
	}
	for (s.accepting || s.working > 0) && ctx.Err() == nil && s.ctx.Err() == nil {
		s.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ctx.Err() != nil {
		return fmt.Errorf("managed server %q stopped during freeze", s.name)
	}
	return nil
}

func (s *Server) resumeConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Clear the Accept deadline before waking loops, even if Serve has not
	// started yet. A concurrent Close may already have closed the listener.
	s.listener.SetDeadline(time.Time{})
	s.paused = false
	s.changed.Broadcast()
}

func (b *connectionBatch) finish(committed bool) {
	for _, s := range b.servers {
		if committed {
			s.Close()
		} else {
			s.resumeConnections()
		}
	}
	if committed {
		for _, s := range b.servers {
			s.waitConnectionsClosed(context.Background())
		}
	}
}

func (b *connectionBatch) send(conn *net.UnixConn) error {
	if err := writeFrame(conn, message{Kind: "connections", Count: len(b.connections)}); err != nil {
		return err
	}
	for _, f := range b.connections {
		file, err := connectionFile(f.session.raw)
		if err != nil {
			return err
		}
		err = sendFile(conn, file)
		file.Close()
		if err != nil {
			return err
		}
		if err := writeFrame(conn, f.state); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) receiveConnections() error {
	var header message
	if err := readFrame(m.control, &header); err != nil {
		return err
	}
	if header.Kind != "connections" || header.Count < 0 || header.Count > maxConnections {
		return errors.New("invalid connection batch")
	}
	for i := 0; i < header.Count; i++ {
		file, err := receiveFile(m.control)
		if err != nil {
			return err
		}
		raw, err := net.FileConn(file)
		file.Close()
		if err != nil {
			return err
		}
		var state connectionSnapshot
		if err := readFrame(m.control, &state); err != nil {
			raw.Close()
			return err
		}
		if err := m.restoreConnection(raw, &state); err != nil {
			raw.Close()
			return err
		}
	}
	return writeFrame(m.control, message{Kind: "prepared"})
}

func (m *Manager) restoreConnection(raw net.Conn, state *connectionSnapshot) error {
	if _, ok := raw.(*net.TCPConn); !ok {
		return errors.New("inherited connection is not TCP")
	}
	s := m.servers[state.Server]
	if s == nil {
		return fmt.Errorf("no managed server registered for %q", state.Server)
	}
	// Retain the wire field to accept existing message snapshots and reject
	// legacy custom-handler snapshots, which cannot restore a Session.
	if state.HandlerMode != "messages" {
		return errors.New("unsupported handler mode in connection snapshot")
	}
	if len(state.ReadBuffer) > s.options.MaxBuffer || len(state.WriteBuffer) > s.options.MaxBuffer || len(state.WireBuffer) > s.options.MaxBuffer || len(state.HandlerState) > maxStateSize {
		return errors.New("inherited connection exceeds buffer limit")
	}
	if (state.TLS != nil) != (s.options.TLSConfig != nil) {
		return errors.New("TLS mode changed across upgrade")
	}
	c := s.newSession(raw, state)
	var err error
	c.handler, err = s.newMessageHandler(state.HandlerState)
	if err != nil {
		return fmt.Errorf("restore protocol: %w", err)
	}
	if state.TLS != nil {
		c.tls, err = mtls.Restore(c.transport, s.options.TLSConfig, state.TLS)
		if err != nil {
			return err
		}
	}
	if !s.addSession(c) {
		return fmt.Errorf("managed server %q closed", s.name)
	}
	go c.run()
	return nil
}

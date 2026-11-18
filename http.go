package graceupgrade

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPOptions configures a managed net/http server.
type HTTPOptions struct {
	// DrainTimeout bounds Shutdown after a successful handoff or Manager.Close.
	// It defaults to 30 seconds. On timeout the framework closes remaining HTTP
	// connections and Wait returns the shutdown error.
	DrainTimeout time.Duration
}

type managedHTTPServer struct {
	name         string
	server       *http.Server
	listener     net.Listener
	drainTimeout time.Duration
	// Lifecycle and errors are protected by Manager.mu.
	started  bool
	served   bool
	drained  bool
	serveErr error
	err      error
}

// ServeHTTP registers and starts an unused HTTP server on a created or inherited
// named TCP listener. Call before Ready in every generation. The framework owns
// Serve, Shutdown and Close; do not call those methods on server yourself.
// Upgrade failures leave the old HTTP server running. After a successful handoff
// or Manager.Close, the framework drains requests automatically. Call Wait before
// main returns; neither Done nor http.Server.Serve returning means draining ended.
// Hijacked connections (such as WebSockets) need application-managed shutdown.
func (m *Manager) ServeHTTP(name, network, address string, server *http.Server, options HTTPOptions) error {
	if server == nil {
		return errors.New("graceupgrade: HTTP server is required")
	}
	if options.DrainTimeout < 0 {
		return errors.New("graceupgrade: HTTP DrainTimeout must not be negative")
	}
	if options.DrainTimeout == 0 {
		options.DrainTimeout = 30 * time.Second
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.httpServers {
		if s.server == server {
			return errors.New("graceupgrade: HTTP server already registered")
		}
	}
	ln, err := m.listenWithLocked(name, network, address, nil)
	if err != nil {
		return err
	}
	s := &managedHTTPServer{
		name: name, server: server, listener: ln, drainTimeout: options.DrainTimeout,
	}
	if m.httpServers == nil {
		m.httpServers = make(map[string]*managedHTTPServer)
	}
	m.httpServers[name] = s
	go s.serve(m)
	return nil
}

// Wait waits for a successful outgoing handoff or Close and for all managed HTTP
// servers to finish draining. Unexpected HTTP serving failures stop the manager
// and are returned along with any drain errors. Wait is safe for concurrent and
// repeated calls. It does not handle operating-system signals or exit the process.
func (m *Manager) Wait() error {
	err := m.waitForStop()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.httpServers {
		for !s.drained {
			m.changed.Wait()
		}
		err = errors.Join(err, s.err)
	}
	return err
}

func (m *Manager) waitForStop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	for m.phase != phaseClosed && m.phase != phaseHandedOff {
		// Defer failure-driven shutdown until the ownership transaction ends.
		if m.phase != phaseUpgrading && m.httpFailedLocked() {
			err = m.closeLocked()
			break
		}
		m.changed.Wait()
	}
	return err
}

func (m *Manager) httpFailedLocked() bool {
	for _, s := range m.httpServers {
		if s.serveErr != nil {
			return true
		}
	}
	return false
}

func (s *managedHTTPServer) serve(m *Manager) {
	err := s.server.Serve(&httpStartListener{Listener: s.listener, start: func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		s.started = true
		m.changed.Broadcast()
	}})
	s.finishServing(m, err)
	// Request contexts must stay alive during a graceful drain. Use a fresh
	// context, independent of startup and the already-finished upgrade handshake.
	ctx, cancel := context.WithTimeout(context.Background(), s.drainTimeout)
	defer cancel()
	err = ignoreClosed(s.server.Shutdown(ctx))
	var drainErr error
	if err != nil {
		drainErr = fmt.Errorf("graceupgrade: drain HTTP server %q: %w", s.name, err)
		drainErr = errors.Join(drainErr, ignoreClosed(s.server.Close()))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s.err = errors.Join(s.serveErr, drainErr)
	s.drained = true
	m.changed.Broadcast()
}

func (s *managedHTTPServer) finishServing(m *Manager, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stopping := m.phase == phaseClosed || m.phase == phaseHandedOff
	if err != nil && !(stopping && (errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed))) {
		s.serveErr = fmt.Errorf("graceupgrade: HTTP server %q stopped: %w", s.name, err)
	}
	s.served = true
	m.changed.Broadcast()
}

func (m *Manager) waitHTTPStarted(ctx context.Context, s *managedHTTPServer) error {
	stop := wakeOnCancel(ctx, m.changed)
	defer stop()
	m.mu.Lock()
	defer m.mu.Unlock()
	for !s.started && !s.served && m.phase != phaseClosed && ctx.Err() == nil {
		m.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.phase == phaseClosed {
		return ErrClosed
	}
	if !s.served {
		// Serve reached Accept, which remains gated by Manager.Ready.
		return nil
	}
	if s.serveErr != nil {
		return s.serveErr
	}
	return fmt.Errorf("graceupgrade: HTTP server %q stopped before Ready", s.name)
}

type httpStartListener struct {
	net.Listener
	start func()
	once  sync.Once
}

func (l *httpStartListener) Accept() (net.Conn, error) {
	l.once.Do(l.start)
	return l.Listener.Accept()
}

package graceupgrade

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mtls "github.com/koleter/go-grace-upgrade/mtls"
)

const maxBufferSize = 256 << 10

type ServerOptions struct {
	// OnMessage handles complete messages using Codec. Both are required;
	// the framework snapshots and restores Session automatically.
	OnMessage MessageHandler
	Codec     Codec
	TLSConfig *mtls.Config
	// Listen optionally creates the raw TCP listener on cold start only. An
	// upgrading process claims the inherited listener without calling it.
	Listen func() (net.Listener, error)
	// MaxBuffer bounds each plaintext/ciphertext buffer, default 256 KiB.
	MaxBuffer        int
	HandshakeTimeout time.Duration
	OnError          func(error)
}

// Server owns accepting, TLS, protocol buffers and per-connection state. Unlike
// a bare Listen listener, its established connections move during handoff.
type Server struct {
	manager   *Manager
	name      string
	listener  *gatedListener
	options   ServerOptions
	mu        sync.Mutex
	changed   *sync.Cond // Uses mu; wakes paused loops and the upgrade coordinator.
	sessions  map[uint64]*session
	started   bool
	paused    bool
	accepting bool
	working   int // Connections initializing or executing one I/O/callback step.
	ctx       context.Context
	cancel    context.CancelFunc
}

var nextConnectionID atomic.Uint64

// NewServer registers a listener with Codec and OnMessage.
// Register and Serve it before Manager.Ready in every generation. Use this API
// on the first generation too; existing unmanaged net.Conn/crypto/tls.Conn
// instances cannot be retroactively captured.
func (m *Manager) NewServer(name, network, address string, options ServerOptions) (*Server, error) {
	if err := options.validateMessages(); err != nil {
		return nil, err
	}
	if options.MaxBuffer == 0 {
		options.MaxBuffer = maxBufferSize
	}
	if options.MaxBuffer < 1024 || options.MaxBuffer > maxBufferSize {
		return nil, errors.New("graceupgrade: MaxBuffer must be between 1 KiB and 256 KiB")
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = 5 * time.Second
	}
	if options.HandshakeTimeout < 0 {
		return nil, errors.New("graceupgrade: invalid handshake timeout")
	}
	if options.TLSConfig != nil {
		options.TLSConfig = options.TLSConfig.Clone()
	}
	ln, err := m.ListenWith(name, network, address, options.Listen)
	if err != nil {
		return nil, err
	}
	s := &Server{manager: m, name: name, listener: ln.(*gatedListener), options: options, sessions: make(map[uint64]*session)}
	s.changed = sync.NewCond(&s.mu)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutable(); err != nil {
		ln.Close()
		s.cancel()
		return nil, err
	}
	if m.phase != phaseInitializing {
		ln.Close()
		s.cancel()
		return nil, errors.New("graceupgrade: register server before Ready")
	}
	if m.servers == nil {
		m.servers = make(map[string]*Server)
	}
	m.servers[name] = s
	return s, nil
}

func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Connections counts live and staged connections currently owned by this server.
func (s *Server) Connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// Serve runs the accept loop until Close, context cancellation or successful
// handoff. Successful handoff returns nil, and does not send TLS close_notify.
func (s *Server) Serve(ctx context.Context) error {
	if err := s.startServing(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { s.Close() })
	defer stop()
	for {
		c, err := s.acceptSession()
		if err != nil {
			if timeout(err) {
				// Pausing interrupts Accept by expiring its deadline.
				continue
			}
			return ignoreClosed(err)
		}
		go c.run()
	}
}

func (s *Server) startServing() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("graceupgrade: Serve called twice")
	}
	s.started = true
	return nil
}

func (s *Server) acceptSession() (*session, error) {
	s.mu.Lock()
	for s.paused && s.ctx.Err() == nil {
		s.changed.Wait()
	}
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, net.ErrClosed
	}
	s.accepting = true
	s.mu.Unlock()

	raw, err := s.listener.Accept()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accepting = false
	defer s.changed.Broadcast()
	if err != nil {
		return nil, err
	}
	if s.ctx.Err() != nil {
		raw.Close()
		return nil, net.ErrClosed
	}
	// Register before acknowledging Accept's completion so freeze cannot miss
	// a socket accepted just as it requested the pause.
	c := s.newSession(raw, nil)
	s.addSessionLocked(c)
	return c, nil
}

func (s *Server) addSession(c *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return false
	}
	s.addSessionLocked(c)
	return true
}

func (s *Server) addSessionLocked(c *session) {
	s.sessions[c.id] = c
	// Reserve initialization before starting the goroutine. A pause must also
	// wait for a connection whose TLS handshake has not started yet.
	s.working++
}

func (s *Server) removeSession(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
	s.changed.Broadcast()
}

func (s *Server) beginWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.paused && s.ctx.Err() == nil {
		s.changed.Wait()
	}
	if s.ctx.Err() != nil {
		return false
	}
	s.working++
	return true
}

func (s *Server) finishWork(c *session, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// A failed step must disappear before freeze can observe an idle
		// server, especially when TLS initialization did not finish.
		c.raw.Close()
		delete(s.sessions, c.id)
	}
	s.working--
	if s.working == 0 {
		s.changed.Broadcast()
	}
}

func (s *Server) waitConnectionsClosed(ctx context.Context) error {
	stop := wakeOnCancel(ctx, s.changed)
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.sessions) > 0 && ctx.Err() == nil {
		s.changed.Wait()
	}
	return ctx.Err()
}

func (s *Server) sessionSnapshot() []*session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionsLocked()
}

func (s *Server) sessionsLocked() []*session {
	conns := make([]*session, 0, len(s.sessions))
	for _, c := range s.sessions {
		conns = append(conns, c)
	}
	return conns
}

// Close stops this server and its connections. Successful migration already
// detached the old copies; closing the old server cannot close the new owner's copies.
func (s *Server) Close() error {
	conns, stopped := s.stopSessions()
	if !stopped {
		return nil
	}
	err := s.listener.Close()
	for _, c := range conns {
		c.raw.Close()
	}
	return err
}

func (s *Server) stopSessions() ([]*session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, false
	}
	s.cancel()
	s.changed.Broadcast()
	return s.sessionsLocked(), true
}

func (s *Server) newSession(raw net.Conn, snapshot *connectionSnapshot) *session {
	c := &session{server: s, id: nextConnectionID.Add(1), raw: raw}
	c.transport = &recordTransport{Conn: raw, limit: s.options.MaxBuffer}
	if snapshot != nil {
		c.readBuffer, c.writeBuffer = snapshot.ReadBuffer, snapshot.WriteBuffer
		c.transport.pending = snapshot.WireBuffer
		c.peerEOF = snapshot.PeerEOF
	}
	return c
}

type session struct {
	server                  *Server
	id                      uint64
	raw                     net.Conn
	transport               *recordTransport
	tls                     *mtls.Conn
	handler                 *messageHandler
	readBuffer, writeBuffer []byte
	peerEOF                 bool
}

func (c *session) run() {
	defer func() {
		// Closing only the raw FD is essential when ownership moved: calling
		// TLS.Close would emit close_notify on the shared client connection.
		c.raw.Close()
		c.server.removeSession(c.id)
	}()
	ctx, cancel := context.WithCancel(c.server.ctx)
	defer cancel()
	err := c.initialize(ctx)
	c.server.finishWork(c, err)
	if err == nil {
		err = c.serve(ctx)
	}
	if err != io.EOF && ctx.Err() == nil {
		c.report(ignoreClosed(err))
	}
}

func (c *session) initialize(ctx context.Context) error {
	if err := c.server.manager.waitActive(ctx); err != nil {
		return err
	}
	if c.handler == nil {
		var err error
		c.handler, err = c.server.newMessageHandler(nil)
		if err != nil {
			return err
		}
		if c.server.options.TLSConfig != nil {
			c.raw.SetDeadline(time.Now().Add(c.server.options.HandshakeTimeout))
			c.transport.handshaking = true
			c.tls = mtls.Server(c.transport, c.server.options.TLSConfig)
			if err := c.tls.Handshake(); err != nil {
				return err
			}
			c.raw.SetDeadline(time.Time{})
			c.transport.handshaking = false
		}
	}
	return nil
}

func (c *session) serve(ctx context.Context) error {
	var input [16 << 10]byte
	for c.server.beginWork() {
		err := c.step(ctx, input[:])
		c.server.finishWork(c, err)
		if err != nil {
			return err
		}
	}
	return nil
}

// step advances connection I/O or one message callback. The server waits
// for current steps to finish before reading connection snapshots directly.
func (c *session) step(ctx context.Context, input []byte) error {
	if len(c.transport.pending) != 0 {
		if err := c.transport.flush(); err != nil {
			if timeout(err) {
				return nil
			}
			return err
		}
		if len(c.transport.pending) > 0 {
			return nil
		}
	}
	if len(c.writeBuffer) != 0 {
		// Encode at most one bounded chunk, then flush before encoding more.
		// Snapshots retain both pending plaintext and wire bytes.
		size := min(len(c.writeBuffer), 16<<10, c.server.options.MaxBuffer-512)
		var n int
		var err error
		if c.tls != nil {
			n, err = c.tls.Write(c.writeBuffer[:size])
		} else {
			n, err = c.transport.Write(c.writeBuffer[:size])
		}
		c.writeBuffer = c.writeBuffer[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		return nil
	}
	if len(c.readBuffer) != 0 {
		n, out, err := c.handler.Handle(ctx, c.readBuffer)
		if err != nil {
			return err
		}
		if n < 0 || n > len(c.readBuffer) || n == 0 && len(out) > 0 {
			return errors.New("invalid protocol progress")
		}
		if len(out) > c.server.options.MaxBuffer {
			return errors.New("write buffer exceeds limit")
		}
		if n > 0 {
			c.readBuffer = append([]byte(nil), c.readBuffer[n:]...)
			c.writeBuffer = out
			return nil
		}
	}
	if c.peerEOF {
		return io.EOF
	}
	space := c.server.options.MaxBuffer - len(c.readBuffer)
	if space == 0 {
		return errors.New("incomplete frame exceeds read buffer limit")
	}
	readInto := input[:min(len(input), space)]
	c.raw.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var n int
	var err error
	if c.tls != nil {
		n, err = c.tls.Read(readInto)
	} else {
		n, err = c.transport.Read(readInto)
	}
	if n > 0 {
		if len(c.readBuffer)+n > c.server.options.MaxBuffer {
			return errors.New("read buffer exceeds limit")
		}
		c.readBuffer = append(c.readBuffer, input[:n]...)
	}
	if n > 0 && err == io.EOF {
		c.peerEOF = true
		return nil
	}
	if timeout(err) {
		return nil
	}
	return err
}

func (c *session) report(err error) {
	if f := c.server.options.OnError; f != nil && err != nil {
		f(fmt.Errorf("graceupgrade: server %s connection %d: %w", c.server.name, c.id, err))
	}
}

// TLS record writes go to a bounded buffer. Network backpressure is handled
// below TLS, so a timeout never poisons TLS.Write or re-encrypts partially sent
// records. WireBuffer carries the unsent ciphertext and its exact byte offset.
type recordTransport struct {
	net.Conn
	pending     []byte
	limit       int
	handshaking bool
}

func (w *recordTransport) Write(p []byte) (int, error) {
	if len(w.pending)+len(p) > w.limit {
		return 0, errors.New("ciphertext write buffer exceeds limit")
	}
	w.pending = append(w.pending, p...)
	return len(p), nil
}

func (w *recordTransport) Read(p []byte) (int, error) {
	for len(w.pending) > 0 {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return w.Conn.Read(p)
}

func (w *recordTransport) flush() error {
	if !w.handshaking {
		w.Conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	}
	n, err := w.Conn.Write(w.pending)
	w.pending = w.pending[n:]
	if err == nil && n == 0 {
		return io.ErrNoProgress
	}
	return err
}

func timeout(err error) bool { var e net.Error; return errors.As(err, &e) && e.Timeout() }

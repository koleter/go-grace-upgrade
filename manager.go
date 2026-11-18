package graceupgrade

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrUnsupported = errors.New("graceupgrade: supported platforms are Linux and macOS")
	ErrBusy        = errors.New("graceupgrade: upgrade in progress")
	ErrClosed      = errors.New("graceupgrade: manager closed")
	ErrNotReady    = errors.New("graceupgrade: call Ready before upgrading")
	ErrHandedOff   = errors.New("graceupgrade: process already handed off to a replacement")
)

// Options configures a Manager. UpgradeSocket is required; ReadyTimeout defaults
// to 30 seconds.
type Options struct {
	// ReadyTimeout bounds starting the replacement and completing its handshake.
	ReadyTimeout time.Duration
	// UpgradeSocket enables startup-driven upgrades over this filesystem UDS.
	// Starting another instance with the same path attempts a handoff. This
	// path is required; the parent directory must already exist.
	UpgradeSocket string
	// Snapshot optionally exports process-wide state for startup-driven upgrades.
	Snapshot func(context.Context) ([]byte, error)
	// OnUpgradeError reports failed incoming upgrade attempts after rollback.
	OnUpgradeError func(error)
}

type listenerRecord struct {
	info    listenerInfo
	raw     *net.TCPListener
	guarded *gatedListener
	claimed bool
}

type managerPhase uint8

const (
	phaseInitializing managerPhase = iota
	phasePreparing
	phaseReady
	phaseUpgrading
	phaseHandedOff
	phaseClosed
)

// Manager owns the listening sockets for a single application process.
// A process must create only one Manager. Methods are safe for concurrent use.
type Manager struct {
	mu             sync.Mutex
	listeners      map[string]*listenerRecord
	servers        map[string]*Server
	httpServers    map[string]*managedHTTPServer
	changed        *sync.Cond // Uses mu; signals lifecycle changes and HTTP failures.
	control        *net.UnixConn
	state          []byte
	phase          managerPhase // Lifecycle, protected by mu; state holds application data.
	timeout        time.Duration
	upgradeSocket  string
	endpoint       *upgradeEndpoint
	snapshot       func(context.Context) ([]byte, error)
	onUpgradeError func(error)
	done           chan struct{}
}

// New initializes a cold instance or receives sockets from the current owner
// discovered through UpgradeSocket.
// Call it before creating other application resources. In a replacement process,
// failure must abort startup; do not fall back to binding fresh sockets.
func New(options Options) (*Manager, error) {
	if options.ReadyTimeout < 0 {
		return nil, errors.New("graceupgrade: ReadyTimeout must not be negative")
	}
	if options.ReadyTimeout == 0 {
		options.ReadyTimeout = 30 * time.Second
	}
	if options.UpgradeSocket == "" {
		return nil, errors.New("graceupgrade: UpgradeSocket is required")
	}
	path, err := filepath.Abs(options.UpgradeSocket)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		listeners: make(map[string]*listenerRecord),
		timeout:   options.ReadyTimeout, upgradeSocket: path,
		done: make(chan struct{}), snapshot: options.Snapshot, onUpgradeError: options.OnUpgradeError,
	}
	m.changed = sync.NewCond(&m.mu)
	if err := m.discoverUpgrade(); err != nil {
		m.Close()
		return nil, err
	}
	return m, nil
}

// Inherited reports whether this is a replacement process.
func (m *Manager) Inherited() bool { return m.control != nil }

// State returns a copy of the application snapshot provided by the old owner.
func (m *Manager) State() []byte { return append([]byte(nil), m.state...) }

// Done closes after a successful outgoing handoff or Close. On handoff, drain
// application-owned connections, clean up and return from main. With ServeHTTP,
// use Wait instead: Done does not wait for managed HTTP requests to drain.
// The framework never exits the application process itself.
func (m *Manager) Done() <-chan struct{} { return m.done }

// Listen creates or claims a named TCP listener. Names, networks, and requested
// addresses must match across generations, including an address using port 0.
// Supported networks are tcp, tcp4, and tcp6. Call before Ready.
// Accept blocks until Ready commits, so a failed replacement cannot take traffic.
func (m *Manager) Listen(name, network, address string) (net.Listener, error) {
	return m.ListenWith(name, network, address, nil)
}

// ListenWith is Listen with an optional application-supplied creation function.
// create is called only on cold start and must return a raw *net.TCPListener.
// The manager owns the returned listener. Do not bind before calling ListenWith:
// a replacement process must reuse the inherited socket. create must not call
// back into this Manager. A nil create uses net.Listen.
func (m *Manager) ListenWith(name, network, address string, create func() (net.Listener, error)) (net.Listener, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listenWithLocked(name, network, address, create)
}

func (m *Manager) listenWithLocked(name, network, address string, create func() (net.Listener, error)) (net.Listener, error) {
	if err := m.mutable(); err != nil {
		return nil, err
	}
	if m.phase != phaseInitializing {
		return nil, errors.New("graceupgrade: listeners must be registered before Ready")
	}
	info := listenerInfo{Name: name, Network: network, Address: address}
	if err := info.validate(); err != nil {
		return nil, err
	}
	if record, ok := m.listeners[name]; ok {
		if record.claimed {
			return nil, fmt.Errorf("graceupgrade: listener %q already claimed", name)
		}
		if record.info != info {
			return nil, fmt.Errorf("graceupgrade: listener %q network/address changed across upgrade", name)
		}
		record.claimed = true
		return record.guarded, nil
	}
	if len(m.listeners) >= maxListeners {
		return nil, fmt.Errorf("graceupgrade: at most %d listeners supported", maxListeners)
	}
	if create == nil {
		create = func() (net.Listener, error) { return net.Listen(network, address) }
	}
	raw, err := create()
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, errors.New("graceupgrade: listener creation returned nil")
	}
	tcp, ok := raw.(*net.TCPListener)
	if !ok {
		raw.Close()
		return nil, errors.New("graceupgrade: listener creation must return a raw TCP listener; configure TLS with ServerOptions.TLSConfig")
	}
	if tcp == nil {
		return nil, errors.New("graceupgrade: listener creation returned nil TCP listener")
	}
	record := m.add(info, tcp)
	record.claimed = true
	return record.guarded, nil
}

func (m *Manager) add(info listenerInfo, raw *net.TCPListener) *listenerRecord {
	r := &listenerRecord{info: info, raw: raw, guarded: &gatedListener{
		TCPListener: raw,
	}}
	r.guarded.changed = sync.NewCond(&r.guarded.mu)
	m.listeners[info.Name] = r
	return r
}

// Ready declares application initialization complete. Start serving goroutines
// before calling it; their Accept calls remain gated until the handshake commits.
// If Ready fails, abort this process. Ready is idempotent after success.
func (m *Manager) Ready(ctx context.Context) error {
	alreadyReady, err := m.beginReady()
	if err != nil || alreadyReady {
		return err
	}
	defer m.rollbackReady()
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, s := range m.httpServers {
		if err := m.waitHTTPStarted(ctx, s); err != nil {
			return err
		}
	}
	if m.control != nil {
		ctx, cancel := context.WithTimeout(ctx, m.timeout)
		defer cancel()
		stop := bindContext(ctx, m.control)
		defer stop()
		defer m.control.Close()
		if err := writeFrame(m.control, message{Kind: "ready"}); err != nil {
			return fmt.Errorf("graceupgrade: send readiness: %w", contextError(ctx, err))
		}
		if err := m.receiveConnections(); err != nil {
			return fmt.Errorf("graceupgrade: restore live connections: %w", contextError(ctx, err))
		}
		var ack message
		if err := readFrame(m.control, &ack); err != nil {
			return fmt.Errorf("graceupgrade: wait for commit: %w", contextError(ctx, err))
		}
		if ack.Kind != "commit" {
			return fmt.Errorf("graceupgrade: unexpected handshake %q", ack.Kind)
		}
	}
	return m.commitReady()
}

func (m *Manager) beginReady() (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutable(); err != nil {
		return false, err
	}
	if m.phase == phaseReady {
		return true, nil
	}
	if m.phase == phasePreparing {
		return false, ErrBusy
	}
	for name, r := range m.listeners {
		if !r.claimed {
			return false, fmt.Errorf("graceupgrade: inherited listener %q was not claimed", name)
		}
	}
	m.phase = phasePreparing
	return false, nil
}

func (m *Manager) rollbackReady() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Restore initialization only if Ready did not commit. A concurrent
	// Close or a subsequent handoff must keep its newer phase.
	if m.phase == phasePreparing {
		m.phase = phaseInitializing
	}
}

func (m *Manager) commitReady() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.phase == phaseClosed {
		return ErrClosed
	}
	m.phase = phaseReady
	for _, r := range m.listeners {
		r.guarded.activate()
	}
	m.changed.Broadcast()
	m.startEndpointLocked()
	return nil
}

func (m *Manager) waitActive(ctx context.Context) error {
	stop := wakeOnCancel(ctx, m.changed)
	defer stop()
	m.mu.Lock()
	defer m.mu.Unlock()
	for (m.phase == phaseInitializing || m.phase == phasePreparing) && ctx.Err() == nil {
		m.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.phase == phaseClosed {
		return ErrClosed
	}
	return nil
}

// Close closes this process's listener copies and pending control channel.
// Managed HTTP servers drain asynchronously; call Wait before exiting. Drain
// connections accepted through bare listeners separately. An active handoff
// returns ErrBusy.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeLocked()
}

func (m *Manager) closeLocked() error {
	if m.phase == phaseUpgrading {
		return ErrBusy
	}
	if m.phase == phaseClosed {
		return nil
	}
	handedOff := m.phase == phaseHandedOff
	m.phase = phaseClosed
	for _, s := range m.servers {
		s.Close()
	}
	if m.control != nil {
		m.control.Close()
	}
	var errs []error
	if m.endpoint != nil {
		errs = append(errs, m.endpoint.close())
	}
	for _, r := range m.listeners {
		errs = append(errs, r.guarded.Close())
	}
	if !handedOff {
		close(m.done)
	}
	m.changed.Broadcast()
	return errors.Join(errs...)
}

func (m *Manager) mutable() error {
	switch m.phase {
	case phaseClosed:
		return ErrClosed
	case phaseUpgrading:
		return ErrBusy
	case phaseHandedOff:
		return ErrHandedOff
	}
	return nil
}

type gatedListener struct {
	*net.TCPListener
	mu       sync.Mutex
	changed  *sync.Cond
	active   bool
	closed   bool
	closeErr error
}

func (l *gatedListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	for !l.active && !l.closed {
		l.changed.Wait()
	}
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	return l.TCPListener.Accept()
}

func (l *gatedListener) activate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active = true
	l.changed.Broadcast()
}

func (l *gatedListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		l.closeErr = ignoreClosed(l.TCPListener.Close())
		l.changed.Broadcast()
	}
	return l.closeErr
}

// wakeOnCancel makes condition waits observe cancellation. Taking the same lock
// as Wait prevents a cancellation broadcast from being lost before Wait begins.
func wakeOnCancel(ctx context.Context, changed *sync.Cond) func() bool {
	return context.AfterFunc(ctx, func() {
		changed.L.Lock()
		defer changed.L.Unlock()
		changed.Broadcast()
	})
}

// Socket close errors are normalized at the resource boundary so callers can
// close owned resources repeatedly without classifying net.ErrClosed again.
func ignoreClosed(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

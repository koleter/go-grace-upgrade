package graceupgrade

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

// owned is protected by Manager.mu. A receiver does not own the
// path until Ready commits, so failed startup cannot remove the old endpoint.
type upgradeEndpoint struct {
	listener *net.UnixListener
	path     string
	info     os.FileInfo
	lease    *os.File
	owned    bool
}

func (m *Manager) discoverUpgrade() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	for {
		conn, err := (&net.Dialer{}).DialContext(ctx, "unix", m.upgradeSocket)
		if err == nil {
			return m.inheritUpgrade(conn.(*net.UnixConn))
		}
		if !missingEndpoint(err) {
			return fmt.Errorf("graceupgrade: connect upgrade socket: %w", err)
		}
		lease, err := lockUpgradePath(ctx, m.upgradeSocket)
		if err == ErrBusy {
			// The old owner is alive, or a concurrent cold start is binding.
			// Retry discovery; never unlink its path based on ECONNREFUSED.
			select {
			case <-ctx.Done():
				return fmt.Errorf("graceupgrade: endpoint owner is still active: %w", ctx.Err())
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("graceupgrade: lock upgrade socket: %w", err)
		}
		// Check again after taking the lease, covering another cold start
		// that bound the path between the first dial and lock acquisition.
		conn, err = (&net.Dialer{}).DialContext(ctx, "unix", m.upgradeSocket)
		if err == nil {
			lease.Close()
			return m.inheritUpgrade(conn.(*net.UnixConn))
		}
		if !missingEndpoint(err) {
			lease.Close()
			return err
		}
		return m.createEndpoint(lease)
	}
}

func missingEndpoint(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED)
}

func (m *Manager) inheritUpgrade(conn *net.UnixConn) error {
	m.control = conn
	m.control.SetDeadline(time.Now().Add(m.timeout))
	if err := writeFrame(m.control, message{Kind: "upgrade", Count: protocolVersion}); err != nil {
		return err
	}
	var reply message
	if err := readFrame(m.control, &reply); err != nil {
		return fmt.Errorf("graceupgrade: discovery handshake: %w", err)
	}
	switch reply.Kind {
	case "accepted":
	case "busy":
		return ErrBusy
	case "not-ready":
		return ErrNotReady
	default:
		return fmt.Errorf("graceupgrade: discovery rejected: %s (%s)", reply.Kind, reply.Error)
	}
	if err := m.receive(); err != nil {
		return fmt.Errorf("graceupgrade: inherit via UDS: %w", err)
	}
	return nil
}

func (m *Manager) createEndpoint(lease *os.File) (err error) {
	defer func() {
		if err != nil {
			lease.Close()
		}
	}()
	info, statErr := os.Lstat(m.upgradeSocket)
	if statErr == nil {
		// A refused socket is stale. A symlink/regular file must never be
		// removed or silently treated as a cold-start endpoint.
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("graceupgrade: upgrade path exists and is not a socket")
		}
		if err := os.Remove(m.upgradeSocket); err != nil {
			return err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: m.upgradeSocket, Net: "unix"})
	if err != nil {
		return err
	}
	ln.SetUnlinkOnClose(false)
	if err := os.Chmod(m.upgradeSocket, 0600); err != nil {
		ln.Close()
		os.Remove(m.upgradeSocket)
		return err
	}
	info, err = os.Lstat(m.upgradeSocket)
	if err != nil {
		ln.Close()
		return err
	}
	m.endpoint = &upgradeEndpoint{listener: ln, path: m.upgradeSocket, info: info, lease: lease, owned: true}
	return nil
}

func (m *Manager) receiveEndpoint(path string) error {
	if m.upgradeSocket != path {
		return errors.New("graceupgrade: upgrade socket path changed across upgrade")
	}
	f, err := receiveFile(m.control)
	if err != nil {
		return err
	}
	ln, err := net.FileListener(f)
	f.Close()
	if err != nil {
		return err
	}
	unix, ok := ln.(*net.UnixListener)
	if !ok {
		ln.Close()
		return errors.New("graceupgrade: inherited upgrade listener is not Unix")
	}
	unix.SetUnlinkOnClose(false)
	lease, err := receiveFile(m.control)
	if err != nil {
		unix.Close()
		return err
	}
	leaseInfo, err := lease.Stat()
	if err != nil || !leaseInfo.Mode().IsRegular() {
		unix.Close()
		lease.Close()
		return errors.New("graceupgrade: invalid inherited endpoint lease")
	}
	info, err := os.Lstat(path)
	if err != nil {
		unix.Close()
		lease.Close()
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		unix.Close()
		lease.Close()
		return errors.New("graceupgrade: inherited upgrade path is not a socket")
	}
	m.endpoint = &upgradeEndpoint{listener: unix, path: path, info: info, lease: lease}
	return nil
}

func (m *Manager) startEndpointLocked() {
	e := m.endpoint
	// Ready changes phase before calling this, so the endpoint starts once.
	e.owned = true
	go func() {
		for {
			conn, err := e.listener.AcceptUnix()
			if err != nil {
				return
			}
			go m.handleUpgradeRequest(conn)
		}
	}()
}

func (m *Manager) handleUpgradeRequest(conn *net.UnixConn) {
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	stop := bindContext(ctx, conn)
	defer stop()
	var hello message
	if err := readFrame(conn, &hello); err != nil {
		return
	}
	if hello.Kind != "upgrade" || hello.Count != protocolVersion {
		writeFrame(conn, message{Kind: "error", Error: "unsupported upgrade protocol"})
		return
	}
	if err := m.beginUpgrade(); err != nil {
		kind := "error"
		switch err {
		case ErrBusy:
			kind = "busy"
		case ErrNotReady:
			kind = "not-ready"
		}
		writeFrame(conn, message{Kind: kind, Error: err.Error()})
		return
	}
	success := false
	batch := &connectionBatch{}
	var upgradeErr error
	defer func() {
		// Close the control connection before resuming the old owner on failure;
		// the replacement is still gated and cannot activate without commit.
		conn.Close()
		batch.finish(success)
		m.finishUpgrade(success)
		if upgradeErr != nil && m.onUpgradeError != nil {
			m.onUpgradeError(upgradeErr)
		}
	}()
	if upgradeErr = writeFrame(conn, message{Kind: "accepted"}); upgradeErr != nil {
		return
	}
	var state []byte
	if m.snapshot != nil {
		state, upgradeErr = m.snapshot(ctx)
		if upgradeErr != nil {
			return
		}
	}
	if len(state) > maxStateSize {
		upgradeErr = errors.New("graceupgrade: state exceeds 1 MiB")
		return
	}
	upgradeErr = m.transferTo(ctx, conn, state, batch)
	success = upgradeErr == nil
}

func (m *Manager) beginUpgrade() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.mutable(); err != nil {
		return err
	}
	if m.phase != phaseReady {
		return ErrNotReady
	}
	if len(m.listeners) == 0 {
		return errors.New("graceupgrade: no listeners registered")
	}
	m.phase = phaseUpgrading
	return nil
}

func (m *Manager) finishUpgrade(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.changed.Broadcast()
	m.phase = phaseReady
	if success {
		m.phase = phaseHandedOff
		// Stop HTTP Accept after commit so each serving goroutine can drain its
		// remaining requests. Rollback leaves these listeners open.
		for _, s := range m.httpServers {
			s.listener.Close()
		}
		if m.endpoint != nil {
			// The replacement has the same UDS listening FD. Closing this copy
			// must not unlink its discovery path.
			m.endpoint.owned = false
			m.endpoint.close()
		}
		close(m.done)
	}
}

func (m *Manager) transferTo(ctx context.Context, conn *net.UnixConn, state []byte, batch *connectionBatch) error {
	if err := m.send(conn, state); err != nil {
		return fmt.Errorf("graceupgrade: transfer listeners: %w", contextError(ctx, err))
	}
	var ready message
	if err := readFrame(conn, &ready); err != nil {
		return fmt.Errorf("graceupgrade: wait for readiness: %w", contextError(ctx, err))
	}
	if ready.Kind != "ready" {
		return fmt.Errorf("graceupgrade: unexpected handshake %q", ready.Kind)
	}
	if err := m.freeze(ctx, batch); err != nil {
		return fmt.Errorf("graceupgrade: pause connections: %w", err)
	}
	if err := batch.send(conn); err != nil {
		return fmt.Errorf("graceupgrade: transfer connections: %w", contextError(ctx, err))
	}
	var prepared message
	if err := readFrame(conn, &prepared); err != nil {
		return fmt.Errorf("graceupgrade: prepare connection batch: %w", contextError(ctx, err))
	}
	if prepared.Kind != "prepared" {
		return fmt.Errorf("graceupgrade: unexpected batch response %q", prepared.Kind)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeFrame(conn, message{Kind: "commit"}); err != nil {
		return fmt.Errorf("graceupgrade: commit: %w", contextError(ctx, err))
	}
	return nil
}

func (e *upgradeEndpoint) close() error {
	defer func() {
		if e.lease != nil {
			e.lease.Close()
			e.lease = nil
		}
	}()
	if !e.owned {
		return ignoreClosed(e.listener.Close())
	}
	closeErr := ignoreClosed(e.listener.Close())
	info, err := os.Lstat(e.path)
	if errors.Is(err, os.ErrNotExist) {
		return closeErr
	}
	if err != nil {
		return errors.Join(closeErr, err)
	}
	if os.SameFile(e.info, info) {
		return errors.Join(closeErr, os.Remove(e.path))
	}
	return closeErr
}

// Cancellation interrupts reads/writes without leaving a goroutine waiting.
func bindContext(ctx context.Context, conn net.Conn) func() {
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		conn.SetDeadline(time.Now())
		close(finished)
	})
	return func() {
		if !stop() {
			<-finished
		}
	}
}

func contextError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}

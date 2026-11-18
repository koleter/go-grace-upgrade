//go:build linux || darwin

package graceupgrade

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
)

func sendFile(conn *net.UnixConn, file *os.File) error {
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var rights []byte
	if err := raw.Control(func(fd uintptr) { rights = syscall.UnixRights(int(fd)) }); err != nil {
		return err
	}
	n, oobn, err := conn.WriteMsgUnix([]byte{1}, rights, nil)
	if err != nil {
		return err
	}
	if n != 1 || oobn != len(rights) {
		return errors.New("short SCM_RIGHTS write")
	}
	return nil
}

// TCPListener.File followed by File.Fd can set O_NONBLOCK off on the shared
// socket. If an upgrade is cancelled before the new owner restores that flag,
// the old owner's Accept/Close can hang. Duplicate through SyscallConn instead;
// os.NewFile preserves the existing nonblocking mode.
func listenerFile(listener *net.TCPListener) (*os.File, error) {
	raw, err := listener.SyscallConn()
	if err != nil {
		return nil, err
	}
	return duplicateSocket(raw)
}

func connectionFile(conn net.Conn) (*os.File, error) {
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		return nil, err
	}
	return duplicateSocket(raw)
}

func unixListenerFile(listener *net.UnixListener) (*os.File, error) {
	raw, err := listener.SyscallConn()
	if err != nil {
		return nil, err
	}
	return duplicateSocket(raw)
}

func duplicateFile(file *os.File) (*os.File, error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	return duplicateSocket(raw)
}

// This lock lives as long as the endpoint and is passed to its successor using
// SCM_RIGHTS. A refused UDS is stale only if nobody still holds this lease.
// Never unlink the file: all generations must lock the same inode.
func lockUpgradePath(ctx context.Context, path string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("graceupgrade: upgrade lock is not a regular file")
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return f, nil
}

func duplicateSocket(raw syscall.RawConn) (*os.File, error) {
	fd := -1
	var dupErr error
	err := raw.Control(func(original uintptr) {
		syscall.ForkLock.RLock()
		defer syscall.ForkLock.RUnlock()
		fd, dupErr = syscall.Dup(int(original))
		if dupErr == nil {
			syscall.CloseOnExec(fd)
		}
	})
	if err != nil {
		return nil, err
	}
	if dupErr != nil {
		return nil, dupErr
	}
	return os.NewFile(uintptr(fd), "graceupgrade-listener-copy"), nil
}

func receiveFile(conn *net.UnixConn) (*os.File, error) {
	var data [1]byte
	oob := make([]byte, syscall.CmsgSpace(4*maxListeners))
	n, oobn, flags, _, err := conn.ReadMsgUnix(data[:], oob)
	if err != nil {
		return nil, err
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, err
	}
	var fds []int
	for _, msg := range msgs {
		if msg.Header.Level != syscall.SOL_SOCKET || msg.Header.Type != syscall.SCM_RIGHTS {
			continue
		}
		rights, parseErr := syscall.ParseUnixRights(&msg)
		if parseErr != nil {
			for _, fd := range fds {
				syscall.Close(fd)
			}
			return nil, parseErr
		}
		fds = append(fds, rights...)
	}
	if n != 1 || data[0] != 1 || flags&syscall.MSG_CTRUNC != 0 || len(fds) != 1 {
		for _, fd := range fds {
			syscall.Close(fd)
		}
		return nil, errors.New("invalid SCM_RIGHTS message")
	}
	syscall.CloseOnExec(fds[0])
	return os.NewFile(uintptr(fds[0]), "graceupgrade-listener"), nil
}

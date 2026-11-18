//go:build !linux && !darwin

package graceupgrade

import (
	"context"
	"net"
	"os"
)

const platformSupported = false

func sendFile(*net.UnixConn, *os.File) error                    { return ErrUnsupported }
func receiveFile(*net.UnixConn) (*os.File, error)               { return nil, ErrUnsupported }
func listenerFile(*net.TCPListener) (*os.File, error)           { return nil, ErrUnsupported }
func connectionFile(net.Conn) (*os.File, error)                 { return nil, ErrUnsupported }
func unixListenerFile(*net.UnixListener) (*os.File, error)      { return nil, ErrUnsupported }
func duplicateFile(*os.File) (*os.File, error)                  { return nil, ErrUnsupported }
func lockUpgradePath(context.Context, string) (*os.File, error) { return nil, ErrUnsupported }

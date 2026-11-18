// Package graceupgrade implements startup-driven hot upgrades for Go TCP services.
//
// Configure Options.UpgradeSocket, register listeners or managed servers, start
// serving goroutines and call Manager.Ready. Starting a new instance with the
// same UDS path transfers listening sockets, managed TCP connections, buffers,
// session data and TLS 1.2/1.3 state. A failed restore rolls back to the old owner.
// After handoff, Manager.Done closes. With managed HTTP servers, use Manager.Wait
// to wait for request draining before cleaning up and exiting the old process.
//
// NewServer with Codec and OnMessage provides framing and automatic Session
// migration. Applications can implement the Codec interface for custom formats.
// ServeHTTP manages a standard http.Server, automatically draining it after
// handoff or Close. Connections accepted through a bare Listen stay
// application-owned and must be drained.
//
// Linux and macOS are supported. The package does not install signal handlers
// or exit the process. See examples/tcp and examples/tls for complete programs.
package graceupgrade

# go-grace-upgrade

English | [简体中文](README.zh-CN.md)

A UDS-based hot-upgrade module for Go services on Linux and macOS, using Go 1.25.11. Deploy a new version by starting a new instance with the same UDS path. The framework transfers resources automatically; application code does not need to listen for upgrade signals.

- Custom TCP/TLS: migrate listeners, active connections, unconsumed partial messages, pending output, and TLS 1.2/1.3 state without reconnecting clients.
- Standard HTTP: the new instance takes over the listener, and the framework drains requests in the old instance. Clients establish new connections.
- Application code normally only supplies a complete-message handler. The framework automatically saves and restores message sequence numbers and `Session.Values`.

## Integrating custom TCP/TLS

```go
decoder, err := graceupgrade.DelimiterDecoder("#") // Inbound: split messages on #.
if err != nil { return err }
encoder, err := graceupgrade.DelimiterEncoder("\n") // Outbound: append a newline.
if err != nil { return err }

manager, err := graceupgrade.New(graceupgrade.Options{
    UpgradeSocket: "/tmp/my-service.sock", // Use the same path for every generation; the parent directory must exist.
    ReadyTimeout: 15 * time.Second,
})
if err != nil { return err }
defer manager.Close()

server, err := manager.NewServer("service", "tcp", ":9090", graceupgrade.ServerOptions{
    Decoder: decoder,
    Encoder: encoder, // Omit to send the bytes returned by OnMessage unchanged.
    OnMessage: func(ctx context.Context, session *graceupgrade.Session, message []byte) ([]byte, error) {
        return []byte(fmt.Sprintf("reply-%d: %s", session.Sequence, message)), nil
    },
    // TLSConfig: &mtls.Config{...},
})
if err != nil { return err }
go server.Serve(ctx)
if err := manager.Ready(ctx); err != nil { return err }

<-manager.Done()
// All managed connections have been transferred. Clean up application resources and return from main.
```

Message processing follows the [inbound/outbound separation in Netty Pipeline](https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html): `network input → Decoder → OnMessage → Encoder → network output`. Application code configures request framing, the handler, and response framing independently. The framework manages buffers and socket I/O.

`Decoder` and `OnMessage` are required. `Encoder` is optional and defaults to `RawEncoder()`, which sends the returned bytes without appending anything. For example, receiving `hello#world#` delivers `hello` and `world` to the handler separately. With `DelimiterEncoder("\n")`, each response gets a trailing newline; omitting Encoder sends the raw response. The handler can also assemble the complete response, including a newline, and use the default raw encoder. When responses are sent unchanged, the receiver must identify response boundaries according to its protocol.

`DelimiterDecoder(delimiter)` matches the exact delimiter bytes and strips the trailing delimiter. Examples include `"#"`, `"\r\n"`, `"<END>"`, and Chinese strings. It handles multiple messages in one read and partial messages, including delimiters split across reads. The request delimiter does not restrict response content: responses may contain `#` even when requests are split on `#`.

`DelimiterEncoder(delimiter)` only frames outbound messages. It does not escape content: encoding fails if the response body contains the outbound delimiter, or if the end of the body combines with the appended delimiter to create an earlier match. This prevents clients from splitting responses incorrectly. A non-nil empty response is encoded as the delimiter alone. Both delimiter constructors return `(interface, error)` and reject empty delimiters. A complete frame, including its delimiter, must fit within `MaxBuffer`, up to 256 KiB.

For custom formats, implement the interface for each direction you need and assign it to `ServerOptions.Decoder` or `ServerOptions.Encoder`:

```go
type Decoder interface {
    Name() string
    Decode(input []byte) (consumed int, message []byte, err error)
}

type Encoder interface {
    Name() string
    Encode(message []byte) ([]byte, error)
}
```

`Name()` identifies the format, configuration, and version for its direction. It must be 1–256 bytes long and remain stable while the service is running. Upgrades validate both Decoder and Encoder; a change on either side rejects restoration and rolls back to the old instance. Each `Decode` call returns one complete message and the number of input bytes it occupies. For incomplete input, return `0, nil, nil` without consuming input early or retaining connection-specific parser state. `Encode` only processes responses. Instances are called concurrently by different connections, so methods must be concurrency-safe and must not retain input slices or connection-specific state.

For length-prefixed formats, use `LengthPrefixDecoder(width)` and `LengthPrefixEncoder(width)`. They support 1/2/4/8-byte unsigned big-endian lengths, excluding the prefix itself. The constructors for each direction can be used independently, including with different formats. For line-based communication, combine `DelimiterDecoder("\n")` with `DelimiterEncoder("\n")`. Session snapshots remain compatible when both protocol identifiers stay unchanged. The default Encoder and an explicit `RawEncoder()` are also mutually compatible during restoration.

`OnMessage` receives a complete message and returns a response body, which the framework passes to Encoder before sending. Returning nil skips both the response and Encoder. A non-nil empty slice still calls Encoder; with RawEncoder, an empty response produces no network bytes. Callbacks run serially within one connection and may run in parallel across connections.

`Session.Sequence` increments automatically. To retain small amounts of session data, such as an authenticated user, write to `Session.Values`, for example `session.Values["user"] = []byte(userID)`. Do not modify Sequence, access Session outside the callback, or retain input slices; copy input first if you need to keep it.

Store per-connection application state that must survive upgrades in `Session.Values`. The framework snapshots and restores it automatically. Decoder/Encoder instances themselves are not migrated.

To migrate from the former `Factory` API, move protocol decoding to Decoder, response encoding to Encoder, application handling to OnMessage, and session state to Session.Values. Old custom Handler snapshots cannot be restored directly as Session snapshots; upgrades reject them and roll back to the old instance.

If the application needs custom socket options, provide a function that creates a raw `*net.TCPListener` through `ServerOptions.Listen`. It is called only on cold startup; upgrading instances automatically claim the inherited listener. Configure TLS through this module's `TLSConfig`.

## Integrating standard HTTP

Supply an `http.Server`. The framework handles Serve, Shutdown after an upgrade, and Close on timeout, without application code listening for upgrade events or signals:

```go
manager, err := graceupgrade.New(graceupgrade.Options{
    UpgradeSocket: "/tmp/my-http-service.sock",
})
if err != nil { return err }
defer manager.Close()

server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
if err := manager.ServeHTTP("http", "tcp", ":8080", server, graceupgrade.HTTPOptions{
    DrainTimeout: 30 * time.Second,
}); err != nil { return err }
if err := manager.Ready(context.Background()); err != nil { return err }
return manager.Wait()
```

`ServeHTTP` registers and starts the service before `Ready` in every generation. Pass an `http.Server` that has not been started, and do not subsequently call its Serve, Shutdown, or Close methods yourself. Draining starts only after the handoff is committed; if the upgrade fails, the old HTTP service keeps running. `DrainTimeout` defaults to 30 seconds. On timeout, remaining HTTP connections are closed and `Wait` returns the drain error.

`Done()` still means the handoff has completed or the Manager has been closed; HTTP requests may still be running at that point. `Wait()` also waits for all managed HTTP services to drain and returns service or drain errors. Multiple HTTP services drain in parallel. To stop the host application, call `manager.Close()` and then wait for `manager.Wait()`. The library does not install operating system signal handlers or call os.Exit. The HTTP example upgrades without signal-handling code; stopping it with Ctrl+C/SIGTERM uses the operating system's default behavior. Applications must still manage hijacked connections, such as WebSockets, separately.

## Upgrade flow

1. `New` connects to the configured UDS. If no old endpoint exists, it acquires the ownership lock and performs a cold startup. If an old endpoint exists, the protocol handshake must succeed.
2. The new instance receives listeners, initializes the application, and calls `Ready`. Before commit, it does not accept application traffic or read/write inherited connections.
3. The old instance pauses Accept and connection I/O, then transfers network buffers, sessions, and TLS state. Commit happens only after the new instance successfully restores the entire batch.
4. After a successful handoff, the old instance closes its copies of the migrated FDs and closes `Done`. Managed HTTP services automatically drain old requests. Once `Wait` returns, the application cleans up and exits.
5. If initialization or restoration in the new instance fails, startup aborts and the old instance resumes service, allowing a retry.

The UDS listener FD and ownership lock FD are transferred together, so the old instance exiting does not remove the new instance's endpoint. Stale sockets are removed only after confirming that no active owner exists. Regular files, permission errors, and handshake failures at active endpoints are not treated as cold startups. The `<socket>.lock` file is retained to keep the lock inode stable; concurrent takeover attempts return `ErrBusy`.

Upgrades wait for running callbacks to finish and roll back on timeout. The framework does not migrate Go call stacks, background tasks, or arbitrary application objects, and it does not replay callbacks or database writes that have already executed. If the new instance crashes after ownership is committed, the old instance is not automatically revived.

## Examples and validation

Each example demonstrates one integration style. Upgrade all of them by starting a new instance directly:

| Example | Contents | Validation command |
| --- | --- | --- |
| [examples/tcp](examples/tcp/main.go) | TCP requests delimited by `#`, newline-terminated responses, and a client that connects once | `go run scripts/smoke_connections.go` |
| [examples/tls](examples/tls/README.md) | TLS 1.2/1.3, certificate generation and verification | `go run scripts/smoke_connections.go -example ./examples/tls` |
| [examples/http](examples/http/main.go) | Standard net/http and draining requests in the old instance | `python3 scripts/smoke_http.py` |

```bash
go test -race -timeout=90s ./...
go vet ./...
```

Tests cover concurrent UDS startup, stale endpoints, rollback and retries, as well as TCP/TLS partial messages, pending plaintext/ciphertext, TLS record fragments, exporters, and restoration failures across multiple connections. The connection smoke test builds v1/v2/v3 and continues using the original connections after the old processes have actually exited. The HTTP smoke test checks draining of old requests and continuous handling of new requests.

To try TCP manually, start v1, the client, and v2 in three separate terminals:

```bash
# Terminal 1
go build -ldflags '-X main.version=v1' -o bin/tcp.v1 ./examples/tcp
./bin/tcp.v1 -addr 127.0.0.1:9090 -upgrade-socket /tmp/tcp-upgrade.sock

# Terminal 2: establish just one connection
go run ./examples/tcp -mode client -addr 127.0.0.1:9090

# Terminal 3: start the new version directly
go build -ldflags '-X main.version=v2' -o bin/tcp.v2 ./examples/tcp
./bin/tcp.v2 -addr 127.0.0.1:9090 -upgrade-socket /tmp/tcp-upgrade.sock
```

v1 exits automatically. Client output switches to the new PID/version while the counter continues increasing. Each generation is supervised independently by the deployment tool. Stop the current instance with Ctrl+C in its terminal. No PID file or upgrade signal is required.

## Code structure and limitations

| File | Responsibility |
| --- | --- |
| `manager.go` | New, listener registration, Ready, Done, Close, and lifecycle management |
| `http.go` | Managed HTTP services, automatic draining, and Wait |
| `uds.go` | Old-instance discovery, endpoint ownership, and handoff coordination |
| `server.go` | Accept, per-connection I/O loops, TLS, and buffers |
| `connections.go` | Pausing, connection snapshots, batch restoration, and rollback |
| `message.go` | Inbound Decoder, outbound Encoder, application callbacks, and automatic session state |
| `protocol.go` / `fd_*.go` | Control messages, listener manifests, FD passing, and platform implementations |
| `mtls/` | Independent TLS implementation and Snapshot/Restore |

Internal upgrade coordination follows this sequence: pause service → wait for current operations → take synchronous snapshots → transfer → commit or resume. `sync.Mutex` and `sync.Cond` protect pause state, operation counts, and lifecycle state. Connections no longer exchange snapshot requests and replies through channels. Network I/O and application callbacks still run concurrently across connections without holding these state locks; `Done()` remains the public completion notification.

- Create only one Manager per generation. All generations must use the same service names, network, requested addresses, and compatible Decoder/Encoder/state formats. Only control protocol v3 is supported.
- Managed sockets support TCP only. Applications manage UDP startup and shutdown themselves.
- Bare `Manager.Listen / ListenWith` transfers listeners only; applications must drain existing connections. Wrapping an arbitrary net.Conn cannot capture private state in upper layers such as bufio, standard crypto/tls, HTTP/2, or WebSocket.
- Plaintext/ciphertext buffers default to, and cannot exceed, 256 KiB. Serialized session state and process-level Snapshot are each limited to 1 MiB. Up to 128 application listeners and 65,536 managed connections are supported.
- TLS migration uses this module's mtls; do not wrap it in a standard tls.Listener. TLS 1.2 AEAD/CBC and TLS 1.3 are supported. TLS 1.0/1.1 and RC4 are not migrated.
- mtls derives from MOSN's Go 1.12.7 TLS fork. It does not automatically receive security fixes from the current standard library and requires independent maintenance and review.

This module draws on MOSN's FD, network-buffer, and TLS migration mechanisms and uses batch handoffs. It does not implement forwarding of residual responses from the old process. See the [design notes (Chinese)](docs/mosn-migration.md). The root MIT license and the original licenses in the TLS files apply separately; see [mtls/NOTICE](mtls/NOTICE).

# TLS hot-upgrade example

English | [简体中文](README.zh-CN.md)

A standalone TLS server and client example supporting TLS 1.2 and TLS 1.3. The server uses this module's `mtls` and `NewServer` to migrate active connections. The client uses Go's standard `crypto/tls`, verifies the certificate and hostname, and sends requests continuously over a single connection.

Responses show the server PID, binary version, and connection session counter. After an upgrade, the PID/version changes while the counter keeps increasing and the original TLS connection continues working.

The server uses `DelimiterDecoder("\n") → OnMessage → DelimiterEncoder("\n")`, with inbound and outbound framing configured independently. Application code only implements a complete-message handler. The framework handles framing, message counts, and snapshots and restoration of sessions and TLS state. Omitting Encoder sends the handler's bytes unchanged. The client must match the format in each direction; other custom formats can implement the Decoder/Encoder interfaces separately.

Run all commands below from the project root.

## Automated validation

```bash
go run scripts/smoke_connections.go -example ./examples/tls
```

The script generates temporary certificates and builds v1/v2/v3. For both TLS 1.2 and TLS 1.3, it verifies consecutive upgrades, old-process exit, preservation of the original connection and partial messages, session counter continuity, availability of new connections, and certificate/hostname verification by the example client. It cleans up temporary files and test processes when finished.

## 1. Build and generate a test certificate

```bash
mkdir -p bin/tls
go build -ldflags '-X main.version=v1' -o bin/tls/server ./examples/tls
./bin/tls/server -mode cert
```

This generates `bin/tls/server.crt` and `bin/tls/server.key`. The certificate includes `localhost`, `127.0.0.1`, and `::1` and is valid for one year. The private key has `0600` permissions. Existing certificate/key files are not overwritten. The default output is in the Git-ignored `bin/` directory.

## 2. Start the server

Terminal 1:

```bash
./bin/tls/server -addr 127.0.0.1:9443 -upgrade-socket /tmp/tls-upgrade.sock
```

By default, the server uses TLS 1.3 and the certificate from the previous step. Use `-cert` and `-key` for custom certificate paths. Keep `-upgrade-socket` the same across all generations; its default is `/tmp/graceupgrade-tls.sock`.

## 3. Keep the TLS client running

Terminal 2:

```bash
./bin/tls/server -mode client -addr 127.0.0.1:9443
```

By default, the client trusts `bin/tls/server.crt` and verifies the hostname `localhost`, so there is no need to disable certificate verification. Logs show both connection endpoints, the TLS version, cipher suite, and ALPN. A response is printed every 500 ms:

```json
{"pid":12345,"version":"v1","count":1,"payload":"message-1"}
{"pid":12345,"version":"v1","count":2,"payload":"message-2"}
```

Use `-ca` to specify another CA/trusted certificate file and `-server-name` to set the hostname to verify. `-count 10` exits after sending 10 requests; the default is to run continuously. `-interval 100ms` changes the interval. The client exits with an error if the connection breaks or the session counter becomes discontinuous. It does not reconnect.

## 4. Start a new version directly to upgrade

Terminal 3:

```bash
go build -ldflags '-X main.version=v2' -o bin/tls/server.v2 ./examples/tls
./bin/tls/server.v2 -addr 127.0.0.1:9443 -upgrade-socket /tmp/tls-upgrade.sock
```

On startup, the new version discovers the old version through UDS and automatically restores the listener, active connections, partial messages, and TLS state. Output in terminal 2 continues with the new PID and `version=v2`; `count` continues from the previous generation. Terminal 1 prints `TLS connections moved ... remaining=0` and exits. The new server in terminal 3 continues using the original TLS connection.

Build v3 and start it with the same UDS path to test consecutive upgrades. No upgrade signal is required. Keep the same certificate files during deployment so the client continues to verify the same service identity. If initialization or restoration in the new version fails, it exits and the old version resumes service.

## TLS 1.2 and stopping the service

Restart the server and client, specifying TLS 1.2 on both sides:

```bash
./bin/tls/server -tls-version 1.2
./bin/tls/server -mode client -tls-version 1.2
```

Use Ctrl+C in the current server's terminal to stop the service. The client can also be stopped with Ctrl+C. The example does not require a PID file.

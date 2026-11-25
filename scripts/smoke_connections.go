//go:build ignore

// Run with: go run scripts/smoke_connections.go
// The driver uses Go's standard TLS client, including on macOS installations
// whose system Python links an SSL library without TLS 1.3 support.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type reply struct {
	PID, Count       int
	Version, Payload string
}
type scenario struct {
	name string
	tls  uint16
}

type exampleOptions struct {
	path, cert, key string
	roots           *x509.CertPool
}

type smokeProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func (p *smokeProcess) wait(timeout time.Duration) error {
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		return errors.New("process did not exit")
	}
}

func (p *smokeProcess) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		<-p.done
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	example := flag.String("example", "./examples/tcp", "./examples/tcp or ./examples/tls")
	flag.Parse()
	if *example != "./examples/tcp" && *example != "./examples/tls" {
		return errors.New("example must be ./examples/tcp or ./examples/tls")
	}
	options := exampleOptions{path: *example}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return errors.New("run from the repository root")
	}
	dir, err := os.MkdirTemp("/tmp", "graceupgrade-connections-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	binaries := make(map[string]string)
	for _, version := range []string{"v1", "v2", "v3"} {
		path := filepath.Join(dir, version)
		cmd := exec.Command("go", "build", "-ldflags", "-X main.version="+version, "-o", path, options.path)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("build: %w\n%s", err, output)
		}
		binaries[version] = path
	}
	cases := []scenario{{"tcp", 0}}
	if options.path == "./examples/tls" {
		options.cert, options.key = filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
		cmd := exec.Command(binaries["v1"], "-mode", "cert", "-cert", options.cert, "-key", options.key)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("generate certificates: %w\n%s", err, output)
		}
		certificate, err := os.ReadFile(options.cert)
		if err != nil {
			return err
		}
		options.roots = x509.NewCertPool()
		if !options.roots.AppendCertsFromPEM(certificate) {
			return errors.New("invalid generated certificate")
		}
		cases = []scenario{{"tls12", tls.VersionTLS12}, {"tls13", tls.VersionTLS13}}
	}
	for _, test := range cases {
		if err := exercise(filepath.Join(dir, test.name), binaries, test, options); err != nil {
			return fmt.Errorf("%s: %w", test.name, err)
		}
	}
	return nil
}

func exercise(dir string, binaries map[string]string, test scenario, options exampleOptions) (result error) {
	requestDelimiter := "\n"
	if options.path == "./examples/tcp" {
		requestDelimiter = "#"
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	path, logFile := filepath.Join(dir, "server"), filepath.Join(dir, "server.log")
	install := func(version string) error {
		data, err := os.ReadFile(binaries[version])
		if err != nil {
			return err
		}
		if err := os.WriteFile(path+".next", data, 0700); err != nil {
			return err
		}
		return os.Rename(path+".next", path)
	}
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := reserve.Addr().String()
	reserve.Close()
	if err := install("v1"); err != nil {
		return err
	}
	args := []string{"-addr", address, "-upgrade-socket", filepath.Join(dir, "upgrade.sock")}
	if options.path == "./examples/tls" {
		v := "1.2"
		if test.tls == tls.VersionTLS13 {
			v = "1.3"
		}
		args = append(args, "-cert", options.cert, "-key", options.key, "-tls-version", v)
	}
	log, err := os.Create(logFile)
	if err != nil {
		return err
	}
	defer log.Close()
	var processes []*smokeProcess
	start := func() (*smokeProcess, error) {
		cmd := exec.Command(path, args...)
		cmd.Stdout, cmd.Stderr = log, log
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		p := &smokeProcess{cmd: cmd, done: make(chan struct{})}
		go func() { p.err = cmd.Wait(); close(p.done) }()
		processes = append(processes, p)
		return p, nil
	}
	first, err := start()
	if err != nil {
		return err
	}
	defer func() {
		for _, process := range processes {
			process.stop()
		}
		if result != nil {
			data, _ := os.ReadFile(logFile)
			result = fmt.Errorf("%w\n%s", result, data)
		}
	}()
	waitReady := func(process *smokeProcess) error {
		return wait(func() bool {
			data, _ := os.ReadFile(logFile)
			return strings.Contains(string(data), fmt.Sprintf("ready pid=%d ", process.cmd.Process.Pid))
		})
	}
	if err := waitReady(first); err != nil {
		return err
	}
	connect := func() (net.Conn, error) {
		dialer := &net.Dialer{Timeout: 5 * time.Second}
		if test.tls == 0 {
			return dialer.Dial("tcp", address)
		}
		config := &tls.Config{RootCAs: options.roots, ServerName: "localhost", MinVersion: test.tls, MaxVersion: test.tls}
		if options.roots != nil {
			config.NextProtos = []string{"grace-upgrade-demo"}
		}
		return tls.DialWithDialer(dialer, "tcp", address, config)
	}
	conn, err := connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	local, remote := conn.LocalAddr().String(), conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)
	read := func() (reply, error) {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		data, err := reader.ReadBytes('\n')
		if err != nil {
			return reply{}, err
		}
		var value reply
		err = json.Unmarshal(data, &value)
		return value, err
	}
	exchange := func(text string) (reply, error) {
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.WriteString(conn, text+requestDelimiter); err != nil {
			return reply{}, err
		}
		return read()
	}
	before, err := exchange("before")
	if err != nil {
		return err
	}
	if before.PID != first.cmd.Process.Pid || before.Count != 1 {
		return fmt.Errorf("unexpected initial response %+v", before)
	}
	if err := install("v2"); err != nil {
		return err
	}
	prefix, suffix, payload := "partial", "-after"+requestDelimiter, "partial-after"
	if _, err := io.WriteString(conn, prefix); err != nil {
		return err
	}
	time.Sleep(30 * time.Millisecond)
	replacement, err := start()
	if err != nil {
		return err
	}
	if err := first.wait(15 * time.Second); err != nil {
		return err
	}
	if err := waitReady(replacement); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, suffix); err != nil {
		return err
	}
	after, err := read()
	if err != nil {
		return err
	}
	if after.Count != 2 || after.Payload != payload {
		return fmt.Errorf("half-frame or state lost: %+v", after)
	}
	if after.PID == before.PID || after.Version != "v2" {
		return fmt.Errorf("connection not moved: %+v", after)
	}
	active := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for count := 3; ; count++ {
			value, err := exchange(fmt.Sprintf("message-%d", count))
			if err != nil {
				done <- err
				return
			}
			if value.Count != count {
				done <- fmt.Errorf("counter lost: %+v wanted %d", value, count)
				return
			}
			if count == 3 {
				close(active)
			}
			if value.Version == "v3" {
				done <- nil
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	select {
	case <-active:
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		return errors.New("client stalled")
	}
	if err := install("v3"); err != nil {
		return err
	}
	third, err := start()
	if err != nil {
		return err
	}
	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-time.After(15 * time.Second):
		return errors.New("v3 never served the original socket")
	}
	if err := replacement.wait(15 * time.Second); err != nil {
		return err
	}
	if conn.LocalAddr().String() != local || conn.RemoteAddr().String() != remote {
		return errors.New("client reconnected")
	}
	if err := waitReady(third); err != nil {
		return err
	}
	fresh, err := connect()
	if err != nil {
		return err
	}
	defer fresh.Close()
	fresh.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(fresh, "fresh"+requestDelimiter); err != nil {
		return err
	}
	data, err := bufio.NewReader(fresh).ReadBytes('\n')
	if err != nil {
		return err
	}
	var value reply
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value.Version != "v3" || value.Count != 1 {
		return fmt.Errorf("inherited listener failed: %+v", value)
	}
	if options.path == "./examples/tls" {
		v := "1.2"
		if test.tls == tls.VersionTLS13 {
			v = "1.3"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := exec.CommandContext(ctx, path, "-mode", "client", "-addr", address, "-ca", options.cert, "-tls-version", v, "-count", "3", "-interval", "1ms")
		var diagnostics bytes.Buffer
		client.Stderr = &diagnostics
		output, err := client.Output()
		if err != nil {
			return fmt.Errorf("TLS example client: %w\n%s", err, diagnostics.String())
		}
		decoder := json.NewDecoder(bytes.NewReader(output))
		for count := 1; count <= 3; count++ {
			var reply reply
			if err := decoder.Decode(&reply); err != nil {
				return err
			}
			if reply.Count != count || reply.Version != "v3" {
				return fmt.Errorf("example client: unexpected response %+v", reply)
			}
		}
		invalid := exec.CommandContext(ctx, path, "-mode", "client", "-addr", address, "-ca", options.cert, "-tls-version", v, "-server-name", "invalid.example", "-count", "1")
		if err := invalid.Run(); err == nil {
			return errors.New("example client accepted an incorrect certificate hostname")
		}
		fmt.Printf("PASS: %s standalone client verifies the certificate, hostname and session counter\n", test.name)
	}
	fmt.Printf("PASS: %s v1->v2->v3, startup via UDS, same client socket, old PID exited, half-frame, buffers and session counter preserved\n", test.name)
	return nil
}

func wait(condition func() bool) error {
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if condition() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("timed out waiting for condition")
}

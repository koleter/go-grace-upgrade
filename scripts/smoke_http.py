#!/usr/bin/env python3
"""Exercise the HTTP example across two real executable replacements."""

import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def wait_until(fn, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = fn()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("timed out waiting for condition")


def main():
    if os.name != "posix":
        raise SystemExit("Linux or macOS required")
    with tempfile.TemporaryDirectory(prefix="graceupgrade-smoke-", dir="/tmp") as tmp:
        directory = Path(tmp)
        executable = directory / "server"
        log_path = directory / "server.log"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]

        def build(version):
            next_binary = directory / "server.next"
            subprocess.run([
                "go", "build", "-ldflags", f"-X main.version={version}",
                "-o", str(next_binary), "./examples/http",
            ], cwd=ROOT, check=True)
            os.replace(next_binary, executable)

        def request(path="/"):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=8)
            try:
                conn.request("GET", path)
                response = conn.getresponse()
                assert response.status == 200, response.status
                return json.loads(response.read())
            finally:
                conn.close()

        def version_is(version, old_pid=None):
            try:
                data = request()
                return data if data["version"] == version and data["pid"] != old_pid else None
            except (OSError, http.client.HTTPException):
                return None

        stop = threading.Event()
        failures = []
        count = [0]

        def load():
            while not stop.is_set():
                try:
                    request()
                    count[0] += 1
                except Exception as exc:
                    failures.append(repr(exc))
                time.sleep(0.005)

        build("v1")
        with log_path.open("w+") as log:
            processes = []

            def launch():
                process = subprocess.Popen([
                    str(executable), "-addr", f"127.0.0.1:{port}",
                    "-ready-timeout", "5s",
                    "-drain-timeout", "10s", "-upgrade-socket", str(directory / "u.sock"),
                ], stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                processes.append(process)
                return process

            process = launch()
            worker = None
            try:
                first = wait_until(lambda: version_is("v1"))
                build("v2")
                worker = threading.Thread(target=load, daemon=True)
                worker.start()
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    slow = pool.submit(request, "/slow?duration=3s")
                    wait_until(lambda: "slow request started" in log_path.read_text())
                    replacement = launch()
                    second = wait_until(lambda: version_is("v2", first["pid"]))
                    assert second["inherited"] is True
                    old_response = slow.result(timeout=8)
                    assert old_response["pid"] == first["pid"]
                assert process.wait(timeout=8) == 0
                print("PASS: v1 -> v2 via UDS startup, same port, old in-flight request completes", flush=True)

                build("v3")
                launch()
                third = wait_until(lambda: version_is("v3", second["pid"]))
                assert replacement.wait(timeout=8) == 0
                print("PASS: v2 -> v3 via UDS startup, consecutive upgrade and new serving PID", flush=True)
                stop.set()
                worker.join(timeout=10)
                assert not failures, failures[:5]
                assert count[0] > 0
                print(f"PASS: {count[0]} continuous HTTP requests, 0 failures", flush=True)
            except BaseException:
                print(log_path.read_text(), flush=True)
                raise
            finally:
                stop.set()
                if worker:
                    worker.join(timeout=10)
                # Each independently started generation has its own process group.
                for instance in processes:
                    if instance.poll() is None:
                        try:
                            os.killpg(instance.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                for instance in processes:
                    try:
                        instance.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(instance.pid, signal.SIGKILL)
                        instance.wait(timeout=5)



if __name__ == "__main__":
    main()

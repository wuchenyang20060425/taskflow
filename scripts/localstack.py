"""Isolated Linux processes for the benchmark and recovery checks."""
import contextlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait(check, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.04)
    raise TimeoutError("condition not met before deadline")


def build(root, project=ROOT, api_only=False):
    for cmd in (("api",) if api_only else ("api", "worker")):
        subprocess.run(["go", "build", "-o", str(root / cmd), f"./cmd/{cmd}"],
                       cwd=project, check=True)


class Stack(contextlib.AbstractContextManager):
    def __init__(self, directory, binaries, profile=False):
        self.directory = directory
        self.directory.mkdir()
        self.binaries = binaries
        self.http_port, self.redis_port = port(), port()
        self.profile_ports = (port(), port()) if profile else ()
        self.processes = []
        self.env = dict(os.environ, DATA_DIR=str(directory / "data"),
                        HTTP_ADDR=f"127.0.0.1:{self.http_port}",
                        REDIS_ADDR=f"127.0.0.1:{self.redis_port}", REDIS_PASSWORD="",
                        PPROF_ADDR="", LOG_LEVEL="warn",
                        WORKER_TIMEOUT="25s", WORKER_LEASE="40s",
                        WORKER_MIN_IDLE="45s", WORKER_MAX_ATTEMPTS="3")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        for process in reversed(self.processes):
            self.stop(process)
        if exc[0]:
            for log in sorted(self.directory.glob("*.log")):
                print(f"--- {log.name} ---")
                print(log.read_text(errors="replace")[-3000:])

    def launch(self, name, args, extra=None):
        with (self.directory / f"{name}.log").open("wb") as log:
            process = subprocess.Popen(args, env=self.env | (extra or {}),
                                       stdout=log, stderr=log, start_new_session=True)
        self.processes.append(process)
        return process

    @staticmethod
    def stop(process, force=False):
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    def start(self):
        self.redis = self.launch("redis", ["redis-server", "--bind", "127.0.0.1", "--port",
                                 str(self.redis_port), "--dir", str(self.directory),
                                 "--appendonly", "yes", "--appendfsync", "everysec", "--save", ""])
        extra = {"PPROF_ADDR": f"127.0.0.1:{self.profile_ports[0]}"} if self.profile_ports else {}
        self.api = self.launch("api", [str(self.binaries / "api")], extra)

        def ready():
            try:
                return self.get("/ready")[0] == 200
            except OSError:
                return False
        wait(ready)

    def worker(self, name, **extra):
        if self.profile_ports and name == "worker-0":
            extra["PPROF_ADDR"] = f"127.0.0.1:{self.profile_ports[1]}"
        return self.launch(name, [str(self.binaries / "worker")], extra)

    def get(self, path):
        return self.request("GET", path)

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.http_port, timeout=10)
        try:
            conn.request(method, path, body, headers or {})
            response = conn.getresponse()
            return response.status, response.read()
        finally:
            conn.close()

    def submit(self, body):
        status, response = self.request("POST", "/tasks", body, {
            "Content-Type": "multipart/form-data; boundary=taskflow-fixture"})
        if status != 202:
            raise RuntimeError(f"submit returned {status}: {response!r}")
        return json.loads(response)["id"]

    def task(self, task_id):
        status, body = self.get(f"/tasks/{task_id}")
        if status != 200:
            raise RuntimeError(f"lookup returned {status}: {body!r}")
        return json.loads(body)


def upload_body(content):
    return (b"--taskflow-fixture\r\n"
            b'Content-Disposition: form-data; name="file"; filename="fixture.pdf"\r\n'
            b"Content-Type: application/pdf\r\n\r\n" + content +
            b"\r\n--taskflow-fixture--\r\n")

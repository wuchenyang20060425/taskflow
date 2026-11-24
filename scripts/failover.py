"""Run real Redis, two Worker processes, forced termination and stale commits."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from localstack import ROOT, Stack, build, upload_body, wait


def main():
    poppler = shutil.which("pdftotext")
    if not poppler:
        raise RuntimeError("pdftotext is required")
    with tempfile.TemporaryDirectory(prefix="taskflow-failover-") as tmp:
        root = Path(tmp)
        build(root)
        with Stack(root / "crash", root) as stack:
            stack.start()
            marker = root / "started"
            slow = root / "slow-pdftotext"
            slow.write_text('#!/bin/sh\n: > "$FAULT_MARKER"\nsleep 15\nexec "$REAL_POPPLER" "$@"\n')
            slow.chmod(0o700)
            config = {"WORKER_TIMEOUT": "4s", "WORKER_LEASE": "5s", "WORKER_MIN_IDLE": "100ms"}
            old = stack.worker("old", PDFTOTEXT_BIN=str(slow), FAULT_MARKER=str(marker),
                               REAL_POPPLER=poppler, **config)
            task_id = stack.submit(upload_body((ROOT / "examples/sample.pdf").read_bytes()))
            wait(marker.exists)
            assert stack.task(task_id)["status"] == "running"
            stack.worker("replacement", **config)
            # Exercise an early XAUTOCLAIM of this worker's original message
            # before the lease expires. It must remain recoverable after SIGKILL.
            def pending_reassigned():
                output = subprocess.check_output([
                    "redis-cli", "-p", str(stack.redis_port), "--json", "XPENDING",
                    "taskflow:stream", "taskflow:workers", "-", "+", "1"], text=True)
                rows = json.loads(output)
                return rows and rows[0][3] >= 2
            wait(pending_reassigned, timeout=3)
            stack.stop(old, force=True)
            wait(lambda: stack.task(task_id)["status"] in ("succeeded", "failed"))
            state = stack.task(task_id)
            assert state["status"] == "succeeded" and state["attempts"] == 2, state
            status, txt = stack.get(f"/tasks/{task_id}/result")
            assert status == 200 and b"background worker" in txt
            assert txt == (ROOT / "examples/sample.txt").read_bytes()
            print(json.dumps({"fault": "SIGKILL after early reclaim", "attempts": state["attempts"],
                              "status": state["status"], "result_bytes": len(txt)}))
        env = os.environ | {"TASKFLOW_INTEGRATION": "1"}
        subprocess.run(["go", "test", "-count=1", "-v", "./internal/integration",
                        "-run", "TestStaleCannotReplaceDownloadedResult"], cwd=ROOT, env=env, check=True)


if __name__ == "__main__":
    main()

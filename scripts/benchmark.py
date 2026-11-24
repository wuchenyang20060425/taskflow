"""Generate a fixed PDF and benchmark isolated Linux/WSL TaskFlow processes."""
import argparse
import concurrent.futures as cf
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import resource
import subprocess
import tempfile
import threading
import time
import urllib.request

from localstack import ROOT, Stack, build, upload_body, wait

COUNTS = (1, 2, 4, 8)


def fixture(pages):
    font_id = 3 + 2 * pages
    kids = " ".join(f"{3 + 2*i} 0 R" for i in range(pages))
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>",
               f"<< /Type /Pages /Count {pages} /Kids [{kids}] >>".encode()]
    for number in range(pages):
        content = "BT /F1 11 Tf 45 750 Td 16 TL\n" + "".join(
            f"(TaskFlow fixed fixture page {number+1:03} row {row:02} asynchronous conversion) Tj T*\n"
            for row in range(36)) + "ET\n"
        objects += [
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {4+2*number} 0 R >>".encode(),
            f"<< /Length {len(content)} >>\nstream\n{content}endstream".encode(),
        ]
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, obj in enumerate(objects, 1):
        offsets.append(len(out))
        out.extend(f"{i} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref = len(out)
    out.extend(f"xref\n0 {len(objects)+1}\n0000000000 65535 f \n".encode())
    for offset in offsets:
        out.extend(f"{offset:010} 00000 n \n".encode())
    out.extend(f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    return bytes(out)


def proc(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return ((int(fields[11])+int(fields[12]))/ticks, (int(fields[13])+int(fields[14]))/ticks,
            int(fields[21])*resource.getpagesize())


class Monitor:
    def __init__(self, processes):
        self.pids = [process.pid for process in processes]
        self.initial = [proc(pid) for pid in self.pids]
        self.stop = threading.Event()
        self.peak = [0, 0]
        self.thread = threading.Thread(target=self.sample)

    def sample(self):
        while True:
            current = [proc(pid) for pid in self.pids]
            self.peak[0] = max(self.peak[0], current[0][2])
            self.peak[1] = max(self.peak[1], sum(row[2] for row in current[1:]))
            if self.stop.wait(0.025):
                return

    def start(self):
        self.thread.start()

    def finish(self, seconds):
        self.stop.set()
        self.thread.join()
        diff = [tuple(a-b for a,b in zip(proc(pid), before)) for pid,before in zip(self.pids,self.initial)]
        return {"api_cpu_percent": round(diff[0][0]/seconds*100, 2),
                "worker_cpu_percent": round(sum(row[0] for row in diff[1:])/seconds*100, 2),
                "worker_children_cpu_percent": round(sum(row[1] for row in diff[1:])/seconds*100, 2),
                "api_peak_rss_mib": round(self.peak[0]/2**20, 2),
                "workers_aggregate_peak_rss_mib": round(self.peak[1]/2**20, 2)}


def quantiles(values):
    ordered = sorted(values)
    return {f"p{p}_ms": ordered[math.ceil(len(ordered)*p/100)-1] for p in (50,95,99)} if ordered else {}


def profile(port, path):
    def ready():
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/debug/pprof/", timeout=1):
                return True
        except OSError:
            return False
    wait(ready, timeout=5)
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/debug/pprof/profile?seconds=5", timeout=12) as response:
        path.write_bytes(response.read())


def http_run(root, binaries, n, body, args, profiles=None):
    with Stack(root, binaries, profile=profiles is not None) as stack:
        stack.start()
        workers = [stack.worker(f"worker-{i}") for i in range(n)]
        monitor = Monitor([stack.api, *workers])
        errors, accepted, terminal = [], [], []
        timer = time.monotonic()
        monitor.start()
        try:
            with cf.ThreadPoolExecutor(max_workers=args.clients) as pool:
                profilers = []
                if profiles is not None:
                    profilers = [pool.submit(profile, port, profiles / f"{name}.pprof")
                                 for port, name in zip(stack.profile_ports, ("api", "worker"))]
                futures = [pool.submit(stack.submit, body) for _ in range(args.tasks)]
                for future in cf.as_completed(futures):
                    try:
                        accepted.append(future.result())
                    except (OSError, RuntimeError) as err:
                        errors.append(str(err))
                submitted = time.monotonic()
                pending = set(accepted)
                if n:
                    deadline = time.monotonic() + args.drain_timeout
                    while pending and time.monotonic() < deadline:
                        lookups = {pool.submit(stack.task, task): task for task in pending}
                        for future in cf.as_completed(lookups):
                            try:
                                task = future.result()
                            except (OSError, RuntimeError):
                                continue
                            if task["status"] in ("failed", "succeeded"):
                                terminal.append(task)
                                pending.remove(lookups[future])
                        if pending:
                            time.sleep(0.04)
                ended = time.monotonic()
                resources = monitor.finish(ended-timer)
                for f in profilers:
                    f.result()
        finally:
            if monitor.thread.is_alive():
                monitor.finish(max(time.monotonic()-timer, 0.001))
        success = sum(t["status"]=="succeeded" for t in terminal)
        mode = "end_to_end" if n else "api_only"
        created = [t["created_ms"] for t in terminal]
        queue = [max(0,t["started_ms"]-t["created_ms"]) for t in terminal]
        latency = [max(0,t["finished_ms"]-t["created_ms"]) for t in terminal]
        return {"mode":mode,"workers":n,"accepted":len(accepted),"terminal":len(terminal),
                "succeeded":success,"failed":len(terminal)-success,"unfinished":len(pending) if n else None,
                "submit_per_second":round(len(accepted)/(submitted-timer),2),
                "finish_per_second":round(len(terminal)/(ended-timer),2) if n else None,
                "redis_completion_window_ms":max(t["finished_ms"] for t in terminal)-min(created) if created else None,
                "avg_queue_ms":round(sum(queue)/len(queue),2) if queue else None,
                "error_rate":round((args.tasks-(success if n else len(accepted)))/args.tasks,4),
                "submit_error_examples":errors[:3], **quantiles(latency), **resources}


def poppler_run(pdf, root, n, tasks):
    def convert(i):
        dest = root / f"poppler-{i}.txt"
        with open(os.devnull, "wb") as devnull:
            process = subprocess.Popen(["pdftotext","-layout","-enc","UTF-8",str(pdf),str(dest)],
                                       stdout=devnull, stderr=devnull)
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
        size = dest.stat().st_size if process.returncode == 0 else 0
        dest.unlink(missing_ok=True)
        return process.returncode, usage, size
    before = time.monotonic()
    with cf.ThreadPoolExecutor(max_workers=n) as pool:
        stats = list(pool.map(convert, range(tasks)))
    seconds = time.monotonic()-before
    ok = sum(code==0 for code,_,_ in stats)
    return {"mode":"poppler_only","workers":n,"succeeded":ok,"convert_per_second":round(ok/seconds,2),
            "cpu_percent":round(sum(u.ru_utime+u.ru_stime for _,u,_ in stats)/seconds*100,2),
            "peak_single_process_rss_mib":round(max(u.ru_maxrss for _,u,_ in stats)/1024,2),
            "input_bytes_total":pdf.stat().st_size*tasks,"output_bytes_total":sum(size for _,_,size in stats),
            "error_rate":round((tasks-ok)/tasks,4)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks",type=int,default=160)
    parser.add_argument("--pages",type=int,default=32)
    parser.add_argument("--clients",type=int,default=12)
    parser.add_argument("--repeats",type=int,default=1)
    parser.add_argument("--drain-timeout",type=int,default=180)
    parser.add_argument("--output",type=Path,default=Path("benchmark-output/results.json"))
    parser.add_argument("--api-only",action="store_true")
    parser.add_argument("--project",type=Path,default=ROOT,help="build binaries from this checkout")
    parser.add_argument("--profile-dir",type=Path,
                        help="add separate one-Worker profile run")
    args = parser.parse_args()
    if min(args.tasks,args.pages,args.repeats,args.drain_timeout)<1 or args.clients<3:
        parser.error("positive workload values and at least three clients required")
    if args.api_only and args.profile_dir:
        parser.error("--profile-dir requires Workers")
    with tempfile.TemporaryDirectory(prefix="taskflow-bench-") as temp:
        root = Path(temp)
        build(root,args.project.resolve(),api_only=args.api_only)
        pdf = root / "fixed.pdf"
        content = fixture(args.pages)
        if len(content)>20*2**20:
            parser.error("generated fixture exceeds API upload limit")
        pdf.write_bytes(content)
        body = upload_body(content)
        machine = platform.processor()
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                machine = line.split(":",1)[1].strip()
                break
        report = {"platform":platform.platform(),"logical_cpu":len(os.sched_getaffinity(0)),"cpu":machine,
                  "fixture_bytes":len(content),"fixture_sha256":hashlib.sha256(content).hexdigest(),
                  "pages":args.pages,"tasks":args.tasks,"clients":args.clients,
                  "redis":subprocess.check_output(["redis-server","--version"],text=True).strip(),
                  "go":subprocess.check_output(["go","version"],text=True).strip(),
                  "poppler":subprocess.run(["pdftotext","-v"],text=True,capture_output=True,check=True).stderr.splitlines()[0],
                  "runs":[]}
        for repeat in range(args.repeats):
            for n in ((0,) if args.api_only else (0,*COUNTS)):
                dst = root / f"http-{repeat}-{n}"
                stats = http_run(dst,root,n,body,args)
                report["runs"].append({"repeat":repeat+1,**stats})
                print(json.dumps(stats),flush=True)
                if n:
                    stats = poppler_run(pdf,dst,n,args.tasks)
                    report["runs"].append({"repeat":repeat+1,**stats})
                    print(json.dumps(stats),flush=True)
        if args.profile_dir:
            args.profile_dir.mkdir(parents=True,exist_ok=True)
            report["profile_run"] = http_run(root/"profile",root,1,body,args,args.profile_dir)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2)+"\n")


if __name__=="__main__":
    main()

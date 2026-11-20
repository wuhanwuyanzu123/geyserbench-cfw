#!/usr/bin/env python3
"""
官方口径竞速守护进程（cpa82）。

周期性调用 shrederxyz/geyserbench（订阅 transactions，按交易签名匹配），
解析 Detailed test results 表格，推到 Worker 的 /ingest-shred-race。

不再使用自写的 slots 播报口径。
"""
import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request

GB_DIR = "/opt/geyserbench-shreder"
GB_BIN = GB_DIR + "/target/release/geyserbench"
CONFIG = GB_DIR + "/config_shred.toml"

# Endpoint name already carries the IP (e.g. gRPC-01-1.2.3.4); extract at runtime.
# No node addresses are hardcoded here - they come from local config_shred.toml (untracked).
import os as _os
import re as _re
_GRPC_PORT = int(_os.environ.get("SHRED_GRPC_PORT", "10000"))
def addr_of(name):
    m = _re.search(r"(\d{1,3}(?:\.\d{1,3}){3})", name)
    return f"{m.group(1)}:{_GRPC_PORT}" if m else name
NOTE = ("Transaction-level: geyserbench (shrederxyz fork) subscribes the Solana "
        "transactions filter on both sides and matches identical transaction signatures. "
        "Shreds are FEC-reassembled into entries before comparison, so this measures "
        "when a tradeable transaction is actually available - not when a slot number appears.")

ROW = re.compile(r"^\s*[│|]\s*(.+?)\s*[┆|]\s*([\d.]+)\s*[┆|]\s*([\d.]+)\s*[┆|]\s*([\d.]+)\s*[┆|]\s*([\d.]+)\s*[┆|]\s*(\d+)\s*[┆|]\s*(\d+)\s*[┆|]\s*(\d+)")


def parse_output(text):
    rows = []
    in_table = False
    for line in text.splitlines():
        if "Detailed test results" in line:
            in_table = True
            continue
        if not in_table:
            continue
        m = ROW.match(line)
        if not m:
            continue
        name = m.group(1).strip()
        if name == "Endpoint" or name.startswith("─") or name.startswith("="):
            continue
        rows.append({
            "name": name,
            "first": float(m.group(2)),
            "p50": float(m.group(3)),
            "p95": float(m.group(4)),
            "p99": float(m.group(5)),
            "valid": int(m.group(6)),
            "firsts": int(m.group(7)),
            "backfill": int(m.group(8)),
        })
    return rows


def push(url, token, payload):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "content-type": "application/json", "authorization": "Bearer " + token,
        "user-agent": "geyserbench-daemon/1.0",
        "accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "replace")


def cycle(url, token, timeout):
    try:
        p = subprocess.run([GB_BIN, "--private", "--config", CONFIG],
                           cwd=GB_DIR, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True, timeout=timeout)
        out = p.stdout or ""
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or b"").decode("utf-8", "replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
    rows = parse_output(out)
    if not rows:
        raise RuntimeError("no result rows parsed; tail=" + out[-300:].replace("\n", " "))
    sources = []
    for r in rows:
        is_shred = "shred" in r["name"].lower() or "localshred" in r["name"].lower()
        sources.append({
            "name": r["name"] if is_shred else addr_of(r["name"]),
            "kind": "shred" if is_shred else "grpc",
            "win_rate": r["first"],
            "p50": r["p50"], "p95": r["p95"], "p99": r["p99"],
            "samples": r["valid"], "firsts": r["firsts"], "backfill": r["backfill"],
        })
    payload = {
        "ran_at": int(time.time()), "seconds": timeout,
        "matched_slots": max((r["valid"] for r in rows), default=0),
        "method": "transaction", "note": NOTE, "sources": sources,
    }
    return payload, push(url, token, payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=180.0)
    ap.add_argument("--timeout", type=float, default=150.0)
    ap.add_argument("--url", default=_os.environ.get("GB_PUSH_URL", ""))
    ap.add_argument("--token-file", default=_os.environ.get("GB_TOKEN_FILE", ""))
    args = ap.parse_args()
    try:
        token = open(args.token_file, encoding="utf-8").read().strip()
    except OSError as exc:
        print("[gb] cannot read token: %s" % exc, file=sys.stderr)
        return 1
    print("[gb] start interval=%ss timeout=%ss method=transaction" %
          (args.interval, args.timeout), flush=True)
    while True:
        t0 = time.time()
        try:
            payload, (status, body) = cycle(args.url, token, args.timeout)
            print("[gb] pushed %d sources, %d tx -> %s %s" %
                  (len(payload["sources"]), payload["matched_slots"], status, body[:100]), flush=True)
        except Exception as exc:
            print("[gb] cycle error: %s" % exc, flush=True)
        time.sleep(max(5.0, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Seed a demo database so the UI has data to explore:
   python3 scripts/seed_demo.py [db_path]
Then start the service with the same db: python3 run.py --db <db_path>
"""
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from replay_service.core import Service
from replay_service.db import Database


def dev_ts(t):
    return round(t * 1.001 + 5000)  # device clock: +0.1% skew, +5s offset


def make_pkg(device_id, batch_id):
    evs = []

    def add(kind, t, v):
        evs.append({"seq": sum(1 for e in evs if e["kind"] == kind) + 1, "kind": kind,
                    "device_ts": dev_ts(t), "server_ts": t + 100, "value": v})

    temps = [50, 52, 90, 91, 92, 51, 50, 49, 50, 51]
    mater = [10, 12, 80, 85, 30, 32, 33, 34, 35, 36]
    for i in range(10):
        add("temp", i * 1000, temps[i])
        add("pressure", i * 1000, 5.0)
        add("material", i * 1000, mater[i])
    for i in range(5):
        add("temp", 45000 + i * 1000, 50)
        add("pressure", 45000 + i * 1000, 5.0)
        add("material", 45000 + i * 1000, 37 + i)
    return {"device_id": device_id, "batch_id": batch_id, "events": evs}


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "replay.db"
    svc = Service(Database(db_path))
    try:
        dev = svc.create_device("printer-1", "一号打印机")
        print("device secret (shown once):", dev["secret"])
    except Exception as e:
        print("device exists, skipping:", e)
    pkg = make_pkg("printer-1", "batch-001")
    payload = json.dumps(pkg).encode()
    sha = hashlib.sha256(payload).hexdigest()
    s = svc.open_upload("printer-1", "batch-001", 1, sha)
    svc.put_chunk(s["session_id"], 0, payload, hashlib.sha256(payload).hexdigest())
    svc.seal_upload(s["session_id"])
    print("import:", svc.import_batch("printer-1", "batch-001"))
    corr = svc.create_correction("printer-1")
    tl = svc.build_timeline("printer-1", corr["version_id"])
    print("timeline:", tl["timeline_id"][:8], "hash:", tl["timeline_hash"][:16],
          "findings:", tl["new_findings"])
    print("done. start the UI with: python3 run.py --db", db_path)


if __name__ == "__main__":
    main()

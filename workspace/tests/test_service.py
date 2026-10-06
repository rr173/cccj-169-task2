"""End-to-end tests for the replay & risk-review service (stdlib unittest)."""
import hashlib
import hmac
import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from unittest import mock

from replay_service import detect
from replay_service.core import Service, ApiError, canonical
from replay_service.db import Database
from replay_service.server import make_handler

SKEW_NUM, SKEW_DEN = 1001, 1000   # device clock runs 0.1% fast
OFFSET = 5000                     # device clock offset, ms
NET_DELAY = 100                   # constant gateway delay, ms


def dev_ts(true_ts):
    return round(true_ts * SKEW_NUM / SKEW_DEN + OFFSET)


def make_package(device_id, batch_id):
    """Synthetic batch: normal telemetry + one exceed run + one gap + meter jumps."""
    events = []

    def add(kind, t, value):
        events.append({
            "seq": sum(1 for e in events if e["kind"] == kind) + 1,
            "kind": kind,
            "device_ts": dev_ts(t),
            "server_ts": t + NET_DELAY,
            "value": value,
        })

    temps = [50, 52, 90, 91, 92, 51, 50, 49, 50, 51]   # 3 consecutive > 80 at t=2000..4000
    press = [5.0] * 10
    mater = [10, 12, 80, 85, 30, 32, 33, 34, 35, 36]   # +68 jump at t=2000, reset at t=4000
    for i in range(10):
        t = i * 1000
        add("temp", t, temps[i])
        add("pressure", t, press[i])
        add("material", t, mater[i])
    for i in range(10, 15):                            # resume after a 36s hole
        t = 45000 + (i - 10) * 1000
        add("temp", t, 50)
        add("pressure", t, 5.0)
        add("material", t, 37 + i - 10)
    return {"device_id": device_id, "batch_id": batch_id, "events": events}


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = Database(os.path.join(self.dir, "test.db"))
        self.svc = Service(self.db)
        self.dev = self.svc.create_device("printer-1", "一号打印机")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    # -- helpers -------------------------------------------------------------
    def upload(self, device_id, batch_id, pkg, chunk_size=64, session=None):
        payload = json.dumps(pkg).encode("utf-8")
        sha = hashlib.sha256(payload).hexdigest()
        chunks = [payload[i:i + chunk_size] for i in range(0, len(payload), chunk_size)] or [b""]
        if session is None:
            session = self.svc.open_upload(device_id, batch_id, len(chunks), sha)
        for i, c in enumerate(chunks):
            self.svc.put_chunk(session["session_id"], i, c, hashlib.sha256(c).hexdigest())
        return session

    def import_pkg(self, device_id="printer-1", batch_id="batch-001"):
        pkg = make_package(device_id, batch_id)
        s = self.upload(device_id, batch_id, pkg)
        self.svc.seal_upload(s["session_id"])
        return self.svc.import_batch(device_id, batch_id)

    def build(self):
        corr = self.svc.create_correction("printer-1")
        return self.svc.build_timeline("printer-1", corr["version_id"])


class TestUploadImport(Base):
    def test_chunked_upload_resume(self):
        pkg = make_package("printer-1", "b1")
        payload = json.dumps(pkg).encode()
        sha = hashlib.sha256(payload).hexdigest()
        chunks = [payload[i:i + 100] for i in range(0, len(payload), 100)]
        s1 = self.svc.open_upload("printer-1", "b1", len(chunks), sha)
        for i in range(3):  # upload part, then "crash"
            self.svc.put_chunk(s1["session_id"], i, chunks[i], hashlib.sha256(chunks[i]).hexdigest())
        # client reconnects: same (device, batch, hash) resumes the same session
        s2 = self.svc.open_upload("printer-1", "b1", len(chunks), sha)
        self.assertEqual(s1["session_id"], s2["session_id"])
        self.assertTrue(s2["resumed"])
        self.assertEqual(s2["received_chunks"], [0, 1, 2])
        for i in range(3, len(chunks)):
            self.svc.put_chunk(s2["session_id"], i, chunks[i], hashlib.sha256(chunks[i]).hexdigest())
        self.assertTrue(self.svc.seal_upload(s2["session_id"])["sealed"])
        self.assertEqual(self.svc.import_batch("printer-1", "b1")["event_count"], 45)

    def test_duplicate_chunk_and_seal_and_import_idempotent(self):
        pkg = make_package("printer-1", "b1")
        payload = json.dumps(pkg).encode()
        sha = hashlib.sha256(payload).hexdigest()
        s = self.svc.open_upload("printer-1", "b1", 1, sha)
        csha = hashlib.sha256(payload).hexdigest()
        r1 = self.svc.put_chunk(s["session_id"], 0, payload, csha)
        r2 = self.svc.put_chunk(s["session_id"], 0, payload, csha)  # duplicate
        self.assertTrue(r1["stored"] and not r1["idempotent"])
        self.assertFalse(r2["stored"])
        self.assertTrue(r2["idempotent"])
        # same index, different content -> conflict
        with self.assertRaises(ApiError) as cm:
            self.svc.put_chunk(s["session_id"], 0, b"garbage", hashlib.sha256(b"garbage").hexdigest())
        self.assertEqual(cm.exception.status, 409)
        self.assertFalse(self.svc.seal_upload(s["session_id"])["idempotent"])
        self.assertTrue(self.svc.seal_upload(s["session_id"])["idempotent"])  # duplicate seal
        first = self.svc.import_batch("printer-1", "b1")
        second = self.svc.import_batch("printer-1", "b1")                     # duplicate package
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(len(self.svc.list_events("printer-1")), 45)

    def test_conflicting_batch_content_rejected(self):
        self.import_pkg(batch_id="b1")
        pkg2 = make_package("printer-1", "b1")
        pkg2["events"][0]["value"] = 999.0     # same batch_id, different content
        payload = json.dumps(pkg2).encode()
        sha = hashlib.sha256(payload).hexdigest()
        s = self.svc.open_upload("printer-1", "b1", 1, sha)
        self.svc.put_chunk(s["session_id"], 0, payload, hashlib.sha256(payload).hexdigest())
        with self.assertRaises(ApiError) as cm:
            self.svc.seal_upload(s["session_id"])
        self.assertEqual(cm.exception.status, 409)

    def test_incomplete_seal_rejected(self):
        pkg = make_package("printer-1", "b1")
        payload = json.dumps(pkg).encode()
        sha = hashlib.sha256(payload).hexdigest()
        s = self.svc.open_upload("printer-1", "b1", 2, sha)
        half = payload[:len(payload) // 2]
        self.svc.put_chunk(s["session_id"], 0, half, hashlib.sha256(half).hexdigest())
        with self.assertRaises(ApiError) as cm:
            self.svc.seal_upload(s["session_id"])
        self.assertEqual(cm.exception.status, 409)


class TestTimeline(Base):
    def test_drift_correction_accuracy(self):
        self.import_pkg()
        corr = self.svc.create_correction("printer-1")
        tl = self.svc.build_timeline("printer-1", corr["version_id"])
        pts = self.svc.timeline_points(tl["timeline_id"])
        for p in pts:
            # correction aligns the device clock to the reference (gateway) clock
            self.assertLessEqual(abs(p["corrected_ts"] - p["server_ts"]), 2)

    def test_rebuild_is_reproducible_and_idempotent(self):
        self.import_pkg()
        corr = self.svc.create_correction("printer-1")
        t1 = self.svc.build_timeline("printer-1", corr["version_id"])
        t2 = self.svc.build_timeline("printer-1", corr["version_id"])
        self.assertEqual(t1["timeline_id"], t2["timeline_id"])     # idempotent
        self.assertEqual(t1["timeline_hash"], t2["timeline_hash"]) # reproducible
        self.assertEqual(t2["new_findings"], 0)
        # identical correction parameters are deduplicated
        corr2 = self.svc.create_correction("printer-1")
        self.assertEqual(corr["version_id"], corr2["version_id"])

    def test_detection_finds_exceed_gap_and_meter_jump(self):
        self.import_pkg()
        tl = self.build()
        findings = self.svc.list_findings("printer-1")
        types = [f["type"] for f in findings]
        self.assertEqual(types.count("exceed"), 1)
        self.assertEqual(types.count("gap"), 1)
        self.assertEqual(types.count("meter_jump"), 2)
        exceed = next(f for f in findings if f["type"] == "exceed")
        d = json.loads(exceed["details"])
        self.assertEqual(d["kind"], "temp")
        self.assertEqual(d["count"], 3)
        self.assertEqual(d["max_value"], 92)
        gap = next(f for f in findings if f["type"] == "gap")
        self.assertGreater(json.loads(gap["details"])["gap_ms"], 30000)
        resets = [f for f in findings if f["type"] == "meter_jump"
                  and json.loads(f["details"])["counter_reset"]]
        self.assertEqual(len(resets), 1)
        self.assertGreater(tl["new_findings"], 0)


class TestReview(Base):
    def setUp(self):
        super().setUp()
        self.import_pkg()
        self.build()
        self.finding = self.svc.list_findings("printer-1", "open")[0]

    def test_accept_reject_return_flow(self):
        f = self.finding
        ok = self.svc.review(f["finding_id"], "accept", "alice", "确认", f["version"])
        self.assertEqual(ok["status"], "accepted")
        # terminal finding cannot be reviewed again
        with self.assertRaises(ApiError) as cm:
            self.svc.review(f["finding_id"], "reject", "bob", "", 1)
        self.assertEqual(cm.exception.status, 409)
        others = self.svc.list_findings("printer-1", "open")
        g = others[0]
        self.svc.review(g["finding_id"], "return", "alice", "重新算", g["version"])
        self.assertEqual(self.svc.get_finding(g["finding_id"])["status"], "returned")
        h = self.svc.finding_history(g["finding_id"])
        self.assertEqual([r["action"] for r in h], ["return"])

    def test_stale_version_conflict(self):
        f = self.finding
        with self.assertRaises(ApiError) as cm:
            self.svc.review(f["finding_id"], "accept", "alice", "", f["version"] + 5)
        self.assertEqual(cm.exception.status, 409)

    def test_concurrent_reviews_keep_single_result(self):
        f = self.finding
        barrier = threading.Barrier(2)
        results, errors = [], []

        def attempt(name):
            barrier.wait()
            try:
                results.append(self.svc.review(f["finding_id"], "accept", name, "", f["version"]))
            except ApiError as e:
                errors.append(e)

        t1 = threading.Thread(target=attempt, args=("alice",))
        t2 = threading.Thread(target=attempt, args=("bob",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(results), 1, "exactly one concurrent review may win")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].status, 409)
        self.assertEqual(self.svc.get_finding(f["finding_id"])["status"], "accepted")
        hist = self.svc.finding_history(f["finding_id"])
        self.assertEqual(len([r for r in hist if r["action"] == "accept"]), 1)

    def test_recompute_success_creates_new_timeline(self):
        f = self.finding
        self.svc.review(f["finding_id"], "return", "alice", "", f["version"])
        tl = self.svc.recompute(f["finding_id"], "alice")
        self.assertIn("timeline_id", tl)
        hist = self.svc.finding_history(f["finding_id"])
        self.assertEqual([r["action"] for r in hist], ["return", "recompute_ok"])
        self.assertEqual(self.svc.get_finding(f["finding_id"])["status"], "returned")

    def test_recompute_failure_rolls_back(self):
        f = self.finding
        self.svc.review(f["finding_id"], "return", "alice", "", f["version"])
        count = lambda t: self.db.one("SELECT COUNT(*) c FROM %s" % t)["c"]
        before = {t: count(t) for t in
                  ("correction_versions", "timelines", "timeline_points", "findings")}
        with mock.patch.object(detect, "detect_findings",
                               side_effect=RuntimeError("detector exploded")):
            with self.assertRaises(ApiError) as cm:
                self.svc.recompute(f["finding_id"], "alice")
        self.assertEqual(cm.exception.status, 500)
        after = {t: count(t) for t in before}
        self.assertEqual(before, after, "failed recompute must not leave partial writes")
        g = self.svc.get_finding(f["finding_id"])
        self.assertEqual(g["status"], "open", "finding must be reopened after rollback")
        acts = [r["action"] for r in self.svc.finding_history(f["finding_id"])]
        self.assertEqual(acts, ["return", "recompute_failed"])


class TestCorrectionImmutability(Base):
    def test_referenced_version_cannot_be_overwritten_or_deleted(self):
        self.import_pkg()
        corr = self.svc.create_correction("printer-1")
        vid = corr["version_id"]
        # unreferenced: mutable
        self.svc.update_correction(vid, 1, 1, 0, 1, note="manual identity")
        self.svc.build_timeline("printer-1", vid)  # now referenced
        with self.assertRaises(ApiError) as cm:
            self.svc.update_correction(vid, 2, 1, 0, 1)
        self.assertEqual(cm.exception.status, 409)
        with self.assertRaises(ApiError) as cm:
            self.svc.delete_correction(vid)
        self.assertEqual(cm.exception.status, 409)
        # data is still intact and readable
        kept = [c for c in self.svc.list_corrections("printer-1") if c["version_id"] == vid]
        self.assertEqual(kept[0]["a"], "1/1")
        # explicit supersede is allowed (audited, non-destructive)
        self.assertEqual(self.svc.supersede_correction(vid)["status"], "superseded")


class TestReports(Base):
    def setUp(self):
        super().setUp()
        self.import_pkg()
        self.tl = self.build()
        self.other = self.svc.create_device("printer-2", "二号打印机")

    def test_export_and_load_roundtrip(self):
        env = self.svc.export_report("printer-1", self.tl["timeline_id"])
        res = self.svc.load_report(env)
        self.assertTrue(res["ok"])
        self.assertEqual(res["payload"]["timeline_hash"], self.tl["timeline_hash"])

    def test_tampered_payload_rejected(self):
        env = self.svc.export_report("printer-1", self.tl["timeline_id"])
        env["payload"]["findings"] = []                      # tamper: erase findings
        with self.assertRaises(ApiError) as cm:
            self.svc.load_report(env)
        self.assertEqual(cm.exception.status, 403)

    def test_cross_device_report_rejected(self):
        env = self.svc.export_report("printer-1", self.tl["timeline_id"])
        # 1) claim another device id in the envelope
        forged = dict(env, device_id="printer-2")
        with self.assertRaises(ApiError) as cm:
            self.svc.load_report(forged)
        self.assertEqual(cm.exception.status, 403)
        # 2) re-sign a fully consistent envelope with the WRONG device's key
        payload = dict(env["payload"], device_id="printer-2")
        sig = hmac.new(self.dev["secret"].encode(), canonical(payload).encode(),
                       hashlib.sha256).hexdigest()
        with self.assertRaises(ApiError) as cm:
            self.svc.load_report({"device_id": "printer-2", "payload": payload,
                                  "signature": sig})
        self.assertEqual(cm.exception.status, 403)
        # 3) unknown device
        with self.assertRaises(ApiError) as cm:
            self.svc.load_report({"device_id": "ghost", "payload": env["payload"],
                                  "signature": env["signature"]})
        self.assertEqual(cm.exception.status, 403)


class TestHttp(unittest.TestCase):
    """Smoke-test the HTTP layer end to end."""

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        svc = Service(Database(os.path.join(cls.dir, "http.db")))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(svc))
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        shutil.rmtree(cls.dir, ignore_errors=True)

    def req(self, method, path, body=None, raw=None, headers=None):
        url = "http://127.0.0.1:%d%s" % (self.port, path)
        data = raw if raw is not None else (
            json.dumps(body).encode() if body is not None else None)
        r = urllib.request.Request(url, data=data, method=method)
        if body is not None:
            r.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            r.add_header(k, v)
        try:
            with urllib.request.urlopen(r) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_full_flow_over_http(self):
        st, dev = self.req("POST", "/api/devices",
                           {"device_id": "p1", "name": "打印机"})
        self.assertEqual(st, 201)
        pkg = json.dumps(make_package("p1", "b1")).encode()
        sha = hashlib.sha256(pkg).hexdigest()
        st, sess = self.req("POST", "/api/uploads",
                            {"device_id": "p1", "batch_id": "b1",
                             "total_chunks": 1, "package_sha256": sha})
        self.assertEqual(st, 200)
        st, _ = self.req("PUT", "/api/uploads/%s/chunks/0" % sess["session_id"],
                         raw=pkg, headers={"X-Chunk-SHA256": sha})
        self.assertEqual(st, 200)
        st, _ = self.req("POST", "/api/uploads/%s/seal" % sess["session_id"])
        self.assertEqual(st, 200)
        st, imp = self.req("POST", "/api/batches/import",
                           {"device_id": "p1", "batch_id": "b1"})
        self.assertEqual((st, imp["event_count"]), (200, 45))
        st, corr = self.req("POST", "/api/devices/p1/corrections", {})
        self.assertEqual(st, 201)
        st, tl = self.req("POST", "/api/timelines/build",
                          {"device_id": "p1", "version_id": corr["version_id"]})
        self.assertEqual(st, 200)
        st, findings = self.req("GET", "/api/findings?device_id=p1&status=open")
        self.assertEqual(st, 200)
        self.assertTrue(findings)
        f = findings[0]
        st, decided = self.req("POST", "/api/findings/%s/reviews" % f["finding_id"],
                               {"action": "accept", "reviewer": "alice",
                                "version": f["version"]})
        self.assertEqual((st, decided["status"]), (200, "accepted"))
        st, env = self.req("POST", "/api/reports/export",
                           {"device_id": "p1", "timeline_id": tl["timeline_id"]})
        self.assertEqual(st, 200)
        st, res = self.req("POST", "/api/reports/load", env)
        self.assertEqual((st, res["ok"]), (200, True))
        env["payload"]["exported_at"] = 0                      # tamper
        st, res = self.req("POST", "/api/reports/load", env)
        self.assertEqual(st, 403)
        with urllib.request.urlopen("http://127.0.0.1:%d/" % self.port) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/html", resp.headers["Content-Type"])


if __name__ == "__main__":
    unittest.main()

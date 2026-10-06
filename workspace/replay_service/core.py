"""Core service: chunked idempotent import, drift-corrected timeline rebuild,
finding review workflow, signed audit reports."""
import hashlib
import hmac
import json
import time
import uuid
from fractions import Fraction

from . import detect, drift

ALGO_VERSION = "1.0.0"
EVENT_KINDS = ("temp", "pressure", "material")
TERMINAL_ACTIONS = ("accept", "reject")


def now_ms():
    return int(time.time() * 1000)


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _row_to_dict(row):
    return {k: row[k] for k in row.keys()}


class Service:
    def __init__(self, db):
        self.db = db

    # ---------------------------------------------------------------- devices
    def create_device(self, device_id, name, params=None):
        if not device_id or not name:
            raise ApiError(400, "device_id and name are required")
        params = detect.validate_params(params or {})
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        try:
            with self.db.tx() as conn:
                conn.execute(
                    "INSERT INTO devices(device_id,name,secret,params,created_at) VALUES(?,?,?,?,?)",
                    (device_id, name, secret, canonical(params), now_ms()))
        except Exception as e:
            if "UNIQUE" in str(e):
                raise ApiError(409, "device already exists: %s" % device_id)
            raise
        return {"device_id": device_id, "name": name, "secret": secret, "params": params}

    def get_device(self, device_id):
        row = self.db.one("SELECT * FROM devices WHERE device_id=?", (device_id,))
        if not row:
            raise ApiError(404, "unknown device: %s" % device_id)
        return row

    def list_devices(self):
        rows = self.db.query("SELECT device_id,name,params,created_at FROM devices ORDER BY device_id")
        return [_row_to_dict(r) for r in rows]

    def _params(self, conn, device_id):
        row = conn.execute("SELECT params FROM devices WHERE device_id=?", (device_id,)).fetchone()
        if not row:
            raise ApiError(404, "unknown device: %s" % device_id)
        return detect.validate_params(json.loads(row["params"] or "{}"))

    # ---------------------------------------------------------------- uploads
    def open_upload(self, device_id, batch_id, total_chunks, package_sha256):
        """Idempotent: same (device, batch, package hash) reopens the existing session
        so an interrupted upload can resume."""
        self.get_device(device_id)
        if not batch_id or total_chunks < 1:
            raise ApiError(400, "batch_id and total_chunks>=1 are required")
        with self.db.tx() as conn:
            row = conn.execute(
                "SELECT * FROM upload_sessions WHERE device_id=? AND batch_id=? AND package_sha256=?",
                (device_id, batch_id, package_sha256)).fetchone()
            if row:
                return self._upload_state(conn, row, resumed=True)
            session_id = uuid.uuid4().hex
            ts = now_ms()
            conn.execute(
                "INSERT INTO upload_sessions(session_id,device_id,batch_id,total_chunks,"
                "package_sha256,status,created_at,updated_at) VALUES(?,?,?,?,?,'open',?,?)",
                (session_id, device_id, batch_id, total_chunks, package_sha256, ts, ts))
            row = conn.execute("SELECT * FROM upload_sessions WHERE session_id=?", (session_id,)).fetchone()
            return self._upload_state(conn, row, resumed=False)

    def _upload_state(self, conn, row, resumed):
        chunks = [r["chunk_index"] for r in conn.execute(
            "SELECT chunk_index FROM upload_chunks WHERE session_id=? ORDER BY chunk_index",
            (row["session_id"],))]
        return {
            "session_id": row["session_id"],
            "device_id": row["device_id"],
            "batch_id": row["batch_id"],
            "total_chunks": row["total_chunks"],
            "status": row["status"],
            "received_chunks": chunks,
            "resumed": resumed,
        }

    def get_upload(self, session_id):
        with self.db.tx() as conn:
            row = conn.execute("SELECT * FROM upload_sessions WHERE session_id=?", (session_id,)).fetchone()
            if not row:
                raise ApiError(404, "unknown upload session")
            return self._upload_state(conn, row, resumed=False)

    def put_chunk(self, session_id, chunk_index, data: bytes, chunk_sha256: str):
        actual = sha256_hex(data)
        if actual != chunk_sha256:
            raise ApiError(400, "chunk checksum mismatch")
        with self.db.tx() as conn:
            sess = conn.execute("SELECT * FROM upload_sessions WHERE session_id=?", (session_id,)).fetchone()
            if not sess:
                raise ApiError(404, "unknown upload session")
            if sess["status"] != "open":
                raise ApiError(409, "session is %s" % sess["status"])
            if not (0 <= chunk_index < sess["total_chunks"]):
                raise ApiError(400, "chunk index out of range")
            existing = conn.execute(
                "SELECT sha256 FROM upload_chunks WHERE session_id=? AND chunk_index=?",
                (session_id, chunk_index)).fetchone()
            if existing:
                if existing["sha256"] == actual:
                    return {"stored": False, "idempotent": True, "chunk_index": chunk_index}
                raise ApiError(409, "chunk %d already uploaded with different content" % chunk_index)
            conn.execute(
                "INSERT INTO upload_chunks(session_id,chunk_index,sha256,size,data) VALUES(?,?,?,?,?)",
                (session_id, chunk_index, actual, len(data), data))
            conn.execute("UPDATE upload_sessions SET updated_at=? WHERE session_id=?",
                         (now_ms(), session_id))
            return {"stored": True, "idempotent": False, "chunk_index": chunk_index}

    def seal_upload(self, session_id):
        """Assemble chunks, verify package hash, create the batch. Idempotent."""
        with self.db.tx() as conn:
            sess = conn.execute("SELECT * FROM upload_sessions WHERE session_id=?", (session_id,)).fetchone()
            if not sess:
                raise ApiError(404, "unknown upload session")
            if sess["status"] == "sealed":
                batch = conn.execute("SELECT * FROM batches WHERE device_id=? AND batch_id=?",
                                     (sess["device_id"], sess["batch_id"])).fetchone()
                return {"sealed": True, "idempotent": True, "batch_id": sess["batch_id"],
                        "event_count": batch["event_count"] if batch else 0}
            chunks = conn.execute(
                "SELECT chunk_index,data FROM upload_chunks WHERE session_id=? ORDER BY chunk_index",
                (session_id,)).fetchall()
            if len(chunks) != sess["total_chunks"]:
                missing = sorted(set(range(sess["total_chunks"])) - {c["chunk_index"] for c in chunks})
                raise ApiError(409, "upload incomplete, missing chunks: %s" % missing)
            payload = b"".join(c["data"] for c in chunks)
            if sha256_hex(payload) != sess["package_sha256"]:
                raise ApiError(400, "package checksum mismatch")
            existing = conn.execute("SELECT * FROM batches WHERE device_id=? AND batch_id=?",
                                    (sess["device_id"], sess["batch_id"])).fetchone()
            if existing and existing["package_sha256"] != sess["package_sha256"]:
                raise ApiError(409, "batch %s already exists with different content"
                               % sess["batch_id"])
            if not existing:
                conn.execute(
                    "INSERT INTO batches(device_id,batch_id,package_sha256,status,created_at)"
                    " VALUES(?,?,?,'sealed',?)",
                    (sess["device_id"], sess["batch_id"], sess["package_sha256"], now_ms()))
            conn.execute("UPDATE upload_sessions SET status='sealed', updated_at=? WHERE session_id=?",
                         (now_ms(), session_id))
            return {"sealed": True, "idempotent": False, "batch_id": sess["batch_id"]}

    # ---------------------------------------------------------------- import
    def import_batch(self, device_id, batch_id):
        """Parse a sealed package and store events. Re-importing the same package
        is a no-op (idempotent); a different package under the same batch_id conflicts."""
        with self.db.tx() as conn:
            batch = conn.execute("SELECT * FROM batches WHERE device_id=? AND batch_id=?",
                                 (device_id, batch_id)).fetchone()
            if not batch:
                raise ApiError(404, "batch not found (seal the upload first)")
            if batch["status"] == "imported":
                return {"imported": True, "idempotent": True, "event_count": batch["event_count"]}
            pkg = self._load_package(conn, device_id, batch_id, batch["package_sha256"])
            if pkg.get("device_id") != device_id or pkg.get("batch_id") != batch_id:
                raise ApiError(400, "package device_id/batch_id mismatch")
            events = pkg.get("events")
            if not isinstance(events, list) or not events:
                raise ApiError(400, "package contains no events")
            arrival = now_ms()
            inserted = 0
            for e in events:
                self._validate_event(e)
                cur = conn.execute(
                    "INSERT OR IGNORE INTO events(device_id,batch_id,kind,seq,device_ts,server_ts,value,raw)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (device_id, batch_id, e["kind"], int(e["seq"]), int(e["device_ts"]),
                     int(e.get("server_ts") or arrival), float(e["value"]), canonical(e)))
                inserted += cur.rowcount
            conn.execute(
                "UPDATE batches SET status='imported', event_count=?, imported_at=?"
                " WHERE device_id=? AND batch_id=?",
                (inserted, arrival, device_id, batch_id))
            return {"imported": True, "idempotent": False, "event_count": inserted}

    def _load_package(self, conn, device_id, batch_id, expect_hash):
        sess = conn.execute(
            "SELECT * FROM upload_sessions WHERE device_id=? AND batch_id=? AND package_sha256=?",
            (device_id, batch_id, expect_hash)).fetchone()
        if not sess:
            raise ApiError(500, "upload session for batch is missing")
        chunks = conn.execute(
            "SELECT data FROM upload_chunks WHERE session_id=? ORDER BY chunk_index",
            (sess["session_id"],)).fetchall()
        payload = b"".join(c["data"] for c in chunks)
        if sha256_hex(payload) != expect_hash:
            raise ApiError(500, "stored package failed integrity check")
        try:
            return json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise ApiError(400, "package is not valid JSON: %s" % e)

    @staticmethod
    def _validate_event(e):
        if not isinstance(e, dict):
            raise ApiError(400, "event must be an object")
        if e.get("kind") not in EVENT_KINDS:
            raise ApiError(400, "bad event kind: %r" % e.get("kind"))
        for field in ("seq", "device_ts"):
            if not isinstance(e.get(field), int):
                raise ApiError(400, "event.%s must be an integer" % field)
        if not isinstance(e.get("value"), (int, float)) or isinstance(e.get("value"), bool):
            raise ApiError(400, "event.value must be numeric")
        if "server_ts" in e and e["server_ts"] is not None and not isinstance(e["server_ts"], int):
            raise ApiError(400, "event.server_ts must be an integer")

    # ---------------------------------------------------------------- corrections
    def create_correction(self, device_id, source_batch_id=None, note=""):
        with self.db.tx() as conn:
            self._params(conn, device_id)
            version_id = self._create_correction_tx(conn, device_id, source_batch_id, note)
            return self._correction_dict(conn, version_id)

    def _create_correction_tx(self, conn, device_id, source_batch_id, note):
        if source_batch_id:
            rows = conn.execute(
                "SELECT device_ts,server_ts FROM events WHERE device_id=? AND batch_id=?",
                (device_id, source_batch_id)).fetchall()
            if not rows:
                raise ApiError(404, "no events for batch %s" % source_batch_id)
        else:
            rows = conn.execute(
                "SELECT device_ts,server_ts FROM events WHERE device_id=?", (device_id,)).fetchall()
            if not rows:
                raise ApiError(404, "no events imported for device %s" % device_id)
        a, b = drift.estimate_correction([(r["device_ts"], r["server_ts"]) for r in rows])
        existing = conn.execute(
            "SELECT version_id FROM correction_versions"
            " WHERE device_id=? AND a_num=? AND a_den=? AND b_num=? AND b_den=?",
            (device_id, a.numerator, a.denominator, b.numerator, b.denominator)).fetchone()
        if existing:  # identical correction already exists -> idempotent reuse
            return existing["version_id"]
        version_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO correction_versions(version_id,device_id,a_num,a_den,b_num,b_den,"
            "source_batch_id,note,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (version_id, device_id, a.numerator, a.denominator, b.numerator, b.denominator,
             source_batch_id, note, now_ms()))
        return version_id

    def _correction_dict(self, conn, version_id):
        r = conn.execute("SELECT * FROM correction_versions WHERE version_id=?",
                         (version_id,)).fetchone()
        if not r:
            raise ApiError(404, "unknown correction version")
        d = _row_to_dict(r)
        d["a"] = drift.frac_to_str(Fraction(r["a_num"], r["a_den"]))
        d["b"] = drift.frac_to_str(Fraction(r["b_num"], r["b_den"]))
        return d

    def list_corrections(self, device_id):
        with self.db.tx() as conn:
            self._params(conn, device_id)
            rows = conn.execute(
                "SELECT version_id FROM correction_versions WHERE device_id=? ORDER BY created_at",
                (device_id,)).fetchall()
            return [self._correction_dict(conn, r["version_id"]) for r in rows]

    def _check_mutable(self, conn, version_id):
        r = conn.execute("SELECT referenced FROM correction_versions WHERE version_id=?",
                         (version_id,)).fetchone()
        if not r:
            raise ApiError(404, "unknown correction version")
        refs = conn.execute("SELECT COUNT(*) c FROM timelines WHERE version_id=?",
                            (version_id,)).fetchone()["c"]
        if r["referenced"] or refs:
            raise ApiError(409, "correction version is referenced and cannot be overwritten")

    def update_correction(self, version_id, a_num, a_den, b_num, b_den, note=None):
        with self.db.tx() as conn:
            self._check_mutable(conn, version_id)
            conn.execute(
                "UPDATE correction_versions SET a_num=?,a_den=?,b_num=?,b_den=?,note=COALESCE(?,note)"
                " WHERE version_id=?", (a_num, a_den, b_num, b_den, note, version_id))
            return self._correction_dict(conn, version_id)

    def delete_correction(self, version_id):
        with self.db.tx() as conn:
            self._check_mutable(conn, version_id)
            conn.execute("DELETE FROM correction_versions WHERE version_id=?", (version_id,))
            return {"deleted": True}

    def supersede_correction(self, version_id):
        """Explicit, audited state transition — never silent, data is preserved."""
        with self.db.tx() as conn:
            r = conn.execute("SELECT version_id FROM correction_versions WHERE version_id=?",
                             (version_id,)).fetchone()
            if not r:
                raise ApiError(404, "unknown correction version")
            conn.execute("UPDATE correction_versions SET status='superseded' WHERE version_id=?",
                         (version_id,))
            return self._correction_dict(conn, version_id)

    # ---------------------------------------------------------------- timelines
    def build_timeline(self, device_id, version_id):
        with self.db.tx() as conn:
            params = self._params(conn, device_id)
            return self._build_timeline_tx(conn, device_id, version_id, params)

    def _build_timeline_tx(self, conn, device_id, version_id, params,
                           generation=0, idempotent=True):
        corr = conn.execute("SELECT * FROM correction_versions WHERE version_id=?",
                            (version_id,)).fetchone()
        if not corr or corr["device_id"] != device_id:
            raise ApiError(404, "correction version not found for device")
        events = conn.execute(
            "SELECT * FROM events WHERE device_id=? ORDER BY kind,seq", (device_id,)).fetchall()
        if not events:
            raise ApiError(404, "no events imported for device %s" % device_id)
        a = Fraction(corr["a_num"], corr["a_den"])
        b = Fraction(corr["b_num"], corr["b_den"])

        input_hash = sha256_hex(canonical(
            [[e["id"], e["kind"], e["seq"], e["device_ts"], e["server_ts"], e["value"]]
             for e in events]).encode("utf-8"))
        points = [{
            "event_id": e["id"],
            "kind": e["kind"],
            "value": e["value"],
            "corrected_ts": drift.correct(a, b, e["device_ts"]),
        } for e in events]
        points.sort(key=lambda p: (p["corrected_ts"], p["event_id"]))
        timeline_hash = sha256_hex(canonical(
            [[p["event_id"], p["corrected_ts"], p["kind"], p["value"]]
             for p in points]).encode("utf-8"))

        existing = conn.execute(
            "SELECT * FROM timelines WHERE version_id=? AND input_hash=?"
            " AND algo_version=? AND generation=?",
            (version_id, input_hash, ALGO_VERSION, generation)).fetchone()
        if idempotent and existing:  # identical rebuild is idempotent
            conn.execute("UPDATE correction_versions SET referenced=1 WHERE version_id=?",
                         (version_id,))
            return self._timeline_dict(conn, existing, new_findings=0)

        timeline_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO timelines(timeline_id,device_id,version_id,algo_version,generation,"
            "input_hash,timeline_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (timeline_id, device_id, version_id, ALGO_VERSION, generation,
             input_hash, timeline_hash, now_ms()))
        conn.executemany(
            "INSERT INTO timeline_points(timeline_id,event_id,corrected_ts,kind,value)"
            " VALUES(?,?,?,?,?)",
            [(timeline_id, p["event_id"], p["corrected_ts"], p["kind"], p["value"])
             for p in points])
        conn.execute("UPDATE correction_versions SET referenced=1 WHERE version_id=?",
                     (version_id,))

        new_findings = 0
        for f in detect.detect_findings(points, params):
            finding_id = "F-" + hashlib.sha256(
                canonical([timeline_id, f["type"], f["start_ts"], f["end_ts"], f["details"]]
                          ).encode("utf-8")).hexdigest()[:16]
            ts = now_ms()
            cur = conn.execute(
                "INSERT OR IGNORE INTO findings(finding_id,timeline_id,device_id,type,start_ts,"
                "end_ts,details,status,version,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,'open',0,?,?)",
                (finding_id, timeline_id, device_id, f["type"], f["start_ts"], f["end_ts"],
                 canonical(f["details"]), ts, ts))
            new_findings += cur.rowcount
        return self._timeline_dict(
            conn, conn.execute("SELECT * FROM timelines WHERE timeline_id=?",
                               (timeline_id,)).fetchone(), new_findings)

    def _timeline_dict(self, conn, row, new_findings):
        return {
            "timeline_id": row["timeline_id"],
            "device_id": row["device_id"],
            "version_id": row["version_id"],
            "algo_version": row["algo_version"],
            "generation": row["generation"],
            "input_hash": row["input_hash"],
            "timeline_hash": row["timeline_hash"],
            "created_at": row["created_at"],
            "new_findings": new_findings,
        }

    def list_timelines(self, device_id):
        with self.db.tx() as conn:
            rows = conn.execute(
                "SELECT * FROM timelines WHERE device_id=? ORDER BY created_at", (device_id,)).fetchall()
            return [self._timeline_dict(conn, r, 0) for r in rows]

    def timeline_points(self, timeline_id):
        rows = self.db.query(
            "SELECT p.event_id, p.corrected_ts, p.kind, p.value,"
            "       e.device_ts, e.server_ts, e.seq, e.batch_id"
            " FROM timeline_points p JOIN events e ON e.id=p.event_id"
            " WHERE p.timeline_id=? ORDER BY p.corrected_ts, p.event_id", (timeline_id,))
        if not rows:
            raise ApiError(404, "unknown timeline or no points")
        return [_row_to_dict(r) for r in rows]

    def list_events(self, device_id):
        self.get_device(device_id)
        rows = self.db.query(
            "SELECT id,batch_id,kind,seq,device_ts,server_ts,value FROM events"
            " WHERE device_id=? ORDER BY kind,seq", (device_id,))
        return [_row_to_dict(r) for r in rows]

    # ---------------------------------------------------------------- findings & reviews
    def list_findings(self, device_id=None, status=None):
        sql = "SELECT * FROM findings"
        cond, args = [], []
        if device_id:
            cond.append("device_id=?"); args.append(device_id)
        if status:
            cond.append("status=?"); args.append(status)
        if cond:
            sql += " WHERE " + " AND ".join(cond)
        sql += " ORDER BY created_at"
        return [_row_to_dict(r) for r in self.db.query(sql, args)]

    def get_finding(self, finding_id):
        row = self.db.one("SELECT * FROM findings WHERE finding_id=?", (finding_id,))
        if not row:
            raise ApiError(404, "unknown finding")
        return _row_to_dict(row)

    def review(self, finding_id, action, reviewer, note="", expect_version=None):
        """Accept / reject / return. The conditional UPDATE guarantees that
        concurrent reviews of the same finding produce exactly one outcome."""
        if action not in ("accept", "reject", "return"):
            raise ApiError(400, "action must be accept|reject|return")
        if not reviewer:
            raise ApiError(400, "reviewer is required")
        if expect_version is None:
            raise ApiError(400, "expect_version is required (optimistic concurrency)")
        new_status = {"accept": "accepted", "reject": "rejected", "return": "returned"}[action]
        with self.db.tx() as conn:
            cur = conn.execute(
                "UPDATE findings SET status=?, version=version+1, updated_at=?"
                " WHERE finding_id=? AND status='open' AND version=?",
                (new_status, now_ms(), finding_id, expect_version))
            if cur.rowcount == 0:
                row = conn.execute("SELECT status,version FROM findings WHERE finding_id=?",
                                   (finding_id,)).fetchone()
                if not row:
                    raise ApiError(404, "unknown finding")
                raise ApiError(409, "finding already decided (status=%s, version=%d)"
                               % (row["status"], row["version"]))
            conn.execute(
                "INSERT INTO reviews(finding_id,action,reviewer,note,decided_at) VALUES(?,?,?,?,?)",
                (finding_id, action, reviewer, note, now_ms()))
            return self.get_finding(finding_id)

    def recompute(self, finding_id, reviewer, note=""):
        """Rebuild correction + timeline + findings for a returned finding.
        Runs in one transaction: any failure rolls back completely and the
        finding is reopened with a failure record."""
        with self.db.tx() as conn:
            f = conn.execute("SELECT * FROM findings WHERE finding_id=?", (finding_id,)).fetchone()
            if not f:
                raise ApiError(404, "unknown finding")
            if f["status"] != "returned":
                raise ApiError(409, "only returned findings can be recomputed (status=%s)"
                              % f["status"])
            device_id = f["device_id"]
        try:
            with self.db.tx() as conn:
                params = self._params(conn, device_id)
                version_id = self._create_correction_tx(
                    conn, device_id, None, "recompute for %s" % finding_id)
                # A returned finding demands a fresh pipeline run: always build a
                # new timeline generation rather than reusing the idempotent one.
                generation = conn.execute(
                    "SELECT COALESCE(MAX(generation),0)+1 g FROM timelines WHERE device_id=?",
                    (device_id,)).fetchone()["g"]
                tl = self._build_timeline_tx(conn, device_id, version_id, params,
                                             generation=generation, idempotent=False)
                conn.execute(
                    "INSERT INTO reviews(finding_id,action,reviewer,note,decided_at)"
                    " VALUES(?,?,?,?,?)",
                    (finding_id, "recompute_ok", reviewer,
                     canonical({"timeline_id": tl["timeline_id"],
                                "timeline_hash": tl["timeline_hash"], "note": note}), now_ms()))
                return tl
        except ApiError:
            raise
        except Exception as e:
            with self.db.tx() as conn:
                conn.execute(
                    "UPDATE findings SET status='open', version=version+1, updated_at=?"
                    " WHERE finding_id=? AND status='returned'", (now_ms(), finding_id))
                conn.execute(
                    "INSERT INTO reviews(finding_id,action,reviewer,note,decided_at)"
                    " VALUES(?,?,?,?,?)",
                    (finding_id, "recompute_failed", reviewer, str(e), now_ms()))
            raise ApiError(500, "recompute failed and was rolled back: %s" % e)

    def finding_history(self, finding_id):
        self.get_finding(finding_id)
        rows = self.db.query(
            "SELECT * FROM reviews WHERE finding_id=? ORDER BY decided_at, id", (finding_id,))
        return [_row_to_dict(r) for r in rows]

    def list_reviews(self, device_id=None):
        if device_id:
            rows = self.db.query(
                "SELECT r.* FROM reviews r JOIN findings f ON f.finding_id=r.finding_id"
                " WHERE f.device_id=? ORDER BY r.decided_at, r.id", (device_id,))
        else:
            rows = self.db.query("SELECT * FROM reviews ORDER BY decided_at, id")
        return [_row_to_dict(r) for r in rows]

    # ---------------------------------------------------------------- reports
    def export_report(self, device_id, timeline_id):
        device = self.get_device(device_id)
        with self.db.tx() as conn:
            tl = conn.execute("SELECT * FROM timelines WHERE timeline_id=? AND device_id=?",
                              (timeline_id, device_id)).fetchone()
            if not tl:
                raise ApiError(404, "timeline not found for device")
            corr = self._correction_dict(conn, tl["version_id"])
            findings = [_row_to_dict(r) for r in conn.execute(
                "SELECT * FROM findings WHERE timeline_id=? ORDER BY start_ts", (timeline_id,))]
            fids = [f["finding_id"] for f in findings]
            reviews = []
            if fids:
                marks = ",".join("?" * len(fids))
                reviews = [_row_to_dict(r) for r in conn.execute(
                    "SELECT * FROM reviews WHERE finding_id IN (%s) ORDER BY decided_at,id" % marks,
                    fids)]
            payload = {
                "type": "audit-report",
                "device_id": device_id,
                "timeline_id": timeline_id,
                "timeline_hash": tl["timeline_hash"],
                "algo_version": tl["algo_version"],
                "correction": {"version_id": corr["version_id"], "a": corr["a"], "b": corr["b"]},
                "findings": findings,
                "reviews": reviews,
                "exported_at": now_ms(),
            }
            payload_canonical = canonical(payload)
            signature = hmac.new(device["secret"].encode("utf-8"),
                                 payload_canonical.encode("utf-8"),
                                 hashlib.sha256).hexdigest()
            report_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO reports(report_id,device_id,timeline_id,payload,signature,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (report_id, device_id, timeline_id, payload_canonical, signature, now_ms()))
            return {"device_id": device_id, "payload": payload, "signature": signature,
                    "report_id": report_id}

    def load_report(self, envelope):
        """Verify a signed report envelope. Tampered payloads and reports signed
        by a different device (cross-device) are rejected."""
        if not isinstance(envelope, dict):
            raise ApiError(400, "envelope must be an object")
        device_id = envelope.get("device_id")
        payload = envelope.get("payload")
        signature = envelope.get("signature")
        if not device_id or not isinstance(payload, dict) or not isinstance(signature, str):
            raise ApiError(400, "envelope must contain device_id, payload and signature")
        device = self.db.one("SELECT * FROM devices WHERE device_id=?", (device_id,))
        if not device:
            raise ApiError(403, "unknown device: report refused")
        if payload.get("device_id") != device_id:
            raise ApiError(403, "payload device mismatch: report refused")
        expected = hmac.new(device["secret"].encode("utf-8"),
                            canonical(payload).encode("utf-8"),
                            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ApiError(403, "signature invalid: tampered or cross-device report refused")
        return {"ok": True, "payload": payload}

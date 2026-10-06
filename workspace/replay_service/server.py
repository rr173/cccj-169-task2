"""HTTP API + static UI, built on the standard library only."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .core import Service, ApiError

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def make_handler(service: Service):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        # ---------------- plumbing ----------------
        def log_message(self, *args):
            pass

        def _send_json(self, status, obj):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(self, status, body, content_type):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def _json_body(self):
            raw = self._body()
            if not raw:
                return {}
            try:
                return json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise ApiError(400, "request body is not valid JSON")

        # ---------------- routing ----------------
        def _dispatch(self, method):
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            qs = parse_qs(parsed.query)
            seg = [s for s in path.split("/") if s]
            try:
                if path == "/" and method == "GET":
                    return self._serve_index()
                if seg and seg[0] == "api":
                    return self._api(method, seg[1:], qs)
                self._send_json(404, {"error": "not found"})
            except ApiError as e:
                self._send_json(e.status, {"error": e.message})
            except BrokenPipeError:
                pass
            except Exception as e:  # pragma: no cover - defensive
                self._send_json(500, {"error": "internal error: %s" % e})

        def _serve_index(self):
            with open(os.path.join(STATIC_DIR, "index.html"), "rb") as f:
                self._send_bytes(200, f.read(), "text/html; charset=utf-8")

        def _api(self, method, seg, qs):
            svc = service
            body = None

            def json_body():
                nonlocal body
                if body is None:
                    body = self._json_body()
                return body

            # ---- devices ----
            if seg == ["devices"] and method == "POST":
                b = json_body()
                return self._send_json(201, svc.create_device(
                    b.get("device_id"), b.get("name"), b.get("params")))
            if seg == ["devices"] and method == "GET":
                return self._send_json(200, svc.list_devices())

            # ---- uploads (chunked, resumable, idempotent) ----
            if seg == ["uploads"] and method == "POST":
                b = json_body()
                return self._send_json(200, svc.open_upload(
                    b.get("device_id"), b.get("batch_id"),
                    int(b.get("total_chunks") or 0), b.get("package_sha256")))
            if len(seg) == 2 and seg[0] == "uploads" and method == "GET":
                return self._send_json(200, svc.get_upload(seg[1]))
            if len(seg) == 4 and seg[0] == "uploads" and seg[2] == "chunks" and method == "PUT":
                result = svc.put_chunk(seg[1], int(seg[3]), self._body(),
                                       self.headers.get("X-Chunk-SHA256") or "")
                return self._send_json(200, result)
            if len(seg) == 3 and seg[0] == "uploads" and seg[2] == "seal" and method == "POST":
                return self._send_json(200, svc.seal_upload(seg[1]))

            # ---- batch import ----
            if seg == ["batches", "import"] and method == "POST":
                b = json_body()
                return self._send_json(200, svc.import_batch(b.get("device_id"), b.get("batch_id")))

            # ---- corrections ----
            if len(seg) == 3 and seg[0] == "devices" and seg[2] == "corrections":
                if method == "POST":
                    b = json_body()
                    return self._send_json(201, svc.create_correction(
                        seg[1], b.get("source_batch_id"), b.get("note", "")))
                if method == "GET":
                    return self._send_json(200, svc.list_corrections(seg[1]))
            if len(seg) == 2 and seg[0] == "corrections":
                if method == "PUT":
                    b = json_body()
                    return self._send_json(200, svc.update_correction(
                        seg[1], int(b["a_num"]), int(b["a_den"]),
                        int(b["b_num"]), int(b["b_den"]), b.get("note")))
                if method == "DELETE":
                    return self._send_json(200, svc.delete_correction(seg[1]))
            if len(seg) == 3 and seg[0] == "corrections" and seg[2] == "supersede" and method == "POST":
                return self._send_json(200, svc.supersede_correction(seg[1]))

            # ---- timelines ----
            if seg == ["timelines", "build"] and method == "POST":
                b = json_body()
                return self._send_json(200, svc.build_timeline(
                    b.get("device_id"), b.get("version_id")))
            if len(seg) == 3 and seg[0] == "devices" and seg[2] == "timelines" and method == "GET":
                return self._send_json(200, svc.list_timelines(seg[1]))
            if len(seg) == 3 and seg[0] == "timelines" and seg[2] == "points" and method == "GET":
                return self._send_json(200, svc.timeline_points(seg[1]))
            if len(seg) == 3 and seg[0] == "devices" and seg[2] == "events" and method == "GET":
                return self._send_json(200, svc.list_events(seg[1]))

            # ---- findings & reviews ----
            if seg == ["findings"] and method == "GET":
                return self._send_json(200, svc.list_findings(
                    qs.get("device_id", [None])[0], qs.get("status", [None])[0]))
            if len(seg) == 2 and seg[0] == "findings" and method == "GET":
                return self._send_json(200, svc.get_finding(seg[1]))
            if len(seg) == 3 and seg[0] == "findings" and seg[2] == "reviews" and method == "POST":
                b = json_body()
                return self._send_json(200, svc.review(
                    seg[1], b.get("action"), b.get("reviewer"),
                    b.get("note", ""), b.get("version")))
            if len(seg) == 3 and seg[0] == "findings" and seg[2] == "recompute" and method == "POST":
                b = json_body()
                return self._send_json(200, svc.recompute(
                    seg[1], b.get("reviewer", "system"), b.get("note", "")))
            if len(seg) == 3 and seg[0] == "findings" and seg[2] == "history" and method == "GET":
                return self._send_json(200, svc.finding_history(seg[1]))
            if seg == ["reviews"] and method == "GET":
                return self._send_json(200, svc.list_reviews(qs.get("device_id", [None])[0]))

            # ---- signed audit reports ----
            if seg == ["reports", "export"] and method == "POST":
                b = json_body()
                return self._send_json(200, svc.export_report(
                    b.get("device_id"), b.get("timeline_id")))
            if seg == ["reports", "load"] and method == "POST":
                return self._send_json(200, svc.load_report(json_body()))

            self._send_json(404, {"error": "not found"})

        do_GET = lambda self: self._dispatch("GET")
        do_POST = lambda self: self._dispatch("POST")
        do_PUT = lambda self: self._dispatch("PUT")
        do_DELETE = lambda self: self._dispatch("DELETE")

    return Handler


def serve(db_path, host="127.0.0.1", port=8000):
    from .db import Database
    svc = Service(Database(db_path))
    server = ThreadingHTTPServer((host, port), make_handler(svc))
    print("replay-service listening on http://%s:%d (db=%s)" % (host, port, db_path))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

#!/usr/bin/env python3
"""Entry point: python3 run.py [--db replay.db] [--host 127.0.0.1] [--port 8000]"""
import argparse

from replay_service.server import serve

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="cross-device process event replay & risk review service")
    ap.add_argument("--db", default="replay.db")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    serve(args.db, args.host, args.port)

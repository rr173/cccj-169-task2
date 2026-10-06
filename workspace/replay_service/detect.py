"""Timeline anomaly detectors: continuous limit-exceedance, missing intervals,
and consumable-meter jumps. Pure functions over corrected timeline points."""

DEFAULT_PARAMS = {
    "temp_limit": 80.0,               # °C
    "pressure_limit": 10.0,           # bar
    "exceed_min_consecutive": 3,      # points
    "exceed_min_duration_ms": 5000,
    "gap_threshold_ms": 10000,
    "meter_jump_threshold": 50.0,     # absolute delta in meter reading
}

_KIND_LIMIT = (("temp", "temp_limit"), ("pressure", "pressure_limit"))


def validate_params(params):
    for key, default in DEFAULT_PARAMS.items():
        v = params.get(key, default)
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            raise ValueError("param %r must be numeric, got %r" % (key, v))
        if v <= 0:
            raise ValueError("param %r must be positive, got %r" % (key, v))
    return {**DEFAULT_PARAMS, **params}


def detect_findings(points, params):
    """points: [{event_id, kind, value, corrected_ts}] sorted by corrected_ts.
    Returns [{type, start_ts, end_ts, details}]."""
    params = validate_params(params)
    findings = []
    findings += _detect_exceed(points, params)
    findings += _detect_gaps(points, params)
    findings += _detect_meter_jumps(points, params)
    findings.sort(key=lambda f: (f["start_ts"], f["type"]))
    return findings


def _detect_exceed(points, params):
    out = []
    for kind, limit_key in _KIND_LIMIT:
        limit = params[limit_key]
        series = [p for p in points if p["kind"] == kind]
        run = []
        for p in series:
            if p["value"] > limit:
                run.append(p)
            else:
                _flush_run(run, kind, limit, params, out)
                run = []
        _flush_run(run, kind, limit, params, out)
    return out


def _flush_run(run, kind, limit, params, out):
    if not run:
        return
    duration = run[-1]["corrected_ts"] - run[0]["corrected_ts"]
    if len(run) >= params["exceed_min_consecutive"] or duration >= params["exceed_min_duration_ms"]:
        out.append({
            "type": "exceed",
            "start_ts": run[0]["corrected_ts"],
            "end_ts": run[-1]["corrected_ts"],
            "details": {
                "kind": kind,
                "limit": limit,
                "count": len(run),
                "duration_ms": duration,
                "max_value": max(p["value"] for p in run),
            },
        })


def _detect_gaps(points, params):
    out = []
    threshold = params["gap_threshold_ms"]
    ordered = sorted(points, key=lambda p: (p["corrected_ts"], p["event_id"]))
    for prev, cur in zip(ordered, ordered[1:]):
        gap = cur["corrected_ts"] - prev["corrected_ts"]
        if gap > threshold:
            out.append({
                "type": "gap",
                "start_ts": prev["corrected_ts"],
                "end_ts": cur["corrected_ts"],
                "details": {"gap_ms": gap, "threshold_ms": threshold},
            })
    return out


def _detect_meter_jumps(points, params):
    out = []
    threshold = params["meter_jump_threshold"]
    series = [p for p in points if p["kind"] == "material"]
    for prev, cur in zip(series, series[1:]):
        delta = cur["value"] - prev["value"]
        if delta < 0 or abs(delta) > threshold:
            out.append({
                "type": "meter_jump",
                "start_ts": prev["corrected_ts"],
                "end_ts": cur["corrected_ts"],
                "details": {
                    "prev_value": prev["value"],
                    "cur_value": cur["value"],
                    "delta": delta,
                    "threshold": threshold,
                    "counter_reset": delta < 0,
                },
            })
    return out

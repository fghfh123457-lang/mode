#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Continuous Performance and Reliability Testing Platform for Web Services

Single file, standard library only (Python 3.8+). Just run it:
    python perfmon.py
Then open http://127.0.0.1:8080 (a free port is picked automatically if busy).

Features:
  1) Continuous monitoring: up / slow / down, response time, success & error rate
  2) SQLite storage + performance trend (improving / degrading)
  3) Load test and capacity test (P50 / P95 / P99, throughput)
  4) Event log, reports, settings, light/dark dashboard, CSV export

Note: only run load tests against services you own or have permission to test.
"""

import argparse
import csv
import io
import json
import math
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(BASE_DIR, "perfmon.db")
RETENTION_DAYS = 30            #
MAX_LOAD_CONCURRENCY = 200     #
MAX_LOAD_REQUESTS = 5000       #
CAPACITY_ERROR_LIMIT = 5.0     # %
CAPACITY_P95_LIMIT_MS = 2000   #  P95
TREND_THRESHOLD = 10.0         # %    /

SLOW_MS_DEFAULT = 1000       #     "" (   )

SCHEMA = """
CREATE TABLE IF NOT EXISTS targets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'GET',
    interval_sec INTEGER NOT NULL DEFAULT 30,
    timeout_sec INTEGER NOT NULL DEFAULT 10,
    expected_status INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS checks(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id INTEGER NOT NULL,
    ts REAL NOT NULL,
    status_code INTEGER,
    response_ms REAL,
    success INTEGER NOT NULL,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_checks_target_ts ON checks(target_id, ts);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id INTEGER NOT NULL,
    ts REAL NOT NULL,
    type TEXT NOT NULL,
    message TEXT
);
CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS load_tests(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id INTEGER NOT NULL,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    params TEXT,
    result TEXT
);
"""


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def execute(self, sql, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur.lastrowid

    def query(self, sql, params=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
def is_success(status, expected):
    if status is None:
        return False
    if expected:
        return status == expected
    return 200 <= status < 400


def http_request(url, method, timeout):
    """\u064a\u0646\u0641\u0630 \u0637\u0644\u0628 HTTP \u0648\u0627\u062d\u062f \u0648\u064a\u0631\u062c\u0639 (status, response_ms, error)."""
    start = time.perf_counter()
    status, err = None, None
    try:
        req = urllib.request.Request(
            url, method=method, headers={"User-Agent": "PerfMonitor/1.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read(65536)
            status = resp.status
    except urllib.error.HTTPError as e:
        status, err = e.code, "HTTP %d" % e.code
    except urllib.error.URLError as e:
        err = str(e.reason)
    except Exception as e:  # timeout, ssl, connection reset ...
        err = "%s: %s" % (type(e).__name__, e)
    ms = (time.perf_counter() - start) * 1000.0
    return status, ms, err


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(math.floor(k))
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def latency_stats(lat):
    if not lat:
        return {"avg": 0, "min": 0, "max": 0, "p50": 0, "p95": 0, "p99": 0}
    s = sorted(lat)
    return {
        "avg": sum(s) / len(s),
        "min": s[0],
        "max": s[-1],
        "p50": percentile(s, 50),
        "p95": percentile(s, 95),
        "p99": percentile(s, 99),
    }


def summarize(rows):
    """rows: \u0642\u0627\u0626\u0645\u0629 dict \u0641\u064a\u0647\u0627 response_ms \u0648 success."""
    total = len(rows)
    ok_rows = [r for r in rows if r["success"]]
    st = latency_stats([r["response_ms"] for r in ok_rows if r["response_ms"] is not None])
    st["total"] = total
    st["ok"] = len(ok_rows)
    st["success_rate"] = (len(ok_rows) / total * 100.0) if total else 0.0
    st["error_rate"] = (100.0 - st["success_rate"]) if total else 0.0
    return st


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
def run_load_test(target, concurrency, total, progress=None):
    url, method = target["url"], target["method"]
    timeout, expected = target["timeout_sec"], target["expected_status"]
    lat, codes, errs, ok, done = [], {}, {}, 0, 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(http_request, url, method, timeout) for _ in range(total)]
        for f in as_completed(futs):
            status, ms, err = f.result()
            done += 1
            key = str(status) if status else "no-response"
            codes[key] = codes.get(key, 0) + 1
            if is_success(status, expected):
                ok += 1
                lat.append(ms)
            else:
                e = err or ("HTTP %s" % status)
                errs[e] = errs.get(e, 0) + 1
            if progress and done % 10 == 0:
                progress(done, total)
    dur = time.perf_counter() - t0
    res = latency_stats(lat)
    res.update({
        "total": total,
        "concurrency": concurrency,
        "duration_s": dur,
        "rps": total / dur if dur > 0 else 0,
        "success_rate": ok / total * 100.0,
        "error_rate": (total - ok) / total * 100.0,
        "status_codes": codes,
        "errors": dict(sorted(errs.items(), key=lambda kv: -kv[1])[:5]),
    })
    return res


def run_capacity_test(target, max_concurrency, job):
    """\u064a\u0631\u0641\u0639 \u0627\u0644\u062d\u0645\u0644 \u062a\u062f\u0631\u064a\u062c\u064a\u0627\u064b \u0648\u064a\u062a\u0648\u0642\u0641 \u0639\u0646\u062f \u062a\u062c\u0627\u0648\u0632 \u062d\u062f\u0648\u062f \u0627\u0644\u0623\u062e\u0637\u0627\u0621 \u0623\u0648 \u0632\u0645\u0646 \u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629."""
    levels = [l for l in (1, 5, 10, 20, 40, 80, 150, 200) if l <= max_concurrency]
    if not levels:
        levels = [1]
    steps, max_ok, reason = [], 0, "\u062a\u062d\u0645\u0644\u062a \u0627\u0644\u062e\u062f\u0645\u0629 \u0643\u0644 \u0627\u0644\u0645\u0633\u062a\u0648\u064a\u0627\u062a \u0627\u0644\u0645\u062e\u062a\u0628\u0631\u0629"
    for lvl in levels:
        n = min(max(lvl * 10, 30), 1000)
        job["progress"] = "\u0627\u062e\u062a\u0628\u0627\u0631 \u0645\u0633\u062a\u0648\u0649 %d \u0645\u0633\u062a\u062e\u062f\u0645 \u0645\u062a\u0632\u0627\u0645\u0646..." % lvl
        r = run_load_test(target, lvl, n)
        passed = r["error_rate"] <= CAPACITY_ERROR_LIMIT and r["p95"] <= CAPACITY_P95_LIMIT_MS
        steps.append({
            "concurrency": lvl, "requests": n, "rps": r["rps"], "avg": r["avg"],
            "p95": r["p95"], "error_rate": r["error_rate"], "passed": passed,
        })
        if passed:
            max_ok = lvl
        else:
            reason = "\u062a\u0648\u0642\u0641 \u0627\u0644\u0627\u062e\u062a\u0628\u0627\u0631 \u0639\u0646\u062f %d: %s" % (
                lvl,
                "\u0646\u0633\u0628\u0629 \u0627\u0644\u0623\u062e\u0637\u0627\u0621 \u062a\u062c\u0627\u0648\u0632\u062a %.0f%%" % CAPACITY_ERROR_LIMIT
                if r["error_rate"] > CAPACITY_ERROR_LIMIT
                else "P95 \u062a\u062c\u0627\u0648\u0632 %d ms" % CAPACITY_P95_LIMIT_MS,
            )
            break
    return {"steps": steps, "max_ok_concurrency": max_ok, "reason": reason,
            "limits": {"error_rate": CAPACITY_ERROR_LIMIT, "p95_ms": CAPACITY_P95_LIMIT_MS}}


class JobManager:
    """\u064a\u0634\u063a\u0644 \u0627\u062e\u062a\u0628\u0627\u0631 \u062d\u0645\u0644 \u0648\u0627\u062d\u062f \u0641\u0642\u0637 \u0641\u064a \u0646\u0641\u0633 \u0627\u0644\u0648\u0642\u062a \u062d\u062a\u0649 \u0644\u0627 \u0646\u0636\u063a\u0637 \u0639\u0644\u0649 \u0627\u0644\u062c\u0647\u0627\u0632/\u0627\u0644\u062e\u062f\u0645\u0629."""

    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self.current = None

    def start(self, target, kind, concurrency, total):
        with self.lock:
            if self.current and self.current["status"] == "running":
                raise RuntimeError("\u064a\u0648\u062c\u062f \u0627\u062e\u062a\u0628\u0627\u0631 \u0642\u064a\u062f \u0627\u0644\u062a\u0646\u0641\u064a\u0630 \u062d\u0627\u0644\u064a\u0627\u064b\u060c \u0627\u0646\u062a\u0638\u0631 \u0627\u0646\u062a\u0647\u0627\u0621\u0647")
            job = {"id": int(time.time() * 1000), "target_id": target["id"],
                   "target_name": target["name"], "kind": kind, "status": "running",
                   "progress": "\u062c\u0627\u0631\u064a \u0627\u0644\u062a\u062d\u0636\u064a\u0631...", "result": None, "error": None}
            self.current = job
        threading.Thread(target=self._run, args=(job, target, kind, concurrency, total),
                         daemon=True).start()
        return job

    def _run(self, job, target, kind, concurrency, total):
        try:
            if kind == "capacity":
                res = run_capacity_test(target, concurrency, job)
            else:
                def cb(done, tot):
                    job["progress"] = "\u062a\u0645 \u062a\u0646\u0641\u064a\u0630 %d \u0645\u0646 %d \u0637\u0644\u0628" % (done, tot)
                res = run_load_test(target, concurrency, total, cb)
            self.db.execute(
                "INSERT INTO load_tests(target_id,ts,kind,params,result) VALUES(?,?,?,?,?)",
                (target["id"], time.time(), kind,
                 json.dumps({"concurrency": concurrency, "total": total}), json.dumps(res)))
            job["result"] = res
            job["status"] = "done"
        except Exception as e:
            job["error"] = str(e)
            job["status"] = "error"


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
class Monitor(threading.Thread):
    def __init__(self, db):
        super().__init__(daemon=True)
        self.db = db
        self.stop_event = threading.Event()
        self.last_run = {}
        self.state = {}
        self.last_purge = 0

    def log_event(self, tid, etype, msg):
        self.db.execute("INSERT INTO events(target_id,ts,type,message) VALUES(?,?,?,?)",
                        (tid, time.time(), etype, msg))

    def check(self, t):
        status, ms, err = http_request(t["url"], t["method"], t["timeout_sec"])
        ok = is_success(status, t["expected_status"])
        if not ok and not err:
            err = "HTTP %s (\u0627\u0644\u0645\u062a\u0648\u0642\u0639 %s)" % (status, t["expected_status"])
        self.db.execute(
            "INSERT INTO checks(target_id,ts,status_code,response_ms,success,error) VALUES(?,?,?,?,?,?)",
            (t["id"], time.time(), status, ms, 1 if ok else 0, err))
        slow_ms = get_slow_ms(self.db)
        prev = self.state.get(t["id"])
        # A received HTTP response (for example 404/500) means the server responded;
        # classify it separately from a connection/timeout failure. It remains an
        # unsuccessful check for reliability calculations.
        if status is not None and status >= 400:
            new = "http_error"
        elif not ok:
            new = "down"
        elif ms > slow_ms or (prev == "slow" and ms >= slow_ms * 0.8):
            new = "slow"
        else:
            new = "up"
        if new != prev:
            if new == "down":
                self.log_event(t["id"], "DOWN", "\u0627\u0644\u062e\u062f\u0645\u0629 \u0645\u062a\u0648\u0642\u0641\u0629 \u0623\u0648 \u0644\u0627 \u062a\u0633\u062a\u062c\u064a\u0628: %s" % err)
            elif new == "http_error":
                self.log_event(t["id"], "HTTP_ERROR", "HTTP %s - \u0627\u0644\u062e\u0627\u062f\u0645 \u064a\u0631\u062f \u0628\u062d\u0627\u0644\u0629 HTTP \u063a\u064a\u0631 \u0646\u0627\u062c\u062d\u0629" % status)
            elif new == "slow":
                self.log_event(t["id"], "SLOW", "\u0627\u0633\u062a\u062c\u0627\u0628\u0629 \u0628\u0637\u064a\u0626\u0629 (%.0f ms)" % ms)
            else:
                self.log_event(t["id"], "UP", "\u0627\u0644\u062e\u062f\u0645\u0629 \u062a\u0639\u0645\u0644 \u0628\u0634\u0643\u0644 \u0637\u0628\u064a\u0639\u064a (%.0f ms)" % ms)
        self.state[t["id"]] = new

    def run(self):
        pool = ThreadPoolExecutor(max_workers=16)
        while not self.stop_event.is_set():
            try:
                now = time.time()
                for t in self.db.query("SELECT * FROM targets WHERE enabled=1"):
                    if now - self.last_run.get(t["id"], 0) >= t["interval_sec"]:
                        self.last_run[t["id"]] = now
                        pool.submit(self._safe_check, t)
                if now - self.last_purge > 3600:
                    self.last_purge = now
                    cutoff = now - RETENTION_DAYS * 86400
                    self.db.execute("DELETE FROM checks WHERE ts<?", (cutoff,))
            except Exception as e:
                print("Monitor error:", e)
            self.stop_event.wait(1)
        pool.shutdown(wait=False)

    def _safe_check(self, t):
        try:
            self.check(t)
        except Exception as e:
            print("Check error:", e)


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
def build_summary(db, t, slow_ms=SLOW_MS_DEFAULT):
    now = time.time()
    hour = 3600
    q = "SELECT response_ms,success FROM checks WHERE target_id=? AND ts>=? AND ts<?"
    cur = db.query(q, (t["id"], now - hour, now + 1))
    prev = db.query(q, (t["id"], now - 2 * hour, now - hour))
    day = db.query(q, (t["id"], now - 86400, now + 1))
    last = db.query("SELECT * FROM checks WHERE target_id=? ORDER BY ts DESC LIMIT 1", (t["id"],))
    s_cur, s_prev, s_day = summarize(cur), summarize(prev), summarize(day)

    trend, change = "unknown", None
    if s_cur["ok"] >= 3 and s_prev["ok"] >= 3 and s_prev["avg"] > 0:
        change = (s_cur["avg"] - s_prev["avg"]) / s_prev["avg"] * 100.0
        if change > TREND_THRESHOLD:
            trend = "degrading"
        elif change < -TREND_THRESHOLD:
            trend = "improving"
        else:
            trend = "stable"
        if s_cur["success_rate"] - s_prev["success_rate"] < -5:
            trend = "degrading"

    status = "unknown"
    if last:
        if last[0]["status_code"] is not None and last[0]["status_code"] >= 400:
            status = "http_error"
        elif not last[0]["success"]:
            status = "down"
        elif (last[0]["response_ms"] or 0) > slow_ms:
            status = "slow"
        else:
            status = "up"
    return {
        "id": t["id"], "name": t["name"], "url": t["url"], "method": t["method"],
        "interval_sec": t["interval_sec"], "status": status,
        "last_ms": last[0]["response_ms"] if last else None,
        "last_error": last[0]["error"] if last else None,
        "last_ts": last[0]["ts"] if last else None,
        "uptime_24h": s_day["success_rate"] if s_day["total"] else None,
        "hour": s_cur, "trend": trend, "trend_change": change,
    }


def build_history(db, tid, minutes, max_points=600):
    since = time.time() - minutes * 60
    rows = db.query("SELECT ts,response_ms,success FROM checks WHERE target_id=? AND ts>=? ORDER BY ts",
                    (tid, since))
    if len(rows) <= max_points:
        return [{"t": r["ts"], "v": r["response_ms"], "ok": bool(r["success"])} for r in rows]
    size = int(math.ceil(len(rows) / float(max_points)))
    out = []
    for i in range(0, len(rows), size):
        ch = rows[i:i + size]
        out.append({"t": ch[0]["ts"], "v": sum(c["response_ms"] or 0 for c in ch) / len(ch),
                    "ok": all(c["success"] for c in ch)})
    return out


def get_setting(db, key, default):
    rows = db.query("SELECT value FROM settings WHERE key=?", (key,))
    return rows[0]["value"] if rows else default


def get_slow_ms(db):
    try:
        return max(50, int(get_setting(db, "slow_ms", SLOW_MS_DEFAULT)))
    except Exception:
        return SLOW_MS_DEFAULT


def build_overview(db, minutes, target_id=None, buckets=48):
    """\u0645\u0624\u0634\u0631\u0627\u062a \u0622\u062e\u0631 24 \u0633\u0627\u0639\u0629 (\u0645\u0642\u0627\u0628\u0644 24 \u0633\u0627\u0639\u0629 \u0642\u0628\u0644\u0647\u0627) + \u0633\u0644\u0633\u0644\u0629 \u0632\u0645\u0646\u064a\u0629 \u0644\u0631\u0633\u0645 \u0632\u0645\u0646 \u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629."""
    now = time.time()

    def kpi(t_from, t_to):
        rows = db.query(
            "SELECT target_id, COUNT(*) n, SUM(success) ok, "
            "AVG(CASE WHEN success=1 THEN response_ms END) avg "
            "FROM checks WHERE ts>=? AND ts<? GROUP BY target_id", (t_from, t_to))
        n = sum(r["n"] for r in rows)
        if not n:
            return None
        ok = sum(r["ok"] or 0 for r in rows)
        per = [(r["ok"] or 0) / r["n"] * 100.0 for r in rows]
        wn = sum(r["ok"] or 0 for r in rows if r["avg"] is not None)
        wsum = sum((r["avg"] or 0) * (r["ok"] or 0) for r in rows if r["avg"] is not None)
        return {"availability": sum(per) / len(per), "success_rate": ok / n * 100.0,
                "error_rate": 100.0 - ok / n * 100.0,
                "avg_ms": (wsum / wn) if wn else None, "checks": n}

    span = minutes * 60.0
    size = span / buckets
    start = now - span
    sql = ("SELECT CAST((ts-?)/? AS INTEGER) b, AVG(CASE WHEN success=1 THEN response_ms END) v, "
           "COUNT(*) n, SUM(1-success) f FROM checks WHERE ts>=?")
    params = [start, size, start]
    if target_id:
        sql += " AND target_id=?"
        params.append(target_id)
    sql += " GROUP BY b ORDER BY b"
    series = [{"t": start + (r["b"] + 0.5) * size, "v": r["v"], "n": r["n"], "f": r["f"] or 0}
              for r in db.query(sql, params) if 0 <= r["b"] <= buckets]
    return {"kpi_cur": kpi(now - 86400, now + 1), "kpi_prev": kpi(now - 172800, now - 86400),
            "series": series, "minutes": minutes}


def build_report(db, days):
    since = time.time() - days * 86400
    out = []
    for t in db.query("SELECT * FROM targets ORDER BY id"):
        rows = db.query("SELECT response_ms,success FROM checks WHERE target_id=? AND ts>=?",
                        (t["id"], since))
        st = summarize(rows)
        outages = db.query("SELECT COUNT(*) c FROM events WHERE target_id=? AND ts>=? AND type='DOWN'",
                           (t["id"], since))[0]["c"]
        out.append({"id": t["id"], "name": t["name"], "url": t["url"], "checks": st["total"],
                    "uptime": st["success_rate"], "avg": st["avg"], "p95": st["p95"],
                    "p99": st["p99"], "max": st["max"], "outages": outages})
    return out


def validate_target(d):
    url = str(d.get("url", "")).strip()
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.netloc:
        raise ValueError("\u0627\u0644\u0631\u0627\u0628\u0637 \u063a\u064a\u0631 \u0635\u0627\u0644\u062d (\u064a\u062c\u0628 \u0623\u0646 \u064a\u0628\u062f\u0623 \u0628\u0640 http:// \u0623\u0648 https://)")
    method = str(d.get("method", "GET")).upper()
    if method not in ("GET", "HEAD"):
        raise ValueError("\u0627\u0644\u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u0645\u0633\u0645\u0648\u062d\u0629 GET \u0623\u0648 HEAD \u0641\u0642\u0637")
    interval = max(5, min(int(d.get("interval_sec") or 30), 3600))
    timeout = max(1, min(int(d.get("timeout_sec") or 10), 60))
    exp = d.get("expected_status")
    exp = int(exp) if str(exp or "").strip() else None
    name = str(d.get("name", "")).strip() or p.netloc
    return name[:80], url, method, interval, timeout, exp


# ----------------------------------------------------------------------------
#   (API + Dashboard)
# ----------------------------------------------------------------------------
class App:
    db = None
    jobs = None
    monitor = None


class Handler(BaseHTTPRequestHandler):
    server_version = "PerfMonitor/1.0"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8", headers=None):
        if not isinstance(body, bytes):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 100000:
            raise ValueError("\u0627\u0644\u0637\u0644\u0628 \u0643\u0628\u064a\u0631 \u062c\u062f\u0627\u064b")
        return json.loads(self.rfile.read(n) or b"{}")

    def _target(self, tid):
        rows = App.db.query("SELECT * FROM targets WHERE id=?", (int(tid),))
        if not rows:
            raise ValueError("\u0627\u0644\u062e\u062f\u0645\u0629 \u063a\u064a\u0631 \u0645\u0648\u062c\u0648\u062f\u0629")
        return rows[0]

    def do_GET(self):
        try:
            u = urlparse(self.path)
            qs = {k: v[0] for k, v in parse_qs(u.query).items()}
            db = App.db
            if u.path == "/":
                return self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
            if u.path == "/api/summary":
                slow = get_slow_ms(db)
                return self._send(200, {"slow_ms": slow, "targets": [
                    build_summary(db, t, slow) for t in db.query("SELECT * FROM targets ORDER BY id")]})
            if u.path == "/api/overview":
                minutes = max(5, min(int(qs.get("minutes", 1440)), 43200))
                tid = int(qs["target_id"]) if qs.get("target_id") else None
                return self._send(200, build_overview(db, minutes, tid))
            if u.path == "/api/report":
                return self._send(200, build_report(db, max(1, min(int(qs.get("days", 1)), 30))))
            if u.path == "/api/settings":
                return self._send(200, {"slow_ms": get_slow_ms(db), "retention_days": RETENTION_DAYS,
                                        "version": "2.0"})
            if u.path == "/api/history":
                minutes = max(1, min(int(qs.get("minutes", 60)), 60 * 24 * 30))
                return self._send(200, build_history(db, int(qs["target_id"]), minutes))
            if u.path == "/api/events":
                limit = max(1, min(int(qs.get("limit", 50)), 500))
                rows = db.query(
                    "SELECT e.ts,e.type,e.message,t.name FROM events e "
                    "LEFT JOIN targets t ON t.id=e.target_id ORDER BY e.ts DESC LIMIT ?", (limit,))
                return self._send(200, rows)
            if u.path == "/api/loadtests":
                rows = db.query("SELECT * FROM load_tests WHERE target_id=? ORDER BY ts DESC LIMIT 5",
                                (int(qs["target_id"]),))
                for r in rows:
                    r["params"] = json.loads(r["params"] or "{}")
                    r["result"] = json.loads(r["result"] or "{}")
                return self._send(200, rows)
            if u.path == "/api/job":
                return self._send(200, App.jobs.current or {"status": "idle"})
            if u.path == "/api/export.csv":
                tid = int(qs["target_id"])
                minutes = max(1, min(int(qs.get("minutes", 1440)), 60 * 24 * 30))
                rows = db.query("SELECT * FROM checks WHERE target_id=? AND ts>=? ORDER BY ts",
                                (tid, time.time() - minutes * 60))
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["time", "status_code", "response_ms", "success", "error"])
                for r in rows:
                    w.writerow([datetime.fromtimestamp(r["ts"]).isoformat(sep=" ", timespec="seconds"),
                                r["status_code"], round(r["response_ms"] or 0, 1), r["success"], r["error"] or ""])
                return self._send(200, ("\ufeff" + buf.getvalue()).encode("utf-8"),
                                  "text/csv; charset=utf-8",
                                  {"Content-Disposition": "attachment; filename=target_%d.csv" % tid})
            self._send(404, {"error": "\u063a\u064a\u0631 \u0645\u0648\u062c\u0648\u062f"})
        except Exception as e:
            self._send(400, {"error": str(e)})

    def do_POST(self):
        try:
            u = urlparse(self.path)
            d = self._body()
            if u.path == "/api/targets":
                name, url, method, interval, timeout, exp = validate_target(d)
                tid = App.db.execute(
                    "INSERT INTO targets(name,url,method,interval_sec,timeout_sec,expected_status,created_at) "
                    "VALUES(?,?,?,?,?,?,?)", (name, url, method, interval, timeout, exp, time.time()))
                return self._send(200, {"id": tid})
            if u.path == "/api/settings":
                slow = max(50, min(int(d.get("slow_ms")), 60000))
                App.db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('slow_ms',?)", (str(slow),))
                return self._send(200, {"slow_ms": slow})
            if u.path == "/api/loadtest":
                t = self._target(d["target_id"])
                kind = "capacity" if d.get("kind") == "capacity" else "load"
                conc = max(1, min(int(d.get("concurrency") or 10), MAX_LOAD_CONCURRENCY))
                total = max(1, min(int(d.get("total") or 200), MAX_LOAD_REQUESTS))
                if not d.get("confirmed"):
                    raise ValueError("\u064a\u062c\u0628 \u062a\u0623\u0643\u064a\u062f \u0623\u0646\u0643 \u062a\u0645\u0644\u0643 \u0627\u0644\u062e\u062f\u0645\u0629 \u0623\u0648 \u0644\u062f\u064a\u0643 \u0625\u0630\u0646 \u0644\u0627\u062e\u062a\u0628\u0627\u0631\u0647\u0627")
                job = App.jobs.start(t, kind, conc, total)
                return self._send(200, {"job_id": job["id"]})
            self._send(404, {"error": "\u063a\u064a\u0631 \u0645\u0648\u062c\u0648\u062f"})
        except Exception as e:
            self._send(400, {"error": str(e)})

    def do_DELETE(self):
        try:
            parts = urlparse(self.path).path.strip("/").split("/")
            if len(parts) == 3 and parts[:2] == ["api", "targets"]:
                tid = int(parts[2])
                for tbl in ("checks", "events", "load_tests"):
                    App.db.execute("DELETE FROM %s WHERE target_id=?" % tbl, (tid,))
                App.db.execute("DELETE FROM targets WHERE id=?", (tid,))
                return self._send(200, {"ok": True})
            self._send(404, {"error": "\u063a\u064a\u0631 \u0645\u0648\u062c\u0648\u062f"})
        except Exception as e:
            self._send(400, {"error": str(e)})


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Web Service Monitor | &#1605;&#1606;&#1589;&#1577; &#1575;&#1582;&#1578;&#1576;&#1575;&#1585; &#1571;&#1583;&#1575;&#1569; &#1608;&#1605;&#1608;&#1579;&#1608;&#1602;&#1610;&#1577; &#1582;&#1583;&#1605;&#1575;&#1578; &#1575;&#1604;&#1608;&#1610;&#1576;</title>
<style>
:root{--bg0:#050b1c;--side:#07112a;--card:#0d1c3f;--card2:#0a1632;--border:#17335f;--text:#e8eefc;--muted:#8fa3c8;--accent:#2f7cff;--good:#22c55e;--bad:#ef4444;--warn:#f59e0b;--purple:#8b5cf6;--grid:#1a2e57;--field:#07122b;--shadow:0 10px 30px rgba(0,0,0,.35)}
[data-theme=light]{--bg0:#eef2fa;--side:#fff;--card:#fff;--card2:#fff;--border:#dbe3f1;--text:#14213d;--muted:#64748b;--accent:#2563eb;--good:#16a34a;--bad:#dc2626;--warn:#d97706;--purple:#7c3aed;--grid:#e6ecf5;--field:#f3f6fb;--shadow:0 4px 18px rgba(30,60,120,.08)}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%}
body{font-family:"Segoe UI",Tahoma,Arial,sans-serif;font-size:14px;color:var(--text);background:radial-gradient(1100px 520px at 72% -8%,#10275a 0%,var(--bg0) 62%) fixed}
[data-theme=light] body{background:var(--bg0)}
button,input,select{font:inherit;color:inherit}
small{color:var(--muted)}
/* top bar */
.top{height:64px;display:flex;align-items:center;gap:14px;padding:0 22px;background:var(--side);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:30}
.grow{flex:1}
.brand{display:flex;gap:12px;align-items:center;direction:ltr;text-align:left}
.brand b{display:block;font-size:17px}.brand small{font-size:12px}
.logo{width:40px;height:40px;color:var(--accent);filter:drop-shadow(0 0 8px rgba(47,124,255,.7))}
.uchip{display:flex;align-items:center;gap:10px;direction:ltr;padding:4px 6px;cursor:default}
.uchip b{display:block;font-size:14px}.uchip small{font-size:12px}
.av{width:38px;height:38px;border-radius:50%;background:linear-gradient(135deg,#3b82f6,#1d4ed8);display:grid;place-items:center;color:#fff}
.vsep{width:1px;height:34px;background:var(--border)}
.tgl{display:flex;gap:4px}
.tgl button,.bell,.iconbtn{background:transparent;border:0;color:var(--muted);width:34px;height:34px;border-radius:8px;display:grid;place-items:center;cursor:pointer;position:relative}
.tgl button.on{color:var(--accent)}
.tgl button:hover,.bell:hover,.iconbtn:hover{background:rgba(127,150,200,.15)}
.bell i{position:absolute;top:-2px;left:-2px;background:var(--bad);color:#fff;font-style:normal;font-size:11px;min-width:18px;height:18px;border-radius:9px;display:none;place-items:center;padding:0 4px;font-weight:700}
/* layout */
.body{display:grid;grid-template-columns:1fr 240px;min-height:calc(100vh - 64px)}
main{padding:22px;min-width:0}
.side{background:var(--side);border-inline-start:1px solid var(--border);padding:16px 14px;position:sticky;top:64px;height:calc(100vh - 64px);display:flex;flex-direction:column;gap:6px}
.nav{display:flex;flex-direction:column;gap:6px}
.nav a{display:flex;align-items:center;gap:14px;direction:ltr;padding:12px 14px;border-radius:10px;color:var(--muted);cursor:pointer;text-decoration:none;font-size:14px}
.nav a:hover{background:rgba(127,150,200,.12);color:var(--text)}
.nav a.on{background:var(--accent);color:#fff;box-shadow:0 6px 18px rgba(47,124,255,.35)}
.sys{margin-top:auto;border:1px solid var(--border);border-radius:12px;padding:12px;background:rgba(127,150,200,.07);font-size:13px}
.sys .r{display:flex;gap:8px;align-items:center;font-weight:600}
.sys small{display:block;margin-top:6px}
.page{display:none}.page.on{display:block}
.hgrid{display:grid;grid-template-columns:330px 1fr;gap:20px;align-items:start}
.titlebar{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:18px;flex-wrap:wrap}
h1{margin:0 0 4px;font-size:26px}.titlebar p{margin:0;color:var(--muted)}
.upd{display:flex;align-items:center;gap:10px;direction:ltr}
.upd small{display:block;font-size:12px}.upd span{font-size:13px}
.upd .iconbtn{border:1px solid var(--border);width:40px;height:40px;color:var(--text)}
.card{background:linear-gradient(180deg,var(--card),var(--card2));border:1px solid var(--border);border-radius:14px;padding:18px;box-shadow:var(--shadow);margin-bottom:18px;min-width:0}
.card h3{margin:0 0 14px;font-size:16px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}
.kpi{margin:0;padding:16px}
.kh{display:flex;align-items:center;gap:12px}
.kic{width:46px;height:46px;border-radius:12px;display:grid;place-items:center;flex:none}
.kt b{display:block;font-size:15px}.kt small{font-size:12px}
.kb{display:flex;justify-content:space-between;align-items:flex-end;margin-top:14px;gap:8px}
.kv{font-size:34px;font-weight:700;direction:ltr;line-height:1}
.kv em{font-size:20px;font-style:normal;margin-inline-start:4px}
.kd{font-size:13px;font-weight:600;text-align:start}
.kd small{display:block;font-weight:400;font-size:11px}
.kd.good{color:var(--good)}.kd.bad{color:var(--bad)}.kd.flat{color:var(--muted)}
.k-good{border-color:rgba(34,197,94,.4);background:linear-gradient(180deg,rgba(34,197,94,.13),var(--card2))}.k-good .kic{background:rgba(34,197,94,.2);color:var(--good)}.k-good .kv{color:var(--good)}
.k-blue{border-color:rgba(47,124,255,.45);background:linear-gradient(180deg,rgba(47,124,255,.14),var(--card2))}.k-blue .kic{background:rgba(47,124,255,.22);color:var(--accent)}.k-blue .kv{color:#5b9bff}
.k-purple{border-color:rgba(139,92,246,.45);background:linear-gradient(180deg,rgba(139,92,246,.14),var(--card2))}.k-purple .kic{background:rgba(139,92,246,.22);color:#a78bfa}
.k-bad{border-color:rgba(239,68,68,.4);background:linear-gradient(180deg,rgba(239,68,68,.12),var(--card2))}.k-bad .kic{background:rgba(239,68,68,.2);color:var(--bad)}.k-bad .kv{color:#f87171}
[data-theme=light] .k-blue .kv{color:var(--accent)}[data-theme=light] .k-bad .kv{color:var(--bad)}
.row2{display:grid;grid-template-columns:300px 1fr;gap:14px}
.chead{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:6px}
.chead h3{margin:0;direction:ltr}
.tabs{display:flex;border:1px solid var(--border);border-radius:99px;padding:3px;gap:2px}
.tab{background:transparent;border:0;padding:6px 16px;border-radius:99px;color:var(--muted);cursor:pointer;font-size:13px}
.tab.on{background:var(--accent);color:#fff}
.cwrap{position:relative}
canvas{width:100%;height:270px;display:block}
.chip{display:inline-flex;gap:6px;align-items:center;background:rgba(47,124,255,.15);color:var(--accent);padding:3px 10px;border-radius:99px;font-size:12px;cursor:pointer;margin-inline-start:8px}
.donutwrap{display:flex;flex-direction:column;align-items:center;gap:14px}
.donut{width:170px;height:170px;position:relative}
.donut svg{width:100%;height:100%}
.dc{position:absolute;inset:0;display:grid;place-content:center;text-align:center}
.dc b{font-size:30px;line-height:1}.dc small{font-size:11px}
.leg{width:100%;display:flex;flex-direction:column;gap:8px}
.leg div{display:flex;align-items:center;gap:8px}.leg b{margin-inline-start:auto}
.dot{width:10px;height:10px;border-radius:50%;flex:none;background:var(--muted)}
.dot.good{background:var(--good)}.dot.bad{background:var(--bad)}.dot.warn{background:var(--warn)}
.tw{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:860px}
th{color:var(--muted);font-weight:600;font-size:12px;padding:10px 8px;text-align:start;border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:12px 8px;border-bottom:1px solid var(--border);white-space:nowrap}
tbody tr{cursor:pointer}tbody tr:hover{background:rgba(127,150,200,.08)}tbody tr.sel{background:rgba(47,124,255,.14)}
td.num{direction:ltr;text-align:start;font-variant-numeric:tabular-nums}
.svc b{display:block}.svc span{color:var(--muted);font-size:12px;direction:ltr;display:block;text-align:start}
.bd{display:inline-block;padding:3px 14px;border-radius:8px;font-size:12px;font-weight:700;border:1px solid}
.bd.up{background:rgba(34,197,94,.16);color:var(--good);border-color:rgba(34,197,94,.45)}
.bd.slow{background:rgba(245,158,11,.16);color:var(--warn);border-color:rgba(245,158,11,.45)}
.bd.down{background:rgba(239,68,68,.16);color:var(--bad);border-color:rgba(239,68,68,.45)}
.bd.unknown{background:rgba(127,150,200,.15);color:var(--muted);border-color:var(--border)}
.av2{display:flex;gap:8px;align-items:center;direction:ltr;justify-content:flex-end}
.kebab{background:transparent;border:0;color:var(--muted);cursor:pointer;border-radius:6px;padding:4px}
.kebab:hover{background:rgba(127,150,200,.2)}
#menu{position:fixed;display:none;z-index:60;background:var(--card);border:1px solid var(--border);border-radius:10px;box-shadow:var(--shadow);min-width:180px;padding:6px}
#menu a{display:block;padding:9px 12px;border-radius:7px;cursor:pointer}#menu a:hover{background:rgba(127,150,200,.15)}#menu a.red{color:var(--bad)}
/* forms */
label{display:block;font-size:13px;color:var(--muted);margin-bottom:12px}
input,select{width:100%;margin-top:6px;padding:11px 12px;border-radius:10px;border:1px solid var(--border);background:var(--field);color:var(--text);outline:none}
input:focus,select:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(47,124,255,.2)}
.seg{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:6px}
.seg button{padding:10px;border-radius:10px;border:1px solid var(--border);background:var(--field);cursor:pointer;font-weight:700;color:var(--muted)}
.seg button.on{background:var(--accent);color:#fff;border-color:var(--accent)}
.btn{width:100%;display:flex;gap:8px;align-items:center;justify-content:center;padding:12px;border:0;border-radius:10px;background:linear-gradient(180deg,#3b86ff,#2563eb);color:#fff;font-weight:700;cursor:pointer;box-shadow:0 8px 20px rgba(37,99,235,.35)}
.btn:hover{filter:brightness(1.08)}
.btn.sec{background:var(--field);color:var(--text);border:1px solid var(--border);box-shadow:none;width:auto;padding:10px 16px}
.errtxt{color:var(--bad);font-size:13px;margin-top:8px;min-height:1em}
.evi{display:flex;align-items:flex-start;gap:12px;padding:12px 0;border-bottom:1px solid var(--border)}
.evi:last-child{border-bottom:0}.evi .dot{margin-top:6px}
.evt{flex:1;min-width:0}.evt b{display:block}.evt small{display:block;direction:rtl}
.evi time{color:var(--muted);font-size:11px;direction:ltr;white-space:nowrap}
.more{display:block;margin-top:12px;color:var(--accent);cursor:pointer;font-size:13px}
.formgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}
.chk{display:flex;gap:8px;align-items:center;margin:6px 0 10px;color:var(--text)}.chk input{width:auto;margin:0}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:12px 0}
.stat{background:var(--field);border:1px solid var(--border);border-radius:10px;padding:12px}
.stat small{display:block;margin-bottom:4px}.stat b{font-size:18px;direction:ltr;display:block;text-align:start}
.hint{color:var(--muted);font-size:13px;margin:4px 0 12px}
.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}
.bar{height:6px;border-radius:3px;background:var(--field);overflow:hidden;margin:10px 0}
.bar i{display:block;height:100%;width:35%;background:var(--accent);border-radius:3px;animation:mv 1.2s infinite ease-in-out}
@keyframes mv{0%{margin-inline-start:-35%}100%{margin-inline-start:100%}}
details{border:1px solid var(--border);border-radius:10px;padding:10px 14px;margin-top:10px}summary{cursor:pointer}
.chips{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
#tip{position:fixed;display:none;pointer-events:none;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:8px 10px;font-size:12px;z-index:70;box-shadow:var(--shadow);direction:rtl}
#toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:var(--accent);color:#fff;padding:10px 18px;border-radius:10px;display:none;z-index:80;box-shadow:var(--shadow)}
@media (max-width:1200px){.hgrid{grid-template-columns:1fr}.panel{order:2}.kpis{grid-template-columns:repeat(2,1fr)}.row2{grid-template-columns:1fr}}
@media (max-width:860px){
  .body{grid-template-columns:1fr}
  .side{position:fixed;bottom:0;left:0;right:0;top:auto;height:62px;flex-direction:row;padding:6px;z-index:40;border-top:1px solid var(--border);border-inline-start:0}
  .nav{flex-direction:row;width:100%;justify-content:space-around}.nav a span{display:none}.nav a{padding:10px 12px}.sys{display:none}
  main{padding:14px 14px 80px}.brand small{display:none}.uchip div:last-child{display:none}h1{font-size:20px}
}
@media print{.top,.side,.no-print{display:none!important}.body{display:block}body{background:#fff;color:#000}.card{box-shadow:none;border-color:#ccc;background:#fff}small,th{color:#444}}
</style>
</head>
<body>
<header class="top">
  <div class="uchip"><div class="av" id="ic-user"></div><div><b>admin</b><small>Administrator</small></div></div>
  <div class="vsep"></div>
  <div class="tgl"><button id="th-light" onclick="setTheme('light')" title="&#1601;&#1575;&#1578;&#1581;"></button><button id="th-dark" onclick="setTheme('dark')" title="&#1583;&#1575;&#1603;&#1606;"></button></div>
  <button class="bell" onclick="go('events')" title="&#1575;&#1604;&#1578;&#1606;&#1576;&#1610;&#1607;&#1575;&#1578;"><span id="ic-bell"></span><i id="bellcnt">0</i></button>
  <div class="grow"></div>
  <div class="brand"><div class="logo" id="logo"></div><div><b>Web Service Monitor</b><small>Performance &amp; Reliability Testing</small></div></div>
</header>

<div class="body">
<main>
  <!-- &#1575;&#1604;&#1585;&#1574;&#1610;&#1587;&#1610;&#1577; -->
  <section class="page on" id="p-home">
    <div class="hgrid">
      <div class="panel">
        <div class="card" id="addcard">
          <h3>&#1573;&#1590;&#1575;&#1601;&#1577; &#1582;&#1583;&#1605;&#1577; &#1580;&#1583;&#1610;&#1583;&#1577;</h3>
          <label>&#1575;&#1587;&#1605; &#1575;&#1604;&#1582;&#1583;&#1605;&#1577;<input id="f_name" placeholder="&#1605;&#1579;&#1575;&#1604;: Google"></label>
          <label>&#1585;&#1575;&#1576;&#1591; &#1575;&#1604;&#1582;&#1583;&#1605;&#1577;<input id="f_url" dir="ltr" placeholder="https://example.com"></label>
          <label>&#1591;&#1585;&#1610;&#1602;&#1577; &#1575;&#1604;&#1591;&#1604;&#1576;
            <div class="seg" id="mseg"><button data-m="GET" class="on" onclick="setMethod('GET')">GET</button><button data-m="HEAD" onclick="setMethod('HEAD')">HEAD</button></div></label>
          <label>&#1575;&#1604;&#1601;&#1575;&#1589;&#1604; &#1575;&#1604;&#1586;&#1605;&#1606;&#1610; (&#1579;&#1575;&#1606;&#1610;&#1577;)<input id="f_int" type="number" value="30" min="5"></label>
          <label>&#1575;&#1604;&#1605;&#1607;&#1604;&#1577; &#1575;&#1604;&#1586;&#1605;&#1606;&#1610;&#1577; (&#1579;&#1575;&#1606;&#1610;&#1577;)<input id="f_to" type="number" value="10" min="1"></label>
          <label>&#1603;&#1608;&#1583; &#1575;&#1604;&#1575;&#1587;&#1578;&#1580;&#1575;&#1576;&#1577; &#1575;&#1604;&#1605;&#1578;&#1608;&#1602;&#1593; (&#1575;&#1582;&#1578;&#1610;&#1575;&#1585;&#1610;)<input id="f_exp" type="number" placeholder="&#1605;&#1579;&#1575;&#1604;: 200"></label>
          <button class="btn" onclick="addTarget()"><span id="ic-send"></span> &#1573;&#1590;&#1575;&#1601;&#1577; &#1575;&#1604;&#1582;&#1583;&#1605;&#1577;</button>
          <div class="errtxt" id="f_err"></div>
        </div>
        <div class="card">
          <h3>&#1571;&#1581;&#1583;&#1579; &#1575;&#1604;&#1571;&#1581;&#1583;&#1575;&#1579;</h3>
          <div id="ev-mini"></div>
          <a class="more" onclick="go('events')">&#1593;&#1585;&#1590; &#1580;&#1605;&#1610;&#1593; &#1575;&#1604;&#1571;&#1581;&#1583;&#1575;&#1579; &#8592;</a>
        </div>
      </div>

      <div class="mainc">
        <div class="titlebar">
          <div><h1>&#1604;&#1608;&#1581;&#1577; &#1575;&#1604;&#1578;&#1581;&#1603;&#1605; &#1575;&#1604;&#1585;&#1574;&#1610;&#1587;&#1610;&#1577;</h1><p>&#1605;&#1585;&#1575;&#1602;&#1576;&#1577; &#1571;&#1583;&#1575;&#1569; &#1608;&#1605;&#1608;&#1579;&#1608;&#1602;&#1610;&#1577; &#1582;&#1583;&#1605;&#1575;&#1578; &#1575;&#1604;&#1608;&#1610;&#1576; &#1576;&#1588;&#1603;&#1604; &#1605;&#1587;&#1578;&#1605;&#1585;</p></div>
          <div class="upd"><button class="iconbtn" onclick="refresh()" id="ic-refresh" title="&#1578;&#1581;&#1583;&#1610;&#1579;"></button><div><small>&#1570;&#1582;&#1585; &#1578;&#1581;&#1583;&#1610;&#1579;</small><span id="upd">-</span></div></div>
        </div>
        <div class="kpis" id="kpis"></div>
        <div class="row2">
          <div class="card">
            <h3>&#1581;&#1575;&#1604;&#1577; &#1575;&#1604;&#1582;&#1583;&#1605;&#1575;&#1578;</h3>
            <div class="donutwrap"><div class="donut" id="donut"></div><div class="leg" id="legend"></div></div>
          </div>
          <div class="card">
            <div class="chead"><div class="tabs" id="tabs1"></div><h3>Response Time Over Time <small id="csub"></small></h3></div>
            <div class="cwrap"><canvas id="chart1"></canvas></div>
          </div>
        </div>
        <div class="card">
          <h3>&#1575;&#1604;&#1582;&#1583;&#1605;&#1575;&#1578; &#1575;&#1604;&#1605;&#1585;&#1575;&#1602;&#1576;&#1577; <small>(&#1573;&#1581;&#1589;&#1575;&#1569;&#1575;&#1578; &#1570;&#1582;&#1585; &#1587;&#1575;&#1593;&#1577;&#1548; &#1575;&#1590;&#1594;&#1591; &#1593;&#1604;&#1609; &#1582;&#1583;&#1605;&#1577; &#1604;&#1593;&#1585;&#1590;&#1607;&#1575; &#1601;&#1610; &#1575;&#1604;&#1585;&#1587;&#1605;)</small></h3>
          <div class="tw"><table><thead class="th"></thead><tbody id="tb1"></tbody></table></div>
        </div>
      </div>
    </div>
  </section>

  <!-- &#1575;&#1604;&#1582;&#1583;&#1605;&#1575;&#1578; -->
  <section class="page" id="p-services">
    <div class="titlebar"><div><h1>&#1575;&#1604;&#1582;&#1583;&#1605;&#1575;&#1578; &#1575;&#1604;&#1605;&#1585;&#1575;&#1602;&#1576;&#1577;</h1><p>&#1603;&#1604; &#1575;&#1604;&#1582;&#1583;&#1605;&#1575;&#1578; &#1605;&#1593; &#1573;&#1581;&#1589;&#1575;&#1569;&#1575;&#1578; &#1570;&#1582;&#1585; &#1587;&#1575;&#1593;&#1577;</p></div></div>
    <div class="card"><div class="tw"><table><thead class="th"></thead><tbody id="tb2"></tbody></table></div></div>
    <div class="card" id="detail">
      <div class="chead"><div class="tabs" id="tabs2"></div><h3 id="dtitle" style="direction:rtl">&#1578;&#1601;&#1575;&#1589;&#1610;&#1604; &#1575;&#1604;&#1582;&#1583;&#1605;&#1577;</h3></div>
      <div id="dstats"></div>
      <div class="cwrap"><canvas id="chart2"></canvas></div>
    </div>
  </section>

  <!-- &#1575;&#1582;&#1578;&#1576;&#1575;&#1585; &#1575;&#1604;&#1581;&#1605;&#1604; -->
  <section class="page" id="p-load">
    <div class="titlebar"><div><h1>&#1575;&#1582;&#1578;&#1576;&#1575;&#1585; &#1575;&#1604;&#1581;&#1605;&#1604; &#1608;&#1575;&#1604;&#1587;&#1593;&#1577;</h1><p>&#1575;&#1593;&#1585;&#1601; &#1603;&#1605; &#1591;&#1604;&#1576; &#1578;&#1578;&#1581;&#1605;&#1604; &#1575;&#1604;&#1582;&#1583;&#1605;&#1577; &#1608;&#1571;&#1610;&#1606; &#1610;&#1576;&#1583;&#1571; &#1575;&#1604;&#1571;&#1583;&#1575;&#1569; &#1576;&#1575;&#1604;&#1578;&#1585;&#1575;&#1580;&#1593;</p></div></div>
    <div class="card">
      <div class="formgrid">
        <label>&#1575;&#1604;&#1582;&#1583;&#1605;&#1577;<select id="lt_target" onchange="loadTests()"></select></label>
        <label>&#1606;&#1608;&#1593; &#1575;&#1604;&#1575;&#1582;&#1578;&#1576;&#1575;&#1585;<select id="lt_kind"><option value="load">&#1575;&#1582;&#1578;&#1576;&#1575;&#1585; &#1581;&#1605;&#1604; (&#1593;&#1583;&#1583; &#1579;&#1575;&#1576;&#1578;)</option><option value="capacity">&#1575;&#1582;&#1578;&#1576;&#1575;&#1585; &#1587;&#1593;&#1577; (&#1585;&#1601;&#1593; &#1578;&#1583;&#1585;&#1610;&#1580;&#1610;)</option></select></label>
        <label>&#1575;&#1604;&#1605;&#1587;&#1578;&#1582;&#1583;&#1605;&#1608;&#1606; &#1575;&#1604;&#1605;&#1578;&#1586;&#1575;&#1605;&#1606;&#1608;&#1606;<input id="lt_conc" type="number" value="10" min="1" max="200"></label>
        <label>&#1593;&#1583;&#1583; &#1575;&#1604;&#1591;&#1604;&#1576;&#1575;&#1578;<input id="lt_total" type="number" value="200" min="1" max="5000"></label>
      </div>
      <label class="chk"><input type="checkbox" id="lt_ok"> &#1571;&#1572;&#1603;&#1583; &#1571;&#1606;&#1606;&#1610; &#1571;&#1605;&#1604;&#1603; &#1607;&#1584;&#1607; &#1575;&#1604;&#1582;&#1583;&#1605;&#1577; &#1571;&#1608; &#1604;&#1583;&#1610; &#1573;&#1584;&#1606; &#1576;&#1575;&#1582;&#1578;&#1576;&#1575;&#1585;&#1607;&#1575;</label>
      <p class="hint">&#1601;&#1610; &#1575;&#1582;&#1578;&#1576;&#1575;&#1585; &#1575;&#1604;&#1587;&#1593;&#1577; &#1610;&#1605;&#1579;&#1604; "&#1575;&#1604;&#1605;&#1578;&#1586;&#1575;&#1605;&#1606;&#1608;&#1606;" &#1571;&#1602;&#1589;&#1609; &#1605;&#1587;&#1578;&#1608;&#1609; &#1610;&#1589;&#1604; &#1604;&#1607; &#1575;&#1604;&#1575;&#1582;&#1578;&#1576;&#1575;&#1585;. &#1604;&#1575; &#1578;&#1582;&#1578;&#1576;&#1585; &#1605;&#1608;&#1575;&#1602;&#1593; &#1604;&#1575; &#1578;&#1605;&#1604;&#1603;&#1607;&#1575;.</p>
      <button class="btn" style="width:auto;padding:12px 28px" onclick="startTest()">&#9654; &#1575;&#1576;&#1583;&#1571; &#1575;&#1604;&#1575;&#1582;&#1578;&#1576;&#1575;&#1585;</button>
      <div id="lt_msg" style="margin-top:14px"></div>
    </div>
    <div class="card"><h3>&#1570;&#1582;&#1585; &#1575;&#1604;&#1575;&#1582;&#1578;&#1576;&#1575;&#1585;&#1575;&#1578; &#1604;&#1604;&#1582;&#1583;&#1605;&#1577; &#1575;&#1604;&#1605;&#1581;&#1583;&#1583;&#1577;</h3><div id="lt_hist"></div></div>
  </section>

  <!-- &#1575;&#1604;&#1571;&#1581;&#1583;&#1575;&#1579; -->
  <section class="page" id="p-events">
    <div class="titlebar"><div><h1>&#1587;&#1580;&#1604; &#1575;&#1604;&#1571;&#1581;&#1583;&#1575;&#1579;</h1><p>&#1578;&#1608;&#1602;&#1601; &#1575;&#1604;&#1582;&#1583;&#1605;&#1575;&#1578; &#1608;&#1576;&#1591;&#1572;&#1607;&#1575; &#1608;&#1585;&#1580;&#1608;&#1593;&#1607;&#1575; &#1604;&#1604;&#1593;&#1605;&#1604;</p></div></div>
    <div class="card"><div class="chips" id="evchips"></div><div id="ev-all"></div></div>
  </section>

  <!-- &#1575;&#1604;&#1578;&#1602;&#1575;&#1585;&#1610;&#1585; -->
  <section class="page" id="p-reports">
    <div class="titlebar"><div><h1>&#1575;&#1604;&#1578;&#1602;&#1575;&#1585;&#1610;&#1585;</h1><p>&#1605;&#1604;&#1582;&#1589; &#1575;&#1604;&#1571;&#1583;&#1575;&#1569; &#1608;&#1575;&#1604;&#1605;&#1608;&#1579;&#1608;&#1602;&#1610;&#1577; &#1604;&#1603;&#1604; &#1582;&#1583;&#1605;&#1577;</p></div>
      <div class="no-print" style="display:flex;gap:8px"><select id="rp_days" onchange="loadReport()" style="width:auto;margin:0"><option value="1">&#1570;&#1582;&#1585; 24 &#1587;&#1575;&#1593;&#1577;</option><option value="7">&#1570;&#1582;&#1585; 7 &#1571;&#1610;&#1575;&#1605;</option><option value="30">&#1570;&#1582;&#1585; 30 &#1610;&#1608;&#1605;</option></select><button class="btn sec" onclick="window.print()">&#128424; &#1591;&#1576;&#1575;&#1593;&#1577; / PDF</button></div></div>
    <div class="card"><div class="tw"><table><thead><tr><th>&#1575;&#1604;&#1582;&#1583;&#1605;&#1577;</th><th>&#1575;&#1604;&#1601;&#1581;&#1608;&#1589;&#1575;&#1578;</th><th>&#1575;&#1604;&#1578;&#1608;&#1601;&#1585; %</th><th>&#1605;&#1578;&#1608;&#1587;&#1591; (ms)</th><th>P95 (ms)</th><th>P99 (ms)</th><th>&#1575;&#1604;&#1571;&#1602;&#1589;&#1609; (ms)</th><th>&#1605;&#1585;&#1575;&#1578; &#1575;&#1604;&#1578;&#1608;&#1602;&#1601;</th></tr></thead><tbody id="rep"></tbody></table></div></div>
  </section>

  <!-- &#1575;&#1604;&#1573;&#1593;&#1583;&#1575;&#1583;&#1575;&#1578; -->
  <section class="page" id="p-settings">
    <div class="titlebar"><div><h1>&#1575;&#1604;&#1573;&#1593;&#1583;&#1575;&#1583;&#1575;&#1578;</h1><p>&#1578;&#1582;&#1589;&#1610;&#1589; &#1581;&#1583;&#1608;&#1583; &#1575;&#1604;&#1578;&#1606;&#1576;&#1610;&#1607; &#1608;&#1575;&#1604;&#1605;&#1592;&#1607;&#1585;</p></div></div>
    <div class="card" style="max-width:640px">
      <label>&#1581;&#1583; &#1575;&#1604;&#1575;&#1587;&#1578;&#1580;&#1575;&#1576;&#1577; &#1575;&#1604;&#1576;&#1591;&#1610;&#1574;&#1577; (ms) &#8212; &#1571;&#1610; &#1575;&#1587;&#1578;&#1580;&#1575;&#1576;&#1577; &#1571;&#1576;&#1591;&#1571; &#1605;&#1606;&#1607; &#1578;&#1615;&#1593;&#1578;&#1576;&#1585; "&#1576;&#1591;&#1610;&#1574;&#1577;"<input id="s_slow" type="number" min="50" max="60000"></label>
      <label>&#1575;&#1604;&#1578;&#1581;&#1583;&#1610;&#1579; &#1575;&#1604;&#1578;&#1604;&#1602;&#1575;&#1574;&#1610; &#1604;&#1604;&#1608;&#1581;&#1577;
        <select id="s_ref" onchange="setRefresh(this.value)"><option value="3">&#1603;&#1604; 3 &#1579;&#1608;&#1575;&#1606;&#1610;</option><option value="5">&#1603;&#1604; 5 &#1579;&#1608;&#1575;&#1606;&#1610;</option><option value="10">&#1603;&#1604; 10 &#1579;&#1608;&#1575;&#1606;&#1610;</option><option value="30">&#1603;&#1604; 30 &#1579;&#1575;&#1606;&#1610;&#1577;</option></select></label>
      <button class="btn" style="width:auto;padding:12px 28px" onclick="saveSettings()">&#1581;&#1601;&#1592; &#1575;&#1604;&#1573;&#1593;&#1583;&#1575;&#1583;&#1575;&#1578;</button>
      <p class="hint" id="s_info" style="margin-top:14px"></p>
    </div>
  </section>
</main>

<aside class="side">
  <nav class="nav" id="nav"></nav>
  <div class="sys" id="sysbox"></div>
</aside>
</div>

<div id="menu"></div><div id="tip"></div><div id="toast"></div>

<script>
const $=s=>document.querySelector(s),$$=s=>[...document.querySelectorAll(s)];
const esc=s=>String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const f=(n,d=0)=>(n==null||isNaN(n))?"-":Number(n).toFixed(d);
const pad=n=>String(n).padStart(2,"0");
const dt=ts=>{const d=new Date(ts*1000);return d.getFullYear()+"-"+pad(d.getMonth()+1)+"-"+pad(d.getDate())+" "+pad(d.getHours())+":"+pad(d.getMinutes());};
const hm=ts=>{const d=new Date(ts*1000);return pad(d.getHours())+":"+pad(d.getMinutes());};
const md=ts=>{const d=new Date(ts*1000);return pad(d.getMonth()+1)+"-"+pad(d.getDate());};
const ls={get(k,d){try{const v=localStorage.getItem(k);return v==null?d:v;}catch(e){return d;}},set(k,v){try{localStorage.setItem(k,v);}catch(e){}}};

const ICONS={
 home:'<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
 activity:'<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
 plus:'<circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/>',
 bars:'<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
 list:'<line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/>',
 file:'<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>',
 gear:'<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
 bell:'<path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/>',
 sun:'<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>',
 moon:'<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>',
 shield:'<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/>',
 zap:'<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>',
 check:'<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
 alert:'<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
 more:'<circle cx="12" cy="12" r="1"/><circle cx="12" cy="5" r="1"/><circle cx="12" cy="19" r="1"/>',
 refresh:'<polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>',
 user:'<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>',
 send:'<line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>',
 up:'<line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/>',
 down:'<line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/>'
};
const ic=(n,s=20)=>'<svg viewBox="0 0 24 24" width="'+s+'" height="'+s+'" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'+ICONS[n]+'</svg>';

const S={targets:[],slow:1000,selected:null,range:1440,page:"home",events:[],ov:null,evFilter:"ALL",refreshSec:5,timer:null,sig:""};
const RANGES=[[60,"\u0633\u0627\u0639\u0629"],[10080,"\u0623\u0633\u0628\u0648\u0639"],[1440,"24 \u0633\u0627\u0639\u0629"],[43200,"\u0634\u0647\u0631"]];
const NAV=[["home","\u0627\u0644\u0631\u0626\u064a\u0633\u064a\u0629","home"],["services","\u0627\u0644\u062e\u062f\u0645\u0627\u062a \u0627\u0644\u0645\u0631\u0627\u0642\u0628\u0629","activity"],["add","\u0625\u0636\u0627\u0641\u0629 \u062e\u062f\u0645\u0629 \u062c\u062f\u064a\u062f\u0629","plus"],["load","\u0627\u062e\u062a\u0628\u0627\u0631 \u0627\u0644\u062d\u0645\u0644 \u0648\u0627\u0644\u0633\u0639\u0629","bars"],["events","\u0633\u062c\u0644 \u0627\u0644\u0623\u062d\u062f\u0627\u062b","list"],["reports","\u0627\u0644\u062a\u0642\u0627\u0631\u064a\u0631","file"],["settings","\u0627\u0644\u0625\u0639\u062f\u0627\u062f\u0627\u062a","gear"]];
const STAT={up:"\u062a\u0639\u0645\u0644",slow:"\u0628\u0637\u064a\u0626\u0629",down:"\u0645\u062a\u0648\u0642\u0641\u0629",http_error:"\u062e\u0637\u0623 HTTP",unknown:"\u2014"};
const EVC={UP:"good",DOWN:"bad",SLOW:"warn"};
const EVN={UP:"\u0639\u0645\u0644",DOWN:"\u062a\u0648\u0642\u0641",SLOW:"\u0628\u0637\u0621"};
let method="GET";

async function api(p,o){const r=await fetch(p,o);const d=await r.json().catch(()=>({}));if(!r.ok)throw new Error(d.error||r.statusText);return d;}
function toast(m){const t=$("#toast");t.textContent=m;t.style.display="block";clearTimeout(t._t);t._t=setTimeout(()=>t.style.display="none",3000);}

/* ---------- \u062a\u0647\u064a\u0626\u0629 \u0627\u0644\u0648\u0627\u062c\u0647\u0629 ---------- */
function initStatic(){
  $("#logo").innerHTML=ic("activity",40);$("#ic-user").innerHTML=ic("user",20);$("#ic-bell").innerHTML=ic("bell",20);
  $("#th-light").innerHTML=ic("sun",19);$("#th-dark").innerHTML=ic("moon",19);$("#ic-refresh").innerHTML=ic("refresh",18);$("#ic-send").innerHTML=ic("send",16);
  const th='<tr><th>\u0627\u0644\u062e\u062f\u0645\u0629</th><th>\u0627\u0644\u062d\u0627\u0644\u0629</th><th>\u0622\u062e\u0631 \u0641\u062d\u0635</th><th>\u0627\u0644\u0648\u0642\u062a (ms)</th><th>\u0627\u0644\u0646\u062c\u0627\u062d %</th><th>\u0627\u0644\u0623\u062e\u0637\u0627\u0621 %</th><th>\u0645\u062a\u0648\u0633\u0637 (ms)</th><th>P95 (ms)</th><th>\u0627\u0644\u062a\u0648\u0641\u0631 24\u0633</th><th></th></tr>';
  $$(".th").forEach(x=>x.innerHTML=th);
}
function renderNav(){
  $("#nav").innerHTML=NAV.map(n=>'<a class="'+(n[0]===S.page?"on":"")+'" onclick="go(\''+n[0]+'\')">'+ic(n[2],22)+'<span>'+n[1]+'</span></a>').join("");
}
function go(p){
  if(p==="add"){p="home";setTimeout(()=>{$("#addcard").scrollIntoView({behavior:"smooth",block:"center"});$("#f_url").focus();},60);}
  S.page=p;history.replaceState(null,"","#"+p);
  $$(".page").forEach(x=>x.classList.toggle("on",x.id==="p-"+p));
  renderNav();renderAll();
  if(p==="reports")loadReport();if(p==="load")loadTests();if(p==="settings")loadSettings();
  window.scrollTo(0,0);
}
function setTheme(t){document.documentElement.dataset.theme=t;ls.set("theme",t);$("#th-light").classList.toggle("on",t==="light");$("#th-dark").classList.toggle("on",t==="dark");renderCharts();}
function setRefresh(v){S.refreshSec=+v;ls.set("refresh",v);clearInterval(S.timer);S.timer=setInterval(refresh,S.refreshSec*1000);}

/* ---------- \u062c\u0644\u0628 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a ---------- */
async function refresh(){
  try{
    const q="/api/overview?minutes="+S.range+(S.selected?"&target_id="+S.selected:"");
    const r=await Promise.all([api("/api/summary"),api(q),api("/api/events?limit=200")]);
    S.targets=r[0].targets;S.slow=r[0].slow_ms;S.ov=r[1];S.events=r[2];
    if(S.selected&&!S.targets.find(t=>t.id===S.selected))S.selected=null;
    const d=new Date();$("#upd").textContent=dt(d.getTime()/1000)+":"+pad(d.getSeconds());
    renderAll();
  }catch(e){$("#upd").textContent="\u062e\u0637\u0623: "+e.message;}
}
function renderAll(){
  renderKPIs();renderDonut();renderTables();renderEvents();renderSys();renderBell();renderCharts();renderDetail();renderTargetSelect();
}

/* ---------- \u0645\u0624\u0634\u0631\u0627\u062a KPI ---------- */
function renderKPIs(){
  const o=S.ov||{},c=o.kpi_cur,p=o.kpi_prev;
  const defs=[
    ["bad","\u0646\u0633\u0628\u0629 \u0627\u0644\u0623\u062e\u0637\u0627\u0621","Error Rate","alert","error_rate","pct",true],
    ["purple","\u0646\u0633\u0628\u0629 \u0627\u0644\u0646\u062c\u0627\u062d","Success Rate","check","success_rate","pct",false],
    ["blue","\u0645\u062a\u0648\u0633\u0637 \u0632\u0645\u0646 \u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629","Avg. Response Time","zap","avg_ms","ms",true],
    ["good","\u0627\u0644\u062a\u0648\u0641\u0631","Availability","shield","availability","pct",false]];
  $("#kpis").innerHTML=defs.map(d=>{
    const cur=c?c[d[4]]:null,prev=p?p[d[4]]:null;
    let val="-",dl='<span>\u0644\u0627 \u062a\u0648\u062c\u062f \u0645\u0642\u0627\u0631\u0646\u0629</span>',dc="flat";
    if(cur!=null)val=d[5]==="ms"?f(cur,0)+"<em>ms</em>":f(cur,1)+"<em>%</em>";
    if(cur!=null&&prev!=null){
      const diff=d[5]==="ms"?(prev>0?(cur-prev)/prev*100:0):(cur-prev);
      if(Math.abs(diff)<0.05){dl="<span>\u062b\u0627\u0628\u062a</span>";}
      else{const upv=diff>0;dc=(upv!==d[6])?"good":"bad";dl='<span>'+ic(upv?"up":"down",13)+" "+f(Math.abs(diff),1)+"%</span>";}
    }
    return '<div class="card kpi k-'+d[0]+'"><div class="kh"><div class="kic">'+ic(d[3],24)+'</div><div class="kt"><b>'+d[1]+'</b><small>'+d[2]+'</small></div></div><div class="kb"><div class="kd '+dc+'">'+dl+'<small>\u062e\u0644\u0627\u0644 24 \u0633\u0627\u0639\u0629</small></div><div class="kv">'+val+'</div></div></div>';
  }).join("");
}

/* ---------- \u0627\u0644\u062f\u0648\u0646\u0627\u062a ---------- */
function renderDonut(){
  const cnt={up:0,slow:0,down:0,http_error:0,unknown:0};S.targets.forEach(t=>{cnt[t.status]=(cnt[t.status]||0)+1;});
  const total=S.targets.length,r=48,C=2*Math.PI*r;
  const segs=[[cnt.up,"var(--good)"],[cnt.slow,"var(--warn)"],[cnt.down+cnt.http_error,"var(--bad)"],[cnt.unknown,"var(--muted)"]];
  let off=0,svg='<svg viewBox="0 0 120 120"><circle cx="60" cy="60" r="48" fill="none" style="stroke:var(--grid)" stroke-width="12"/>';
  if(total){segs.forEach(s=>{if(!s[0])return;const len=s[0]/total*C;svg+='<circle cx="60" cy="60" r="48" fill="none" style="stroke:'+s[1]+'" stroke-width="12" stroke-linecap="butt" stroke-dasharray="'+Math.max(len-(total>1?2:0),.1)+' '+C+'" stroke-dashoffset="'+(-off)+'" transform="rotate(-90 60 60)"/>';off+=len;});}
  svg+='</svg><div class="dc"><b>'+total+'</b><small>\u0625\u062c\u0645\u0627\u0644\u064a \u0627\u0644\u062e\u062f\u0645\u0627\u062a</small></div>';
  $("#donut").innerHTML=svg;
  $("#legend").innerHTML=[["good","\u062a\u0639\u0645\u0644",cnt.up],["bad","\u0645\u062a\u0648\u0642\u0641\u0629 / HTTP \u062e\u0637\u0623",cnt.down+cnt.http_error],["warn","\u062a\u062d\u0630\u064a\u0631 (\u0628\u0637\u064a\u0626\u0629)",cnt.slow]].map(l=>'<div><span class="dot '+l[0]+'"></span>'+l[1]+'<b>'+l[2]+'</b></div>').join("");
}

/* ---------- \u0627\u0644\u062c\u062f\u0627\u0648\u0644 ---------- */
function rowHTML(t){
  const h=t.hour,up=t.uptime_24h,uc=up==null?"var(--muted)":up>=99?"var(--good)":up>=95?"var(--warn)":"var(--bad)";
  return '<tr class="'+(t.id===S.selected?"sel":"")+'" onclick="pick('+t.id+')">'+
   '<td><div class="svc"><b>'+esc(t.name)+'</b><span>'+esc(t.url)+'</span></div></td>'+
   '<td><span class="bd '+t.status+'">'+STAT[t.status]+'</span></td>'+
   '<td class="num">'+(t.last_ts?dt(t.last_ts):"-")+'</td>'+
   '<td class="num">'+(t.last_ms!=null&&t.status!=="down"?f(t.last_ms):"-")+'</td>'+
   '<td class="num">'+(h.total?f(h.success_rate,1)+"%":"-")+'</td>'+
   '<td class="num">'+(h.total?f(h.error_rate,1)+"%":"-")+'</td>'+
   '<td class="num">'+(h.ok?f(h.avg):"-")+'</td>'+
   '<td class="num">'+(h.ok?f(h.p95):"-")+'</td>'+
   '<td><div class="av2"><span>'+(up!=null?f(up,1)+"%":"-")+'</span><span class="dot" style="background:'+uc+'"></span></div></td>'+
   '<td><button class="kebab" onclick="event.stopPropagation();openMenu(event,'+t.id+')">'+ic("more",18)+'</button></td></tr>';
}
function renderTables(){
  const h=S.targets.map(rowHTML).join("")||'<tr><td colspan="10" style="text-align:center;color:var(--muted)">\u0644\u0627 \u062a\u0648\u062c\u062f \u062e\u062f\u0645\u0627\u062a \u0628\u0639\u062f\u060c \u0623\u0636\u0641 \u062e\u062f\u0645\u0629 \u0645\u0646 \u0627\u0644\u0646\u0645\u0648\u0630\u062c</td></tr>';
  $("#tb1").innerHTML=h;$("#tb2").innerHTML=h;
}
function pick(id){S.selected=(S.selected===id?null:id);refresh();}
function openMenu(ev,id){
  const m=$("#menu");
  m.innerHTML='<a onclick="pick('+id+')">\ud83d\udcc8 \u0639\u0631\u0636 \u0641\u064a \u0627\u0644\u0631\u0633\u0645</a><a onclick="goLoad('+id+')">\u26a1 \u0627\u062e\u062a\u0628\u0627\u0631 \u0627\u0644\u062d\u0645\u0644</a><a onclick="exportCsv('+id+')">\u2b07 \u062a\u0635\u062f\u064a\u0631 CSV</a><a class="red" onclick="delTarget('+id+')">\ud83d\uddd1 \u062d\u0630\u0641 \u0627\u0644\u062e\u062f\u0645\u0629</a>';
  const r=ev.currentTarget.getBoundingClientRect();
  m.style.display="block";m.style.top=Math.min(r.bottom+4,innerHeight-190)+"px";m.style.left=Math.max(8,r.left)+"px";
}
document.addEventListener("click",e=>{if(!e.target.closest(".kebab"))$("#menu").style.display="none";});
function goLoad(id){S.selected=id;go("load");refresh();setTimeout(()=>{$("#lt_target").value=id;loadTests();},300);}
function exportCsv(id){location.href="/api/export.csv?target_id="+id+"&minutes="+Math.max(S.range,1440);}
async function delTarget(id){if(!confirm("\u062d\u0630\u0641 \u0627\u0644\u062e\u062f\u0645\u0629 \u0648\u0643\u0644 \u0628\u064a\u0627\u0646\u0627\u062a\u0647\u0627\u061f"))return;try{await api("/api/targets/"+id,{method:"DELETE"});if(S.selected===id)S.selected=null;toast("\u062a\u0645 \u062d\u0630\u0641 \u0627\u0644\u062e\u062f\u0645\u0629");refresh();}catch(e){toast(e.message);}}

/* ---------- \u0627\u0644\u0623\u062d\u062f\u0627\u062b \u0648\u0627\u0644\u062a\u0646\u0628\u064a\u0647\u0627\u062a ---------- */
function evItem(e){return '<div class="evi"><span class="dot '+(EVC[e.type]||"")+'"></span><div class="evt"><b>'+esc(e.name||"-")+'</b><small>'+esc(e.message)+'</small></div><time>'+dt(e.ts)+'</time></div>';}
function renderEvents(){
  $("#ev-mini").innerHTML=S.events.slice(0,5).map(evItem).join("")||'<small>\u0644\u0627 \u062a\u0648\u062c\u062f \u0623\u062d\u062f\u0627\u062b \u0628\u0639\u062f</small>';
  if(S.page==="events"){
    const fl=[["ALL","\u0627\u0644\u0643\u0644"],["DOWN","\u062a\u0648\u0642\u0641"],["SLOW","\u0628\u0637\u0621"],["UP","\u0639\u0645\u0644"]];
    $("#evchips").innerHTML=fl.map(x=>'<button class="tab '+(S.evFilter===x[0]?"on":"")+'" style="border:1px solid var(--border)" onclick="S.evFilter=\''+x[0]+'\';renderEvents()">'+x[1]+'</button>').join("");
    const list=S.events.filter(e=>S.evFilter==="ALL"||e.type===S.evFilter);
    $("#ev-all").innerHTML=list.map(evItem).join("")||'<small>\u0644\u0627 \u062a\u0648\u062c\u062f \u0623\u062d\u062f\u0627\u062b</small>';
  }
}
function renderBell(){
  const lim=Date.now()/1000-86400,n=S.events.filter(e=>e.ts>lim&&(e.type==="DOWN"||e.type==="SLOW")).length;
  const b=$("#bellcnt");b.textContent=n>99?"99+":n;b.style.display=n?"grid":"none";
}
function renderSys(){
  const down=S.targets.filter(t=>t.status==="down").length,slow=S.targets.filter(t=>t.status==="slow").length;
  let c="good",m="\u0627\u0644\u0646\u0638\u0627\u0645 \u064a\u0639\u0645\u0644 \u0628\u0634\u0643\u0644 \u0637\u0628\u064a\u0639\u064a";
  if(down){c="bad";m="\u064a\u0648\u062c\u062f "+down+" \u062e\u062f\u0645\u0629 \u0645\u062a\u0648\u0642\u0641\u0629";}else if(slow){c="warn";m="\u064a\u0648\u062c\u062f "+slow+" \u062e\u062f\u0645\u0629 \u0628\u0637\u064a\u0626\u0629";}
  $("#sysbox").innerHTML='<div class="r"><span class="dot '+c+'"></span>'+m+'</div><small>\u0622\u062e\u0631 \u062a\u062d\u062f\u064a\u062b: '+$("#upd").textContent+'</small>';
}

/* ---------- \u0627\u0644\u0631\u0633\u0648\u0645 \u0627\u0644\u0628\u064a\u0627\u0646\u064a\u0629 ---------- */
function tabsHTML(){return RANGES.map(r=>'<button class="tab '+(r[0]===S.range?"on":"")+'" onclick="setRange('+r[0]+')">'+r[1]+'</button>').join("");}
function setRange(r){S.range=r;refresh();}
function niceMax(v){const p=Math.pow(10,Math.floor(Math.log10(v||1))),n=v/p;return (n<=1?1:n<=2?2:n<=5?5:10)*p;}
function renderCharts(){
  $("#tabs1").innerHTML=tabsHTML();$("#tabs2").innerHTML=tabsHTML();
  const t=S.targets.find(x=>x.id===S.selected);
  $("#csub").innerHTML="\u2014 "+(t?esc(t.name)+' <span class="chip" onclick="pick('+t.id+')">\u2715 \u0643\u0644 \u0627\u0644\u062e\u062f\u0645\u0627\u062a</span>':"\u0643\u0644 \u0627\u0644\u062e\u062f\u0645\u0627\u062a");
  const pts=S.ov?S.ov.series:[];
  if(S.page==="home")drawArea($("#chart1"),pts);
  if(S.page==="services"&&S.selected)drawArea($("#chart2"),pts);
}
function drawArea(cv,pts){
  const dpr=window.devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;if(!w)return;
  cv.width=w*dpr;cv.height=h*dpr;const g=cv.getContext("2d");g.setTransform(dpr,0,0,dpr,0,0);g.clearRect(0,0,w,h);
  const st=getComputedStyle(document.body),acc=st.getPropertyValue("--accent").trim(),muted=st.getPropertyValue("--muted").trim(),grid=st.getPropertyValue("--grid").trim(),bad=st.getPropertyValue("--bad").trim();
  g.direction="ltr";g.font="11px Segoe UI,Tahoma,sans-serif";
  const vals=pts.filter(p=>p.v!=null);
  if(!vals.length){g.fillStyle=muted;g.textAlign="center";g.font="14px Segoe UI,Tahoma,sans-serif";g.fillText("\u0644\u0627 \u062a\u0648\u062c\u062f \u0628\u064a\u0627\u0646\u0627\u062a \u0641\u064a \u0647\u0630\u0647 \u0627\u0644\u0641\u062a\u0631\u0629 \u0628\u0639\u062f",w/2,h/2);cv._geo=null;return;}
  const L=46,R=14,T=16,B=28,t0=pts[0].t,t1=pts[pts.length-1].t,max=niceMax(Math.max.apply(null,vals.map(p=>p.v))*1.05);
  const X=t=>L+(t1===t0?.5:(t-t0)/(t1-t0))*(w-L-R),Y=v=>T+(1-v/max)*(h-T-B);
  g.textAlign="right";g.lineWidth=1;
  for(let i=0;i<=4;i++){const y=T+i*(h-T-B)/4;g.strokeStyle=grid;g.beginPath();g.moveTo(L,y);g.lineTo(w-R,y);g.stroke();g.fillStyle=muted;g.fillText(Math.round(max*(1-i/4)),L-8,y+4);}
  const n=Math.min(7,pts.length);g.textAlign="center";g.fillStyle=muted;
  for(let i=0;i<n;i++){const p=pts[Math.round(i*(pts.length-1)/Math.max(n-1,1))];g.fillText(S.range<=1440?hm(p.t):md(p.t),X(p.t),h-8);}
  // area
  const gr=g.createLinearGradient(0,T,0,h-B);gr.addColorStop(0,acc+"66");gr.addColorStop(1,acc+"05");
  let seg=[];const segs=[];pts.forEach(p=>{if(p.v==null){if(seg.length)segs.push(seg);seg=[];}else seg.push(p);});if(seg.length)segs.push(seg);
  segs.forEach(s=>{
    g.beginPath();g.moveTo(X(s[0].t),h-B);s.forEach(p=>g.lineTo(X(p.t),Y(p.v)));g.lineTo(X(s[s.length-1].t),h-B);g.closePath();g.fillStyle=gr;g.fill();
    g.beginPath();s.forEach((p,i)=>i?g.lineTo(X(p.t),Y(p.v)):g.moveTo(X(p.t),Y(p.v)));g.strokeStyle=acc;g.lineWidth=2;g.lineJoin="round";g.stroke();
  });
  if(pts.length<=70){g.fillStyle=acc;vals.forEach(p=>{g.beginPath();g.arc(X(p.t),Y(p.v),2.8,0,7);g.fill();});}
  pts.forEach(p=>{if(p.f>0){g.fillStyle=bad;g.beginPath();g.arc(X(p.t),h-B-5,4,0,7);g.fill();}});
  // legend
  g.textAlign="right";g.fillStyle=muted;g.fillText("\u0645\u062a\u0648\u0633\u0637 \u0632\u0645\u0646 \u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629 (ms)",w-R-24,T+2);g.strokeStyle=acc;g.lineWidth=3;g.beginPath();g.moveTo(w-R-20,T-1);g.lineTo(w-R,T-1);g.stroke();
  cv._geo={pts:pts,X:X};hover(cv);
}
function hover(cv){
  if(cv._h)return;cv._h=1;const tip=$("#tip");
  cv.addEventListener("mousemove",e=>{
    const geo=cv._geo;if(!geo)return;const x=e.clientX-cv.getBoundingClientRect().left;let best=null,bd=1e9;
    geo.pts.forEach(p=>{const d=Math.abs(geo.X(p.t)-x);if(d<bd){bd=d;best=p;}});
    if(!best||bd>45){tip.style.display="none";return;}
    tip.innerHTML="<b>"+dt(best.t)+"</b><br>"+(best.v!=null?f(best.v)+" ms":"\u0644\u0627 \u062a\u0648\u062c\u062f \u0627\u0633\u062a\u062c\u0627\u0628\u0629")+"<br><small>"+best.n+" \u0641\u062d\u0635 \u00b7 "+best.f+" \u0641\u0627\u0634\u0644</small>";
    tip.style.display="block";tip.style.left=(e.clientX+14)+"px";tip.style.top=(e.clientY-10)+"px";});
  cv.addEventListener("mouseleave",()=>tip.style.display="none");
}
function renderDetail(){
  if(S.page!=="services")return;
  const t=S.targets.find(x=>x.id===S.selected);
  if(!t){$("#dtitle").textContent="\u0627\u062e\u062a\u0631 \u062e\u062f\u0645\u0629 \u0645\u0646 \u0627\u0644\u062c\u062f\u0648\u0644 \u0644\u0639\u0631\u0636 \u062a\u0641\u0627\u0635\u064a\u0644\u0647\u0627";$("#dstats").innerHTML="";$("#chart2").style.display="none";return;}
  $("#chart2").style.display="block";$("#dtitle").textContent="\u062a\u0641\u0627\u0635\u064a\u0644: "+t.name;const h=t.hour;
  const items=[["\u0627\u0644\u062d\u0627\u0644\u0629",STAT[t.status]],["\u0627\u0644\u062a\u0648\u0641\u0631 24\u0633",t.uptime_24h!=null?f(t.uptime_24h,2)+"%":"-"],["\u0645\u062a\u0648\u0633\u0637 (ms)",f(h.avg)],["P50 (ms)",f(h.p50)],["P95 (ms)",f(h.p95)],["P99 (ms)",f(h.p99)],["\u0627\u0644\u0627\u062a\u062c\u0627\u0647",({improving:"\u064a\u062a\u062d\u0633\u0646",degrading:"\u064a\u062a\u0631\u0627\u062c\u0639",stable:"\u0645\u0633\u062a\u0642\u0631",unknown:"\u2014"})[t.trend]+(t.trend_change!=null?" ("+f(t.trend_change,0)+"%)":"")],["\u0641\u0627\u0635\u0644 \u0627\u0644\u0641\u062d\u0635",t.interval_sec+" \u062b"]];
  $("#dstats").innerHTML='<div class="stats">'+items.map(i=>'<div class="stat"><small>'+i[0]+'</small><b>'+i[1]+'</b></div>').join("")+'</div>'+(t.last_error?'<p class="bad">\u0622\u062e\u0631 \u062e\u0637\u0623: '+esc(t.last_error)+'</p>':"");
}

/* ---------- \u0625\u0636\u0627\u0641\u0629 \u062e\u062f\u0645\u0629 ---------- */
function setMethod(m){method=m;$$("#mseg button").forEach(b=>b.classList.toggle("on",b.dataset.m===m));}
async function addTarget(){
  $("#f_err").textContent="";
  try{
    await api("/api/targets",{method:"POST",body:JSON.stringify({name:$("#f_name").value,url:$("#f_url").value,method:method,interval_sec:$("#f_int").value,timeout_sec:$("#f_to").value,expected_status:$("#f_exp").value})});
    ["f_name","f_url","f_exp"].forEach(i=>$("#"+i).value="");toast("\u062a\u0645\u062a \u0625\u0636\u0627\u0641\u0629 \u0627\u0644\u062e\u062f\u0645\u0629\u060c \u0633\u064a\u0628\u062f\u0623 \u0627\u0644\u0641\u062d\u0635 \u062e\u0644\u0627\u0644 \u062b\u0648\u0627\u0646\u064d");refresh();setTimeout(refresh,3000);
  }catch(e){$("#f_err").textContent=e.message;}
}

/* ---------- \u0627\u062e\u062a\u0628\u0627\u0631 \u0627\u0644\u062d\u0645\u0644 ---------- */
function renderTargetSelect(){
  const sig=S.targets.map(t=>t.id+t.name).join("|");if(sig===S.sig)return;S.sig=sig;
  const sel=$("#lt_target"),cur=sel.value||S.selected;
  sel.innerHTML=S.targets.map(t=>'<option value="'+t.id+'">'+esc(t.name)+'</option>').join("");
  if(cur&&S.targets.find(t=>String(t.id)===String(cur)))sel.value=cur;
}
function mini(a){return '<div class="stats">'+a.map(x=>'<div class="stat"><small>'+x[0]+'</small><b>'+x[1]+'</b></div>').join("")+'</div>';}
function renderResult(kind,r){
  if(kind==="capacity"){
    return '<p><b>\u0623\u0642\u0635\u0649 \u062d\u0645\u0644 \u0645\u0642\u0628\u0648\u0644: <span class="good">'+r.max_ok_concurrency+' \u0645\u0633\u062a\u062e\u062f\u0645 \u0645\u062a\u0632\u0627\u0645\u0646</span></b><br><small>'+esc(r.reason)+' (\u0627\u0644\u062d\u062f\u0648\u062f: \u0623\u062e\u0637\u0627\u0621 \u2264 '+r.limits.error_rate+'% \u0648 P95 \u2264 '+r.limits.p95_ms+'ms)</small></p><div class="tw"><table style="min-width:520px"><thead><tr><th>\u0627\u0644\u0645\u062a\u0632\u0627\u0645\u0646\u0648\u0646</th><th>\u0627\u0644\u0637\u0644\u0628\u0627\u062a</th><th>\u0637\u0644\u0628/\u062b</th><th>\u0645\u062a\u0648\u0633\u0637</th><th>P95</th><th>\u0623\u062e\u0637\u0627\u0621 %</th><th>\u0627\u0644\u0646\u062a\u064a\u062c\u0629</th></tr></thead><tbody>'+
     r.steps.map(s=>'<tr style="cursor:default"><td>'+s.concurrency+'</td><td>'+s.requests+'</td><td>'+f(s.rps,1)+'</td><td>'+f(s.avg)+' ms</td><td>'+f(s.p95)+' ms</td><td>'+f(s.error_rate,1)+'</td><td class="'+(s.passed?"good":"bad")+'">'+(s.passed?"\u0646\u062c\u062d":"\u0641\u0634\u0644")+'</td></tr>').join("")+'</tbody></table></div>';
  }
  const codes=Object.entries(r.status_codes||{}).map(e=>esc(e[0])+": "+e[1]).join(" \u060c "),errs=Object.entries(r.errors||{}).map(e=>esc(e[0])+" ("+e[1]+")").join(" \u060c ");
  return mini([["\u0627\u0644\u0637\u0644\u0628\u0627\u062a",r.total],["\u0627\u0644\u0645\u062a\u0632\u0627\u0645\u0646\u0648\u0646",r.concurrency],["\u0627\u0644\u0645\u062f\u0629 (\u062b)",f(r.duration_s,2)],["\u0627\u0644\u0625\u0646\u062a\u0627\u062c\u064a\u0629 (\u0637\u0644\u0628/\u062b)",f(r.rps,1)],["\u0646\u0633\u0628\u0629 \u0627\u0644\u0646\u062c\u0627\u062d %",f(r.success_rate,1)],["\u0646\u0633\u0628\u0629 \u0627\u0644\u0623\u062e\u0637\u0627\u0621 %",f(r.error_rate,1)],["\u0627\u0644\u0645\u062a\u0648\u0633\u0637 (ms)",f(r.avg)],["P50 (ms)",f(r.p50)],["P95 (ms)",f(r.p95)],["P99 (ms)",f(r.p99)],["\u0627\u0644\u0623\u0642\u0635\u0649 (ms)",f(r.max)]])+'<small>\u0623\u0643\u0648\u0627\u062f \u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629: '+(codes||"-")+'</small>'+(errs?'<p class="bad">\u0627\u0644\u0623\u062e\u0637\u0627\u0621: '+errs+'</p>':"");
}
async function loadTests(){
  const id=$("#lt_target").value;if(!id){$("#lt_hist").innerHTML="";return;}
  const rows=await api("/api/loadtests?target_id="+id);
  $("#lt_hist").innerHTML=rows.map(r=>'<details><summary>'+(r.kind==="capacity"?"\u0627\u062e\u062a\u0628\u0627\u0631 \u0633\u0639\u0629":"\u0627\u062e\u062a\u0628\u0627\u0631 \u062d\u0645\u0644")+' \u2014 '+dt(r.ts)+'</summary>'+renderResult(r.kind,r.result)+'</details>').join("")||'<small>\u0644\u0627 \u062a\u0648\u062c\u062f \u0627\u062e\u062a\u0628\u0627\u0631\u0627\u062a \u0633\u0627\u0628\u0642\u0629 \u0644\u0647\u0630\u0647 \u0627\u0644\u062e\u062f\u0645\u0629</small>';
}
async function startTest(){
  const id=$("#lt_target").value;if(!id){$("#lt_msg").innerHTML='<span class="bad">\u0627\u062e\u062a\u0631 \u062e\u062f\u0645\u0629 \u0623\u0648\u0644\u0627\u064b</span>';return;}
  try{
    await api("/api/loadtest",{method:"POST",body:JSON.stringify({target_id:id,kind:$("#lt_kind").value,concurrency:$("#lt_conc").value,total:$("#lt_total").value,confirmed:$("#lt_ok").checked})});
    $("#lt_msg").innerHTML='<div class="bar"><i></i></div>\u0628\u062f\u0623 \u0627\u0644\u0627\u062e\u062a\u0628\u0627\u0631...';pollJob();
  }catch(e){$("#lt_msg").innerHTML='<span class="bad">'+esc(e.message)+'</span>';}
}
let jobT=null;
async function pollJob(){
  clearTimeout(jobT);const j=await api("/api/job");
  if(j.status==="running"){$("#lt_msg").innerHTML='<div class="bar"><i></i></div>\u23f3 '+esc(j.progress)+" \u2014 "+esc(j.target_name);jobT=setTimeout(pollJob,1000);}
  else if(j.status==="done"){$("#lt_msg").innerHTML='<b class="good">\u2705 \u0627\u0646\u062a\u0647\u0649 \u0627\u0644\u0627\u062e\u062a\u0628\u0627\u0631 \u2014 '+esc(j.target_name)+'</b>'+renderResult(j.kind,j.result);loadTests();refresh();}
  else if(j.status==="error"){$("#lt_msg").innerHTML='<span class="bad">'+esc(j.error)+'</span>';}
}

/* ---------- \u0627\u0644\u062a\u0642\u0627\u0631\u064a\u0631 \u0648\u0627\u0644\u0625\u0639\u062f\u0627\u062f\u0627\u062a ---------- */
async function loadReport(){
  const rows=await api("/api/report?days="+$("#rp_days").value);
  $("#rep").innerHTML=rows.map(r=>'<tr style="cursor:default"><td><div class="svc"><b>'+esc(r.name)+'</b><span>'+esc(r.url)+'</span></div></td><td class="num">'+r.checks+'</td><td class="num '+(r.uptime>=99?"good":r.uptime>=95?"warn":"bad")+'">'+(r.checks?f(r.uptime,2):"-")+'</td><td class="num">'+f(r.avg)+'</td><td class="num">'+f(r.p95)+'</td><td class="num">'+f(r.p99)+'</td><td class="num">'+f(r.max)+'</td><td class="num">'+r.outages+'</td></tr>').join("")||'<tr><td colspan="8" style="text-align:center;color:var(--muted)">\u0644\u0627 \u062a\u0648\u062c\u062f \u0628\u064a\u0627\u0646\u0627\u062a</td></tr>';
}
async function loadSettings(){
  const s=await api("/api/settings");$("#s_slow").value=s.slow_ms;$("#s_ref").value=String(S.refreshSec);
  $("#s_info").textContent="\u0645\u062f\u0629 \u0627\u0644\u0627\u062d\u062a\u0641\u0627\u0638 \u0628\u0646\u062a\u0627\u0626\u062c \u0627\u0644\u0641\u062d\u0635: "+s.retention_days+" \u064a\u0648\u0645 \u2014 \u0627\u0644\u0625\u0635\u062f\u0627\u0631 "+s.version;
}
async function saveSettings(){
  try{await api("/api/settings",{method:"POST",body:JSON.stringify({slow_ms:$("#s_slow").value})});toast("\u062a\u0645 \u062d\u0641\u0638 \u0627\u0644\u0625\u0639\u062f\u0627\u062f\u0627\u062a");refresh();}catch(e){toast(e.message);}
}

/* ---------- \u062a\u0634\u063a\u064a\u0644 ---------- */
initStatic();setTheme(ls.get("theme","dark"));
S.refreshSec=+ls.get("refresh","5");
const h0=(location.hash||"#home").slice(1);
go(NAV.find(n=>n[0]===h0&&h0!=="add")?h0:"home");
refresh();setRefresh(S.refreshSec);pollJob();
let rz=null;window.addEventListener("resize",()=>{clearTimeout(rz);rz=setTimeout(renderCharts,150);});
</script>
</body>
</html>
"""


# ----------------------------------------------------------------------------
#
# ----------------------------------------------------------------------------
def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="Continuous Performance & Reliability Testing Platform")
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"), help="Host address (defaults to 0.0.0.0 for cloud hosting)")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")), help="Port (uses PORT environment variable on hosting platforms)")
    ap.add_argument("--db", default=DEFAULT_DB, help="\u0645\u0633\u0627\u0631 \u0642\u0627\u0639\u062f\u0629 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a")
    ap.add_argument("--add", action="append", default=[], metavar="URL", help="\u0625\u0636\u0627\u0641\u0629 \u062e\u062f\u0645\u0629 \u0639\u0646\u062f \u0627\u0644\u062a\u0634\u063a\u064a\u0644 (\u064a\u0645\u0643\u0646 \u062a\u0643\u0631\u0627\u0631\u0647\u0627)")
    ap.add_argument("--no-browser", action="store_true", default=bool(os.environ.get("PORT")), help="Do not open a browser automatically (enabled by default on hosting platforms)")
    args = ap.parse_args()

    App.db = Database(args.db)
    App.jobs = JobManager(App.db)

    for url in args.add:
        try:
            name, u, m, i, to, exp = validate_target({"url": url})
            App.db.execute(
                "INSERT INTO targets(name,url,method,interval_sec,timeout_sec,expected_status,created_at) "
                "VALUES(?,?,?,?,?,?,?)", (name, u, m, i, to, exp, time.time()))
        except Exception as e:
            print("\u062a\u062c\u0627\u0647\u0644 \u0627\u0644\u0631\u0627\u0628\u0637 %s: %s" % (url, e))

    if not App.db.query("SELECT id FROM targets LIMIT 1"):
        for name, url in (("Example", "https://example.com"), ("Google", "https://www.google.com")):
            App.db.execute(
                "INSERT INTO targets(name,url,method,interval_sec,timeout_sec,created_at) VALUES(?,?,?,?,?,?)",
                (name, url, "GET", 30, 10, time.time()))

    App.monitor = Monitor(App.db)
    App.monitor.start()

    server, port = None, args.port
    for port in range(args.port, args.port + 20):
        try:
            server = ThreadingHTTPServer((args.host, port), Handler)
            break
        except OSError:
            continue
    if server is None:
        print("No free port found. Close the old copy and try again.")
        App.monitor.stop_event.set()
        return
    url = "http://%s:%d" % (args.host, port)
    print("=" * 60)
    print(" \u0627\u0644\u0645\u0646\u0635\u0629 \u062a\u0639\u0645\u0644 \u0627\u0644\u0622\u0646:  %s" % url)
    print(" \u0642\u0627\u0639\u062f\u0629 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a:    %s" % args.db)
    print(" \u0644\u0644\u0625\u064a\u0642\u0627\u0641: Ctrl+C")
    print("=" * 60)
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\u062c\u0627\u0631\u064a \u0627\u0644\u0625\u064a\u0642\u0627\u0641...")
    finally:
        App.monitor.stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()

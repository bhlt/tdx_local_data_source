# -*- coding: utf-8 -*-
"""
tick-stock-panel 数据网关（Linux 侧，零第三方依赖）

位置：SC_Linux，监听 127.0.0.1:18082
作用：接收 Windows 推送的全市场快照/五档/分钟线，按 tick-stock-panel 的
      data_providers 契约输出 HTTP 接口，供 data/data_sources/*.yaml 直接引用。

接口（鉴权：X-Api-Key 或 ?token=）：
    POST /ingest                 接收批次（gzip 可选）
    GET  /panel/realtime         全市场快照（契约字段）
    GET  /panel/depth5           ?symbols=a,b（不传=全部已缓存）
    GET  /panel/minute           ?symbols=a,b&start_time=...&end_time=...
    GET  /panel/daily            ?symbols=a,b&start_time=...&end_time=...
    GET  /panel/health           健康检查
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sqlite3
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DB_LOCK = threading.Lock()
DB_PATH = os.path.join("data", "relay", "panel.db")
TOKEN = os.environ.get("PANEL_TOKEN", "change-me")

DDL = """
CREATE TABLE IF NOT EXISTS panel_state (
    code TEXT PRIMARY KEY,
    name TEXT,
    prev_close REAL,
    open REAL,
    high REAL,
    low REAL,
    last REAL,
    volume REAL,
    amount REAL,
    float_shares REAL,
    ts TEXT,
    ts_ms INTEGER
);
CREATE TABLE IF NOT EXISTS depth_state (
    code TEXT PRIMARY KEY,
    asks TEXT, bids TEXT, ask_vols TEXT, bid_vols TEXT, ts_ms INTEGER
);
CREATE TABLE IF NOT EXISTS bars (
    code TEXT NOT NULL, period TEXT NOT NULL, time TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL,
    PRIMARY KEY (code, period, time)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY, kind TEXT, rows INTEGER, received_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_bars_lookup ON bars(period, time);
"""


def init_db(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with sqlite3.connect(path) as c:
        c.executescript(DDL)


def upsert_info(conn, rows):
    conn.executemany(
        "INSERT INTO panel_state(code,name,float_shares) VALUES(?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET name=excluded.name, float_shares=excluded.float_shares",
        [(r["code"], r.get("name"), r.get("float_shares")) for r in rows])


def upsert_seed(conn, rows):
    conn.executemany(
        "INSERT INTO panel_state(code,name,prev_close,open,high,low,last,volume,amount,float_shares,ts,ts_ms) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET "
        "name=COALESCE(excluded.name,panel_state.name),"
        "prev_close=excluded.prev_close,open=excluded.open,high=excluded.high,low=excluded.low,"
        "last=excluded.last,volume=excluded.volume,amount=excluded.amount,"
        "float_shares=COALESCE(excluded.float_shares,panel_state.float_shares),"
        "ts=excluded.ts,ts_ms=excluded.ts_ms",
        [(r["code"], r.get("name"), r.get("prev_close"), r.get("open"), r.get("high"), r.get("low"),
          r.get("last"), r.get("volume"), r.get("amount"), r.get("float_shares"),
          r.get("ts"), r.get("ts_ms")) for r in rows])


def apply_tick(conn, rows):
    """增量更新：维护 high/low，按成交量增量估算成交额"""
    changed = 0
    for r in rows:
        code, last, vol = r["code"], r.get("last"), r.get("volume")
        ts, ts_ms = r.get("ts"), r.get("ts_ms")
        cur = conn.execute(
            "SELECT prev_close,open,high,low,last,volume,amount FROM panel_state WHERE code=?", (code,)).fetchone()
        if cur is None:
            conn.execute(
                "INSERT INTO panel_state(code,last,volume,ts,ts_ms) VALUES(?,?,?,?,?) "
                "ON CONFLICT(code) DO UPDATE SET last=excluded.last,volume=excluded.volume,"
                "ts=excluded.ts,ts_ms=excluded.ts_ms",
                (code, last, vol, ts, ts_ms))
            changed += 1
            continue
        prev_close, op, hi, lo, old_last, old_vol, amount = cur
        hi = max(hi, last) if hi is not None and last is not None else (hi or last)
        lo = min(lo, last) if lo is not None and last is not None else (lo or last)
        if amount is None:
            amount = 0.0
        if old_vol is not None and vol is not None and last is not None and vol > old_vol:
            amount += (vol - old_vol) * 100.0 * last   # 手 → 股 × 价格
        conn.execute(
            "UPDATE panel_state SET last=?,volume=?,high=?,low=?,amount=?,ts=?,ts_ms=? WHERE code=?",
            (last, vol, hi, lo, amount, ts, ts_ms, code))
        changed += 1
    return changed


def upsert_depth(conn, rows):
    conn.executemany(
        "INSERT INTO depth_state(code,asks,bids,ask_vols,bid_vols,ts_ms) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET asks=excluded.asks,bids=excluded.bids,"
        "ask_vols=excluded.ask_vols,bid_vols=excluded.bid_vols,ts_ms=excluded.ts_ms",
        [(r["code"], json.dumps(r.get("asks", [])), json.dumps(r.get("bids", [])),
          json.dumps(r.get("ask_volumes", [])), json.dumps(r.get("bid_volumes", [])),
          r.get("ts_ms")) for r in rows])
    return len(rows)


def apply_beat(conn, rows):
    """心跳：本轮全市场快照已刷新，更新所有标的的行情时间。"""
    if not rows:
        return 0
    ts = rows[0].get("ts")
    ts_ms = rows[0].get("ts_ms")
    cur = conn.execute("UPDATE panel_state SET ts=?, ts_ms=?", (ts, ts_ms))
    return cur.rowcount


def upsert_bars(conn, rows):
    conn.executemany(
        "INSERT OR IGNORE INTO bars(code,period,time,open,high,low,close,volume,amount) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        [(r["code"], r.get("period", "1d"), r["time"], r.get("open"), r.get("high"),
          r.get("low"), r.get("close"), r.get("volume"), r.get("amount")) for r in rows])
    return len(rows)


def realtime_rows(conn, symbols):
    sql = ("SELECT code,name,prev_close,open,high,low,last,volume,amount,float_shares,ts_ms "
           "FROM panel_state WHERE last IS NOT NULL")
    args = []
    if symbols:
        sql += " AND code IN (%s)" % ",".join("?" * len(symbols))
        args = symbols
    out = []
    for code, name, pc, op, hi, lo, last, vol, amt, fshare, ts_ms in conn.execute(sql, args):
        if last is None:
            continue
        pc = pc or 0.0
        change_pct = (last / pc - 1.0) if pc else None
        amplitude = ((hi - lo) / pc) if (pc and hi is not None and lo is not None) else None
        turnover = (vol / (fshare * 100.0)) if (fshare and vol is not None) else None
        out.append({
            "symbol": code, "name": name,
            "last_price": last, "prev_close": pc,
            "open": op if op is not None else last,
            "high": hi if hi is not None else last,
            "low": lo if lo is not None else last,
            "volume": vol, "amount": amt,
            "change_pct": change_pct, "amplitude": amplitude,
            "turnover_rate": turnover,
            "timestamp": int(ts_ms) if ts_ms else int(time.time() * 1000),
        })
    return out


class Handler(BaseHTTPRequestHandler):
    server_version = "TDX-Panel-Gateway/0.1"

    def _auth(self, qs):
        return (self.headers.get("X-Api-Key") or self.headers.get("X-Auth-Token")
                or qs.get("token", [""])[0]) == TOKEN

    def _send(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        if "gzip" in (self.headers.get("Accept-Encoding") or ""):
            body = gzip.compress(body)
            self.send_response(status)
            self.send_header("Content-Encoding", "gzip")
        else:
            self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        if not self._auth(qs):
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        symbols = [c for c in qs.get("symbols", [""])[0].split(",") if c]
        with DB_LOCK, sqlite3.connect(DB_PATH) as conn:
            if u.path in ("/panel/realtime", "/realtime"):
                rows = realtime_rows(conn, None)
                return self._send({"ok": True, "count": len(rows), "data": rows})
            if u.path in ("/panel/depth5", "/depth5"):
                sql = "SELECT code,asks,bids,ask_vols,bid_vols,ts_ms FROM depth_state"
                args = []
                if symbols:
                    sql += " WHERE code IN (%s)" % ",".join("?" * len(symbols))
                    args = symbols
                data = {}
                for code, a, b, av, bv, ts in conn.execute(sql, args):
                    data[code] = {"asks": json.loads(a), "bids": json.loads(b),
                                  "ask_volumes": json.loads(av), "bid_volumes": json.loads(bv),
                                  "timestamp": int(ts) if ts else None}
                return self._send({"ok": True, "count": len(data), "data": data})
            if u.path in ("/panel/minute", "/panel/daily", "/minute", "/daily"):
                period = "1m" if "minute" in u.path else "1d"
                start = qs.get("start_time", [""])[0].replace("T", " ")
                end = qs.get("end_time", [""])[0].replace("T", " ")
                sql = ("SELECT code,time,open,high,low,close,volume,amount FROM bars "
                       "WHERE period=?")
                args = [period]
                if symbols:
                    sql += " AND code IN (%s)" % ",".join("?" * len(symbols))
                    args += symbols
                if start:
                    sql += " AND time >= ?"
                    args.append(start[:10] if period == "1d" else start[:19])
                if end:
                    sql += " AND time <= ?"
                    args.append(end[:10] if period == "1d" else end[:19])
                sql += " ORDER BY time"
                rows = [{"symbol": c, "datetime" if period == "1m" else "date": t,
                         "open": o, "high": h, "low": l, "close": cl,
                         "volume": v, "amount": a,
                         **({"freq": "1m"} if period == "1m" else {})}
                        for c, t, o, h, l, cl, v, a in conn.execute(sql, args)]
                return self._send({"ok": True, "count": len(rows), "data": {"rows": rows}})
            if u.path in ("/panel/health", "/health"):
                st = conn.execute("SELECT COUNT(*) FROM panel_state").fetchone()[0]
                dp = conn.execute("SELECT COUNT(*) FROM depth_state").fetchone()[0]
                br = conn.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
                last = conn.execute("SELECT MAX(ts_ms) FROM panel_state").fetchone()[0]
                return self._send({"ok": True, "panel_state": st, "depth": dp, "bars": br,
                                   "last_ts_ms": last, "db": DB_PATH})
        return self._send({"ok": False, "error": "not found"}, 404)

    def do_POST(self):  # noqa: N802
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        if not self._auth(qs):
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        if u.path != "/ingest":
            return self._send({"ok": False, "error": "not found"}, 404)

        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.headers.get("Content-Encoding", "").lower() == "gzip":
            raw = gzip.decompress(raw)
        try:
            p = json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            return self._send({"ok": False, "error": f"bad json: {exc}"}, 400)

        batch_id, kind, rows = p.get("batch_id", ""), p.get("kind", ""), p.get("rows", [])
        if not batch_id or not kind:
            return self._send({"ok": False, "error": "batch_id/kind required"}, 400)

        t0 = time.perf_counter()
        with DB_LOCK, sqlite3.connect(DB_PATH) as conn:
            if conn.execute("SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)).fetchone():
                return self._send({"ok": True, "duplicate": True, "accepted": 0})
            if kind == "panel_info":
                upsert_info(conn, rows); accepted = len(rows)
            elif kind == "panel_seed":
                upsert_seed(conn, rows); accepted = len(rows)
            elif kind == "panel_tick":
                accepted = apply_tick(conn, rows)
            elif kind == "panel_beat":
                accepted = apply_beat(conn, rows)
            elif kind == "depth":
                accepted = upsert_depth(conn, rows)
            elif kind == "bars":
                accepted = upsert_bars(conn, rows)
            else:
                return self._send({"ok": False, "error": f"unknown kind: {kind}"}, 400)
            conn.execute("INSERT INTO batches(batch_id,kind,rows,received_at) VALUES(?,?,?,?)",
                         (batch_id, kind, len(rows), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
        return self._send({"ok": True, "batch_id": batch_id, "kind": kind,
                           "received": len(rows), "accepted": accepted,
                           "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1)})


def main() -> int:
    global DB_PATH, TOKEN
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18082)
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--token", default=TOKEN)
    args = ap.parse_args()
    DB_PATH, TOKEN = args.db, args.token
    if TOKEN == "change-me":
        print("WARN: using default token, set PANEL_TOKEN")
    init_db(DB_PATH)
    print(f"panel gateway on http://{args.host}:{args.port}  db={DB_PATH}")
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

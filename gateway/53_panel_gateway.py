# -*- coding: utf-8 -*-
"""
tick-stock-panel 数据网关（Linux 侧，零第三方依赖）

位置：SC_Linux，监听 127.0.0.1:18082
作用：接收 Windows 推送的全市场快照/五档/分钟线，按 tick-stock-panel 的
      data_providers 契约输出 HTTP 接口，供 data/data_sources/*.yaml 直接引用。

接口（鉴权：X-Api-Key 或 ?token=）：
    POST /ingest                 接收批次（gzip 可选）
    GET  /panel/realtime         全市场快照（契约字段 + asset_type）
    GET  /panel/realtime?asset_type=stock        只返回股票（可逗号分隔多类型）
    GET  /panel/instruments      ?asset_type=stock,etf（资产类型 + 名称 + 股本）
    GET  /panel/depth5           ?symbols=a,b（不传=全部已缓存）
    GET  /panel/minute           ?symbols=a,b&start_time=...&end_time=...
    GET  /panel/daily            ?symbols=a,b&start_time=...&end_time=...
    GET  /panel/health           健康检查
    GET  /panel/audit            口径自检（资产类型、成交额量级、别名一致性）

口径约定（2026-11-25 起）：
    - 每条快照记录都带 asset_type：stock / etf / index / board / bond / reit / other
    - volume 统一为「手」，amount 统一为「元」（人民币）
    - amount 只对 stock / etf 做当日累计；index / board 的 amount 由成分股
      成交额汇总，绝不用 volume × price 回填（指数 close 是点位，不是价格）
    - 同一指数只保留规范代码；999999.SH 在入库时归一为 000001.SH
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sqlite3
import threading
import time
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DB_LOCK = threading.Lock()
DB_PATH = os.path.join("data", "relay", "panel.db")
TOKEN = os.environ.get("PANEL_TOKEN", "change-me")

ASSET_TYPES = ("stock", "etf", "index", "board", "bond", "reit", "other")

# 同一标的的多种代码。入库时统一成规范代码，避免同一指数被重复累加。
ALIASES = {
    "999999.SH": "000001.SH",
}

# 通达信代码段。顺序：板块 -> 指数 -> ETF -> 股票 -> REIT -> 债券 -> 其他
_BOARD_PREFIX = {"SH": ("880", "881", "882", "883", "884", "885")}
_INDEX_PREFIX = {"SH": ("000", "950"), "SZ": ("399",), "BJ": ("899",)}
_ETF_PREFIX = {"SH": ("501", "502", "506", "51", "56", "58"), "SZ": ("15", "16")}
_STOCK_PREFIX = {"SH": ("60", "68"), "SZ": ("00", "30"), "BJ": ("43", "83", "87", "92")}
_REIT_PREFIX = {"SH": ("508",), "SZ": ("180",)}
_BOND_PREFIX = {"SH": ("10", "11", "12", "13"), "SZ": ("10", "11", "12", "13")}

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
    asset_type TEXT,
    trade_date TEXT,
    volume_unit TEXT,
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
CREATE TABLE IF NOT EXISTS index_members (
    index_code TEXT NOT NULL, member_code TEXT NOT NULL,
    PRIMARY KEY (index_code, member_code)
);
CREATE INDEX IF NOT EXISTS idx_bars_lookup ON bars(period, time);
-- 分钟批量回补后单日 1m 可达百万行，按代码+时间查询需要独立索引
CREATE INDEX IF NOT EXISTS idx_bars_code ON bars(code, period, time);
"""


# ---------------------------------------------------------------------------
# 代码 / 资产类型
# ---------------------------------------------------------------------------
def split_code(code: str) -> tuple[str, str]:
    body, _, exchange = str(code or "").strip().partition(".")
    return body, exchange.upper()


def canonical_code(code: str) -> str:
    code = str(code or "").strip().upper()
    return ALIASES.get(code, code)


def _derived_asset_type(code: str) -> str:
    body, exchange = split_code(code)
    if body.startswith(_BOARD_PREFIX.get(exchange, ())):
        return "board"
    if body.startswith(_INDEX_PREFIX.get(exchange, ())):
        return "index"
    if body.startswith(_ETF_PREFIX.get(exchange, ())):
        return "etf"
    if body.startswith(_STOCK_PREFIX.get(exchange, ())):
        return "stock"
    if body.startswith(_REIT_PREFIX.get(exchange, ())):
        return "reit"
    if body.startswith(_BOND_PREFIX.get(exchange, ())):
        return "bond"
    return "other"


def classify_asset(code: str, declared: str | None = None) -> str:
    """给标的分类。

    优先采信上游声明；但如果代码明显是指数/板块/ETF，而上游错误地声明为
    stock，则按代码纠正。这样旧版采集器（没有 asset_type）和误标数据都能兜底。
    """
    code = canonical_code(code)
    derived = _derived_asset_type(code)
    declared = (declared or "").strip().lower()
    if declared not in ASSET_TYPES:
        return derived
    if declared == "stock" and derived in ("index", "board", "etf", "bond", "reit"):
        return derived
    if declared == "index" and derived == "board":
        return "board"
    return declared


def _volume_divisor(row: dict, asset_type: str) -> float:
    """把入站 volume 折算成「手」。

    新版采集器会显式带 volume_unit；旧版没有，只能按资产类型推断：
    指数/板块的实时 volume 是「股」，股票/ETF 是「手」。
    """
    unit = str(row.get("volume_unit") or "").strip().lower()
    if unit in ("share", "shares", "股"):
        return 100.0
    if unit in ("lot", "lots", "手"):
        return 1.0
    return 100.0 if asset_type in ("index", "board") else 1.0


def normalize_volume(value, row: dict, asset_type: str):
    if value is None:
        return None
    try:
        return float(value) / _volume_divisor(row, asset_type)
    except (TypeError, ValueError):
        return None


def _trade_date(ts) -> str:
    text = str(ts or "")
    if len(text) >= 10:
        return text[:10]
    return datetime.now().strftime("%Y-%m-%d")


def _is_preopen(ts) -> bool:
    """09:25 之前的快照可能仍是上一交易日的累计量/额，种子要归零。"""
    hm = str(ts or "")[11:16]
    return bool(hm) and hm < "09:25"


# ---------------------------------------------------------------------------
# 建库 / 迁移
# ---------------------------------------------------------------------------
def _ensure_columns(conn) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(panel_state)")}
    for name, decl in (("asset_type", "TEXT"), ("trade_date", "TEXT"),
                       ("volume_unit", "TEXT")):
        if name not in cols:
            conn.execute(f"ALTER TABLE panel_state ADD COLUMN {name} {decl}")


def migrate(conn) -> None:
    """兼容旧库：补列、合并别名、回填资产类型、把旧指数 volume 从股折成手。"""
    _ensure_columns(conn)

    for alias, canonical in ALIASES.items():
        row = conn.execute(
            "SELECT name,prev_close,open,high,low,last,volume,amount,float_shares,ts,ts_ms "
            "FROM panel_state WHERE code=?", (alias,)).fetchone()
        if row is None:
            continue
        if conn.execute("SELECT 1 FROM panel_state WHERE code=?", (canonical,)).fetchone():
            # 两个代码都在：保留规范代码，只用别名补缺字段，随后删除别名
            conn.execute(
                "UPDATE panel_state SET "
                "name=COALESCE(name,?),prev_close=COALESCE(prev_close,?),"
                "open=COALESCE(open,?),high=COALESCE(high,?),low=COALESCE(low,?),"
                "last=COALESCE(last,?),volume=COALESCE(volume,?),amount=COALESCE(amount,?),"
                "float_shares=COALESCE(float_shares,?),ts=COALESCE(ts,?),ts_ms=COALESCE(ts_ms,?) "
                "WHERE code=?", (*row, canonical))
            conn.execute("DELETE FROM panel_state WHERE code=?", (alias,))
        else:
            conn.execute("UPDATE panel_state SET code=? WHERE code=?", (canonical, alias))
        # 别名对应的五档 / 日线 / 成分映射也一并归一
        conn.execute("UPDATE OR IGNORE depth_state SET code=? WHERE code=?", (canonical, alias))
        conn.execute("DELETE FROM depth_state WHERE code=?", (alias,))
        conn.execute("UPDATE OR IGNORE bars SET code=? WHERE code=?", (canonical, alias))
        conn.execute("DELETE FROM bars WHERE code=?", (alias,))
        conn.execute("UPDATE OR IGNORE index_members SET index_code=? WHERE index_code=?",
                     (canonical, alias))
        conn.execute("DELETE FROM index_members WHERE index_code=?", (alias,))

    rows = conn.execute(
        "SELECT code,volume,volume_unit,ts,asset_type,trade_date FROM panel_state").fetchall()
    for code, volume, unit, ts, declared, trade_date in rows:
        at = classify_asset(code, declared)
        if at in ("index", "board") and unit not in ("lot", "lots", "手"):
            # 旧版网关把指数 volume 按「股」原样落库，这里只折算一次
            if volume is not None:
                volume = float(volume) / 100.0
            unit = "lot"
        elif unit is None:
            unit = "lot"
        conn.execute(
            "UPDATE panel_state SET asset_type=?,volume=?,volume_unit=?,trade_date=?,"
            "amount=CASE WHEN ? IN ('index','board') THEN NULL ELSE amount END WHERE code=?",
            (at, volume, unit, trade_date or _trade_date(ts), at, code))

    # 历史指数行不能再保留 volume×price 的假成交额
    conn.execute("UPDATE panel_state SET amount=NULL WHERE asset_type IN ('index','board')")


def init_db(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(DDL)
        migrate(conn)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 写入
# ---------------------------------------------------------------------------
def upsert_info(conn, rows):
    values = []
    for r in rows:
        code = canonical_code(r.get("code"))
        if not code:
            continue
        values.append((code, r.get("name"), r.get("float_shares"),
                       classify_asset(code, r.get("asset_type"))))
    conn.executemany(
        "INSERT INTO panel_state(code,name,float_shares,asset_type) VALUES(?,?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET name=COALESCE(excluded.name,panel_state.name),"
        "float_shares=COALESCE(excluded.float_shares,panel_state.float_shares),"
        "asset_type=excluded.asset_type",
        values)


def upsert_seed(conn, rows):
    """全市场种子快照：写入真实的 prev_close/open/high/low/volume/amount。

    种子是权威快照，OHLC 与累计量/额直接覆盖旧值（不再和旧值取 max/min），
    否则盘中重启时会把盘前写入的昨日累计值一直保留下来。
    09:25 之前的快照可能仍是上一交易日累计量，统一归零。
    index / board 的 amount 一律置空，由 realtime_rows() 按成分股汇总。
    """
    for r in rows:
        code = canonical_code(r.get("code"))
        if not code:
            continue
        at = classify_asset(code, r.get("asset_type"))
        vol = normalize_volume(r.get("volume"), r, at)
        last = r.get("last")
        amount = r.get("amount")
        if at in ("index", "board"):
            amount = None
        if _is_preopen(r.get("ts")):
            # 盘前拿到的累计量/额可能是上一交易日的，不能写进当日
            vol = 0.0 if vol is not None else vol
            amount = 0.0 if at in ("stock", "etf") else None
        td = _trade_date(r.get("ts"))
        cur = conn.execute("SELECT amount FROM panel_state WHERE code=?", (code,)).fetchone()
        if cur is None:
            if at in ("stock", "etf") and amount is None:
                amount = 0.0
            conn.execute(
                "INSERT INTO panel_state(code,name,prev_close,open,high,low,last,volume,amount,"
                "float_shares,asset_type,trade_date,volume_unit,ts,ts_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (code, r.get("name"), r.get("prev_close"), r.get("open"), r.get("high"),
                 r.get("low"), last, vol, amount, r.get("float_shares"), at, td, "lot",
                 r.get("ts"), r.get("ts_ms")))
            continue

        old_amount = cur[0]
        if at in ("stock", "etf") and amount is None:
            # 局部种子（快照失败/停牌兜底）没有 amount，保留已有累计值
            amount = old_amount if old_amount is not None else 0.0
        conn.execute(
            "UPDATE panel_state SET name=COALESCE(?,name),prev_close=?,open=?,high=?,low=?,"
            "last=?,volume=?,amount=?,float_shares=COALESCE(?,float_shares),asset_type=?,"
            "trade_date=?,volume_unit='lot',ts=?,ts_ms=? WHERE code=?",
            (r.get("name"), r.get("prev_close"), r.get("open"), r.get("high"), r.get("low"),
             last, vol, amount, r.get("float_shares"), at, td, r.get("ts"), r.get("ts_ms"), code))


def apply_tick(conn, rows):
    """增量更新：维护 high/low，股票/ETF 按成交量增量估算当日累计成交额。

    index / board 不做任何 volume×price 回填；它们的 amount 在查询时由
    成分股成交额汇总得到。
    """
    changed = 0
    for r in rows:
        code = canonical_code(r.get("code"))
        if not code:
            continue
        at = classify_asset(code, r.get("asset_type"))
        vol = normalize_volume(r.get("volume"), r, at)
        last = r.get("last")
        ts, ts_ms = r.get("ts"), r.get("ts_ms")
        td = _trade_date(ts)
        cur = conn.execute(
            "SELECT prev_close,open,high,low,last,volume,amount,trade_date,asset_type "
            "FROM panel_state WHERE code=?", (code,)).fetchone()
        if cur is None:
            amount = 0.0 if at in ("stock", "etf") else None
            conn.execute(
                "INSERT INTO panel_state(code,last,volume,amount,asset_type,trade_date,"
                "volume_unit,ts,ts_ms) VALUES(?,?,?,?,?,?,'lot',?,?) "
                "ON CONFLICT(code) DO UPDATE SET last=excluded.last,volume=excluded.volume,"
                "amount=excluded.amount,asset_type=excluded.asset_type,"
                "trade_date=excluded.trade_date,volume_unit='lot',ts=excluded.ts,ts_ms=excluded.ts_ms",
                (code, last, vol, amount, at, td, ts, ts_ms))
            changed += 1
            continue

        prev_close, op, hi, lo, old_last, old_vol, amount, old_td, _old_at = cur
        if old_td and old_td != td:
            # 新交易日：重置日内高低与成交额，避免沿用昨日累计值
            op, hi, lo = last, last, last
            old_vol = None
            amount = 0.0 if at in ("stock", "etf") else None
        if last is not None:
            hi = last if hi is None else max(hi, last)
            lo = last if lo is None else min(lo, last)
            if op is None:
                op = last
        prev_close = r.get("prev_close") if r.get("prev_close") is not None else prev_close

        if at in ("stock", "etf"):
            if amount is None:
                amount = 0.0
            # 只处理成交量增加；成交量回退（重连/换源）时不倒扣
            if old_vol is not None and vol is not None and last is not None and vol > old_vol:
                amount += (vol - old_vol) * 100.0 * last   # 手 → 股 × 价格
        else:
            amount = None

        conn.execute(
            "UPDATE panel_state SET last=?,volume=?,high=?,low=?,open=?,prev_close=?,amount=?,"
            "asset_type=?,trade_date=?,volume_unit='lot',ts=?,ts_ms=? WHERE code=?",
            (last, vol, hi, lo, op, prev_close, amount, at, td, ts, ts_ms, code))
        changed += 1
    return changed


def upsert_depth(conn, rows):
    conn.executemany(
        "INSERT INTO depth_state(code,asks,bids,ask_vols,bid_vols,ts_ms) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET asks=excluded.asks,bids=excluded.bids,"
        "ask_vols=excluded.ask_vols,bid_vols=excluded.bid_vols,ts_ms=excluded.ts_ms",
        [(canonical_code(r["code"]), json.dumps(r.get("asks", [])),
          json.dumps(r.get("bids", [])), json.dumps(r.get("ask_volumes", [])),
          json.dumps(r.get("bid_volumes", [])), r.get("ts_ms")) for r in rows])
    return len(rows)


def replace_members(conn, rows, replace: bool = False):
    """维护「指数/板块 -> 成分股」映射，用于汇总非股票标的的成交额。"""
    if replace:
        conn.execute("DELETE FROM index_members")
    accepted = 0
    for r in rows:
        index_code = canonical_code(r.get("index_code"))
        if not index_code:
            continue
        conn.execute("DELETE FROM index_members WHERE index_code=?", (index_code,))
        members = [canonical_code(m) for m in (r.get("members") or []) if m]
        conn.executemany(
            "INSERT OR IGNORE INTO index_members(index_code,member_code) VALUES(?,?)",
            [(index_code, m) for m in members])
        accepted += 1
    return accepted


def apply_beat(conn, rows):
    """心跳：本轮全市场快照已刷新，更新所有标的的行情时间。"""
    if not rows:
        return 0
    ts = rows[0].get("ts")
    ts_ms = rows[0].get("ts_ms")
    cur = conn.execute("UPDATE panel_state SET ts=?, ts_ms=?", (ts, ts_ms))
    return cur.rowcount


def upsert_bars(conn, rows, replace: bool = False):
    """bars：忽略重复（实时合成，先到先得）；bars_final：覆盖（历史真实数据回补）。"""
    verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
    conn.executemany(
        f"{verb} INTO bars(code,period,time,open,high,low,close,volume,amount) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        [(canonical_code(r["code"]), r.get("period", "1d"), r["time"], r.get("open"),
          r.get("high"), r.get("low"), r.get("close"), r.get("volume"), r.get("amount"))
         for r in rows])
    return len(rows)


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------
def _parse_asset_types(raw) -> set[str]:
    if not raw:
        return set()
    parts = ",".join(raw) if isinstance(raw, list) else str(raw)
    wanted = {p.strip().lower() for p in parts.split(",") if p.strip()}
    return {p for p in wanted if p in ASSET_TYPES}


def _broad_members(code: str, states: dict[str, dict]) -> list[str]:
    """成分映射缺失时，为宽基指数提供可推导的近似成分。

    只覆盖「成分 = 某交易所/某代码段全部 A 股」的指数；行业/主题/成分股
    子集指数（如深证成指、创业板指、科创50）无法推导，宁可返回 amount=None。
    """
    # 全 A / 全 ETF 等特殊板块
    if code == "880008.SH":
        return [c for c, st in states.items() if st["asset_type"] == "stock"]
    if code == "880096.SH":
        return [c for c, st in states.items() if st["asset_type"] == "etf"]

    rule = {
        "000001.SH": (".SH", ()),   # 上证指数 = 沪市全部 A 股
        "000002.SH": (".SH", ()),   # 上证 A 股指数
        "000888.SH": (".SH", ()),   # 上证收益
        "399107.SZ": (".SZ", ()),   # 深证 A 指
        "000680.SH": (".SH", ("688", "689")),  # 科创综指
    }.get(code)
    if not rule:
        return []
    suffix, prefixes = rule
    return [c for c, st in states.items()
            if st["asset_type"] == "stock" and c.endswith(suffix)
            and (not prefixes or c.startswith(prefixes))]


def realtime_rows(conn, symbols=None, asset_types=None, include_nonstock_amount=False):
    """返回带 asset_type 的快照。

    index / board 的 amount 默认返回 None：全量快照如果直接求和，指数/板块
    成分股会重复计入。显式请求 asset_type=index/board，或用 symbols 指定
    指数代码时，才返回按 index_members 汇总的成分股成交额。
    """
    wanted_symbols = {canonical_code(s) for s in (symbols or []) if s}
    wanted_types = set(asset_types or ())

    raw = conn.execute(
        "SELECT code,name,prev_close,open,high,low,last,volume,amount,float_shares,"
        "asset_type,ts_ms FROM panel_state WHERE last IS NOT NULL").fetchall()

    states: dict[str, dict] = {}
    by_code: dict[str, tuple[float | None, float | None]] = {}
    for (code, name, pc, op, hi, lo, last, vol, amt, fshare, declared, ts_ms) in raw:
        at = classify_asset(code, declared)
        states[code] = {
            "code": code, "name": name, "prev_close": pc, "open": op,
            "high": hi, "low": lo, "last": last, "volume": vol,
            "amount": None if at in ("index", "board") else amt,
            "float_shares": fshare, "asset_type": at, "ts_ms": ts_ms,
        }
        if at in ("stock", "etf"):
            by_code[code] = (vol, amt)

    members: dict[str, list[str]] = defaultdict(list)
    for index_code, member_code in conn.execute(
            "SELECT index_code,member_code FROM index_members"):
        members[index_code].append(member_code)

    out = []
    for code, st in states.items():
        at = st["asset_type"]
        if wanted_types and at not in wanted_types:
            continue
        if wanted_symbols and code not in wanted_symbols:
            continue

        vol, amt = st["volume"], st["amount"]
        amount_source = None
        if at in ("stock", "etf"):
            amount_source = "realtime_delta"
        elif at in ("index", "board"):
            include_amount = (include_nonstock_amount
                              or at in wanted_types
                              or code in wanted_symbols)
            if include_amount:
                member_list = members.get(code) or _broad_members(code, states)
                if member_list:
                    member_amounts = [by_code[m][1] for m in member_list
                                      if m in by_code and by_code[m][1] is not None]
                    member_volumes = [by_code[m][0] for m in member_list
                                      if m in by_code and by_code[m][0] is not None]
                    amt = sum(member_amounts) if member_amounts else None
                    vol = sum(member_volumes) if member_volumes else vol
                    amount_source = "constituents" if amt is not None else None

        last = st["last"]
        pc = st["prev_close"] or 0.0
        hi, lo = st["high"], st["low"]
        fshare = st["float_shares"]
        change_pct = (last / pc - 1.0) if pc else None
        amplitude = ((hi - lo) / pc) if (pc and hi is not None and lo is not None) else None
        turnover = (vol / (fshare * 100.0)) if (at == "stock" and fshare and vol is not None) else None
        ts_ms = st["ts_ms"]
        out.append({
            "symbol": code, "name": st["name"], "asset_type": at,
            "last_price": last, "prev_close": pc,
            "open": st["open"] or last,
            "high": hi or last,
            "low": lo or last,
            "volume": vol, "amount": amt, "amount_source": amount_source,
            "change_pct": change_pct, "amplitude": amplitude,
            "turnover_rate": turnover,
            "timestamp": int(ts_ms) if ts_ms else int(time.time() * 1000),
        })
    return out


def instrument_rows(conn, symbols=None, asset_types=None):
    """导出维表（symbol / name / asset_type / exchange / 股本），供面板对齐 instruments。"""
    wanted_symbols = {canonical_code(s) for s in (symbols or []) if s}
    wanted_types = set(asset_types or ())
    out = []
    for code, name, fshare, declared in conn.execute(
            "SELECT code,name,float_shares,asset_type FROM panel_state"):
        at = classify_asset(code, declared)
        if wanted_types and at not in wanted_types:
            continue
        if wanted_symbols and code not in wanted_symbols:
            continue
        body, exchange = split_code(code)
        out.append({
            "symbol": code, "name": name or "", "asset_type": at,
            "code": body, "exchange": exchange, "region": "CN", "type": at,
            "float_shares": fshare, "total_shares": None,
        })
    return out


def _samples(items, limit=5):
    return items[:limit]


def audit(conn):
    """网关口径自检：资产类型、股票成交额比值、指数成交额量级、别名唯一性。"""
    rows = realtime_rows(conn, include_nonstock_amount=True)
    counts = Counter(r["asset_type"] for r in rows)
    amount_sum: dict[str, float] = defaultdict(float)
    stock_amount = sum(float(r["amount"]) for r in rows
                       if r["asset_type"] == "stock" and r["amount"] is not None)
    stock_bad = []
    index_over = []
    index_missing = []

    for r in rows:
        at = r["asset_type"]
        amt = r["amount"]
        if amt is not None:
            amount_sum[at] += float(amt)
        if at == "stock":
            vol, last = r["volume"], r["last_price"]
            if vol and last and amt is not None:
                ratio = float(amt) / (float(vol) * 100.0 * float(last))
                if not 0.8 <= ratio <= 1.2:
                    stock_bad.append({"symbol": r["symbol"], "ratio": round(ratio, 3)})
        elif at in ("index", "board"):
            if amt is None:
                index_missing.append(r["symbol"])
            # 单指数成交额不应超过全市场股票成交额一个量级
            elif stock_amount and float(amt) > stock_amount * 10:
                index_over.append({"symbol": r["symbol"], "amount": amt})

    aliases = [code for code in ALIASES
               if conn.execute("SELECT 1 FROM panel_state WHERE code=?", (code,)).fetchone()]

    equity_amount = amount_sum.get("stock", 0.0) + amount_sum.get("etf", 0.0)
    naive_total = sum(amount_sum.values())

    checks = {
        "stock_amount_ratio": {
            "status": "pass" if not stock_bad else "warn",
            "bad": len(stock_bad), "samples": _samples(stock_bad),
        },
        "index_amount_present": {
            "status": "pass" if not index_missing else "warn",
            "missing": len(index_missing), "samples": _samples(index_missing),
        },
        "index_amount_sane": {
            "status": "pass" if not index_over else "fail",
            "over": len(index_over), "samples": _samples(index_over),
        },
        "alias_unique": {
            "status": "pass" if not aliases else "fail",
            "unexpected": aliases,
        },
    }
    return {
        "ok": all(v["status"] != "fail" for v in checks.values()),
        "counts": dict(counts),
        "amount_sum": {k: round(v, 2) for k, v in amount_sum.items()},
        "stock_amount": round(stock_amount, 2),
        "equity_amount": round(equity_amount, 2),
        # 直接对全部 asset_type 求和会把指数/板块成分重复计入，仅供排查
        "naive_total_amount": round(naive_total, 2),
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "TDX-Panel-Gateway/0.2"

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
        with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
            if u.path in ("/panel/realtime", "/realtime"):
                types = _parse_asset_types(qs.get("asset_type"))
                include_nonstock = bool(types & {"index", "board"}) or bool(symbols)
                rows = realtime_rows(conn, symbols, types,
                                     include_nonstock_amount=include_nonstock)
                return self._send({
                    "ok": True, "count": len(rows), "data": rows,
                    "units": {"volume": "lot", "amount": "CNY", "price": "CNY"},
                })
            if u.path in ("/panel/audit", "/audit"):
                return self._send({"ok": True, "data": audit(conn)})
            if u.path in ("/panel/instruments", "/instruments"):
                types = _parse_asset_types(qs.get("asset_type"))
                rows = instrument_rows(conn, symbols, types)
                return self._send({"ok": True, "count": len(rows), "data": rows})
            if u.path in ("/panel/depth5", "/depth5"):
                sql = "SELECT code,asks,bids,ask_vols,bid_vols,ts_ms FROM depth_state"
                args = []
                if symbols:
                    sql += " WHERE code IN (%s)" % ",".join("?" * len(symbols))
                    args = [canonical_code(s) for s in symbols]
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
                # 指定了代码时强制走 code 索引：单日 1m 上百万行，
                # 默认计划会先按 (period,time) 扫全天再筛代码，慢 30 倍。
                table = "bars INDEXED BY idx_bars_code" if symbols else "bars"
                sql = (f"SELECT code,time,open,high,low,close,volume,amount FROM {table} "
                       "WHERE period=?")
                args = [period]
                if symbols:
                    sql += " AND code IN (%s)" % ",".join("?" * len(symbols))
                    args += [canonical_code(s) for s in symbols]
                if start:
                    sql += " AND time >= ?"
                    args.append(start[:10] if period == "1d" else start[:19])
                if end:
                    sql += " AND time <= ?"
                    args.append(end[:10] if period == "1d" else end[:19])
                sql += " ORDER BY time"
                today = datetime.now().strftime("%Y-%m-%d")
                now_ms = int(time.time() * 1000)
                rows = []
                for c, t, o, h, l, cl, v, a in conn.execute(sql, args):
                    row = {"symbol": c, "datetime" if period == "1m" else "date": t,
                           "open": o, "high": h, "low": l, "close": cl,
                           "volume": v, "amount": a}
                    if period == "1m":
                        row["freq"] = "1m"
                    elif t == today:
                        # 当日日线带上快照时间，面板据此写入 quote_ts；
                        # 历史日线不带，避免被误判为盘中快照。
                        row["timestamp"] = now_ms
                    rows.append(row)
                return self._send({"ok": True, "count": len(rows), "data": {"rows": rows}})
            if u.path in ("/panel/health", "/health"):
                st = conn.execute("SELECT COUNT(*) FROM panel_state").fetchone()[0]
                dp = conn.execute("SELECT COUNT(*) FROM depth_state").fetchone()[0]
                br = conn.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
                im = conn.execute("SELECT COUNT(*) FROM index_members").fetchone()[0]
                last = conn.execute("SELECT MAX(ts_ms) FROM panel_state").fetchone()[0]
                counts = dict(Counter(
                    classify_asset(code, at) for code, at in
                    conn.execute("SELECT code,asset_type FROM panel_state")))
                return self._send({"ok": True, "panel_state": st, "depth": dp, "bars": br,
                                   "index_members": im, "asset_counts": counts,
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
        with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
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
            elif kind == "panel_members":
                accepted = replace_members(conn, rows, replace=bool(p.get("replace")))
            elif kind == "depth":
                accepted = upsert_depth(conn, rows)
            elif kind in ("bars", "bars_final"):
                # bars_final 来自 65_upload_minute.py：用客户端下载的真实分钟
                # 覆盖盘中快照合成的近似 bar
                accepted = upsert_bars(conn, rows, replace=(kind == "bars_final"))
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

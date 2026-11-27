# -*- coding: utf-8 -*-
"""
Windows → tick-stock-panel 采集代理（零第三方依赖）

从通达信客户端取数，推送到 SC_Linux 的 53_panel_gateway.py。

同步内容：
    1. panel_info   代码 → 名称 / 流通股本（万股），启动时一次
    2. panel_seed   全市场快照种子：prev_close/open/high/low/last/volume/amount（启动 + 每日开盘后）
    3. panel_tick   全市场增量：last / volume，网关侧维护 high/low 与成交额
    4. depth        指定池五档（默认中证500），每 10s
    5. bars         1 分钟线（默认中证500），每 60s

用法：
    set TDX_PYPLUGINS=d:\\program\\new_tdx64\\PYPlugins\\user
    set PANEL_TOKEN=your-secret
    python scripts\\52_push_panel.py --receiver http://127.0.0.1:18082 --market 5 --interval 6

    # 只跑一轮自检
    python scripts\\52_push_panel.py --receiver http://127.0.0.1:18082 --once --limit 300
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

TDX_USER = os.environ.get("TDX_PYPLUGINS", r"d:\program\new_tdx64\PYPlugins\user")
sys.path.insert(0, TDX_USER)

import pandas as pd  # noqa: E402
from tqcenter import tq  # noqa: E402

SNAP_FIELDS = ["Now", "LastClose", "Open", "Max", "Min", "Volume", "Amount",
               "Buyp", "Buyv", "Sellp", "Sellv"]

# tq.get_market_snapshot 在并发调用时会串数据（实测 1000 只 8 线程出现
# 000002.SZ 拿到 000010.SZ 的 OHLC/Amount）。所有快照/板块成分调用串行化。
TQ_LOCK = threading.Lock()

# 与网关保持一致的资产分类规则。采集端在源头打标，网关仍会再兜底一次。
ASSET_TYPES = ("stock", "etf", "index", "board", "bond", "reit", "other")
ALIASES = {"999999.SH": "000001.SH"}
_BOARD_PREFIX = {"SH": ("880", "881", "882", "883", "884", "885")}
_INDEX_PREFIX = {"SH": ("000", "950"), "SZ": ("399",), "BJ": ("899",)}
_ETF_PREFIX = {"SH": ("501", "502", "506", "51", "56", "58"), "SZ": ("15", "16")}
_STOCK_PREFIX = {"SH": ("60", "68"), "SZ": ("00", "30"), "BJ": ("43", "83", "87", "92")}
_REIT_PREFIX = {"SH": ("508",), "SZ": ("180",)}
_BOND_PREFIX = {"SH": ("10", "11", "12", "13"), "SZ": ("10", "11", "12", "13")}


def canonical_code(code: str) -> str:
    code = str(code or "").strip().upper()
    return ALIASES.get(code, code)


def classify_asset(code: str, declared: str | None = None) -> str:
    body, _, exchange = canonical_code(code).partition(".")
    exchange = exchange.upper()
    if body.startswith(_BOARD_PREFIX.get(exchange, ())):
        derived = "board"
    elif body.startswith(_INDEX_PREFIX.get(exchange, ())):
        derived = "index"
    elif body.startswith(_ETF_PREFIX.get(exchange, ())):
        derived = "etf"
    elif body.startswith(_STOCK_PREFIX.get(exchange, ())):
        derived = "stock"
    elif body.startswith(_REIT_PREFIX.get(exchange, ())):
        derived = "reit"
    elif body.startswith(_BOND_PREFIX.get(exchange, ())):
        derived = "bond"
    else:
        derived = "other"
    declared = (declared or "").strip().lower()
    if declared not in ASSET_TYPES:
        return derived
    if declared == "stock" and derived in ("index", "board", "etf", "bond", "reit"):
        return derived
    if declared == "index" and derived == "board":
        return "board"
    return declared


def volume_unit_of(asset_type: str) -> str:
    """指数/板块的实时 volume 是「股」，股票/ETF 是「手」。网关统一折成手。"""
    return "share" if asset_type in ("index", "board") else "lot"


def push(receiver: str, token: str, kind: str, rows: list[dict], timeout: float = 20.0,
         replace: bool = False) -> dict:
    payload = {"batch_id": f"{kind}-{uuid.uuid4().hex[:12]}", "kind": kind, "rows": rows}
    if replace:
        payload["replace"] = True
    body = gzip.compress(json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"))
    import urllib.request
    req = urllib.request.Request(
        receiver.rstrip("/") + "/ingest", data=body, method="POST",
        headers={"Content-Type": "application/json", "Content-Encoding": "gzip", "X-Api-Key": token})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def norm(df):
    if df is None or getattr(df, "empty", True):
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.T
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:  # noqa: BLE001
        return default


def snap_row(code: str, asset_type: str | None = None) -> dict | None:
    try:
        with TQ_LOCK:
            s = tq.get_market_snapshot(stock_code=code, field_list=SNAP_FIELDS)
    except Exception:  # noqa: BLE001
        return None
    if not s or not s.get("Now"):
        return None
    now = to_float(s.get("Now"))
    if now is None:
        return None
    at = classify_asset(code, asset_type)
    # 指数/板块的 Amount 口径不可靠（且 close 是点位），一律不落 amount，
    # 由网关按成分股成交额汇总；股票/ETF 继续用快照里的当日累计成交额。
    amount = None if at in ("index", "board") else (
        (to_float(s.get("Amount"), 0.0) or 0.0) * 10000.0)   # 万元 → 元
    return {
        "code": canonical_code(code),
        "asset_type": at,
        "volume_unit": volume_unit_of(at),
        "last": now,
        "prev_close": to_float(s.get("LastClose")),
        "open": to_float(s.get("Open")),
        "high": to_float(s.get("Max")),
        "low": to_float(s.get("Min")),
        "volume": to_float(s.get("Volume")),
        "amount": amount,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ts_ms": int(time.time() * 1000),
    }


def depth_row(code: str) -> dict | None:
    try:
        with TQ_LOCK:
            s = tq.get_market_snapshot(stock_code=code,
                                       field_list=["Buyp", "Buyv", "Sellp", "Sellv"])
    except Exception:  # noqa: BLE001
        return None
    if not s:
        return None

    def pair(prices, vols):
        out = []
        for p, v in zip(prices or [], vols or []):
            pf, vf = to_float(p), to_float(v)
            if pf is None or vf is None:
                continue
            out.append([pf, vf])
        return out

    return {"code": code,
            "bids": pair(s.get("Buyp"), s.get("Buyv")),
            "asks": pair(s.get("Sellp"), s.get("Sellv")),
            "bid_volumes": [v for _, v in pair(s.get("Buyp"), s.get("Buyv"))],
            "ask_volumes": [v for _, v in pair(s.get("Sellp"), s.get("Sellv"))],
            "ts_ms": int(time.time() * 1000)}


class MinuteAggregator:
    """用实时快照合成当日 1 分钟线（本地 1m 数据只到上一交易日）

    标签与通达信一致：09:30~09:31 的 bar 记为 09:31。
    """

    def __init__(self):
        self.state: dict[str, dict] = {}

    @staticmethod
    def _label(minute_start: str) -> str:
        t = datetime.strptime(minute_start, "%Y-%m-%d %H:%M:%S") + pd.Timedelta(minutes=1)
        return t.strftime("%Y-%m-%d %H:%M:%S")

    def feed(self, code: str, ts: str, last: float, cum_vol: float | None) -> dict | None:
        minute = ts[:16] + ":00"
        st = self.state.get(code)
        done = None
        if st is not None and st["minute"] != minute:
            done = self._finalize(code, st)
            st = None
        if st is None:
            self.state[code] = {"minute": minute, "open": last, "high": last, "low": last,
                                "close": last, "vol0": cum_vol, "last_vol": cum_vol}
        else:
            st["high"] = max(st["high"], last)
            st["low"] = min(st["low"], last)
            st["close"] = last
            st["last_vol"] = cum_vol
        return done

    def _finalize(self, code: str, st: dict) -> dict:
        vol = None
        if st.get("vol0") is not None:
            vol = max(0.0, float(st.get("last_vol") or st["vol0"]) - float(st["vol0"]))
        return {"code": code, "period": "1m", "time": self._label(st["minute"]),
                "open": st["open"], "high": st["high"], "low": st["low"],
                "close": st["close"], "volume": vol, "amount": None}


def in_market_session(now: datetime | None = None) -> bool:
    """是否处于可能产生行情的时段（含集合竞价与收盘缓冲）。

    落库子进程要和推送进程分时使用 TQ（并发会触发原生堆损坏崩溃），
    所以盘中不启动落库，等 08:30 / 15:45 这类盘外时点再跑。
    """
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    hm = now.strftime("%H:%M")
    return "09:15" <= hm <= "15:30"


def valid_minute_label(label: str, today: str) -> bool:
    """只接受今天的连续竞价分钟，避免午休/盘后/节假日生成假 bar。

    通达信 1m 标签为 09:31~11:30、13:01~15:00（bar 记结束时刻）。
    """
    if not label.startswith(today):
        return False
    hm = label[11:16]
    return "09:31" <= hm <= "11:30" or "13:01" <= hm <= "15:00"


def bars_rows(codes: list[str], period: str, count: int) -> list[dict]:
    if not codes:
        return []
    out: list[dict] = []
    batch = 800
    for i in range(0, len(codes), batch):
        sub = codes[i:i + batch]
        try:
            d = tq.get_market_data(
                field_list=["Open", "High", "Low", "Close", "Volume", "Amount"],
                stock_list=sub, start_time="", end_time="", count=count,
                dividend_type="none", period=period, fill_data=True)
        except Exception as exc:  # noqa: BLE001
            print(f"bars {period} batch {i} failed: {exc}")
            continue
        close = norm(d.get("Close"))
        if close is None or close.empty:
            continue
        cols = {k: norm(d.get(k)) for k in ("Open", "High", "Low", "Close", "Volume", "Amount")}
        for code in sub:
            if code not in close.columns:
                continue
            for ts in close.index:
                row = {"code": code, "period": period,
                       "time": f"{ts:%Y-%m-%d %H:%M:%S}" if period != "1d" else f"{ts:%Y-%m-%d}"}
                ok = True
                for k, v in cols.items():
                    if v is None or code not in v.columns:
                        ok = False
                        break
                    val = v.at[ts, code]
                    if pd.isna(val):
                        ok = False
                        break
                    if k == "Volume":
                        val = float(val) / 100.0        # 股 → 手
                    elif k == "Amount":
                        val = float(val) * 10000.0      # 万元 → 元
                    row[k.lower()] = val
                if ok:
                    out.append(row)
    return out


def push_daily(receiver: str, token: str, codes: list[str], count: int) -> int:
    drows = bars_rows(codes, "1d", count)
    for i in range(0, len(drows), 20000):
        try:
            r = push(receiver, token, "bars", drows[i:i + 20000])
            print(f"daily {i + len(drows[i:i + 20000])}/{len(drows)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"daily push failed: {exc}")
    return len(drows)


def _find_tool(name: str) -> str | None:
    """兼容 scripts/（开发仓库）与 tools/（发布仓库）两种目录布局。"""
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    for path in (os.path.join(here, name),
                 os.path.join(root, "scripts", name),
                 os.path.join(root, "tools", name)):
        if os.path.exists(path):
            return path
    return None


def start_minute_archive(args) -> subprocess.Popen:
    """异步启动全市场分钟落库，不阻塞实时推送。"""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = _find_tool("64_archive_minute.py")
    if not script:
        raise FileNotFoundError("64_archive_minute.py")
    out_dir = args.minute_archive_out
    if not os.path.isabs(out_dir):
        out_dir = os.path.join(root, out_dir)
    log_dir = os.path.join(root, "reports")
    os.makedirs(log_dir, exist_ok=True)
    log = open(os.path.join(log_dir, "minute_archive.log"), "a", encoding="utf-8")
    cmd = [sys.executable, script,
           "--days", str(args.minute_archive_days),
           "--out", out_dir]
    print(f"minute archive start: {' '.join(cmd)}")
    return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=root)


def start_minute_upload(args) -> subprocess.Popen:
    """把本地分钟分区补传到网关（只读文件 + HTTP，不占用 TQ 连接）。"""
    script = _find_tool("65_upload_minute.py")
    if not script:
        raise FileNotFoundError("65_upload_minute.py")
    root = os.path.dirname(os.path.dirname(os.path.abspath(script)))
    log_dir = os.path.join(root, "reports")
    os.makedirs(log_dir, exist_ok=True)
    log = open(os.path.join(log_dir, "minute_upload.log"), "a", encoding="utf-8")
    cmd = [sys.executable, script,
           "--gateway", args.minute_upload_gateway or args.receiver,
           "--days", str(args.minute_upload_days),
           "--periods", args.minute_upload_periods]
    if args.minute_upload_all:
        cmd.append("--all")
    print(f"minute upload start: {' '.join(cmd)}")
    # token 走环境变量，避免出现在进程命令行和日志里
    return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                            cwd=root,
                            env={**os.environ, "PANEL_TOKEN": args.token})


def seed_all(args, codes: list[str], asset_map: dict[str, str]) -> None:
    """全市场种子快照：prev_close / open / high / low / amount 的真实值。

    只靠增量 tick 不会更新这些字段，必须每天重新 seed 一次，
    否则面板的涨跌幅、振幅会一直停在旧交易日。
    """
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as ex:
        snapshots = [r for r in ex.map(lambda c: snap_row(c, asset_map.get(c)), codes) if r]
    # 停牌/无快照的股票也要有行，否则面板的当日 live 分区会被截断
    have = {r["code"] for r in snapshots}
    missing = [c for c in codes if c not in have]
    if missing:
        try:
            pv = tq.get_pricevol(stock_list=missing) or {}
        except Exception:  # noqa: BLE001
            pv = {}
        for code in missing:
            v = pv.get(code) or {}
            pc = to_float(v.get("LastClose"), 0.0) or 0.0
            last_px = to_float(v.get("Now"), pc) or pc
            at = classify_asset(code, asset_map.get(code))
            snapshots.append({
                "code": canonical_code(code), "asset_type": at,
                "volume_unit": volume_unit_of(at),
                "last": last_px, "prev_close": pc,
                "open": last_px, "high": last_px, "low": last_px,
                "volume": to_float(v.get("Volume"), 0.0) or 0.0,
                "amount": None,
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ts_ms": int(time.time() * 1000),
            })
        print(f"seed fallback for {len(missing)} symbols without snapshot")
    print(f"seed snapshots={len(snapshots)} in {time.perf_counter() - t0:.1f}s")
    for i in range(0, len(snapshots), 1000):
        chunk = snapshots[i:i + 1000]
        try:
            r = push(args.receiver, args.token, "panel_seed", chunk)
            print(f"seed {i + len(chunk)}/{len(snapshots)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"seed push failed: {exc}")


def push_index_members(args, codes: list[str], asset_map: dict[str, str]) -> None:
    """推送「指数/板块 -> 成分股」映射，网关据此汇总非股票标的成交额。

    映射只在启动时更新一次；成分股盘中不会变化。失败不影响实时推送，
    网关会对没有映射的指数返回 amount=None（而不是伪造 volume×price）。
    """
    targets = [c for c in codes if asset_map.get(c) in ("index", "board")]
    if not targets:
        return

    def one(code: str):
        try:
            with TQ_LOCK:
                members = tq.get_stock_list_in_sector(code) or []
        except Exception as exc:  # noqa: BLE001
            print(f"members {code} failed: {exc}")
            return None
        if not members:
            return None
        return {"index_code": canonical_code(code),
                "members": [canonical_code(m) for m in members if m]}

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as ex:
        raw_rows = [r for r in ex.map(one, targets) if r]

    # 通达信对部分标准指数会回退成「当前板块」，返回同一组很小的股票集合。
    # 过滤规则：成员 < 10 的丢弃；同一成员集合被 3 个以上指数复用的全部丢弃。
    sig = Counter(tuple(sorted(r["members"])) for r in raw_rows)
    rows = []
    for r in raw_rows:
        key = tuple(sorted(r["members"]))
        if len(r["members"]) < 10:
            print(f"members {r['index_code']} skipped: only {len(r['members'])}")
            continue
        if sig[key] > 2:
            print(f"members {r['index_code']} skipped: duplicate set ({len(r['members'])})")
            continue
        rows.append(r)
    print(f"index members={len(rows)}/{len(targets)} valid "
          f"(raw={len(raw_rows)}) in {time.perf_counter() - t0:.1f}s")
    if not rows:
        try:
            push(args.receiver, args.token, "panel_members", [], replace=True)
            print("members cleared (no valid mappings)")
        except Exception as exc:  # noqa: BLE001
            print(f"members clear failed: {exc}")
        return
    for i in range(0, len(rows), 100):
        chunk = rows[i:i + 100]
        try:
            r = push(args.receiver, args.token, "panel_members", chunk, replace=(i == 0))
            print(f"members {i + len(chunk)}/{len(rows)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"members push failed: {exc}")



def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--receiver", required=True)
    ap.add_argument("--token", default=os.environ.get("PANEL_TOKEN", "change-me"))
    ap.add_argument("--market", default="5", help="5=全部A股，24=中证500")
    ap.add_argument("--include-markets", default="9,31", help="额外并入实时池的市场，如 9,31（指数、ETF）")
    ap.add_argument("--limit", type=int, default=0, help="限制股票数（调试用）")
    ap.add_argument("--interval", type=float, default=6.0)
    ap.add_argument("--depth-interval", type=float, default=10.0)
    ap.add_argument("--minute-interval", type=float, default=60.0)
    ap.add_argument("--depth-market", default="24")
    ap.add_argument("--depth-limit", type=int, default=500)
    ap.add_argument("--minute-market", default="24")
    ap.add_argument("--minute-limit", type=int, default=500)
    ap.add_argument("--live-minute-all", action="store_true",
                    help="当日 1m 快照合成覆盖全部实时池（默认只覆盖 --minute-limit 只）")
    ap.add_argument("--seed-at", default="09:20",
                    help="每天重新 seed 全市场快照的时间 HH:MM（刷新 prev_close/open/high/low），留空关闭")
    ap.add_argument("--no-seed", action="store_true")
    ap.add_argument("--no-members", action="store_true",
                    help="不推送指数/板块成分映射（网关将无法汇总指数成交额）")
    ap.add_argument("--no-depth", action="store_true")
    ap.add_argument("--no-minute", action="store_true")
    ap.add_argument("--daily", action="store_true", help="启动时补历史日线")
    ap.add_argument("--daily-count", type=int, default=250)
    ap.add_argument("--daily-markets", default="", help="日线股票池，逗号分隔，如 5,31,9")
    ap.add_argument("--daily-codes", default="", help="额外日线代码，逗号分隔")
    ap.add_argument("--daily-auto", action="store_true", help="每天固定时间自动补当日日线")
    ap.add_argument("--daily-at", default="15:10", help="自动补日线时间 HH:MM")
    ap.add_argument("--minute-archive-at", default="",
                    help="每天固定时间异步落库全市场分钟线（HH:MM，留空关闭）")
    ap.add_argument("--minute-archive-days", type=int, default=5,
                    help="分钟落库回补最近 N 天内的缺失分区")
    ap.add_argument("--minute-archive-out", default="data/minute",
                    help="分钟落库输出目录")
    ap.add_argument("--minute-upload", action="store_true",
                    help="把本地分钟分区补传到网关（65_upload_minute.py）")
    ap.add_argument("--minute-upload-days", type=int, default=3,
                    help="启动时补传最近 N 个分区")
    ap.add_argument("--minute-upload-periods", default="1m",
                    help="补传周期，逗号分隔（网关 /panel/minute 只服务 1m）")
    ap.add_argument("--minute-upload-gateway", default="",
                    help="补传网关地址，留空复用 --receiver")
    ap.add_argument("--minute-upload-all", action="store_true",
                    help="补传全部分区（首次历史回补用）")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    tq.initialize(__file__)
    _market_asset = {"5": "stock", "9": "index", "31": "etf"}
    base_codes = tq.get_stock_list(args.market) or []
    asset_map: dict[str, str] = {
        canonical_code(c): classify_asset(c, _market_asset.get(str(args.market), "stock"))
        for c in base_codes
    }
    codes = list(asset_map)
    if args.include_markets:
        for m in [x.strip() for x in args.include_markets.split(",") if x.strip()]:
            try:
                extra = tq.get_stock_list(m) or []
                print(f"include market {m}: {len(extra)} symbols")
                for c in extra:
                    cc = canonical_code(c)
                    asset_map.setdefault(cc, classify_asset(cc, _market_asset.get(m)))
                    codes.append(cc)
            except Exception as exc:  # noqa: BLE001
                print(f"include market {m} failed: {exc}")
        codes = list(dict.fromkeys(codes))
    # market 9 不含 000001.SH / 000002.SH / 399107.SZ 等标准指数代码，
    # 用 --daily-codes 显式并入实时池，否则面板市场看板拿不到上证指数。
    extra_codes: list[str] = []
    if args.daily_codes:
        extra_codes = [canonical_code(c) for c in args.daily_codes.split(",") if c.strip()]
        if extra_codes:
            for c in extra_codes:
                asset_map.setdefault(c, classify_asset(c))
            codes = list(dict.fromkeys(codes + extra_codes))
            print(f"extra realtime codes: {extra_codes}")
    # 规范代码去重：999999.SH 是 000001.SH 的别名，只推规范代码
    for alias, canonical in ALIASES.items():
        if canonical in asset_map:
            codes = [c for c in codes if c != alias]
            asset_map.pop(alias, None)
    if args.limit:
        codes = codes[: args.limit]
    depth_codes = (tq.get_stock_list(args.depth_market) or [])[: args.depth_limit]
    minute_codes = (tq.get_stock_list(args.minute_market) or [])[: args.minute_limit]
    counts = {}
    for at in asset_map.values():
        counts[at] = counts.get(at, 0) + 1
    print(f"universe={len(codes)} depth={len(depth_codes)} minute={len(minute_codes)} "
          f"assets={counts}")

    # 1) 名称与流通股本
    info = []
    for c in codes:
        try:
            i = tq.get_stock_info(stock_code=c, field_list=[])
            info.append({"code": c, "name": i.get("Name"),
                         "asset_type": asset_map.get(c) or classify_asset(c),
                         "float_shares": to_float(i.get("ActiveCapital"), 0.0)})
        except Exception:  # noqa: BLE001
            info.append({"code": c, "name": None,
                         "asset_type": asset_map.get(c) or classify_asset(c),
                         "float_shares": 0.0})
    for i in range(0, len(info), 1000):
        try:
            r = push(args.receiver, args.token, "panel_info", info[i:i + 1000])
            print(f"info {i + len(info[i:i + 1000])}/{len(info)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"info push failed: {exc}")

    # 2) 指数/板块成分映射（网关汇总 index/board 成交额用）
    if not args.no_members:
        push_index_members(args, codes, asset_map)

    # 3) 全市场种子快照
    if not args.no_seed:
        seed_all(args, codes, asset_map)
    elif extra_codes:
        # --no-seed 时仍为补充指数推一次完整快照，否则 prev_close/amount 为空，
        # 涨跌幅算不出来，市场看板显示 "-"。
        extra_snaps = [r for r in (snap_row(c, asset_map.get(c)) for c in extra_codes) if r]
        if extra_snaps:
            try:
                r = push(args.receiver, args.token, "panel_seed", extra_snaps)
                print(f"seed extra codes={len(extra_snaps)} ok={r.get('accepted')}")
            except Exception as exc:  # noqa: BLE001
                print(f"seed extra push failed: {exc}")

    # 4) 历史 1 分钟线（本地数据只到上一交易日，先补齐）
    if not args.no_minute and minute_codes:
        brows = bars_rows(minute_codes, "1m", 240)
        try:
            for i in range(0, len(brows), 20000):
                r = push(args.receiver, args.token, "bars", brows[i:i + 20000])
                print(f"minute history {i + len(brows[i:i + 20000])}/{len(brows)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"minute history failed: {exc}")

    # 5) 历史日线（可选，启动时补齐）
    daily_codes: list[str] = []
    if args.daily or args.daily_auto:
        daily_codes = list(codes)
        if args.daily_markets:
            daily_codes = []
            for m in [x.strip() for x in args.daily_markets.split(",") if x.strip()]:
                try:
                    part = tq.get_stock_list(m) or []
                    print(f"daily market {m}: {len(part)} symbols")
                    daily_codes.extend(part)
                except Exception as exc:  # noqa: BLE001
                    print(f"daily market {m} failed: {exc}")
        if args.daily_codes:
            extra = [x.strip() for x in args.daily_codes.split(",") if x.strip()]
            daily_codes = list(dict.fromkeys(list(daily_codes) + extra))
            print(f"daily extra codes: {extra}")
        daily_codes = list(dict.fromkeys(daily_codes))
        if args.daily:
            push_daily(args.receiver, args.token, daily_codes, args.daily_count)

    # 6) 循环：报价增量 + 五档 + 当日 1 分钟线合成
    last = {}
    agg = MinuteAggregator()
    minute_set = set(codes) if args.live_minute_all else set(minute_codes)
    print(f"live minute: {len(minute_set)} symbols"
          f"{' (all)' if args.live_minute_all else ''}")
    next_depth = 0.0
    last_daily = ""
    last_seed = datetime.now().strftime("%Y-%m-%d") if args.seed_at and not args.no_seed else ""
    weak_since = 0.0
    stale_warned = False
    # 用集合记录"日期 槽位"，单变量无法同时记录 08:30 / 15:45 两个槽，
    # 会导致子进程一退出就被另一槽重新拉起。
    done_archive: set[str] = set()
    archive_proc: subprocess.Popen | None = None
    archive_started = 0.0
    upload_proc: subprocess.Popen | None = None
    if args.minute_upload:
        try:
            upload_proc = start_minute_upload(args)
        except Exception as exc:  # noqa: BLE001
            print(f"minute upload start failed: {exc}")
    cycle = 0
    try:
        while True:
            cycle += 1
            # 落库子进程运行期间主循环完全停用 TQ：两个进程并发调用 tqcenter
            # 原生模块会触发 ntdll 堆损坏（0xc0000374）导致进程崩溃。
            if archive_proc is not None and archive_proc.poll() is None:
                if time.time() - archive_started > 20 * 60:
                    print("minute archive over 20min, kill it")
                    archive_proc.kill()
                    archive_proc.wait(timeout=10)
                else:
                    time.sleep(2.0)
                    continue
            if archive_proc is not None:
                print(f"minute archive finished rc={archive_proc.returncode}")
                archive_proc = None
                try:
                    tq.initialize(__file__)
                except Exception as exc:  # noqa: BLE001
                    print(f"TQ re-init failed: {exc}")
                if args.minute_upload and (upload_proc is None or upload_proc.poll() is not None):
                    try:
                        upload_proc = start_minute_upload(args)
                    except Exception as exc:  # noqa: BLE001
                        print(f"minute upload start failed: {exc}")

            t0 = time.perf_counter()
            # get_pricevol 读的是客户端缓存，不主动刷新会一直停在上一批快照；
            # 实测 refresh_cache 后价格/量才更新，正常盘中每轮刷一次。
            try:
                tq.refresh_cache(market="AG", force=True)
            except Exception as exc:  # noqa: BLE001
                print(f"refresh_cache failed: {exc}")
            pv = tq.get_pricevol(stock_list=codes) or {}
            now_ms = int(time.time() * 1000)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows = []
            # 分钟合成要喂"全部标的"，不能只喂有变化的行：
            # 否则一分钟内没有成交变化的股票不会在分钟翻转时落 bar。
            quotes: list[tuple[str, float | None, float | None]] = []
            for code, v in pv.items():
                now_f, vol_f = to_float(v.get("Now")), to_float(v.get("Volume"))
                cc = canonical_code(code)
                at = asset_map.get(cc) or classify_asset(cc)
                quotes.append((cc, now_f, vol_f))
                key = (now_f, vol_f)
                if last.get(cc) == key:
                    continue
                last[cc] = key
                rows.append({"code": cc, "asset_type": at,
                             "volume_unit": volume_unit_of(at),
                             "last": now_f, "volume": vol_f,
                             "prev_close": to_float(v.get("LastClose")),
                             "ts": ts, "ts_ms": now_ms})
            # 行情源健康度：正常盘中每轮会有大量标的报价变化（远高于 1%）。
            # 若连续 2 分钟变化比例都低于 1%，说明客户端行情连接失联（快照停在旧值），
            # 此时不合成分钟线、也不发心跳，避免灌一整天横盘 0 量假 bar。
            ratio = len(rows) / max(len(quotes), 1)
            if not in_market_session() or ratio >= 0.01:
                weak_since = 0.0
                stale_warned = False
            elif weak_since == 0.0:
                weak_since = time.time()
            market_stale = bool(weak_since) and (time.time() - weak_since > 120)
            if market_stale and not stale_warned:
                print(f"WARN: 盘中报价变化仅 {ratio:.3%}（{len(rows)}/{len(quotes)}），"
                      "疑似行情源失联，暂停合成分钟线与心跳")
                stale_warned = True

            if rows:
                try:
                    r = push(args.receiver, args.token, "panel_tick", rows)
                    print(f"[{ts}] tick rows={len(rows)} accepted={r.get('accepted')} "
                          f"ms={r.get('elapsed_ms')}")
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts}] tick push failed: {exc}")

            # 心跳：即使个股无成交，也让面板知道本轮快照已刷新。
            # 行情源失联时不发心跳，让面板的 quote_age 自然变大（显示更新延迟）。
            if not market_stale:
                try:
                    push(args.receiver, args.token, "panel_beat", [{"ts": ts, "ts_ms": now_ms}])
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts}] beat push failed: {exc}")

            # 当日 1 分钟线：用快照合成，分钟翻转时落一根。
            # 全市场一轮没有变化（午休/盘后/节假日）或行情源失联时不喂。
            if minute_set and rows and not market_stale:
                today_s = ts[:10]
                finished = []
                for code, now_f, vol_f in quotes:
                    if now_f is None or code not in minute_set:
                        continue
                    bar = agg.feed(code, ts, now_f, vol_f)
                    if bar and valid_minute_label(bar["time"], today_s):
                        finished.append(bar)
                if finished:
                    try:
                        r = push(args.receiver, args.token, "bars", finished)
                        print(f"minute bars={len(finished)} ok={r.get('accepted')}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"minute bars push failed: {exc}")

            if not args.no_depth and depth_codes and time.time() >= next_depth:
                with ThreadPoolExecutor(max_workers=4) as ex:
                    drows = [r for r in ex.map(depth_row, depth_codes) if r]
                try:
                    r = push(args.receiver, args.token, "depth", drows)
                    print(f"depth rows={len(drows)} ok={r.get('accepted')}")
                except Exception as exc:  # noqa: BLE001
                    print(f"depth push failed: {exc}")
                next_depth = time.time() + args.depth_interval

            # 每天开盘前重新 seed 一次，刷新 prev_close/open/high/low；
            # 只靠 tick 增量这些字段永远停在启动那天。
            if args.seed_at and not args.no_seed:
                today = datetime.now().strftime("%Y-%m-%d")
                if last_seed != today and datetime.now().strftime("%H:%M") >= args.seed_at:
                    print(f"daily seed start ({today} {args.seed_at})")
                    seed_all(args, codes, asset_map)
                    last_seed = today

            if args.daily_auto and daily_codes:
                today = datetime.now().strftime("%Y-%m-%d")
                if last_daily != today and datetime.now().strftime("%H:%M") >= args.daily_at:
                    print(f"auto daily push start ({today} {args.daily_at})")
                    push_daily(args.receiver, args.token, daily_codes, args.daily_count)
                    last_daily = today

            # 分钟线落库：客户端当天分钟要等次日凌晨下载，因此按“缺失分区”回补。
            # 放到子进程跑（全市场 1m 约 15 分钟），不阻塞实时推送。
            # 支持多个时间点（如 08:30,15:45），每个槽位每天只触发一次。
            if args.minute_archive_at:
                today = datetime.now().strftime("%Y-%m-%d")
                now_hm = datetime.now().strftime("%H:%M")
                slots = [s.strip() for s in str(args.minute_archive_at).split(",") if s.strip()]
                due = [s for s in slots
                       if f"{today} {s}" not in done_archive and now_hm >= s]
                if due and archive_proc is None:
                    if in_market_session():
                        # 盘中不落库：会和实时推送抢 TQ，且用户要的是盘中分钟图
                        pass
                    elif args.once:
                        pass
                    else:
                        try:
                            # 先断开本进程的 TQ，再让子进程独占运行
                            tq.close()
                            archive_proc = start_minute_archive(args)
                            archive_started = time.time()
                            done_archive.update(f"{today} {s}" for s in due)
                        except Exception as exc:  # noqa: BLE001
                            print(f"minute archive start failed: {exc}")
                            try:
                                tq.initialize(__file__)
                            except Exception as exc2:  # noqa: BLE001
                                print(f"TQ re-init failed: {exc2}")
            if args.once:
                break
            time.sleep(max(0.2, args.interval - (time.perf_counter() - t0)))
    except KeyboardInterrupt:
        pass
    finally:
        tq.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

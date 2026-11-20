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
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

TDX_USER = os.environ.get("TDX_PYPLUGINS", r"d:\program\new_tdx64\PYPlugins\user")
sys.path.insert(0, TDX_USER)

import pandas as pd  # noqa: E402
from tqcenter import tq  # noqa: E402

SNAP_FIELDS = ["Now", "LastClose", "Open", "Max", "Min", "Volume", "Amount",
               "Buyp", "Buyv", "Sellp", "Sellv"]


def push(receiver: str, token: str, kind: str, rows: list[dict], timeout: float = 20.0) -> dict:
    payload = {"batch_id": f"{kind}-{uuid.uuid4().hex[:12]}", "kind": kind, "rows": rows}
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


def snap_row(code: str) -> dict | None:
    try:
        s = tq.get_market_snapshot(stock_code=code, field_list=SNAP_FIELDS)
    except Exception:  # noqa: BLE001
        return None
    if not s or not s.get("Now"):
        return None
    now = to_float(s.get("Now"))
    if now is None:
        return None
    return {
        "code": code,
        "last": now,
        "prev_close": to_float(s.get("LastClose")),
        "open": to_float(s.get("Open")),
        "high": to_float(s.get("Max")),
        "low": to_float(s.get("Min")),
        "volume": to_float(s.get("Volume")),
        "amount": (to_float(s.get("Amount"), 0.0) or 0.0) * 10000.0,   # 万元 → 元
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ts_ms": int(time.time() * 1000),
    }


def depth_row(code: str) -> dict | None:
    try:
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


def start_minute_archive(args) -> subprocess.Popen:
    """异步启动全市场分钟落库，不阻塞实时推送。"""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "64_archive_minute.py"),
        os.path.join(root, "scripts", "64_archive_minute.py"),
        os.path.join(root, "tools", "64_archive_minute.py"),
    ]
    script = next((p for p in candidates if os.path.exists(p)), None)
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
    ap.add_argument("--no-seed", action="store_true")
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
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    tq.initialize(__file__)
    codes = tq.get_stock_list(args.market) or []
    if args.include_markets:
        for m in [x.strip() for x in args.include_markets.split(",") if x.strip()]:
            try:
                extra = tq.get_stock_list(m) or []
                print(f"include market {m}: {len(extra)} symbols")
                codes.extend(extra)
            except Exception as exc:  # noqa: BLE001
                print(f"include market {m} failed: {exc}")
        codes = list(dict.fromkeys(codes))
    if args.limit:
        codes = codes[: args.limit]
    depth_codes = (tq.get_stock_list(args.depth_market) or [])[: args.depth_limit]
    minute_codes = (tq.get_stock_list(args.minute_market) or [])[: args.minute_limit]
    print(f"universe={len(codes)} depth={len(depth_codes)} minute={len(minute_codes)}")

    # 1) 名称与流通股本
    info = []
    for c in codes:
        try:
            i = tq.get_stock_info(stock_code=c, field_list=[])
            info.append({"code": c, "name": i.get("Name"),
                         "float_shares": to_float(i.get("ActiveCapital"), 0.0)})
        except Exception:  # noqa: BLE001
            info.append({"code": c, "name": None, "float_shares": 0.0})
    for i in range(0, len(info), 1000):
        try:
            r = push(args.receiver, args.token, "panel_info", info[i:i + 1000])
            print(f"info {i + len(info[i:i + 1000])}/{len(info)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"info push failed: {exc}")

    # 2) 全市场种子快照
    if not args.no_seed:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=8) as ex:
            snapshots = [r for r in ex.map(snap_row, codes) if r]
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
                snapshots.append({
                    "code": code, "last": last_px, "prev_close": pc,
                    "open": last_px, "high": last_px, "low": last_px,
                    "volume": to_float(v.get("Volume"), 0.0) or 0.0, "amount": 0.0,
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

    # 3) 历史 1 分钟线（本地数据只到上一交易日，先补齐）
    if not args.no_minute and minute_codes:
        brows = bars_rows(minute_codes, "1m", 240)
        try:
            for i in range(0, len(brows), 20000):
                r = push(args.receiver, args.token, "bars", brows[i:i + 20000])
                print(f"minute history {i + len(brows[i:i + 20000])}/{len(brows)} ok={r.get('accepted')}")
        except Exception as exc:  # noqa: BLE001
            print(f"minute history failed: {exc}")

    # 4) 历史日线（可选，启动时补齐）
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

    # 5) 循环：报价增量 + 五档 + 当日 1 分钟线合成
    last = {}
    agg = MinuteAggregator()
    minute_set = set(minute_codes)
    next_depth = 0.0
    last_daily = ""
    last_archive = ""
    archive_proc: subprocess.Popen | None = None
    cycle = 0
    try:
        while True:
            cycle += 1
            t0 = time.perf_counter()
            pv = tq.get_pricevol(stock_list=codes) or {}
            now_ms = int(time.time() * 1000)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows = []
            for code, v in pv.items():
                now_f, vol_f = to_float(v.get("Now")), to_float(v.get("Volume"))
                key = (now_f, vol_f)
                if last.get(code) == key:
                    continue
                last[code] = key
                rows.append({"code": code, "last": now_f, "volume": vol_f,
                             "prev_close": to_float(v.get("LastClose")),
                             "ts": ts, "ts_ms": now_ms})
            if rows:
                try:
                    r = push(args.receiver, args.token, "panel_tick", rows)
                    print(f"[{ts}] tick rows={len(rows)} accepted={r.get('accepted')} "
                          f"ms={r.get('elapsed_ms')}")
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts}] tick push failed: {exc}")

            # 心跳：即使个股无成交，也让面板知道本轮快照已刷新
            try:
                push(args.receiver, args.token, "panel_beat", [{"ts": ts, "ts_ms": now_ms}])
            except Exception as exc:  # noqa: BLE001
                print(f"[{ts}] beat push failed: {exc}")

                # 当日 1 分钟线：用快照合成，分钟翻转时落一根
                if minute_set:
                    finished = []
                    for row in rows:
                        if row["code"] in minute_set and row.get("last") is not None:
                            bar = agg.feed(row["code"], ts, row["last"], row.get("volume"))
                            if bar:
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

            if args.daily_auto and daily_codes:
                today = datetime.now().strftime("%Y-%m-%d")
                if last_daily != today and datetime.now().strftime("%H:%M") >= args.daily_at:
                    print(f"auto daily push start ({today} {args.daily_at})")
                    push_daily(args.receiver, args.token, daily_codes, args.daily_count)
                    last_daily = today

            # 分钟线落库：客户端当天分钟要等次日凌晨下载，因此按“缺失分区”回补。
            # 放到子进程跑（全市场 1m 约 15 分钟），不阻塞实时推送。
            # 支持多个时间点（如 08:30,15:45），每次只跑最近的一个。
            if args.minute_archive_at:
                today = datetime.now().strftime("%Y-%m-%d")
                now_hm = datetime.now().strftime("%H:%M")
                slots = [s.strip() for s in str(args.minute_archive_at).split(",") if s.strip()]
                due = [s for s in slots
                       if last_archive != f"{today} {s}" and now_hm >= s]
                if due:
                    if archive_proc is not None and archive_proc.poll() is None:
                        print("minute archive still running, skip this round")
                    else:
                        try:
                            archive_proc = start_minute_archive(args)
                        except Exception as exc:  # noqa: BLE001
                            print(f"minute archive start failed: {exc}")
                    last_archive = f"{today} {due[-1]}"

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

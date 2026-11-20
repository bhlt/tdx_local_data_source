#!/usr/bin/env python
"""导出通达信本地维表（symbol / name / 股本），供面板 instruments 兜底。

背景
----
tick-stock-panel 每天通过 tickflow 免费接口拉 instruments 维表，单交易所
（尤其沪市 2000+ 只）偶发超时。一旦 SH 超时，整张 instruments.parquet
只写入 SZ+BJ，导致沪市股票名称显示「名称待同步」、换手率/市值全为 null。

本脚本从「网关实时快照」取全市场 symbol+name，从「通达信本地客户端」取
总股本/流通股本，输出 CSV 交给 Linux 端的合并脚本。

用法
----
    set PANEL_TOKEN=...
    python scripts/61_export_instruments.py --gateway http://192.168.0.198:18082
    python scripts/61_export_instruments.py --markets all --out reports/instruments.csv

输出列：symbol,name,code,exchange,region,type,float_shares,total_shares
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
from datetime import datetime

TDX_USER = os.environ.get("TDX_PYPLUGINS", r"d:\program\new_tdx64\PYPlugins\user")
sys.path.insert(0, TDX_USER)

# 各市场的 A 股代码前缀（排除 ETF / 指数 / 债券）
_STOCK_PREFIX = {
    "SH": ("60", "68"),
    "SZ": ("00", "30"),
    "BJ": ("43", "83", "87", "92"),
}


def fetch_realtime(gateway: str, token: str, timeout: float = 60.0) -> list[dict]:
    req = urllib.request.Request(
        gateway.rstrip("/") + "/panel/realtime",
        headers={"X-Api-Key": token},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("data") or []


def split_symbol(symbol: str) -> tuple[str, str]:
    code, _, exchange = symbol.partition(".")
    return code, exchange.upper()


def is_stock(symbol: str) -> bool:
    code, exchange = split_symbol(symbol)
    return code.startswith(_STOCK_PREFIX.get(exchange, ()))


def main() -> int:
    ap = argparse.ArgumentParser(description="导出通达信股票维表")
    ap.add_argument("--gateway", default="http://192.168.0.198:18082",
                    help="数据网关地址（提供 /panel/realtime 快照）")
    ap.add_argument("--token", default=os.environ.get("PANEL_TOKEN", ""),
                    help="网关 token，默认读 PANEL_TOKEN")
    ap.add_argument("--markets", default="sh",
                    help="导出的市场：sh / sz / bj / all，逗号分隔")
    ap.add_argument("--date", default=datetime.now().strftime("%Y%m%d"),
                    help="股本日期 YYYYMMDD，默认今天")
    ap.add_argument("--out", default="reports/instruments.csv")
    ap.add_argument("--limit", type=int, default=0, help="只导出前 N 只（调试）")
    args = ap.parse_args()

    if not args.token:
        print("ERROR: 缺少 token，请设置 PANEL_TOKEN 或传 --token", file=sys.stderr)
        return 2

    wanted = {m.strip().upper() for m in args.markets.split(",") if m.strip()}
    if "ALL" in wanted:
        wanted = {"SH", "SZ", "BJ"}

    print(f"拉取网关快照: {args.gateway}/panel/realtime")
    snapshots = fetch_realtime(args.gateway, args.token)
    print(f"快照 {len(snapshots)} 条")

    targets = []
    for row in snapshots:
        symbol = str(row.get("symbol") or "")
        code, exchange = split_symbol(symbol)
        if exchange not in wanted or not is_stock(symbol):
            continue
        name = row.get("name") or ""
        if not name:
            continue
        targets.append((symbol, name, code, exchange))
    if args.limit:
        targets = targets[: args.limit]
    print(f"待导出 {len(targets)} 只（{','.join(sorted(wanted))}）")

    from tqcenter import tq  # noqa: E402

    tq.initialize(__file__)
    started = time.time()
    rows: list[dict] = []
    missing = 0
    try:
        for i, (symbol, name, code, exchange) in enumerate(targets, 1):
            info = tq.get_gb_info(stock_code=symbol, date_list=[args.date], count=1) or []
            item = info[0] if isinstance(info, list) and info else {}
            float_shares = item.get("Ltgb")
            total_shares = item.get("Zgb")
            if float_shares is None and total_shares is None:
                missing += 1
            rows.append({
                "symbol": symbol,
                "name": name,
                "code": code,
                "exchange": exchange,
                "region": "CN",
                "type": "stock",
                "float_shares": float_shares,
                "total_shares": total_shares,
            })
            if i % 200 == 0:
                print(f"  {i}/{len(targets)}  {time.time() - started:.1f}s")
    finally:
        tq.close()

    out = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"已写入 {out}  {len(rows)} 行，无股本 {missing}，"
          f"耗时 {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

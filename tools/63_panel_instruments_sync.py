#!/usr/bin/env python
"""补齐 tick-stock-panel 的 instruments 维表（名称 + 股本），防止整市场缺失。

问题
----
面板每天 9:10 / 15:35 通过 tickflow 免费接口拉 instruments 维表。
单个交易所（尤其沪市 2000+ 只）偶发超时后，该市场整块不会写入
instruments.parquet，结果：

- 涨停梯队 / 自选显示「名称待同步」
- 该市场所有股票换手率、流通市值为空（enriched 依赖 float_shares）

本脚本从本地数据网关的实时快照取名称，从 shares.csv 取股本，
把缺失的股票补回 instruments.parquet，然后让面板重载缓存。

设计要点
--------
- shares.csv 是独立于面板的股本账本：即使面板把整市场覆盖掉，
  也能从这里恢复（面板自身的覆盖不会影响该文件）。
- 每次运行会把 instruments.parquet 里已有的股本回写到 shares.csv，
  因此深市/北交所的股本也会被自动记账。
- 幂等：可反复执行。

在面板容器内运行（推荐）：

    docker exec -i tick-stock-panel-app /app/.venv/bin/python - \\
        61_panel_instruments_sync.py

参数可用环境变量：PANEL_TOKEN / PANEL_GATEWAY / PANEL_API。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import date
from pathlib import Path

import polars as pl

STOCK_PREFIX = {
    "SH": ("60", "68"),
    "SZ": ("00", "30"),
    "BJ": ("43", "83", "87", "92"),
}
SHARES_COLUMNS = ["symbol", "name", "float_shares", "total_shares"]


def is_stock(symbol: str) -> bool:
    code, _, exchange = symbol.partition(".")
    return code.startswith(STOCK_PREFIX.get(exchange.upper(), ()))


def fetch_realtime(gateway: str, token: str, timeout: float = 60.0) -> list[dict]:
    req = urllib.request.Request(
        gateway.rstrip("/") + "/panel/realtime",
        headers={"X-Api-Key": token},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("data") or []


def load_shares(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    df = pl.read_csv(path, infer_schema_length=0)
    out: dict[str, dict] = {}
    for row in df.iter_rows(named=True):
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        out[symbol] = {
            "name": row.get("name") or "",
            "float_shares": _num(row.get("float_shares")),
            "total_shares": _num(row.get("total_shares")),
        }
    return out


def _num(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def save_shares(path: Path, ledger: dict[str, dict]) -> None:
    rows = [
        {
            "symbol": symbol,
            "name": meta.get("name") or "",
            "float_shares": meta.get("float_shares"),
            "total_shares": meta.get("total_shares"),
        }
        for symbol, meta in sorted(ledger.items())
        if is_stock(symbol)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    pl.DataFrame(rows, schema=SHARES_COLUMNS).write_csv(tmp)
    tmp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser(description="补齐面板 instruments 维表")
    ap.add_argument("--data-dir", default=os.environ.get("PANEL_DATA_DIR", "/app/data"))
    ap.add_argument("--gateway", default=os.environ.get("PANEL_GATEWAY", "http://172.25.0.1:18082"))
    ap.add_argument("--panel", default=os.environ.get("PANEL_API", "http://127.0.0.1:3018"))
    ap.add_argument("--token", default=os.environ.get("PANEL_TOKEN", ""))
    ap.add_argument("--shares", default="",
                    help="股本账本 CSV，默认 <data-dir>/instruments_extra/shares.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    target = data_dir / "instruments" / "instruments.parquet"
    shares_path = Path(args.shares) if args.shares else data_dir / "instruments_extra" / "shares.csv"

    if not target.exists():
        print(f"ERROR: 找不到 {target}", file=sys.stderr)
        return 2
    if not args.token:
        print("ERROR: 缺少 PANEL_TOKEN", file=sys.stderr)
        return 2

    base = pl.read_parquet(target)
    base_symbols = set(base["symbol"].to_list())
    ledger = load_shares(shares_path)

    # 1) 面板已有的行先记账（含深市/北交所股本）
    for row in base.iter_rows(named=True):
        symbol = row.get("symbol")
        if not symbol or not is_stock(symbol):
            continue
        meta = ledger.setdefault(symbol, {"name": "", "float_shares": None, "total_shares": None})
        if row.get("name"):
            meta["name"] = row["name"]
        if row.get("float_shares") is not None:
            meta["float_shares"] = row["float_shares"]
        if row.get("total_shares") is not None:
            meta["total_shares"] = row["total_shares"]

    # 2) 网关快照提供最新名称
    try:
        snapshots = fetch_realtime(args.gateway, args.token)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: 拉取网关快照失败，仅用本地账本: {exc}", file=sys.stderr)
        snapshots = []

    today = date.today()
    additions: list[dict] = []
    refreshed = 0
    for row in snapshots:
        symbol = str(row.get("symbol") or "")
        if not symbol or not is_stock(symbol):
            continue
        name = row.get("name") or ""
        meta = ledger.setdefault(symbol, {"name": "", "float_shares": None, "total_shares": None})
        if name and meta.get("name") != name:
            meta["name"] = name
            refreshed += 1
        if symbol not in base_symbols:
            additions.append({
                "symbol": symbol,
                "name": name or meta.get("name") or "",
                "float_shares": meta.get("float_shares"),
                "total_shares": meta.get("total_shares"),
            })
            base_symbols.add(symbol)

    # 3) 账本里存在但面板整块缺失的（停牌/退市边缘标的）也补回
    for symbol, meta in ledger.items():
        if symbol in base_symbols:
            continue
        additions.append({
            "symbol": symbol,
            "name": meta.get("name") or "",
            "float_shares": meta.get("float_shares"),
            "total_shares": meta.get("total_shares"),
        })
        base_symbols.add(symbol)

    print(f"面板 {base.height} 行；快照 {len(snapshots)} 条；账本 {len(ledger)} 只；"
          f"待补 {len(additions)} 行；名称刷新 {refreshed} 条")

    if not args.dry_run:
        save_shares(shares_path, ledger)
        print(f"账本已更新: {shares_path}")

    if not additions:
        print("无需补行")
        if not args.dry_run and refreshed:
            _refresh_cache(args.panel)
        return 0

    incoming = pl.DataFrame(additions)
    aligned = incoming
    for col, dtype in base.schema.items():
        if col in aligned.columns:
            if dtype == pl.Float64:
                aligned = aligned.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))
            else:
                aligned = aligned.with_columns(pl.col(col).cast(dtype, strict=False).alias(col))
        elif col == "as_of":
            aligned = aligned.with_columns(pl.lit(today).alias(col))
        elif col == "tick_size":
            aligned = aligned.with_columns(pl.lit(0.01).cast(dtype).alias(col))
        else:
            aligned = aligned.with_columns(pl.lit(None).cast(dtype).alias(col))
    aligned = aligned.select(base.columns)

    merged = pl.concat([base, aligned], how="vertical_relaxed")
    merged = merged.unique(subset=["symbol"], keep="last", maintain_order=True)
    print(f"合并后 {merged.height} 行")

    if args.dry_run:
        print("dry-run，不写文件")
        return 0

    tmp = target.with_suffix(".parquet.tmp")
    merged.write_parquet(tmp)
    tmp.replace(target)
    print(f"已写入 {target}")

    _refresh_cache(args.panel)
    return 0


def _refresh_cache(panel: str) -> None:
    try:
        req = urllib.request.Request(
            panel.rstrip("/") + "/api/data/refresh-cache", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            print("面板缓存刷新:", resp.read().decode("utf-8")[:120])
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: 面板缓存刷新失败（重启容器也会重载）: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())

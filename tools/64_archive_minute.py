#!/usr/bin/env python
"""全市场分钟线增量落库（通达信本地数据）。

背景
----
通达信客户端只保留最近约 3 个月分钟数据（.lc1/.lc5），且当天数据要等
次日凌晨客户端自动下载后才可用。本脚本在每天开盘前补齐最近 N 个交易日
缺失的分区，长期积累成完整分钟历史。

输出
----
    data/minute/1m/date=YYYY-MM-DD/part.parquet
    data/minute/5m/date=YYYY-MM-DD/part.parquet

列：symbol, datetime, open, high, low, close, volume, amount
（volume 单位手、amount 单位元，与日线一致；不复权）

注意：通达信分钟接口原始单位是「股 / 万元」，脚本默认换算成「手 / 元」
（volume ÷ 100，amount × 10000）。

用法
----
    # 每日增量：补最近 5 个交易日中缺失的分区
    python scripts/64_archive_minute.py --days 5

    # 首次回补全部本地历史（约 3 个月）
    python scripts/64_archive_minute.py --days 90

    # 指定单日
    python scripts/64_archive_minute.py --date 2026-11-19

    # 只落 5 分钟线
    python scripts/64_archive_minute.py --periods 5m --days 5
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

TDX_USER = os.environ.get("TDX_PYPLUGINS", r"d:\program\new_tdx64\PYPlugins\user")
sys.path.insert(0, TDX_USER)

FIELDS = ["Open", "High", "Low", "Close", "Volume", "Amount"]
COLUMNS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="全市场分钟线增量落库")
    ap.add_argument("--periods", default="1m,5m", help="周期，逗号分隔：1m,5m")
    ap.add_argument("--days", type=int, default=5, help="补齐最近 N 个自然日内的缺失分区")
    ap.add_argument("--date", default="", help="只处理指定日期 YYYY-MM-DD")
    ap.add_argument("--out", default="data/minute", help="输出根目录")
    ap.add_argument("--batch", type=int, default=200, help="每批股票数（200 较稳）")
    ap.add_argument("--market", default="5", help="主市场，默认 5=沪深京 A 股")
    ap.add_argument("--include-markets", default="", help="额外市场，如 9,31（指数/ETF）")
    ap.add_argument("--fill", action="store_true", help="前向填充停牌（默认关闭，保留真实数据）")
    ap.add_argument("--force", action="store_true", help="覆盖已存在的分区")
    ap.add_argument("--format", default="auto", choices=["auto", "parquet", "csv"],
                    help="输出格式；auto 优先 parquet，缺少依赖时用 csv.gz")
    ap.add_argument("--raw-units", action="store_true",
                    help="保留接口原始单位（股/万元），默认换算为手/元")
    return ap.parse_args()


def trading_days(start: date, end: date) -> list[date]:
    """优先用通达信交易日历；失败时退化为工作日。"""
    try:
        from tqcenter import tq

        raw = tq.get_trading_calendar(
            market="SH",
            start_time=start.strftime("%Y%m%d"),
            end_time=end.strftime("%Y%m%d"),
        )
        days: list[date] = []
        for item in raw or []:
            text = str(item).replace("-", "")
            if len(text) >= 8:
                try:
                    days.append(datetime.strptime(text[:8], "%Y%m%d").date())
                except ValueError:
                    continue
        if days:
            return sorted(set(days))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: 交易日历不可用，退化为工作日: {exc}", flush=True)

    days = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def output_path(root: Path, period: str, day: date, fmt: str) -> Path:
    suffix = "parquet" if fmt == "parquet" else "csv.gz"
    return root / period / f"date={day.isoformat()}" / f"part.{suffix}"


def resolve_format(requested: str) -> str:
    if requested in ("parquet", "csv"):
        return requested
    try:
        import pyarrow  # noqa: F401
        return "parquet"
    except ImportError:
        try:
            import fastparquet  # noqa: F401
            return "parquet"
        except ImportError:
            return "csv"


def to_long(result: dict, normalize: bool = True) -> pd.DataFrame:
    """把 {字段: DataFrame(index=time, columns=symbol)} 转成长表。

    通达信分钟接口原始单位：Volume=股、Amount=万元。
    normalize=True 时换算为手/元，与日线、面板保持一致。
    """
    close = result.get("Close")
    if close is None or close.empty:
        return pd.DataFrame(columns=COLUMNS)

    symbols = close.columns.to_numpy()
    times = close.index.to_numpy()
    n_time, n_symbol = close.shape

    def block(name: str) -> np.ndarray:
        frame = result.get(name)
        if frame is None or frame.shape != close.shape:
            return np.full(n_time * n_symbol, np.nan, dtype="float64")
        return frame.to_numpy(dtype="float64").T.reshape(-1)

    volume = block("Volume")
    amount = block("Amount")
    if normalize:
        volume = volume / 100.0      # 股 -> 手
        amount = amount * 10000.0    # 万元 -> 元

    return pd.DataFrame({
        "symbol": np.repeat(symbols, n_time),
        "datetime": np.tile(times, n_symbol),
        "open": block("Open"),
        "high": block("High"),
        "low": block("Low"),
        "close": close.to_numpy(dtype="float64").T.reshape(-1),
        "volume": volume,
        "amount": amount,
    })


def write_partition(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if fmt == "parquet":
        df.to_parquet(tmp, index=False, compression="zstd")
    else:
        df.to_csv(tmp, index=False)
    tmp.replace(path)


def archive_day(tq, codes: list[str], period: str, day: date, args, fmt: str) -> int:
    target = output_path(Path(args.out), period, day, fmt)
    if target.exists() and not args.force:
        print(f"  skip {period} {day} (已存在)", flush=True)
        return 0

    start = day.strftime("%Y%m%d")
    end = day.strftime("%Y%m%d") + "235959"
    frames: list[pd.DataFrame] = []
    began = time.time()

    for i in range(0, len(codes), args.batch):
        sub = codes[i:i + args.batch]
        try:
            result = tq.get_market_data(
                field_list=FIELDS, stock_list=sub, period=period,
                start_time=start, end_time=end, count=-1,
                dividend_type="none", fill_data=args.fill,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  {period} {day} batch {i} failed: {exc}", flush=True)
            continue
        long = to_long(result if isinstance(result, dict) else {}, normalize=not args.raw_units)
        if not long.empty:
            frames.append(long)
        if (i // args.batch + 1) % 5 == 0:
            print(f"  {period} {day} {min(i + args.batch, len(codes))}/{len(codes)} "
                  f"{time.time() - began:.0f}s", flush=True)

    if not frames:
        print(f"  {period} {day} 无数据（客户端可能尚未下载当天分钟）", flush=True)
        return 0

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["close"])
    df = df.drop_duplicates(subset=["symbol", "datetime"]).sort_values(["symbol", "datetime"])
    if df.empty:
        print(f"  {period} {day} 无有效数据", flush=True)
        return 0

    write_partition(df, target, fmt)
    print(f"  {period} {day} -> {target}  {len(df)} 行  "
          f"{time.time() - began:.0f}s  {target.stat().st_size / 1e6:.1f} MB", flush=True)
    return len(df)


def main() -> int:
    args = parse_args()
    fmt = resolve_format(args.format)
    print(f"输出格式: {fmt}  输出目录: {Path(args.out).resolve()}")

    from tqcenter import tq

    tq.initialize(__file__)
    try:
        codes = list(tq.get_stock_list(args.market) or [])
        for market in [m.strip() for m in args.include_markets.split(",") if m.strip()]:
            codes.extend(tq.get_stock_list(market) or [])
        codes = list(dict.fromkeys(codes))
        print(f"标的数: {len(codes)}", flush=True)
        if not codes:
            return 2

        if args.date:
            days = [datetime.strptime(args.date, "%Y-%m-%d").date()]
        else:
            end = date.today()
            start = end - timedelta(days=max(args.days, 1))
            days = trading_days(start, end)
        print(f"待处理交易日: {len(days)}  {days[0]} ~ {days[-1]}", flush=True)

        total = 0
        for period in [p.strip() for p in args.periods.split(",") if p.strip()]:
            print(f"== {period} ==", flush=True)
            for day in days:
                total += archive_day(tq, codes, period, day, args, fmt)
        print(f"完成，累计写入 {total} 行", flush=True)
        return 0
    finally:
        tq.close()


if __name__ == "__main__":
    raise SystemExit(main())

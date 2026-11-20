#!/usr/bin/env python
"""把导出的通达信维表合并进 tick-stock-panel 的 instruments.parquet。

用途
----
修补面板 `sync_instruments` 因某个交易所超时而导致的整市场缺失：
沪市股票没有名称（「名称待同步」）、没有股本（换手率/市值为空）。

脚本可重复执行：已存在的 symbol 用新数据覆盖
（名称、总股本、流通股本以本地通达信为准）。

用法
----
    python scripts/62_apply_instruments.py \
        --data-dir /app/data \
        --csv /tmp/instruments_sh.csv

之后调用 `POST http://127.0.0.1:3018/api/data/refresh-cache` 让面板重载。
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl


def main() -> int:
    ap = argparse.ArgumentParser(description="合并股票维表到面板 instruments.parquet")
    ap.add_argument("--data-dir", default="/app/data")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    target = data_dir / "instruments" / "instruments.parquet"
    if not target.exists():
        print(f"ERROR: 找不到 {target}", file=sys.stderr)
        return 2
    if not Path(args.csv).exists():
        print(f"ERROR: 找不到 {args.csv}", file=sys.stderr)
        return 2

    base = pl.read_parquet(target)
    incoming = pl.read_csv(args.csv, infer_schema_length=0)
    if incoming.is_empty():
        print("CSV 为空，跳过")
        return 0

    numeric = {"float_shares", "total_shares", "tick_size", "limit_up", "limit_down"}
    today = date.today()

    # 按目标 schema 对齐列与类型，保证 concat 不改变文件结构。
    aligned = incoming
    for col, dtype in base.schema.items():
        if col in aligned.columns:
            if col in numeric:
                aligned = aligned.with_columns(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )
            else:
                aligned = aligned.with_columns(pl.col(col).cast(dtype).alias(col))
        else:
            if col == "as_of":
                aligned = aligned.with_columns(pl.lit(today).alias(col))
            elif col == "tick_size":
                aligned = aligned.with_columns(pl.lit(0.01).cast(dtype).alias(col))
            else:
                aligned = aligned.with_columns(pl.lit(None).cast(dtype).alias(col))
    aligned = aligned.select(base.columns)

    before = base.height
    merged = pl.concat([base, aligned], how="vertical_relaxed")
    merged = merged.unique(subset=["symbol"], keep="last", maintain_order=True)

    new_symbols = merged.height - before
    overwritten = aligned.height - max(new_symbols, 0)
    print(f"目标 {target}")
    print(f"原有 {before} 行，CSV {aligned.height} 行")
    print(f"新增 {new_symbols} 行，覆盖 {overwritten} 行，合计 {merged.height} 行")

    if args.dry_run:
        print("dry-run，不写文件")
        return 0

    # 原子替换，避免面板读到半写文件。
    tmp = target.with_suffix(".parquet.tmp")
    merged.write_parquet(tmp)
    tmp.replace(target)
    print(f"已写入 {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

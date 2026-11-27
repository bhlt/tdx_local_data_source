#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""本地分钟线分区 → Linux 网关（53_panel_gateway.py）上传器

背景
----
`64_archive_minute.py` 把全市场分钟线落在 Windows 本地：

    data/minute/1m/date=YYYY-MM-DD/part.parquet
    data/minute/5m/date=YYYY-MM-DD/part.parquet

而 `52_push_panel.py` 的 1m 只推中证500 前 500 只（全市场查询太慢，会拖累
6 秒实时循环）。本脚本负责把本地分区整批补进网关，不改网关、不改面板。

要点
----
- 分批 2 万行 gzip POST 到 /ingest（kind=bars），与实时推送共用同一张 bars 表；
- batch_id 由「周期+日期+批号+内容 md5」生成，重复运行天然幂等；
- 已上传批次记在 <root>/.uploaded.json，分区内容变化（md5 变）才会重传；
- 单位自动识别：当前脚本落盘为手/元；历史分区可能是股/万元（未换算），
  上传前按 amount/volume/close 的比例自动判断并换算，避免混单位；
- 只读本地文件 + HTTP，不占用 TQ 连接，可与实时推送同时跑。

用法
----
    set PANEL_TOKEN=your-secret

    # 补传最近 3 个分区（默认，1m）
    python scripts\\65_upload_minute.py --gateway http://192.168.0.198:18082

    # 指定日期 / 全部 / 预演
    python scripts\\65_upload_minute.py --date 2026-11-19
    python scripts\\65_upload_minute.py --all --periods 1m,5m
    python scripts\\65_upload_minute.py --date 2026-11-19 --dry-run

    # 只传几只做联调
    python scripts\\65_upload_minute.py --date 2026-11-19 --symbols 600519.SH,000651.SZ
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="本地分钟线分区上传到面板网关")
    ap.add_argument("--gateway", default=os.environ.get("PANEL_GATEWAY", "http://127.0.0.1:18082"))
    ap.add_argument("--token", default=os.environ.get("PANEL_TOKEN", "change-me"))
    ap.add_argument("--root", default="data/minute", help="分区根目录（相对项目根）")
    ap.add_argument("--periods", default="1m", help="周期，逗号分隔：1m,5m")
    ap.add_argument("--all", action="store_true", help="上传全部已有分区")
    ap.add_argument("--date", default="", help="只上传指定日期 YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=3, help="未指定 --date/--all 时，上传最近 N 个分区")
    ap.add_argument("--batch", type=int, default=20000, help="每批行数")
    ap.add_argument("--symbols", default="", help="只上传指定代码，逗号分隔（联调用）")
    ap.add_argument("--units", default="auto", choices=["auto", "normalized", "raw"],
                    help="分区单位：auto 自动识别；normalized=手/元；raw=股/万元")
    ap.add_argument("--kind", default="bars_final", choices=["bars", "bars_final"],
                    help="bars_final=覆盖（默认，历史真实数据覆盖盘中合成 bar）；bars=仅补缺")
    ap.add_argument("--state", default="", help="已上传状态文件，默认 <root>/.uploaded.json")
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--force", action="store_true", help="忽略状态文件，全部重传")
    ap.add_argument("--dry-run", action="store_true", help="只统计，不发请求")
    return ap.parse_args()


def list_partitions(root: Path, period: str) -> list[tuple[str, Path]]:
    base = root / period
    if not base.is_dir():
        return []
    out: list[tuple[str, Path]] = []
    for folder in sorted(base.iterdir()):
        if not folder.is_dir() or not folder.name.startswith("date="):
            continue
        day = folder.name[5:]
        for name in ("part.parquet", "part.csv.gz"):
            path = folder / name
            if path.exists():
                out.append((day, path))
                break
    return out


def read_partition(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, compression="gzip")


def detect_units(df: pd.DataFrame) -> str:
    """按 amount/volume/close 的比例判断分区单位。

    归一化（手/元）: amount/volume ≈ close * 100
    原始（股/万元）: amount/volume ≈ close / 10000
    """
    m = df[(df["volume"] > 0) & (df["amount"] > 0) & (df["close"] > 0)]
    if m.empty:
        return "normalized"
    ratio = float((m["amount"] / m["volume"] / m["close"]).median())
    return "normalized" if ratio > 1.0 else "raw"


def normalize_units(df: pd.DataFrame, units: str) -> pd.DataFrame:
    if units != "raw":
        return df
    out = df.copy()
    out["volume"] = out["volume"] / 100.0        # 股 -> 手
    out["amount"] = out["amount"] * 10000.0      # 万元 -> 元
    return out


def prepare(df: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    df = df.rename(columns={"symbol": "code"})
    missing = {"code", "datetime", "open", "high", "low", "close"} - set(df.columns)
    if missing:
        raise ValueError(f"分区缺列: {sorted(missing)}")
    if symbols:
        df = df[df["code"].isin(symbols)]
    df = df.dropna(subset=["open", "high", "low", "close"])
    times = pd.to_datetime(df["datetime"])
    df = df.assign(time=times.dt.strftime("%Y-%m-%d %H:%M:%S"))
    for col in ("volume", "amount"):
        if col not in df.columns:
            df[col] = None
    return df[["code", "open", "high", "low", "close", "volume", "amount", "time"]]


def to_num(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def to_rows(chunk: pd.DataFrame, period: str) -> list[dict]:
    rows: list[dict] = []
    for rec in chunk.itertuples(index=False):
        rows.append({
            "code": str(rec.code), "period": period, "time": str(rec.time),
            "open": to_num(rec.open), "high": to_num(rec.high),
            "low": to_num(rec.low), "close": to_num(rec.close),
            "volume": to_num(rec.volume), "amount": to_num(rec.amount),
        })
    return rows


def chunk_md5(rows: list[dict]) -> str:
    payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def post(gateway: str, token: str, batch_id: str, rows: list[dict], timeout: float,
         kind: str = "bars_final") -> dict:
    payload = {"batch_id": batch_id, "kind": kind, "rows": rows}
    body = gzip.compress(json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8"))
    req = urllib.request.Request(
        gateway.rstrip("/") + "/ingest", data=body, method="POST",
        headers={"Content-Type": "application/json", "Content-Encoding": "gzip",
                 "X-Api-Key": token})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_state(path: Path) -> dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return {str(k): str(v) for k, v in data.items()}
    except (OSError, ValueError):
        return {}


def save_state(path: Path, state: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=0, sort_keys=True)
    tmp.replace(path)


def resolve(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else ROOT / path


def main() -> int:
    args = parse_args()
    root = resolve(args.root)
    state_path = resolve(args.state) if args.state else root / ".uploaded.json"
    state = {} if args.force else load_state(state_path)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    periods = [p.strip() for p in args.periods.split(",") if p.strip()]

    targets: list[tuple[str, str, Path]] = []
    for period in periods:
        parts = list_partitions(root, period)
        if args.date:
            parts = [p for p in parts if p[0] == args.date]
        elif not args.all:
            parts = parts[-max(args.days, 1):]
        if not parts:
            print(f"WARN: {period} 没有可上传分区（{root / period}）")
        targets.extend((period, day, path) for day, path in parts)
    if not targets:
        print("没有需要上传的分区")
        return 2

    scope = ("all" if not symbols
             else hashlib.md5(",".join(sorted(symbols)).encode("utf-8")).hexdigest()[:8])
    sent_rows = sent_chunks = skip_chunks = fail_chunks = 0
    began = time.time()
    for period, day, path in targets:
        stat = path.stat()
        sig = f"{stat.st_mtime_ns}:{stat.st_size}"
        file_key = f"{period}|{day}|{scope}|file"
        if not args.dry_run and not args.force and state.get(file_key) == sig:
            print(f"{period} {day} skip（文件未变 {sig}）", flush=True)
            skip_chunks += 1
            continue

        try:
            df = read_partition(path)
        except Exception as exc:  # noqa: BLE001
            print(f"读取失败 {path}: {exc}")
            fail_chunks += 1
            continue

        units = args.units if args.units != "auto" else detect_units(df)
        df = prepare(normalize_units(df, units), symbols)
        total = len(df)
        print(f"{period} {day} rows={total} units={units} file={path.name}", flush=True)
        if total == 0:
            continue

        file_failed = False
        for offset in range(0, total, args.batch):
            chunk = df.iloc[offset:offset + args.batch]
            rows = to_rows(chunk, period)
            if not rows:
                continue
            md5 = chunk_md5(rows)
            # 批号用行偏移，即"这一批是哪几行"，与内容哈希一起构成幂等键
            key = f"{period}|{day}|{scope}|{offset}"
            batch_id = f"m{period}-{day}-{offset}-{md5[:16]}"
            if state.get(key) == md5:
                skip_chunks += 1
                continue
            if args.dry_run:
                sent_rows += len(rows)
                sent_chunks += 1
                continue

            ok = False
            for attempt in range(1, args.retries + 1):
                t0 = time.perf_counter()
                try:
                    resp = post(args.gateway, args.token, batch_id, rows, args.timeout, args.kind)
                except Exception as exc:  # noqa: BLE001
                    if attempt >= args.retries:
                        fail_chunks += 1
                        file_failed = True
                        print(f"  批次 {offset // args.batch + 1} 失败: {exc}", flush=True)
                    else:
                        time.sleep(2.0 * attempt)
                    continue
                state[key] = md5
                save_state(state_path, state)
                sent_rows += len(rows)
                sent_chunks += 1
                ok = True
                print(f"  {offset // args.batch + 1}: rows={len(rows)} "
                      f"accepted={resp.get('accepted')} dup={bool(resp.get('duplicate'))} "
                      f"ms={round((time.perf_counter() - t0) * 1000, 0)}", flush=True)
                break
            if not ok:
                file_failed = True

        if not file_failed and not args.dry_run:
            state[file_key] = sig
            save_state(state_path, state)

    print(f"{'[dry-run] ' if args.dry_run else ''}完成: 上传 {sent_rows} 行 / {sent_chunks} 批，"
          f"跳过 {skip_chunks} 批，失败 {fail_chunks} 批，耗时 {time.time() - began:.0f}s")
    return 1 if fail_chunks else 0


if __name__ == "__main__":
    sys.exit(main())

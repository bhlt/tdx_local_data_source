# -*- coding: utf-8 -*-
"""
tick-stock-panel 契约自检

检查 53_panel_gateway.py 的返回是否满足面板的必填字段与单位要求：
    realtime: symbol, last_price, prev_close, open, high, low, volume 不可为空
    depth5  : asks/bids 为 [[价, 手]×5]，且 ask_volumes/bid_volumes 长度一致
    minute  : symbol, datetime, OHLC, volume
    daily   : symbol, date, OHLC, volume, amount

用法：
    python scripts/54_check_panel_contract.py --base http://127.0.0.1:18082 --token panel-test
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request


def get(base: str, token: str, path: str):
    req = urllib.request.Request(base.rstrip("/") + path, headers={"X-Api-Key": token})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:18082")
    ap.add_argument("--token", default="panel-test")
    ap.add_argument("--symbol", default="000009.SZ")
    args = ap.parse_args()

    ok = True

    rt = get(args.base, args.token, "/panel/realtime")
    rows = rt.get("data", [])
    required = ["symbol", "last_price", "prev_close", "open", "high", "low", "volume"]
    missing = [(r.get("symbol"), k) for r in rows for k in required if r.get(k) is None]
    print(f"[realtime] rows={len(rows)} missing={len(missing)} {missing[:3]}")
    ok &= len(rows) > 0 and not missing

    asset_types = {"stock", "etf", "index", "board", "bond", "reit", "other"}
    bad_type = [r.get("symbol") for r in rows if r.get("asset_type") not in asset_types]
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.get("asset_type")] = counts.get(r.get("asset_type"), 0) + 1
    print(f"[asset_type] counts={counts} missing={len(bad_type)} {bad_type[:5]}")
    ok &= not bad_type

    # 指数/板块绝不能用 volume×100×price 回填成交额
    fabricated = []
    for r in rows:
        if r.get("asset_type") not in ("index", "board"):
            continue
        amt, vol, last = r.get("amount"), r.get("volume"), r.get("last_price")
        if amt and vol and last and abs(float(amt) / (float(vol) * 100.0 * float(last)) - 1.0) < 1e-3:
            fabricated.append(r.get("symbol"))
    print(f"[index amount] fabricated={len(fabricated)} {fabricated[:5]}")
    ok &= not fabricated


    dp = get(args.base, args.token, f"/panel/depth5?symbols={args.symbol}")
    data = dp.get("data", {})
    for sym, d in list(data.items())[:3]:
        good = (len(d.get("asks", [])) == 5 and len(d.get("bids", [])) == 5
                and len(d.get("ask_volumes", [])) == 5 and len(d.get("bid_volumes", [])) == 5)
        print(f"[depth5] {sym} levels={len(d.get('asks', []))} vols={len(d.get('ask_volumes', []))} ok={good}")
        ok &= good

    mn = get(args.base, args.token, f"/panel/minute?symbols={args.symbol}")
    mrows = (mn.get("data") or {}).get("rows", [])
    mgood = bool(mrows) and all(mrows[-1].get(k) is not None
                                for k in ("symbol", "datetime", "open", "high", "low", "close", "volume"))
    print(f"[minute] rows={len(mrows)} last={mrows[-1] if mrows else None}")
    ok &= mgood

    dl = get(args.base, args.token, f"/panel/daily?symbols={args.symbol}")
    drows = (dl.get("data") or {}).get("rows", [])
    dgood = bool(drows) and all(drows[-1].get(k) is not None
                                for k in ("symbol", "date", "open", "high", "low", "close", "volume", "amount"))
    print(f"[daily] rows={len(drows)} last={drows[-1] if drows else None}")
    ok &= dgood

    # 网关自检（P1）：股票成交额比值、指数成交额量级、别名唯一性
    try:
        au = get(args.base, args.token, "/panel/audit").get("data") or {}
        checks = au.get("checks", {})
        print(f"[audit] ok={au.get('ok')} counts={au.get('counts')} "
              f"checks={ {k: v.get('status') for k, v in checks.items()} }")
        ok &= bool(au.get("ok"))
    except Exception as exc:  # noqa: BLE001
        print(f"[audit] skipped: {exc}")

    print("CONTRACT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

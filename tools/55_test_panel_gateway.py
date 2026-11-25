# -*- coding: utf-8 -*-
"""53_panel_gateway.py 口径回归测试（零第三方依赖）。

覆盖 2026-11-25 成交额异常修复的关键约束：
    - asset_type 分类（股票 / ETF / 指数 / 板块 / 债券 / REIT）
    - 股票 amount 按成交量增量累计且日内单调
    - 指数 / 板块 amount 绝不用 volume×price 回填
    - 指数 volume（股）折算为手
    - 999999.SH 归一为 000001.SH
    - 指数成交额按成分股汇总
    - 旧库迁移：补 asset_type、折算旧指数 volume、清掉假 amount

运行：
    python scripts/55_test_panel_gateway.py
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CANDIDATES = [
    os.path.join(ROOT, "scripts", "53_panel_gateway.py"),
    os.path.join(ROOT, "gateway", "53_panel_gateway.py"),
    os.path.join(ROOT, "53_panel_gateway.py"),
]
SCRIPT = next((path for path in _CANDIDATES if os.path.exists(path)), _CANDIDATES[0])


def load_gateway():
    spec = importlib.util.spec_from_file_location("panel_gateway_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


gw = load_gateway()
TODAY = date.today().strftime("%Y-%m-%d")
YESTERDAY = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "panel.db")
        gw.init_db(self.db)
        self.conn = sqlite3.connect(self.db)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def seed(self, **kw):
        row = {
            "code": "600000.SH", "asset_type": "stock", "volume_unit": "lot",
            "last": 10.0, "prev_close": 9.9, "open": 10.0, "high": 10.0,
            "low": 10.0, "volume": 100.0, "amount": 100000.0,
            "ts": f"{TODAY} 10:00:00", "ts_ms": 1,
        }
        row.update(kw)
        gw.upsert_seed(self.conn, [row])
        self.conn.commit()

    def tick(self, **kw):
        row = {
            "code": "600000.SH", "asset_type": "stock", "volume_unit": "lot",
            "last": 10.0, "prev_close": 9.9, "volume": 100.0,
            "ts": f"{TODAY} 10:00:06", "ts_ms": 2,
        }
        row.update(kw)
        gw.apply_tick(self.conn, [row])
        self.conn.commit()

    def realtime(self, **kw):
        return gw.realtime_rows(self.conn, **kw)

    # ------------------------------------------------------------------
    def test_classify_asset(self):
        cases = {
            "600519.SH": "stock", "688318.SH": "stock", "000001.SZ": "stock",
            "430047.BJ": "stock", "920001.BJ": "stock",
            "510300.SH": "etf", "588000.SH": "etf", "159915.SZ": "etf",
            "501018.SH": "etf", "506000.SH": "etf",
            "000001.SH": "index", "399001.SZ": "index", "899601.BJ": "index",
            "880008.SH": "board", "880823.SH": "board",
            "110038.SH": "bond", "123054.SZ": "bond", "508000.SH": "reit",
            "999999.SH": "index",
        }
        for code, expected in cases.items():
            self.assertEqual(gw.classify_asset(code), expected, code)
        # 上游把 880xxx 错标成 stock 时必须纠正
        self.assertEqual(gw.classify_asset("880008.SH", "stock"), "board")

    def test_stock_amount_accumulates_and_is_monotonic(self):
        self.seed()
        self.tick(volume=150.0, last=10.1)
        row = self.realtime(symbols=["600000.SH"])[0]
        self.assertEqual(row["asset_type"], "stock")
        self.assertAlmostEqual(row["amount"], 100000.0 + 50.0 * 100.0 * 10.1)
        self.assertAlmostEqual(row["volume"], 150.0)
        self.assertEqual(row["amount_source"], "realtime_delta")

        # 同日再次 seed 是权威快照，直接覆盖旧值（修复盘前昨日累计值残留）
        self.seed(volume=160.0, amount=1000.0, ts=f"{TODAY} 10:00:10")
        row = self.realtime(symbols=["600000.SH"])[0]
        self.assertAlmostEqual(row["amount"], 1000.0)
        self.assertAlmostEqual(row["volume"], 160.0)

        # 局部种子没有 amount 时保留已有累计值
        self.seed(volume=170.0, amount=None, ts=f"{TODAY} 10:00:20")
        row = self.realtime(symbols=["600000.SH"])[0]
        self.assertAlmostEqual(row["amount"], 1000.0)

        # 跨交易日 seed 重置为当日值
        self.seed(volume=10.0, amount=5000.0, ts=f"{YESTERDAY} 10:00:00")
        row = self.realtime(symbols=["600000.SH"])[0]
        self.assertAlmostEqual(row["amount"], 5000.0)
        self.assertAlmostEqual(row["volume"], 10.0)

    def test_preopen_seed_zeroes_volume_and_amount(self):
        # 09:20 快照可能还是上一交易日的累计量/额，种子必须归零
        self.seed(volume=1.67e6, amount=2.05e9, last=12.3, ts=f"{TODAY} 09:20:00")
        row = self.realtime(symbols=["600000.SH"])[0]
        self.assertEqual(row["volume"], 0.0)
        self.assertEqual(row["amount"], 0.0)

        self.tick(volume=1000.0, last=12.31, ts=f"{TODAY} 09:31:00")
        row = self.realtime(symbols=["600000.SH"])[0]
        self.assertAlmostEqual(row["amount"], 1000.0 * 100.0 * 12.31)

    def test_index_amount_never_uses_volume_times_price(self):
        self.seed(code="000001.SH", asset_type="index", volume_unit="share",
                  last=3511.93, prev_close=3500.0, open=3511.93, high=3511.93,
                  low=3511.93, volume=1.99e8, amount=6.9989e13)
        row = self.realtime(symbols=["000001.SH"])[0]
        self.assertEqual(row["asset_type"], "index")
        self.assertIsNone(row["amount"])
        self.assertAlmostEqual(row["volume"], 1.99e6)   # 股 -> 手
        self.assertIsNone(row["amount_source"])

        self.tick(code="000001.SH", asset_type="index", volume_unit="share",
                  last=3512.94, volume=2.325e8)
        row = self.realtime(symbols=["000001.SH"])[0]
        self.assertIsNone(row["amount"])
        self.assertAlmostEqual(row["volume"], 2.325e6)

    def test_index_alias_is_canonicalised(self):
        self.seed(code="999999.SH", asset_type="index", volume_unit="share",
                  last=3512.94, volume=2.325e8, amount=3.4667e12)
        self.seed(code="000001.SH", asset_type="index", volume_unit="share",
                  last=3512.94, volume=2.325e8)
        rows = self.realtime(symbols=["999999.SH", "000001.SH"])
        self.assertEqual([r["symbol"] for r in rows], ["000001.SH"])
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM panel_state WHERE code='999999.SH'").fetchone()[0],
            0)

    def test_index_amount_sums_constituents(self):
        self.seed(code="600000.SH", volume=10.0, amount=1000.0, last=10.0)
        self.seed(code="600001.SH", volume=20.0, amount=2000.0, last=20.0)
        self.seed(code="000001.SH", asset_type="index", volume_unit="share",
                  volume=1.0, amount=1.0, last=3500.0)
        gw.replace_members(self.conn, [
            {"index_code": "000001.SH", "members": ["600000.SH", "600001.SH"]},
        ])
        self.conn.commit()
        row = self.realtime(symbols=["000001.SH"])[0]
        self.assertAlmostEqual(row["amount"], 3000.0)
        self.assertAlmostEqual(row["volume"], 30.0)
        self.assertEqual(row["amount_source"], "constituents")

        # 全量快照默认不返回指数 amount，避免消费方直接求和时重复计入
        full = {r["symbol"]: r for r in self.realtime()}
        self.assertIsNone(full["000001.SH"]["amount"])
        typed = self.realtime(asset_types={"index"})
        self.assertAlmostEqual(typed[0]["amount"], 3000.0)

    def test_realtime_filters(self):
        self.seed(code="600000.SH")
        self.seed(code="510300.SH", asset_type="etf", volume_unit="lot",
                  volume=10.0, amount=30000.0, last=3.0)
        self.seed(code="000001.SH", asset_type="index", volume_unit="share",
                  volume=100.0, amount=1.0, last=3500.0)
        self.assertEqual({r["asset_type"] for r in self.realtime(asset_types={"stock"})},
                         {"stock"})
        self.assertEqual({r["symbol"] for r in self.realtime(symbols=["510300.SH"])},
                         {"510300.SH"})
        self.assertEqual({r["asset_type"] for r in self.realtime()},
                         {"stock", "etf", "index"})

    def test_broad_index_falls_back_to_exchange_stocks(self):
        self.seed(code="600000.SH", volume=10.0, amount=1000.0, last=10.0)
        self.seed(code="600001.SH", volume=20.0, amount=2000.0, last=20.0)
        self.seed(code="000001.SH", asset_type="index", volume_unit="share",
                  volume=1.0, amount=1.0, last=3500.0)
        row = self.realtime(symbols=["000001.SH"])[0]
        self.assertAlmostEqual(row["amount"], 3000.0)
        self.assertEqual(row["amount_source"], "constituents")

    def test_instrument_rows(self):
        self.seed(code="600000.SH", name="浦发银行")
        self.seed(code="000001.SH", asset_type="index", volume_unit="share",
                  name="上证指数", volume=100.0, amount=1.0, last=3500.0)
        rows = gw.instrument_rows(self.conn, asset_types={"stock"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "600000.SH")
        self.assertEqual(rows[0]["exchange"], "SH")
        self.assertEqual(rows[0]["type"], "stock")
        self.assertIsNone(rows[0]["total_shares"])

    def test_legacy_db_migration(self):
        legacy = os.path.join(self.tmp.name, "legacy.db")
        conn = sqlite3.connect(legacy)
        conn.executescript("""
        CREATE TABLE panel_state (
            code TEXT PRIMARY KEY, name TEXT, prev_close REAL, open REAL,
            high REAL, low REAL, last REAL, volume REAL, amount REAL,
            float_shares REAL, ts TEXT, ts_ms INTEGER
        );
        CREATE TABLE depth_state (
            code TEXT PRIMARY KEY, asks TEXT, bids TEXT, ask_vols TEXT,
            bid_vols TEXT, ts_ms INTEGER
        );
        CREATE TABLE bars (
            code TEXT NOT NULL, period TEXT NOT NULL, time TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL,
            PRIMARY KEY (code, period, time)
        );
        CREATE TABLE batches (
            batch_id TEXT PRIMARY KEY, kind TEXT, rows INTEGER, received_at TEXT
        );
        """)
        conn.execute(
            "INSERT INTO panel_state(code,name,last,volume,amount,ts,ts_ms) "
            "VALUES('880008.SH','全Ａ等权',1185.64,4.508e8,5.3342e13,?,1)", (f"{TODAY} 10:00:00",))
        conn.execute(
            "INSERT INTO panel_state(code,name,last,volume,amount,ts,ts_ms) "
            "VALUES('999999.SH','上证指数',3511.93,1.99e8,6.9989e13,?,1)", (f"{TODAY} 10:00:00",))
        conn.execute(
            "INSERT INTO panel_state(code,name,last,volume,amount,ts,ts_ms) "
            "VALUES('000001.SH','上证指数',3511.93,1.99e8,6.0e13,?,1)", (f"{TODAY} 10:00:00",))
        conn.commit()
        conn.close()

        gw.init_db(legacy)
        conn = sqlite3.connect(legacy)
        board = conn.execute(
            "SELECT asset_type,volume,amount,volume_unit FROM panel_state WHERE code='880008.SH'"
        ).fetchone()
        self.assertEqual(board[0], "board")
        self.assertAlmostEqual(board[1], 4.508e6)   # 旧指数 volume 股 -> 手，只折一次
        self.assertIsNone(board[2])
        self.assertEqual(board[3], "lot")

        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM panel_state WHERE code='999999.SH'").fetchone()[0],
            0)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM panel_state WHERE code='000001.SH'").fetchone()[0],
            1)

        # 再跑一次迁移不能重复除以 100
        gw.init_db(legacy)
        conn.close()
        conn = sqlite3.connect(legacy)
        board = conn.execute(
            "SELECT volume,volume_unit FROM panel_state WHERE code='880008.SH'").fetchone()
        self.assertAlmostEqual(board[0], 4.508e6)
        self.assertEqual(board[1], "lot")
        conn.close()

    def test_audit(self):
        self.seed()
        result = gw.audit(self.conn)
        self.assertTrue(result["ok"])
        self.assertEqual(result["checks"]["alias_unique"]["status"], "pass")
        self.assertEqual(result["checks"]["stock_amount_ratio"]["bad"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

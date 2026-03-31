#!/usr/bin/env python3
"""
Polygon.io Data Puller for ZonkTrader 0DTE Strategy

Pulls 1-min OHLCV bars for:
- SPY (underlying)
- All 0DTE options within ±5% of SPY's opening price

Rotates through multiple API keys (5 calls/min each).
Hot-reloads api_keys.json so new keys can be added while running.
Resumes from where it left off if interrupted.
"""

import urllib.request
import json
import sqlite3
import time
import math
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

KEYS_FILE = Path("/home/abhijay/trading-strategies/api_keys.json")
BASE_URL = "https://api.polygon.io"
DB_PATH = Path("/home/abhijay/trading-strategies/data/options_data.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

RATE_PER_KEY = 5  # calls per minute per key


# ── Key Rotation ─────────────────────────────────────────────────────

class KeyRotator:
    def __init__(self):
        self.keys = []
        self.call_log = {}  # key -> list of timestamps
        self.current_idx = 0
        self.last_reload = 0
        self.reload_keys()

    def reload_keys(self):
        """Hot-reload keys from file."""
        try:
            self.keys = json.loads(KEYS_FILE.read_text())
            for k in self.keys:
                if k not in self.call_log:
                    self.call_log[k] = []
            self.last_reload = time.time()
        except Exception:
            pass

    def get_key(self):
        """Get the next available key, waiting if all are rate-limited."""
        # Reload keys every 30 seconds to pick up new ones
        if time.time() - self.last_reload > 30:
            old_count = len(self.keys)
            self.reload_keys()
            if len(self.keys) > old_count:
                print(f"    [+] New keys detected! Now using {len(self.keys)} keys "
                      f"({len(self.keys) * RATE_PER_KEY} calls/min)")

        while True:
            now = time.time()
            for _ in range(len(self.keys)):
                key = self.keys[self.current_idx]
                self.current_idx = (self.current_idx + 1) % len(self.keys)

                # Clean old timestamps
                self.call_log[key] = [t for t in self.call_log[key] if now - t < 60]

                if len(self.call_log[key]) < RATE_PER_KEY:
                    self.call_log[key].append(now)
                    return key

            # All keys exhausted — wait for the earliest to free up
            earliest = min(self.call_log[k][0] for k in self.keys if self.call_log[k])
            wait = 60 - (now - earliest) + 0.5
            if wait > 0:
                print(f"    All {len(self.keys)} keys rate-limited — waiting {wait:.0f}s   ", end="\r")
                time.sleep(wait)


rotator = KeyRotator()


# ── API ──────────────────────────────────────────────────────────────

def api_call(url_template, retries=3):
    """Make a rate-limited API call with key rotation and retries."""
    for attempt in range(retries):
        key = rotator.get_key()
        url = url_template.replace("{KEY}", key)
        try:
            req = urllib.request.Request(url)
            resp = urllib.request.urlopen(req, timeout=15)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Rate limited
                time.sleep(2)
                continue
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"    FAILED ({e.code}): {e.reason}")
                return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"    FAILED: {e}")
                return None
    return None


# ── Database ─────────────────────────────────────────────────────────

def init_db():
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS spy_bars (
            date TEXT,
            timestamp INTEGER,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER,
            PRIMARY KEY (date, timestamp)
        );
        CREATE TABLE IF NOT EXISTS option_bars (
            date TEXT,
            ticker TEXT,
            strike REAL,
            type TEXT,
            timestamp INTEGER,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER,
            PRIMARY KEY (date, ticker, timestamp)
        );
        CREATE TABLE IF NOT EXISTS pull_log (
            date TEXT PRIMARY KEY,
            spy_done INTEGER DEFAULT 0,
            options_done INTEGER DEFAULT 0,
            spy_open REAL,
            strikes_pulled INTEGER DEFAULT 0,
            completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS vix_daily (
            date TEXT PRIMARY KEY,
            close REAL
        );
        CREATE INDEX IF NOT EXISTS idx_ob_date ON option_bars(date);
        CREATE INDEX IF NOT EXISTS idx_ob_strike ON option_bars(date, strike, type);
    """)
    db.commit()
    return db


# ── Data Pulling ─────────────────────────────────────────────────────

def get_trading_days(start_date, end_date):
    url = (f"{BASE_URL}/v2/aggs/ticker/SPY/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000"
           f"&apiKey={{KEY}}")
    data = api_call(url)
    if not data or not data.get("results"):
        return []
    days = []
    for bar in data["results"]:
        dt = datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d")
        days.append({"date": dt, "open": bar["o"], "close": bar["c"]})
    return days


def pull_spy_bars(db, trade_date):
    url = (f"{BASE_URL}/v2/aggs/ticker/SPY/range/1/minute/"
           f"{trade_date}/{trade_date}?adjusted=true&sort=asc&limit=50000"
           f"&apiKey={{KEY}}")
    data = api_call(url)
    if not data or not data.get("results"):
        return 0
    rows = [(trade_date, b["t"], b["o"], b["h"], b["l"], b["c"], b.get("v", 0))
            for b in data["results"]]
    db.executemany("INSERT OR IGNORE INTO spy_bars VALUES (?,?,?,?,?,?,?)", rows)
    db.commit()
    return len(rows)


def build_option_ticker(trade_date, strike, opt_type):
    dt = datetime.strptime(trade_date, "%Y-%m-%d")
    date_str = dt.strftime("%y%m%d")
    type_char = "C" if opt_type == "call" else "P"
    strike_int = int(strike * 1000)
    return f"O:SPY{date_str}{type_char}{strike_int:08d}"


def pull_option_bars(db, trade_date, ticker, strike, opt_type):
    url = (f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/"
           f"{trade_date}/{trade_date}?adjusted=true&sort=asc&limit=50000"
           f"&apiKey={{KEY}}")
    data = api_call(url)
    if not data or not data.get("results"):
        return 0
    rows = [(trade_date, ticker, strike, opt_type, b["t"],
             b["o"], b["h"], b["l"], b["c"], b.get("v", 0))
            for b in data["results"]]
    db.executemany("INSERT OR IGNORE INTO option_bars VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    db.commit()
    return len(rows)


def pull_vix(db, start_date, end_date):
    url = (f"{BASE_URL}/v2/aggs/ticker/I:VIX/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000"
           f"&apiKey={{KEY}}")
    data = api_call(url)
    if not data or not data.get("results"):
        return 0
    rows = [(datetime.fromtimestamp(b["t"] / 1000).strftime("%Y-%m-%d"), b["c"])
            for b in data["results"]]
    db.executemany("INSERT OR IGNORE INTO vix_daily VALUES (?,?)", rows)
    db.commit()
    return len(rows)


def pull_day(db, trade_date, spy_open):
    """Pull all data for one trading day."""
    row = db.execute(
        "SELECT options_done FROM pull_log WHERE date = ?", (trade_date,)
    ).fetchone()
    if row and row[0]:
        return "skip"

    print(f"\n  {trade_date} | SPY open: ${spy_open:.2f} | "
          f"Keys: {len(rotator.keys)} ({len(rotator.keys)*RATE_PER_KEY} calls/min)")

    # SPY bars
    spy_count = pull_spy_bars(db, trade_date)
    print(f"    SPY: {spy_count} bars")

    # Strike range ±5%
    low_strike = math.floor(spy_open * 0.95)
    high_strike = math.ceil(spy_open * 1.05)
    strikes = list(range(low_strike, high_strike + 1))

    db.execute(
        "INSERT OR REPLACE INTO pull_log (date, spy_done, spy_open) VALUES (?,1,?)",
        (trade_date, spy_open)
    )
    db.commit()

    # Pull options
    total_bars = 0
    strikes_with_data = 0
    t_start = time.time()

    for i, strike in enumerate(strikes):
        for opt_type in ["put", "call"]:
            ticker = build_option_ticker(trade_date, strike, opt_type)
            count = pull_option_bars(db, trade_date, ticker, strike, opt_type)
            total_bars += count
            if count > 0:
                strikes_with_data += 1

        pct = (i + 1) / len(strikes) * 100
        elapsed = time.time() - t_start
        rate = (i + 1) * 2 / max(elapsed, 1) * 60  # calls per min
        eta = (len(strikes) - i - 1) * 2 / max(rate / 60, 0.01)
        print(f"    Options: {strikes_with_data} w/data | "
              f"{total_bars} bars | {pct:.0f}% | "
              f"~{rate:.0f} calls/min | ETA {eta:.0f}s     ", end="\r")

    elapsed = time.time() - t_start
    print(f"    Options: {strikes_with_data} w/data | "
          f"{total_bars} bars | 100% | {elapsed:.0f}s          ")

    db.execute(
        "UPDATE pull_log SET options_done=1, strikes_pulled=?, completed_at=? WHERE date=?",
        (strikes_with_data, datetime.now().isoformat(), trade_date)
    )
    db.commit()
    return total_bars


def run(start_date="2025-03-29", end_date="2026-03-28"):
    db = init_db()

    print(f"Polygon Data Puller — 0DTE SPY Options (±5%)")
    print(f"Range: {start_date} to {end_date}")
    print(f"API keys: {len(rotator.keys)} ({len(rotator.keys)*RATE_PER_KEY} calls/min)")
    print(f"Database: {DB_PATH}")
    print(f"Add keys to {KEYS_FILE} while running — auto-detected!")
    print(f"{'='*55}")

    # VIX
    print(f"\nPulling VIX...")
    vix_count = pull_vix(db, start_date, end_date)
    print(f"  VIX: {vix_count} days")

    # Trading days
    print(f"\nGetting trading days...")
    trading_days = get_trading_days(start_date, end_date)
    print(f"  Found {len(trading_days)} trading days")

    done = db.execute("SELECT COUNT(*) FROM pull_log WHERE options_done = 1").fetchone()[0]
    remaining = len(trading_days) - done
    calls_per_day = 133  # ~66 strikes × 2 (put+call) + 1 SPY
    total_calls = remaining * calls_per_day
    rate = len(rotator.keys) * RATE_PER_KEY
    est_hours = (total_calls / rate) / 60

    print(f"  Done: {done} | Remaining: {remaining}")
    print(f"  Est. time: ~{est_hours:.1f} hours at {rate} calls/min")
    print(f"{'='*55}")

    for i, day in enumerate(trading_days):
        result = pull_day(db, day["date"], day["open"])
        if result == "skip":
            continue

        if (i + 1) % 5 == 0:
            done = db.execute("SELECT COUNT(*) FROM pull_log WHERE options_done = 1").fetchone()[0]
            total = db.execute("SELECT COUNT(*) FROM option_bars").fetchone()[0]
            sz = DB_PATH.stat().st_size / 1e6
            remaining = len(trading_days) - done
            est = (remaining * calls_per_day / (len(rotator.keys) * RATE_PER_KEY)) / 60
            print(f"\n  === {done}/{len(trading_days)} days | "
                  f"{total:,} bars | {sz:.0f}MB | ~{est:.1f}h left ===\n")

    # Done
    done = db.execute("SELECT COUNT(*) FROM pull_log WHERE options_done = 1").fetchone()[0]
    total_spy = db.execute("SELECT COUNT(*) FROM spy_bars").fetchone()[0]
    total_opt = db.execute("SELECT COUNT(*) FROM option_bars").fetchone()[0]
    sz = DB_PATH.stat().st_size / 1e6

    print(f"\n{'='*55}")
    print(f"COMPLETE!")
    print(f"Days: {done} | SPY bars: {total_spy:,} | Option bars: {total_opt:,}")
    print(f"Database: {sz:.0f} MB")
    print(f"{'='*55}")
    db.close()


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2025-03-29"
    end = sys.argv[2] if len(sys.argv) > 2 else "2026-03-28"
    run(start, end)

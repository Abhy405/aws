#!/usr/bin/env python3
"""
ZonkTrader Strategy Optimizer

Sweeps key parameters to find optimal settings:
- Strike distance (% of expected move): 50-100%
- Stop multiplier: 1.5x - 3.0x credit
- Early profit take: close at 50%, 60%, 70%, 80% of max profit
- Exit time: 11:00, 11:30, 12:00, 12:30, 1:00 PM
- VIX skip thresholds
- Gap skip threshold
"""

import sqlite3
import math
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from itertools import product

DB_PATH = Path("/home/abhijay/trading-strategies/data/options_data.db")
DATA_DIR = Path("/home/abhijay/trading-strategies/data")


def ts_to_et(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    month = dt.month
    if month >= 3 and month < 11:
        return dt.astimezone(timezone(timedelta(hours=-4)))
    else:
        return dt.astimezone(timezone(timedelta(hours=-5)))


def build_option_ticker(date, strike, opt_type):
    dt = datetime.strptime(date, "%Y-%m-%d")
    date_str = dt.strftime("%y%m%d")
    type_char = "C" if opt_type == "call" else "P"
    strike_int = int(strike * 1000)
    return f"O:SPY{date_str}{type_char}{strike_int:08d}"


def preload_day_data(db, date):
    """Load all option bars for a day into memory for fast parameter sweeps."""
    bars = {}
    for row in db.execute(
        "SELECT ticker, timestamp, open, high, low, close, volume FROM option_bars WHERE date=? ORDER BY timestamp",
        (date,)
    ):
        ticker = row[0]
        if ticker not in bars:
            bars[ticker] = []
        bars[ticker].append(row[1:])  # (ts, o, h, l, c, v)
    return bars


def get_entry_price(ticker_bars, entry_start_min=571, entry_end_min=585):
    """Get entry price from preloaded bars. Minutes = hour*60+min (571 = 9:31)."""
    if not ticker_bars:
        return None
    for bar in ticker_bars:
        dt = ts_to_et(bar[0])
        bar_min = dt.hour * 60 + dt.minute
        if entry_start_min <= bar_min <= entry_end_min:
            return bar[1]  # open price
    # Fallback: first bar after 9:30
    for bar in ticker_bars:
        dt = ts_to_et(bar[0])
        bar_min = dt.hour * 60 + dt.minute
        if bar_min >= 570:
            return bar[1]
    return None


def simulate_spread(short_bars, long_bars, credit, spread_width,
                    stop_mult, profit_take_pct, exit_minute):
    """
    Simulate a credit spread with configurable parameters.
    profit_take_pct: close when spread value drops to (1-pct)*credit (e.g. 0.5 = take 50% profit)
    exit_minute: minute of day to force exit (e.g. 780 = 1:00 PM)
    """
    if not short_bars or not long_bars or credit <= 0:
        return credit * 100, "no_data"

    short_prices = {}
    for bar in short_bars:
        dt = ts_to_et(bar[0])
        key = dt.hour * 60 + dt.minute
        short_prices[key] = bar

    long_prices = {}
    for bar in long_bars:
        dt = ts_to_et(bar[0])
        key = dt.hour * 60 + dt.minute
        long_prices[key] = bar

    entry_min = 571  # 9:31
    stop_level = credit * stop_mult
    profit_target = credit * (1 - profit_take_pct)  # spread value to close at

    last_spread_value = 0

    for minute in range(entry_min, exit_minute + 1):
        short_bar = short_prices.get(minute)
        long_bar = long_prices.get(minute)

        if short_bar and long_bar:
            spread_worst = short_bar[2] - long_bar[3]  # short high - long low
            spread_mid = short_bar[4] - long_bar[4]    # short close - long close
            last_spread_value = max(0, spread_mid)

            # Stop out
            if spread_worst > 0 and spread_worst >= stop_level:
                return (credit - stop_level) * 100, "stop_out"

            # Profit take
            if profit_take_pct < 1.0 and spread_mid <= profit_target and spread_mid >= 0:
                return (credit - max(0, spread_mid)) * 100, "profit_take"

        elif short_bar:
            last_spread_value = short_bar[4]

    # Exit at target time
    exit_spread = last_spread_value
    for check_min in range(exit_minute, exit_minute - 10, -1):
        s = short_prices.get(check_min)
        l = long_prices.get(check_min)
        if s and l:
            exit_spread = max(0, s[4] - l[4])
            break

    pnl = (credit - exit_spread) * 100
    return pnl, "exit_time" if exit_spread > 0.05 else "expire_worthless"


def run_sweep(db, days, vix_data, params):
    """Run backtest with specific parameters, return metrics."""
    strike_pct = params["strike_pct"]
    stop_mult = params["stop_mult"]
    profit_take = params["profit_take"]
    exit_hour = params["exit_hour"]
    exit_min_of_hour = params["exit_min"]
    vix_max = params["vix_max"]
    vix_min = params["vix_min"]
    gap_max = params["gap_max"]

    exit_minute = exit_hour * 60 + exit_min_of_hour
    balance = 300.0
    peak = 300.0
    max_dd = 0
    wins = 0
    losses = 0
    total_pnl = 0
    prev_spy_close = None

    for day_idx, (trade_date, spy_open, day_bars_cache) in enumerate(days):
        if spy_open is None or spy_open == 0:
            continue

        vix = vix_data.get(trade_date, 18.0)

        gap_pct = 0
        if prev_spy_close and prev_spy_close > 0:
            gap_pct = abs((spy_open - prev_spy_close) / prev_spy_close * 100)

        exp_move = spy_open * (vix / 100) / math.sqrt(252)

        # Get SPY close
        spy_ticker_bars = day_bars_cache  # we'll handle this differently
        prev_spy_close = spy_open  # simplified

        # Skip conditions
        if vix > vix_max or vix < vix_min:
            continue
        if gap_pct > gap_max and day_idx > 0:
            continue
        if exp_move < 1.5:
            continue

        # Strikes
        put_short = round(spy_open - exp_move * strike_pct)
        call_short = round(spy_open + exp_move * strike_pct)

        if vix < 18:
            put_width, call_width = 9, 7
        elif vix < 25:
            put_width, call_width = 10, 8
        else:
            put_width, call_width = 11, 9

        put_long = put_short - put_width
        call_long = call_short + call_width

        # Position sizing
        max_risk = balance * 0.05
        max_loss_per = max(put_width, call_width) * 100
        contracts = max(1, int(max_risk / max_loss_per))

        day_pnl = 0

        # Get option data from cache
        for side_short, side_long, width, opt_type in [
            (put_short, put_long, put_width, "put"),
            (call_short, call_long, call_width, "call")
        ]:
            short_ticker = build_option_ticker(trade_date, side_short, opt_type)
            long_ticker = build_option_ticker(trade_date, side_long, opt_type)

            short_bars = day_bars_cache.get(short_ticker, [])
            long_bars = day_bars_cache.get(long_ticker, [])

            short_entry = get_entry_price(short_bars)
            long_entry = get_entry_price(long_bars)

            if short_entry and long_entry and short_entry > long_entry:
                credit = round(short_entry - long_entry, 2)
                if credit > 0:
                    pnl, _ = simulate_spread(
                        short_bars, long_bars, credit, width,
                        stop_mult, profit_take, exit_minute
                    )
                    day_pnl += pnl * contracts
            else:
                # Try nearby strikes
                for offset in [1, -1, 2, -2]:
                    alt_short = side_short + (offset if opt_type == "put" else -offset)
                    alt_long = alt_short - width if opt_type == "put" else alt_short + width
                    alt_short_ticker = build_option_ticker(trade_date, alt_short, opt_type)
                    alt_long_ticker = build_option_ticker(trade_date, alt_long, opt_type)
                    alt_s_bars = day_bars_cache.get(alt_short_ticker, [])
                    alt_l_bars = day_bars_cache.get(alt_long_ticker, [])
                    s_entry = get_entry_price(alt_s_bars)
                    l_entry = get_entry_price(alt_l_bars)
                    if s_entry and l_entry and s_entry > l_entry:
                        credit = round(s_entry - l_entry, 2)
                        if credit > 0:
                            pnl, _ = simulate_spread(
                                alt_s_bars, alt_l_bars, credit, width,
                                stop_mult, profit_take, exit_minute
                            )
                            day_pnl += pnl * contracts
                            break

        balance += day_pnl
        total_pnl += day_pnl

        if day_pnl > 0:
            wins += 1
        elif day_pnl < 0:
            losses += 1

        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

        if balance <= 0:
            break

    traded = wins + losses
    win_rate = wins / traded * 100 if traded > 0 else 0
    avg_win = total_pnl / wins if wins > 0 else 0  # simplified
    profit_factor = 0
    if losses > 0 and wins > 0:
        # approximate
        avg_w = total_pnl / wins if total_pnl > 0 else 0
        profit_factor = abs(wins * avg_w / (losses * (total_pnl - wins * avg_w))) if (total_pnl - wins * avg_w) != 0 else 99

    # Sharpe-like score: return / drawdown
    score = (total_pnl / 300 * 100) / max(max_dd, 1)

    return {
        "balance": round(balance, 2),
        "total_pnl": round(total_pnl, 2),
        "return_pct": round(total_pnl / 300 * 100, 1),
        "win_rate": round(win_rate, 1),
        "wins": wins,
        "losses": losses,
        "traded": traded,
        "max_dd": round(max_dd, 1),
        "score": round(score, 2),  # return/drawdown ratio
        "params": params,
    }


def optimize():
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")

    # Load VIX
    vix_data = {}
    for row in db.execute("SELECT date, close FROM vix_daily"):
        vix_data[row[0]] = row[1]

    # Load all trading days
    pull_log = db.execute(
        "SELECT date, spy_open FROM pull_log WHERE options_done=1 ORDER BY date"
    ).fetchall()

    print(f"Loading {len(pull_log)} days of option data into memory...")
    days = []
    for i, (date, spy_open) in enumerate(pull_log):
        day_bars = preload_day_data(db, date)
        days.append((date, spy_open, day_bars))
        if (i + 1) % 25 == 0:
            print(f"  Loaded {i+1}/{len(pull_log)} days...")

    # Also get SPY closes for gap calculation
    for i, (date, spy_open, day_bars) in enumerate(days):
        spy_bars_raw = db.execute(
            "SELECT close FROM spy_bars WHERE date=? ORDER BY timestamp DESC LIMIT 1",
            (date,)
        ).fetchone()
        # Store in a simple way - we'll handle gap in the sweep

    print(f"\nData loaded! Starting parameter sweep...\n")

    # Parameter grid
    param_grid = {
        "strike_pct": [0.60, 0.70, 0.75, 0.80, 0.85],
        "stop_mult": [1.5, 2.0, 2.5, 3.0],
        "profit_take": [0.50, 0.65, 0.80, 1.0],  # 1.0 = no early take
        "exit_hour_min": [(11, 0), (11, 30), (12, 0), (12, 30), (13, 0)],
        "vix_max": [28, 30, 35],
        "vix_min": [10, 12, 14],
        "gap_max": [0.8, 1.0, 1.5],
    }

    # Calculate total combinations
    total = (len(param_grid["strike_pct"]) * len(param_grid["stop_mult"]) *
             len(param_grid["profit_take"]) * len(param_grid["exit_hour_min"]) *
             len(param_grid["vix_max"]) * len(param_grid["vix_min"]) *
             len(param_grid["gap_max"]))
    print(f"Total combinations: {total}")
    print(f"{'='*70}\n")

    results = []
    best_score = -999
    best_result = None
    count = 0

    for strike_pct in param_grid["strike_pct"]:
        for stop_mult in param_grid["stop_mult"]:
            for profit_take in param_grid["profit_take"]:
                for exit_h, exit_m in param_grid["exit_hour_min"]:
                    for vix_max in param_grid["vix_max"]:
                        for vix_min in param_grid["vix_min"]:
                            for gap_max in param_grid["gap_max"]:
                                count += 1
                                params = {
                                    "strike_pct": strike_pct,
                                    "stop_mult": stop_mult,
                                    "profit_take": profit_take,
                                    "exit_hour": exit_h,
                                    "exit_min": exit_m,
                                    "vix_max": vix_max,
                                    "vix_min": vix_min,
                                    "gap_max": gap_max,
                                }

                                result = run_sweep(db, days, vix_data, params)
                                results.append(result)

                                if result["score"] > best_score and result["traded"] > 100:
                                    best_score = result["score"]
                                    best_result = result
                                    print(f"  NEW BEST #{count}/{total}: "
                                          f"${result['balance']:,.0f} ({result['return_pct']:+.0f}%) | "
                                          f"WR:{result['win_rate']:.0f}% | DD:{result['max_dd']:.0f}% | "
                                          f"Score:{result['score']:.1f} | "
                                          f"strike:{strike_pct} stop:{stop_mult} "
                                          f"take:{profit_take} exit:{exit_h}:{exit_m:02d} "
                                          f"vix:{vix_min}-{vix_max} gap:{gap_max}")

                                if count % 500 == 0:
                                    print(f"  Progress: {count}/{total} ({count/total*100:.0f}%)")

    # Sort by score
    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n{'='*70}")
    print(f"OPTIMIZATION COMPLETE — {total} combinations tested")
    print(f"{'='*70}\n")

    print(f"TOP 10 CONFIGURATIONS:")
    print(f"{'─'*70}")
    for i, r in enumerate(results[:10]):
        p = r["params"]
        print(f"  #{i+1}: ${r['balance']:>8,.0f} ({r['return_pct']:>+6.0f}%) | "
              f"WR:{r['win_rate']:>4.0f}% | DD:{r['max_dd']:>4.0f}% | "
              f"Score:{r['score']:>5.1f} | "
              f"T:{r['traded']} | "
              f"strike:{p['strike_pct']} stop:{p['stop_mult']} "
              f"take:{p['profit_take']} exit:{p['exit_hour']}:{p['exit_min']:02d} "
              f"vix:{p['vix_min']}-{p['vix_max']} gap:{p['gap_max']}")

    print(f"\n{'─'*70}")
    print(f"\nBEST OVERALL (Score = Return%/MaxDD%):")
    if best_result:
        p = best_result["params"]
        print(f"  Balance:    ${best_result['balance']:,.2f}")
        print(f"  Return:     {best_result['return_pct']:+.1f}%")
        print(f"  Win Rate:   {best_result['win_rate']:.1f}% ({best_result['wins']}W/{best_result['losses']}L)")
        print(f"  Max DD:     {best_result['max_dd']:.1f}%")
        print(f"  Score:      {best_result['score']:.2f}")
        print(f"  Strike:     {p['strike_pct']*100:.0f}% of expected move")
        print(f"  Stop:       {p['stop_mult']}x credit")
        print(f"  Take Profit:{p['profit_take']*100:.0f}% of max")
        print(f"  Exit Time:  {p['exit_hour']}:{p['exit_min']:02d}")
        print(f"  VIX Range:  {p['vix_min']}-{p['vix_max']}")
        print(f"  Max Gap:    {p['gap_max']}%")

    # Save results
    save_data = {
        "best": best_result,
        "top_10": results[:10],
        "total_tested": total,
        "run_at": datetime.now().isoformat(),
    }
    save_path = DATA_DIR / "optimization_results.json"
    save_path.write_text(json.dumps(save_data, indent=2, default=str))
    print(f"\nResults saved to: {save_path}")

    db.close()
    return results


if __name__ == "__main__":
    optimize()

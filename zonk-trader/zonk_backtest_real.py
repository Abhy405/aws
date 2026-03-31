#!/usr/bin/env python3
"""
ZonkTrader 0DTE SPY Credit Spread Backtester — REAL DATA ENGINE

Uses actual 1-min option OHLCV bars from Polygon.io database.
Replicates ZonkTrader's exact strategy:

Strategy Rules (from u/ZonkTrader's actual Reddit posts):
- Sell 0DTE SPY credit spreads (both put + call sides = iron condor)
- Entry: 9:31 AM ET (market open + 1 min)
- Exit: by 1:00 PM ET (out 2 hours before close)
- Strikes: 75% of expected move (VIX-derived), "moderately low deltas"
- Put spread widths: 9-11 wide (adaptive to VIX)
- Call spread widths: 6-9 wide (adaptive to VIX)
- Skip: VIX > 30, VIX < 12, gap > 1%, expected move < $1.50
- No fixed stop loss — he averages in, takes profit early
- For backtest: use 2x credit as stop-out level (practical risk mgmt)

Data: Real 1-min OHLCV from Polygon.io SQLite database
"""

import sqlite3
import json
import math
import csv
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = Path("/home/abhijay/trading-strategies/data/options_data.db")
DATA_DIR = Path("/home/abhijay/trading-strategies/data")

ET = timezone(timedelta(hours=-4))  # Eastern Time (simplified, ignores DST)
# Note: During EST (Nov-Mar) it's UTC-5, during EDT (Mar-Nov) it's UTC-4
# Most trading days are EDT. For accuracy we'd need pytz but this is close enough.


def ts_to_et(ts_ms):
    """Convert millisecond timestamp to ET datetime."""
    # Try EDT first (UTC-4), then check if it's EST period
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    month = dt.month
    # Rough DST: EDT Mar-Nov, EST Nov-Mar
    if month >= 3 and month < 11:
        return dt.astimezone(timezone(timedelta(hours=-4)))
    else:
        return dt.astimezone(timezone(timedelta(hours=-5)))


def get_bar_at_time(bars, target_hour, target_min, tolerance_min=5):
    """Find the bar closest to a target time. bars = list of (timestamp, o, h, l, c, v)."""
    if not bars:
        return None
    best = None
    best_diff = float('inf')
    for bar in bars:
        dt = ts_to_et(bar[0])
        target_minutes = target_hour * 60 + target_min
        bar_minutes = dt.hour * 60 + dt.minute
        diff = abs(bar_minutes - target_minutes)
        if diff < best_diff:
            best_diff = diff
            best = bar
    if best_diff > tolerance_min:
        return None
    return best


def get_bars_in_range(bars, start_hour, start_min, end_hour, end_min):
    """Filter bars within a time range (inclusive)."""
    result = []
    start_minutes = start_hour * 60 + start_min
    end_minutes = end_hour * 60 + end_min
    for bar in bars:
        dt = ts_to_et(bar[0])
        bar_minutes = dt.hour * 60 + dt.minute
        if start_minutes <= bar_minutes <= end_minutes:
            result.append(bar)
    return result


def find_option_entry_price(db, date, strike, opt_type, entry_start=9, entry_start_min=31,
                            entry_end=9, entry_end_min=45):
    """
    Find the entry price for an option near market open.
    Returns the first available price in the entry window, or None.
    """
    ticker_prefix = build_option_ticker(date, strike, opt_type)
    rows = db.execute(
        "SELECT timestamp, open, high, low, close, volume FROM option_bars "
        "WHERE date=? AND ticker=? ORDER BY timestamp",
        (date, ticker_prefix)
    ).fetchall()

    if not rows:
        return None, None

    # Find bars in the entry window
    entry_bars = get_bars_in_range(rows, entry_start, entry_start_min, entry_end, entry_end_min)
    if entry_bars:
        # Use the first available bar's mid price (open)
        return entry_bars[0][1], rows  # entry_price, all_bars

    # If no bars in entry window, try first bar of the day after 9:30
    morning_bars = get_bars_in_range(rows, 9, 30, 10, 0)
    if morning_bars:
        return morning_bars[0][1], rows

    return None, rows


def build_option_ticker(date, strike, opt_type):
    """Build Polygon option ticker."""
    dt = datetime.strptime(date, "%Y-%m-%d")
    date_str = dt.strftime("%y%m%d")
    type_char = "C" if opt_type == "call" else "P"
    strike_int = int(strike * 1000)
    return f"O:SPY{date_str}{type_char}{strike_int:08d}"


def get_spread_credit(db, date, short_strike, long_strike, opt_type):
    """
    Calculate the net credit for a credit spread.
    Credit = short option price - long option price (at entry).
    Returns (credit, short_bars, long_bars) or (None, None, None).
    """
    short_price, short_bars = find_option_entry_price(db, date, short_strike, opt_type)
    long_price, long_bars = find_option_entry_price(db, date, long_strike, opt_type)

    if short_price is None or long_price is None:
        return None, short_bars, long_bars

    credit = round(short_price - long_price, 2)
    if credit <= 0:
        return None, short_bars, long_bars

    return credit, short_bars, long_bars


def simulate_spread_intraday(short_bars, long_bars, credit, spread_width,
                              stop_multiplier=2.0):
    """
    Simulate intraday P&L of a credit spread using real option prices.

    Monitor from entry to 1:00 PM ET:
    - If spread value exceeds stop_multiplier * credit → stop out
    - At exit (1 PM), close at current spread value
    - P&L = credit - exit_spread_value (per contract, per share)

    Returns (pnl_per_contract, exit_reason, exit_time)
    """
    if not short_bars or not long_bars:
        return credit * 100, "no_data_full_credit", None  # assume max profit if no data

    # Build time-aligned spread values from 9:31 to 13:00
    # Create minute-level price maps
    short_prices = {}
    for bar in short_bars:
        dt = ts_to_et(bar[0])
        key = dt.hour * 60 + dt.minute
        short_prices[key] = bar  # (ts, o, h, l, c, v)

    long_prices = {}
    for bar in long_bars:
        dt = ts_to_et(bar[0])
        key = dt.hour * 60 + dt.minute
        long_prices[key] = bar

    # Monitor from 9:31 to 13:00
    entry_min = 9 * 60 + 31
    exit_min = 13 * 60 + 0
    stop_level = credit * stop_multiplier

    last_spread_value = 0
    exit_reason = "expire_worthless"

    for minute in range(entry_min, exit_min + 1):
        short_bar = short_prices.get(minute)
        long_bar = long_prices.get(minute)

        if short_bar and long_bar:
            # Current spread value = short_price - long_price
            # For monitoring, use the worst case: short high - long low (max spread cost)
            spread_worst = short_bar[2] - long_bar[3]  # short high - long low
            spread_mid = short_bar[4] - long_bar[4]    # short close - long close

            last_spread_value = max(0, spread_mid)

            # Check stop-out using worst-case intrabar price
            if spread_worst > 0 and spread_worst >= stop_level:
                # Stop out at stop level
                pnl = (credit - stop_level) * 100
                dt = ts_to_et(short_bar[0])
                return pnl, "stop_out", dt.strftime("%H:%M")

        elif short_bar:
            last_spread_value = short_bar[4]  # just short price if no long data

    # Exit at 1 PM (or last known spread value)
    # Find exit prices near 1 PM
    exit_spread = last_spread_value

    # Try to get actual 1 PM prices
    for check_min in range(exit_min, exit_min - 10, -1):
        s = short_prices.get(check_min)
        l = long_prices.get(check_min)
        if s and l:
            exit_spread = max(0, s[4] - l[4])
            break

    pnl = (credit - exit_spread) * 100
    if exit_spread <= 0.05:
        exit_reason = "expire_worthless"
    else:
        exit_reason = "exit_1pm"

    return pnl, exit_reason, "13:00"


def run_backtest(starting_balance=300, risk_pct=0.05, stop_multiplier=2.0):
    """Run the full ZonkTrader backtest with real options data."""
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")

    # Get all completed trading days
    days = db.execute(
        "SELECT date, spy_open FROM pull_log WHERE options_done=1 ORDER BY date"
    ).fetchall()

    # Load VIX data
    vix_data = {}
    for row in db.execute("SELECT date, close FROM vix_daily"):
        vix_data[row[0]] = row[1]

    print(f"ZonkTrader 0DTE Backtester — REAL OPTIONS DATA")
    print(f"{'='*65}")
    print(f"Database: {DB_PATH} ({DB_PATH.stat().st_size/1e6:.0f}MB)")
    print(f"Trading days: {len(days)}")
    print(f"Starting balance: ${starting_balance:,.2f}")
    print(f"Risk per trade: {risk_pct*100}%")
    print(f"Stop multiplier: {stop_multiplier}x credit")
    print(f"{'='*65}\n")

    balance = starting_balance
    results = []
    prev_spy_close = None

    for day_idx, (trade_date, spy_open) in enumerate(days):
        if spy_open is None or spy_open == 0:
            continue

        # Get VIX
        vix = vix_data.get(trade_date, 18.0)

        # Calculate gap
        gap_pct = 0
        if prev_spy_close and prev_spy_close > 0:
            gap_pct = abs((spy_open - prev_spy_close) / prev_spy_close * 100)

        # Expected move
        exp_move = spy_open * (vix / 100) / math.sqrt(252)

        # Get SPY close for this day (for next day's gap calc)
        spy_bars = db.execute(
            "SELECT timestamp, open, high, low, close, volume FROM spy_bars "
            "WHERE date=? ORDER BY timestamp DESC LIMIT 1",
            (trade_date,)
        ).fetchone()
        if spy_bars:
            prev_spy_close = spy_bars[4]  # close of last bar
        else:
            prev_spy_close = spy_open

        # Skip conditions (ZonkTrader's rules)
        skip_reason = None
        if vix > 30:
            skip_reason = f"VIX {vix:.1f} > 30"
        elif vix < 12:
            skip_reason = f"VIX {vix:.1f} < 12"
        elif gap_pct > 1.0 and day_idx > 0:
            skip_reason = f"Gap {gap_pct:.1f}% > 1%"
        elif exp_move < 1.5:
            skip_reason = f"ExpMove ${exp_move:.2f} too small"

        if skip_reason:
            results.append({
                "date": trade_date, "skip": True, "reason": skip_reason,
                "spy_open": spy_open, "vix": round(vix, 1),
                "pnl": 0, "balance": balance
            })
            print(f"  {trade_date} | SKIP: {skip_reason}")
            continue

        # Calculate strikes (75% of expected move)
        put_short = round(spy_open - exp_move * 0.75)
        call_short = round(spy_open + exp_move * 0.75)

        # Adaptive spread widths based on VIX
        if vix < 18:
            put_width, call_width = 9, 7
        elif vix < 25:
            put_width, call_width = 10, 8
        else:
            put_width, call_width = 11, 9

        put_long = put_short - put_width
        call_long = call_short + call_width

        # Position sizing (ZonkTrader: floor(balance * 5% / $1000))
        # For small accounts: max risk = balance * risk_pct
        max_risk = balance * risk_pct
        # Max loss per contract = spread_width * 100
        max_loss_per = max(put_width, call_width) * 100
        contracts = max(1, int(max_risk / max_loss_per))

        # Get real credit for PUT spread
        put_credit, put_short_bars, put_long_bars = get_spread_credit(
            db, trade_date, put_short, put_long, "put"
        )

        # Get real credit for CALL spread
        call_credit, call_short_bars, call_long_bars = get_spread_credit(
            db, trade_date, call_short, call_long, "call"
        )

        day_pnl = 0
        put_result = "no_data"
        call_result = "no_data"
        put_pnl = 0
        call_pnl = 0

        # Simulate PUT spread
        if put_credit and put_credit > 0:
            put_pnl, put_result, put_exit_time = simulate_spread_intraday(
                put_short_bars, put_long_bars, put_credit, put_width, stop_multiplier
            )
            day_pnl += put_pnl * contracts
        elif put_credit is None:
            # No data for these strikes — try nearby strikes
            for offset in [1, -1, 2, -2]:
                alt_short = put_short + offset
                alt_long = alt_short - put_width
                put_credit, put_short_bars, put_long_bars = get_spread_credit(
                    db, trade_date, alt_short, alt_long, "put"
                )
                if put_credit and put_credit > 0:
                    put_pnl, put_result, _ = simulate_spread_intraday(
                        put_short_bars, put_long_bars, put_credit, put_width, stop_multiplier
                    )
                    day_pnl += put_pnl * contracts
                    put_short = alt_short
                    put_long = alt_long
                    break

        # Simulate CALL spread
        if call_credit and call_credit > 0:
            call_pnl, call_result, call_exit_time = simulate_spread_intraday(
                call_short_bars, call_long_bars, call_credit, call_width, stop_multiplier
            )
            day_pnl += call_pnl * contracts
        elif call_credit is None:
            for offset in [-1, 1, -2, 2]:
                alt_short = call_short + offset
                alt_long = alt_short + call_width
                call_credit, call_short_bars, call_long_bars = get_spread_credit(
                    db, trade_date, alt_short, alt_long, "call"
                )
                if call_credit and call_credit > 0:
                    call_pnl, call_result, _ = simulate_spread_intraday(
                        call_short_bars, call_long_bars, call_credit, call_width, stop_multiplier
                    )
                    day_pnl += call_pnl * contracts
                    call_short = alt_short
                    call_long = alt_long
                    break

        day_pnl = round(day_pnl, 2)
        balance = round(balance + day_pnl, 2)

        result = {
            "date": trade_date,
            "skip": False,
            "spy_open": round(spy_open, 2),
            "vix": round(vix, 1),
            "exp_move": round(exp_move, 2),
            "gap_pct": round(gap_pct, 2),
            "put_spread": f"{put_short}/{put_long}",
            "call_spread": f"{call_short}/{call_long}",
            "put_credit": put_credit or 0,
            "call_credit": call_credit or 0,
            "put_result": put_result,
            "call_result": call_result,
            "contracts": contracts,
            "pnl": day_pnl,
            "balance": balance,
            "reason": None,
        }
        results.append(result)

        status = "WIN" if day_pnl > 0 else "LOSS" if day_pnl < 0 else "FLAT"
        put_info = f"P:{put_credit or 0:.2f}({put_result})" if put_credit else "P:--"
        call_info = f"C:{call_credit or 0:.2f}({call_result})" if call_credit else "C:--"
        print(f"  {trade_date} | {status:4s} ${day_pnl:+8,.2f} | "
              f"{put_info} {call_info} | "
              f"x{contracts} | Bal: ${balance:,.2f}")

        # Progress every 20 days
        if (day_idx + 1) % 20 == 0:
            traded_so_far = [r for r in results if not r["skip"]]
            wins_so_far = sum(1 for r in traded_so_far if r["pnl"] > 0)
            wr = wins_so_far / len(traded_so_far) * 100 if traded_so_far else 0
            print(f"\n  --- Day {day_idx+1}/{len(days)} | "
                  f"Win rate: {wr:.0f}% | Balance: ${balance:,.2f} ---\n")

    # ─── Summary ────────────────────────────────────────────────
    traded = [r for r in results if not r["skip"]]
    wins = [r for r in traded if r["pnl"] > 0]
    losses = [r for r in traded if r["pnl"] < 0]
    flat = [r for r in traded if r["pnl"] == 0]
    skips = [r for r in results if r["skip"]]

    total_pnl = sum(r["pnl"] for r in traded)
    avg_win = sum(r["pnl"] for r in wins) / len(wins) if wins else 0
    avg_loss = sum(r["pnl"] for r in losses) / len(losses) if losses else 0

    # Max drawdown
    max_dd = 0
    peak = starting_balance
    for r in results:
        if r["balance"] > peak:
            peak = r["balance"]
        dd = (peak - r["balance"]) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Streak tracking
    max_win_streak = 0
    max_loss_streak = 0
    current_streak = 0
    streak_type = None
    for r in traded:
        if r["pnl"] > 0:
            if streak_type == "win":
                current_streak += 1
            else:
                current_streak = 1
                streak_type = "win"
            max_win_streak = max(max_win_streak, current_streak)
        elif r["pnl"] < 0:
            if streak_type == "loss":
                current_streak += 1
            else:
                current_streak = 1
                streak_type = "loss"
            max_loss_streak = max(max_loss_streak, current_streak)

    # Exit reason breakdown
    stop_outs = sum(1 for r in traded if r.get("put_result") == "stop_out" or r.get("call_result") == "stop_out")

    print(f"\n{'='*65}")
    print(f"BACKTEST RESULTS — REAL OPTIONS DATA")
    print(f"{'='*65}")
    print(f"Period:           {days[0][0]} to {days[-1][0]}")
    print(f"Starting Balance: ${starting_balance:,.2f}")
    print(f"Final Balance:    ${balance:,.2f}")
    print(f"Total P&L:        ${total_pnl:+,.2f} ({total_pnl/starting_balance*100:+.1f}%)")
    print(f"{'─'*65}")
    print(f"Total Days:       {len(results)}")
    print(f"Traded Days:      {len(traded)}")
    print(f"Skip Days:        {len(skips)}")
    print(f"Flat Days:        {len(flat)}")
    print(f"{'─'*65}")
    if traded:
        print(f"Win Rate:         {len(wins)}/{len(traded)} ({len(wins)/len(traded)*100:.1f}%)")
        print(f"Avg Win:          ${avg_win:+,.2f}")
        print(f"Avg Loss:         ${avg_loss:+,.2f}")
        if losses:
            print(f"Profit Factor:    {abs(sum(r['pnl'] for r in wins) / sum(r['pnl'] for r in losses)):.2f}")
        print(f"Best Day:         ${max(r['pnl'] for r in traded):+,.2f}")
        print(f"Worst Day:        ${min(r['pnl'] for r in traded):+,.2f}")
        print(f"Max Win Streak:   {max_win_streak}")
        print(f"Max Loss Streak:  {max_loss_streak}")
        print(f"Stop-Outs:        {stop_outs} ({stop_outs/len(traded)*100:.0f}% of trades)")
    print(f"Max Drawdown:     {max_dd:.1f}%")
    print(f"{'='*65}")

    # Save results
    csv_path = DATA_DIR / "backtest_real_results.csv"
    if results:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"\nResults saved to: {csv_path}")

    # Save summary
    summary = {
        "engine": "real_polygon_data",
        "starting_balance": starting_balance,
        "final_balance": balance,
        "total_pnl": total_pnl,
        "return_pct": total_pnl / starting_balance * 100,
        "period": f"{days[0][0]} to {days[-1][0]}",
        "total_days": len(results),
        "traded_days": len(traded),
        "skip_days": len(skips),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(traded) * 100 if traded else 0,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "max_drawdown_pct": max_dd,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "stop_outs": stop_outs,
        "config": {
            "risk_pct": risk_pct,
            "stop_multiplier": stop_multiplier,
            "strategy": "ZonkTrader 0DTE credit spreads",
            "data_source": "Polygon.io 1-min OHLCV",
        },
        "run_at": datetime.now().isoformat(),
    }
    summary_path = DATA_DIR / "backtest_real_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"Summary saved to: {summary_path}")

    db.close()
    return results, summary


if __name__ == "__main__":
    balance = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    risk = float(sys.argv[2]) if len(sys.argv) > 2 else 0.05
    stop = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
    run_backtest(balance, risk, stop)

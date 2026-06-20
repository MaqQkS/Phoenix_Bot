"""
scripts/verify_audit4_fixes.py — Post-Audit-4 sanity checks.

Read-only. Safe to run with the bot running (uses sqlite file:?mode=ro URI).

Checks:
  1. block_time column populating on new pumpswap_fees inserts (last 1hr)
  2. Buy/sell event balance (last 1hr) — should near 1:1 post-migration
  3. tier_index populating (no -1 values) in fee_gate_log (last 1hr)
  4. Recent "Price/alert loop error" / "ValueError" lines in bot.log

Run:
    python scripts/verify_audit4_fixes.py
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = "data/bot.db"
LOG_PATH = "bot.log"


def _ro_conn(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0)


def check_block_time(conn: sqlite3.Connection) -> tuple[bool, str]:
    print("=== block_time population (last 1hr) ===")
    row = conn.execute("""
        SELECT COUNT(*) as total, COUNT(block_time) as non_null
        FROM pumpswap_fees
        WHERE received_at > (strftime('%s','now') - 3600)
    """).fetchone()
    total, non_null = row[0], row[1]
    print(f"  total rows (last 1hr):     {total}")
    print(f"  rows with block_time set:  {non_null}")
    if total == 0:
        msg = "NO DATA — no rows in the last hour (bot idle or just restarted)"
        print(f"  ⚠️  {msg}")
        return False, msg
    pct = 100.0 * non_null / total
    print(f"  populated:                 {pct:.1f}%")
    if non_null == total:
        print("  ✅ block_time populated on every recent row")
        return True, f"{non_null}/{total} (100%)"
    if pct >= 99.0:
        print(f"  ✅ block_time populated on {pct:.1f}% of recent rows (acceptable)")
        return True, f"{non_null}/{total} ({pct:.1f}%)"
    print(f"  ❌ block_time missing on {total - non_null} rows ({100-pct:.1f}%)")
    return False, f"only {non_null}/{total} populated"


def check_buy_sell_balance(conn: sqlite3.Connection) -> tuple[bool, str]:
    print()
    print("=== Buy/Sell balance (last 1hr) ===")
    rows = conn.execute("""
        SELECT event_type, COUNT(*)
        FROM pumpswap_fees
        WHERE received_at > (strftime('%s','now') - 3600)
        GROUP BY event_type
    """).fetchall()
    if not rows:
        print("  ⚠️  NO DATA — no rows in the last hour")
        return False, "no data"
    counts = {et: c for et, c in rows}
    buy = counts.get("Buy", 0)
    sell = counts.get("Sell", 0)
    for et, c in rows:
        print(f"  {et:10s} {c}")
    if buy == 0 or sell == 0:
        msg = f"one-sided (buy={buy}, sell={sell}) — low sample or pre-migration"
        print(f"  ⚠️  {msg}")
        return False, msg
    ratio = sell / buy
    print(f"  sell/buy ratio: {ratio:.2f}")
    if 0.8 <= ratio <= 1.25:
        print("  ✅ ratio within healthy 1:1 band")
        return True, f"sell/buy = {ratio:.2f}"
    print(f"  ⚠️  ratio {ratio:.2f} outside 0.8-1.25 band "
          f"(may be residual pre-migration data — wait 1hr post-migration)")
    return False, f"sell/buy = {ratio:.2f} (out of band)"


def check_tier_index(conn: sqlite3.Connection) -> tuple[bool, str]:
    print()
    print("=== Tier index in fee_gate_log (last 1hr) ===")
    rows = conn.execute("""
        SELECT alert_tier, COUNT(*)
        FROM fee_gate_log
        WHERE alert_time > (strftime('%s','now') - 3600)
        GROUP BY alert_tier
    """).fetchall()
    if not rows:
        print("  ⚠️  NO DATA — no fee_gate_log rows in the last hour (no alerts fired)")
        return False, "no alerts in window"
    has_minus_one = False
    for tier, c in rows:
        marker = " ← BAD" if tier == -1 else ""
        print(f"  tier={tier:>3}  count={c}{marker}")
        if tier == -1:
            has_minus_one = True
    if has_minus_one:
        print("  ❌ tier -1 present — hotfix not effective")
        return False, "tier -1 observed"
    print("  ✅ no tier -1 values observed")
    tiers_seen = sorted({t for t, _ in rows})
    return True, f"tiers seen: {tiers_seen}"


def check_bot_log(log_path: str = LOG_PATH) -> tuple[bool, str]:
    print()
    print("=== bot.log: 'Price/alert loop error' / 'ValueError' ===")
    log_file = Path(log_path)
    if not log_file.exists():
        print(f"  ⚠️  {log_path} not found")
        return False, "log missing"
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError as e:
        print(f"  ⚠️  could not read log: {e}")
        return False, str(e)
    tail = lines[-5000:] if len(lines) > 5000 else lines
    needles = ("Price/alert loop error", "ValueError")
    matches = [ln.rstrip("\n") for ln in tail if any(n in ln for n in needles)]
    # Keep only the last 50
    matches = matches[-50:]
    if not matches:
        print("  ✅ No recent alert loop errors")
        return True, "no matches"
    print(f"  ❌ {len(matches)} recent match(es):")
    for ln in matches:
        print(f"     {ln}")
    return False, f"{len(matches)} error line(s) in recent log"


def main():
    db_file = Path(DB_PATH)
    if not db_file.exists():
        print(f"[!] DB not found at {DB_PATH}")
        sys.exit(1)

    results = []
    conn = _ro_conn(DB_PATH)
    try:
        results.append(("block_time populating",   check_block_time(conn)))
        results.append(("buy/sell balance",        check_buy_sell_balance(conn)))
        results.append(("tier_index populating",   check_tier_index(conn)))
    finally:
        conn.close()
    results.append(("bot.log clean",               check_bot_log()))

    # ── Summary ─────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    width = max(len(name) for name, _ in results)
    for name, (ok, detail) in results:
        status = "✅ PASS" if ok else "❌ FAIL"
        print(f"  {name:<{width}}  {status}  — {detail}")
    print()
    any_fail = any(not ok for _, (ok, _) in results)
    if any_fail:
        print("Some checks did not pass. Review output above.")
        print("Note: checks 1-3 need ~1hr of post-migration runtime to be meaningful.")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()

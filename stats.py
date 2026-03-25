"""
alerted_tokens.py — Show all tokens that triggered at least one dip alert.
Run: python alerted_tokens.py
"""

import sqlite3
import os

DB_PATH = "data/bot.db"

# Tier names matching config order (index 0, 1, 2)
TIER_NAMES = ["Tier 1", "Tier 2", "Tier 3"]


def fmt_mcap(v: float) -> str:
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v / 1_000:.0f}k"
    else:
        return f"${v:.0f}"


def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT symbol, current_mcap, ath_mcap, ath_price, current_price, last_alerted_tier, address
        FROM tokens
        WHERE last_alerted_tier >= 0
        ORDER BY last_alerted_tier DESC, symbol ASC
    """).fetchall()

    conn.close()

    if not rows:
        print("No tokens have been alerted yet.")
        return

    # Stats
    total = len(rows)
    all_three = sum(1 for r in rows if r["last_alerted_tier"] >= 2)

    # Header
    print()
    print("═" * 72)
    print("  ALERTED TOKENS SUMMARY")
    print("═" * 72)
    print()
    print(f" {'Token':<16}│ {'MC Now':<10}│ {'ATH':<10}│ {'Drop':<8}│ Tiers Hit")
    print(f" {'─' * 16}┼{'─' * 10}┼{'─' * 10}┼{'─' * 8}┼{'─' * 22}")

    for row in rows:
        symbol = f"${row['symbol']}"
        mc_now = fmt_mcap(row["current_mcap"] or 0)
        ath = fmt_mcap(row["ath_mcap"] or 0)

        # Calculate drop
        ath_price = row["ath_price"] or 0
        cur_price = row["current_price"] or 0
        if ath_price > 0:
            drop = (1 - cur_price / ath_price) * 100
            drop_str = f"-{drop:.0f}%"
        else:
            drop_str = "N/A"

        # Build tiers hit list (progressive: if last_alerted_tier=2, they hit 0,1,2)
        last_tier = row["last_alerted_tier"]
        tiers_hit = ", ".join(TIER_NAMES[i] for i in range(last_tier + 1))

        ca = row["address"]
        print(f" {symbol:<16}│ {mc_now:<9}│ {ath:<9}│ {drop_str:<7}│ {tiers_hit}")
        print(f"   └─ CA: {ca}")

    # Footer
    print()
    print("═" * 72)
    print(f" Total tokens alerted: {total}")
    print(f" Tokens hitting all 3 tiers: {all_three}")
    print("═" * 72)
    print()


if __name__ == "__main__":
    main()
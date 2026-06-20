"""
export_fee_data.py — Export per-token fee gate metrics from pumpswap_fees table.

Outputs: fee_gate_data.csv with raw fee totals + computed gate metrics.
You manually add a 'label' column (scam/organic) in a separate CSV or directly.

Usage: python export_fee_data.py
"""
import sqlite3
import csv
import sys

DB_PATH = "data/bot.db"
OUT_PATH = "fee_gate_data.csv"
LAMPORTS_PER_SOL = 1_000_000_000

def main():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # ── Per-token aggregation ──
    rows = cur.execute("""
        SELECT
            token_address,
            pool_address,
            COUNT(*)                          AS event_count,
            SUM(CASE WHEN event_type='BUY'  THEN 1 ELSE 0 END) AS buy_count,
            SUM(CASE WHEN event_type='SELL' THEN 1 ELSE 0 END) AS sell_count,
            SUM(lp_fee)                       AS total_lp,
            SUM(protocol_fee)                 AS total_proto,
            SUM(COALESCE(creator_fee, 0))     AS total_creator,
            SUM(lp_fee + protocol_fee + COALESCE(creator_fee, 0)) AS total_fees,
            MIN(block_time)                   AS first_event,
            MAX(block_time)                   AS last_event
        FROM pumpswap_fees
        WHERE token_address IS NOT NULL
        GROUP BY token_address
        ORDER BY total_fees DESC
    """).fetchall()

    if not rows:
        print("No data in pumpswap_fees.")
        sys.exit(1)

    # ── Build output ──
    output = []
    for r in rows:
        total_fees = r["total_fees"] or 0
        total_lp = r["total_lp"] or 0
        total_proto = r["total_proto"] or 0
        total_creator = r["total_creator"] or 0
        event_count = r["event_count"] or 1
        first_t = r["first_event"]
        last_t = r["last_event"]

        # SOL conversions
        fees_sol = total_fees / LAMPORTS_PER_SOL
        lp_sol = total_lp / LAMPORTS_PER_SOL
        proto_sol = total_proto / LAMPORTS_PER_SOL
        creator_sol = total_creator / LAMPORTS_PER_SOL

        # Gate metrics
        creator_pct = (total_creator / total_fees * 100) if total_fees > 0 else 0.0
        proto_pct = (total_proto / total_fees * 100) if total_fees > 0 else 0.0
        lp_pct = (total_lp / total_fees * 100) if total_fees > 0 else 0.0
        fee_per_event_sol = fees_sol / event_count if event_count > 0 else 0.0
        lifespan_sec = (last_t - first_t) if (first_t and last_t) else 0

        # Flags
        flags = []
        if creator_pct == 0:
            flags.append("NO_CREATOR")
        if creator_pct > 50:
            flags.append("HIGH_CREATOR")
        if proto_pct > 70:
            flags.append("TIER1_RATIO")
        if event_count < 20:
            flags.append("LOW_EVENTS")
        if lifespan_sec > 0 and lifespan_sec < 300:
            flags.append("FAST_RUG")
        if fee_per_event_sol > 0.5:
            flags.append("HIGH_FEE_PER_EV")

        output.append({
            "token_address": r["token_address"],
            "pool_address": r["pool_address"],
            "event_count": event_count,
            "buys": r["buy_count"],
            "sells": r["sell_count"],
            "lp_sol": round(lp_sol, 6),
            "proto_sol": round(proto_sol, 6),
            "creator_sol": round(creator_sol, 6),
            "total_fees_sol": round(fees_sol, 6),
            "lp_pct": round(lp_pct, 2),
            "proto_pct": round(proto_pct, 2),
            "creator_pct": round(creator_pct, 2),
            "fee_per_event_sol": round(fee_per_event_sol, 6),
            "lifespan_sec": round(lifespan_sec, 1),
            "flags": "|".join(flags) if flags else "",
            "label": "",  # YOU FILL THIS IN: scam / organic
        })

    # ── Write CSV ──
    fieldnames = list(output[0].keys())
    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output)

    print(f"Exported {len(output)} tokens to {OUT_PATH}")
    print(f"\nTop 10 by total fees:")
    print(f"{'Token':<48s} {'Fees(SOL)':>10s} {'Evts':>6s} {'Cr%':>6s} {'Pr%':>6s} {'Fee/Ev':>8s} {'Flags'}")
    print("-" * 100)
    for row in output[:10]:
        print(f"{row['token_address']:<48s} {row['total_fees_sol']:>10.4f} {row['event_count']:>6d} "
              f"{row['creator_pct']:>5.1f}% {row['proto_pct']:>5.1f}% {row['fee_per_event_sol']:>8.6f} {row['flags']}")

    con.close()

if __name__ == "__main__":
    main()
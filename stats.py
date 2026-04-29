"""
stats.py — Show all tokens that triggered at least one dip alert.
Run: python stats.py
      python stats.py --perf         (daily recap)

build_daily_recap(conn) is the canonical plain-text daily recap builder
used by both the CLI (--perf) and the Telegram scheduled send (imported
from modules/telegram_sender.py). Returns plain text — Telegram wrapping
lives in the caller.
"""

import argparse
import sqlite3
import os
import time
from datetime import datetime, timezone

DB_PATH = "data/bot.db"

# Tier names matching config order (index 0, 1, 2)
TIER_NAMES = ["Tier 1", "Tier 2", "Tier 3"]

# Taxonomy buckets always rendered, even when zero
TAXONOMY_BUCKETS = [
    "ORGANIC", "COORDINATED", "BIMODAL",
    "WASH_UNIFORM", "AMBIGUOUS", "INSUFFICIENT",
]


def fmt_mcap(v: float) -> str:
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v / 1_000:.0f}k"
    else:
        return f"${v:.0f}"


def _get_fee_gate_labels(conn, token_addresses: set) -> dict:
    """Legacy helper — kept for back-compat; no longer used by the recap."""
    if not token_addresses:
        return {}
    try:
        placeholders = ",".join("?" for _ in token_addresses)
        rows = conn.execute(f"""
            SELECT token_address, MAX(score) as max_score
            FROM fee_gate_log
            WHERE token_address IN ({placeholders})
            GROUP BY token_address
        """, list(token_addresses)).fetchall()

        result = {}
        for row in rows:
            addr = row["token_address"]
            max_score = row["max_score"]
            label_row = conn.execute(
                "SELECT label FROM fee_gate_log WHERE token_address = ? AND score = ? LIMIT 1",
                (addr, max_score),
            ).fetchone()
            if label_row:
                result[addr] = label_row["label"]
        return result
    except Exception:
        return {}


def _has_columns(conn, table: str, columns: list[str]) -> bool:
    """Check if all given columns exist on table (via PRAGMA table_info)."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return False
    present = {row[1] for row in rows}
    return all(c in present for c in columns)


def _today_utc_label() -> str:
    """Calendar-day UTC heading like 'April 18' (no leading zero)."""
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%B')} {now.day}"


def build_daily_recap(conn: sqlite3.Connection) -> str:
    """Build the daily recap as plain text. No HTML, no Telegram markup.

    Date boundary: calendar-day UTC via DATE(..., 'unixepoch')=DATE('now').
    Sections with no data render their header with '—' so structure is stable.
    """
    conn.row_factory = sqlite3.Row
    lines: list[str] = []
    lines.append(f"📊 Phoenix Daily Recap — {_today_utc_label()}")
    lines.append("")

    # ═════════════════════════════════════════════════════════════════════
    # ALERT PERFORMANCE
    # ═════════════════════════════════════════════════════════════════════
    lines.append("━━━ ALERT PERFORMANCE ━━━")
    alerts = conn.execute("""
        SELECT address, symbol, tier_index, tier_name,
               alert_mcap, peak_mcap_after, alert_time
        FROM alerts
        WHERE DATE(alert_time,'unixepoch')=DATE('now')
    """).fetchall()

    if not alerts:
        lines.append("—")
        lines.append("")
        # Still render the rest of the sections so structure is stable.
    else:
        total_alerts = len(alerts)
        unique_tokens = {a["address"] for a in alerts}
        total_tokens = len(unique_tokens)

        tier_counts = {0: 0, 1: 0, 2: 0}
        for a in alerts:
            ti = a["tier_index"] if a["tier_index"] is not None else -1
            if ti in tier_counts:
                tier_counts[ti] += 1

        lines.append(
            f"{total_tokens} tokens called · {total_alerts} alerts "
            f"({tier_counts[0]}×T1 · {tier_counts[1]}×T2 · {tier_counts[2]}×T3)"
        )
        lines.append("")

        # Best alert per token (highest peak/alert ratio)
        token_best = {}
        for a in alerts:
            addr = a["address"]
            amc = a["alert_mcap"] or 0
            pmc = a["peak_mcap_after"] or 0
            if amc <= 0 or pmc <= 0:
                continue
            x = pmc / amc
            if addr not in token_best or x > token_best[addr]["x"]:
                ti = a["tier_index"] if a["tier_index"] is not None else 0
                token_best[addr] = {
                    "symbol": a["symbol"] or "???",
                    "tier_disp": f"T{ti + 1}" if 0 <= ti <= 2 else "T?",
                    "alert_mcap": amc,
                    "peak_mcap": pmc,
                    "x": x,
                }

        top5 = sorted(token_best.values(), key=lambda b: b["x"], reverse=True)[:5]
        lines.append("🏆 Top bounces:")
        if top5:
            sym_strs = ["$" + b["symbol"] for b in top5]
            amc_strs = [fmt_mcap(b["alert_mcap"]) for b in top5]
            pmc_strs = [fmt_mcap(b["peak_mcap"]) for b in top5]
            sym_w = max(len(s) for s in sym_strs)
            amc_w = max(len(s) for s in amc_strs)
            pmc_w = max(len(s) for s in pmc_strs)
            for b, sym, amc, pmc in zip(top5, sym_strs, amc_strs, pmc_strs):
                lines.append(
                    f"  {sym:<{sym_w}} {b['tier_disp']} "
                    f"@ {amc:<{amc_w}} → {pmc:<{pmc_w}}   ({b['x']:.1f}x)"
                )
        else:
            lines.append("  —")
        lines.append("")

        # Best multiple per token (0 for tokens with no peak data)
        token_best_x: dict[str, float] = {addr: 0.0 for addr in unique_tokens}
        for addr, b in token_best.items():
            token_best_x[addr] = b["x"]

        xs = list(token_best_x.values())
        count_5x = sum(1 for x in xs if x >= 5)
        count_4x = sum(1 for x in xs if 4 <= x < 5)
        count_3x = sum(1 for x in xs if 3 <= x < 4)
        count_2x = sum(1 for x in xs if 2 <= x < 3)
        count_under_2x = sum(1 for x in xs if x < 2)

        successes = count_5x + count_4x + count_3x + count_2x
        hit_pct = round(successes / total_tokens * 100) if total_tokens else 0

        # Deaths (per-token): max(peak) < min(entry) across that token's alerts
        deaths = 0
        per_token_alerts: dict[str, list[sqlite3.Row]] = {}
        for a in alerts:
            per_token_alerts.setdefault(a["address"], []).append(a)
        for addr, rows in per_token_alerts.items():
            entries = [r["alert_mcap"] for r in rows if (r["alert_mcap"] or 0) > 0]
            if not entries:
                continue
            peaks = [(r["peak_mcap_after"] or 0) for r in rows]
            if max(peaks) < min(entries):
                deaths += 1

        lines.append(
            f"Multipliers:  5x+:{count_5x}  4x+:{count_4x}  "
            f"3x+:{count_3x}  2x+:{count_2x}  <2x:{count_under_2x}"
        )
        lines.append(
            f"Hit rate (2x+): {successes}/{total_tokens} ({hit_pct}%) · "
            f"Deaths: {deaths}"
        )
        lines.append("")

    # ═════════════════════════════════════════════════════════════════════
    # TIER BREAKDOWN (per-alert, not per-token)
    # ═════════════════════════════════════════════════════════════════════
    lines.append("━━━ TIER BREAKDOWN ━━━")
    lines.append("         fired    2x+    hit%    deaths")

    if not alerts:
        lines.append("—")
    else:
        for ti in (0, 1, 2):
            tier_alerts = [a for a in alerts if a["tier_index"] == ti]
            fired = len(tier_alerts)
            hits = sum(
                1 for a in tier_alerts
                if (a["alert_mcap"] or 0) > 0
                and (a["peak_mcap_after"] or 0) >= 2 * a["alert_mcap"]
            )
            tier_deaths = sum(
                1 for a in tier_alerts
                if (a["alert_mcap"] or 0) > 0
                and (a["peak_mcap_after"] or 0) < a["alert_mcap"]
            )
            pct_str = f"{round(hits / fired * 100)}%" if fired else "—"
            lines.append(
                f"  T{ti + 1}:    {fired:<7}  {hits:<5}  {pct_str:<6}  {tier_deaths}"
            )
    lines.append("")

    # ═════════════════════════════════════════════════════════════════════
    # TAXONOMY DISTRIBUTION (ghost mode)
    # ═════════════════════════════════════════════════════════════════════
    lines.append("━━━ TAXONOMY DISTRIBUTION (ghost mode) ━━━")
    tax_counts = {b: 0 for b in TAXONOMY_BUCKETS}
    disagree_n = 0
    total_labeled = 0

    if _has_columns(conn, "ante_log", ["label_5m", "label_20sw"]):
        rows = conn.execute("""
            SELECT label_5m, label_20sw
            FROM ante_log
            WHERE DATE(alert_time,'unixepoch')=DATE('now')
        """).fetchall()
        for r in rows:
            l5 = r["label_5m"]
            l20 = r["label_20sw"]
            if l5 in tax_counts:
                tax_counts[l5] += 1
            if l5 is not None and l20 is not None:
                total_labeled += 1
                if l5 != l20:
                    disagree_n += 1

    bucket_width = max(len(b) + 1 for b in TAXONOMY_BUCKETS)  # ":" suffix
    for b in TAXONOMY_BUCKETS:
        label = f"{b}:"
        lines.append(f"  {label:<{bucket_width + 1}}  {tax_counts[b]}")

    if total_labeled > 0:
        dis_pct = round(disagree_n / total_labeled * 100)
        lines.append(
            f"5m vs 20sw disagreement: {disagree_n}/{total_labeled} ({dis_pct}%)"
        )
    else:
        lines.append("5m vs 20sw disagreement: —")
    lines.append("")

    # ═════════════════════════════════════════════════════════════════════
    # FEE GATE LABELS (on alerts that fired)
    # ═════════════════════════════════════════════════════════════════════
    lines.append("━━━ FEE GATE LABELS (on alerts that fired) ━━━")
    fg_counts = {"Normal": 0, "Elevated": 0, "Suspicious": 0}
    fg_rows = conn.execute("""
        SELECT label, COUNT(*) AS c
        FROM fee_gate_log
        WHERE DATE(alert_time,'unixepoch')=DATE('now')
          AND label IN ('Normal', 'Elevated', 'Suspicious')
        GROUP BY label
    """).fetchall()
    for r in fg_rows:
        fg_counts[r["label"]] = r["c"]

    if sum(fg_counts.values()) == 0:
        lines.append("—")
    else:
        lines.append(f"  Normal:      {fg_counts['Normal']}")
        lines.append(f"  Elevated:    {fg_counts['Elevated']}")
        lines.append(f"  Suspicious:  {fg_counts['Suspicious']}")
    lines.append("  (SCAM Likely blocked upstream — see below)")
    lines.append("")

    # ═════════════════════════════════════════════════════════════════════
    # SUPPRESSED ALERTS
    # ═════════════════════════════════════════════════════════════════════
    lines.append("━━━ SUPPRESSED ALERTS ━━━")

    total_suppressed_row = conn.execute("""
        SELECT COUNT(*) FROM alert_block_log
        WHERE DATE(block_time,'unixepoch')=DATE('now')
    """).fetchone()
    total_suppressed = total_suppressed_row[0] if total_suppressed_row else 0

    scam_row = conn.execute("""
        SELECT COUNT(DISTINCT token_address) AS t
        FROM alert_block_log
        WHERE DATE(block_time,'unixepoch')=DATE('now')
          AND block_reason = 'SCAM Likely'
    """).fetchone()
    scam_tokens = scam_row["t"] if scam_row else 0

    nfd_row = conn.execute("""
        SELECT COUNT(DISTINCT token_address) AS t
        FROM alert_block_log
        WHERE DATE(block_time,'unixepoch')=DATE('now')
          AND no_fee_data = 1
    """).fetchone()
    nfd_tokens = nfd_row["t"] if nfd_row else 0

    # Y = tokens that later had a successful alert fire same day
    resolved_row = conn.execute("""
        SELECT COUNT(DISTINCT abl.token_address) AS t
        FROM alert_block_log abl
        INNER JOIN alerts a ON abl.token_address = a.address
        WHERE DATE(abl.block_time,'unixepoch')=DATE('now')
          AND abl.no_fee_data = 1
          AND DATE(a.alert_time,'unixepoch')=DATE('now')
          AND a.alert_time >= abl.block_time
    """).fetchone()
    nfd_resolved = resolved_row["t"] if resolved_row else 0

    if total_suppressed == 0:
        lines.append("—")
    else:
        lines.append(f"{total_suppressed} suppressed:")
        lines.append(
            f"  🚨 SCAM Likely: {scam_tokens} tokens (fee_gate enforcement)"
        )
        token_word = "token" if nfd_tokens == 1 else "token(s)"
        lines.append(
            f"  ⚠️ No fee data: {nfd_tokens} {token_word} ({nfd_resolved} resolved)"
        )

    return "\n".join(lines)


def show_alerted(conn):
    """Original alerted tokens view."""
    rows = conn.execute("""
        SELECT symbol, current_mcap, ath_mcap, ath_price, current_price, last_alerted_tier, address
        FROM tokens
        WHERE last_alerted_tier >= 0
        ORDER BY last_alerted_tier DESC, symbol ASC
    """).fetchall()

    if not rows:
        print("No tokens have been alerted yet.")
        return

    total = len(rows)
    all_three = sum(1 for r in rows if r["last_alerted_tier"] >= 2)

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

        ath_price = row["ath_price"] or 0
        cur_price = row["current_price"] or 0
        if ath_price > 0:
            drop = (1 - cur_price / ath_price) * 100
            drop_str = f"-{drop:.0f}%"
        else:
            drop_str = "N/A"

        last_tier = row["last_alerted_tier"]
        tiers_hit = ", ".join(TIER_NAMES[i] for i in range(last_tier + 1))

        ca = row["address"]
        print(f" {symbol:<16}│ {mc_now:<9}│ {ath:<9}│ {drop_str:<7}│ {tiers_hit}")
        print(f"   └─ CA: {ca}")

    print()
    print("═" * 72)
    print(f" Total tokens alerted: {total}")
    print(f" Tokens hitting all 3 tiers: {all_three}")
    print("═" * 72)
    print()


def show_performance(conn, days: int = 1):
    """Daily recap entry point (days kwarg preserved for CLI compat)."""
    print(build_daily_recap(conn))


def main():
    parser = argparse.ArgumentParser(description="Phoenix Bot Stats")
    parser.add_argument("--perf", action="store_true", help="Show daily recap")
    parser.add_argument("--days", type=int, default=1, help="(unused; recap is today-only)")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    if args.perf:
        show_performance(conn, args.days)
    else:
        show_alerted(conn)

    conn.close()


if __name__ == "__main__":
    main()

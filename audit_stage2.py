"""
Phoenix Bot — Stage 2 fast-dip detector shadow audit.

Read-only. Runs four phases:
  1. Headline pass rate + per-rule fire rate / NULL share
  2. Suppression pattern distribution
  3. Co-fire structure (rules-per-row + per-rule solo vs with-others)
  4. Cold-start bias on pre_dip_vol

Filter: trigger_wall_time > 1777835361 (Stage 2 deploy timestamp).
Usage:  python audit_stage2.py
        python audit_stage2.py > audit_output.txt
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(r".\data\bot.db")
STAGE2_DEPLOY_TS = 1777835361


def run(con, label, sql, params=()):
    """Execute a query and pretty-print results as a fixed-width table."""
    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"{'=' * 70}")
    try:
        cur = con.execute(sql, params)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(f"  ERROR: {e}")
        return

    if not rows:
        print("  (no rows returned)")
        return

    cols = rows[0].keys()
    # Column widths: max of header and any cell value, capped at 24
    widths = {c: min(max(len(c), max((len(str(r[c])) for r in rows), default=0)), 24)
              for c in cols}

    header = "  " + " | ".join(c.ljust(widths[c]) for c in cols)
    sep    = "  " + "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        print("  " + " | ".join(str(r[c]).ljust(widths[c]) for c in cols))


def main():
    if not DB_PATH.exists():
        print(f"ERROR: database not found at {DB_PATH.resolve()}")
        sys.exit(1)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # ------------------------------------------------------------------
    # Sanity: confirm fast_dip_shadow exists and has rows post-deploy
    # ------------------------------------------------------------------
    run(con, "PRE-FLIGHT: row count post-deploy", """
        SELECT COUNT(*) AS rows_post_deploy
        FROM fast_dip_shadow
        WHERE trigger_wall_time > ?;
    """, (STAGE2_DEPLOY_TS,))

    # ------------------------------------------------------------------
    # PHASE 1 — headline + per-rule sanity
    # ------------------------------------------------------------------
    run(con, "Q1A — overall pass rate", f"""
        SELECT
          COUNT(*)                                       AS total_rows,
          SUM(would_alert)                               AS would_alert_1,
          COUNT(*) - SUM(would_alert)                    AS suppressed,
          ROUND(100.0 * SUM(would_alert) / COUNT(*), 2)  AS pct_alert,
          ROUND(100.0 * (COUNT(*) - SUM(would_alert))
                / COUNT(*), 2)                           AS pct_suppressed
        FROM fast_dip_shadow
        WHERE trigger_wall_time > {STAGE2_DEPLOY_TS};
    """)

    run(con, "Q1B — per-rule fire count, NULL count, fire %", f"""
        WITH base AS (
          SELECT * FROM fast_dip_shadow
          WHERE trigger_wall_time > {STAGE2_DEPLOY_TS}
        ),
        totals AS (SELECT COUNT(*) AS n FROM base)
        SELECT 'bs_ratio' AS rule,
               SUM(CASE WHEN suppressions LIKE '%bs_ratio%' THEN 1 ELSE 0 END)  AS fires,
               SUM(CASE WHEN buy_sell_ratio_10s IS NULL THEN 1 ELSE 0 END)      AS feature_null,
               (SELECT n FROM totals)                                           AS total_rows,
               ROUND(100.0 * SUM(CASE WHEN suppressions LIKE '%bs_ratio%' THEN 1 ELSE 0 END)
                     / (SELECT n FROM totals), 2)                               AS fire_pct
        FROM base
        UNION ALL
        SELECT 'depth_velocity',
               SUM(CASE WHEN suppressions LIKE '%depth_velocity%' THEN 1 ELSE 0 END),
               SUM(CASE WHEN depth_velocity_10s IS NULL THEN 1 ELSE 0 END),
               (SELECT n FROM totals),
               ROUND(100.0 * SUM(CASE WHEN suppressions LIKE '%depth_velocity%' THEN 1 ELSE 0 END)
                     / (SELECT n FROM totals), 2)
        FROM base
        UNION ALL
        SELECT 'pre_dip_vol',
               SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' THEN 1 ELSE 0 END),
               SUM(CASE WHEN pre_dip_1m_usd_vol IS NULL THEN 1 ELSE 0 END),
               (SELECT n FROM totals),
               ROUND(100.0 * SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' THEN 1 ELSE 0 END)
                     / (SELECT n FROM totals), 2)
        FROM base
        UNION ALL
        SELECT 'swap_count',
               SUM(CASE WHEN suppressions LIKE '%swap_count%' THEN 1 ELSE 0 END),
               SUM(CASE WHEN swap_count_10s IS NULL THEN 1 ELSE 0 END),
               (SELECT n FROM totals),
               ROUND(100.0 * SUM(CASE WHEN suppressions LIKE '%swap_count%' THEN 1 ELSE 0 END)
                     / (SELECT n FROM totals), 2)
        FROM base
        UNION ALL
        SELECT 'trigger_lag',
               SUM(CASE WHEN suppressions LIKE '%trigger_lag%' THEN 1 ELSE 0 END),
               SUM(CASE WHEN trigger_lag_seconds IS NULL THEN 1 ELSE 0 END),
               (SELECT n FROM totals),
               ROUND(100.0 * SUM(CASE WHEN suppressions LIKE '%trigger_lag%' THEN 1 ELSE 0 END)
                     / (SELECT n FROM totals), 2)
        FROM base;
    """)

    # ------------------------------------------------------------------
    # PHASE 2 — suppression pattern distribution
    # ------------------------------------------------------------------
    run(con, "Q2 — unique suppression patterns, ranked", f"""
        SELECT
          COALESCE(NULLIF(suppressions, ''), '<alert>')      AS pattern,
          COUNT(*)                                           AS cnt,
          ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM fast_dip_shadow
        WHERE trigger_wall_time > {STAGE2_DEPLOY_TS}
        GROUP BY pattern
        ORDER BY cnt DESC;
    """)

    # ------------------------------------------------------------------
    # PHASE 3 — co-fire structure
    # ------------------------------------------------------------------
    run(con, "Q3A — distribution of rules-fired-per-row", f"""
        WITH base AS (
          SELECT *,
                 CASE
                   WHEN suppressions IS NULL OR suppressions = '' THEN 0
                   ELSE 1 + (LENGTH(suppressions) - LENGTH(REPLACE(suppressions, ',', '')))
                 END AS n_fired
          FROM fast_dip_shadow
          WHERE trigger_wall_time > {STAGE2_DEPLOY_TS}
        )
        SELECT n_fired,
               COUNT(*)                                           AS rows,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM base
        GROUP BY n_fired
        ORDER BY n_fired;
    """)

    run(con, "Q3B — per-rule: solo fires vs with-others", f"""
        WITH base AS (
          SELECT *,
                 CASE
                   WHEN suppressions IS NULL OR suppressions = '' THEN 0
                   ELSE 1 + (LENGTH(suppressions) - LENGTH(REPLACE(suppressions, ',', '')))
                 END AS n_fired
          FROM fast_dip_shadow
          WHERE trigger_wall_time > {STAGE2_DEPLOY_TS}
        )
        SELECT 'bs_ratio' AS rule,
               SUM(CASE WHEN suppressions LIKE '%bs_ratio%' AND n_fired = 1 THEN 1 ELSE 0 END) AS solo,
               SUM(CASE WHEN suppressions LIKE '%bs_ratio%' AND n_fired > 1 THEN 1 ELSE 0 END) AS with_others,
               SUM(CASE WHEN suppressions LIKE '%bs_ratio%' THEN 1 ELSE 0 END)                 AS total_fires
        FROM base
        UNION ALL
        SELECT 'depth_velocity',
               SUM(CASE WHEN suppressions LIKE '%depth_velocity%' AND n_fired = 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%depth_velocity%' AND n_fired > 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%depth_velocity%' THEN 1 ELSE 0 END)
        FROM base
        UNION ALL
        SELECT 'pre_dip_vol',
               SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' AND n_fired = 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' AND n_fired > 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' THEN 1 ELSE 0 END)
        FROM base
        UNION ALL
        SELECT 'swap_count',
               SUM(CASE WHEN suppressions LIKE '%swap_count%' AND n_fired = 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%swap_count%' AND n_fired > 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%swap_count%' THEN 1 ELSE 0 END)
        FROM base
        UNION ALL
        SELECT 'trigger_lag',
               SUM(CASE WHEN suppressions LIKE '%trigger_lag%' AND n_fired = 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%trigger_lag%' AND n_fired > 1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN suppressions LIKE '%trigger_lag%' THEN 1 ELSE 0 END)
        FROM base;
    """)

    # ------------------------------------------------------------------
    # PHASE 4 — cold-start bias on pre_dip_vol
    # ------------------------------------------------------------------
    run(con, "Q4 — pre_dip_vol fire rate + value distribution by elapsed-time bucket", f"""
        WITH base AS (
          SELECT *, trigger_wall_time - {STAGE2_DEPLOY_TS} AS elapsed_s
          FROM fast_dip_shadow
          WHERE trigger_wall_time > {STAGE2_DEPLOY_TS}
        )
        SELECT
          CASE
            WHEN elapsed_s < 60     THEN '1. 0-60s'
            WHEN elapsed_s < 300    THEN '2. 60-300s'
            WHEN elapsed_s < 3600   THEN '3. 5min-1h'
            WHEN elapsed_s < 21600  THEN '4. 1h-6h'
            ELSE                         '5. 6h+'
          END                                                                  AS bucket,
          COUNT(*)                                                             AS rows,
          SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' THEN 1 ELSE 0 END)   AS fires,
          ROUND(100.0 * SUM(CASE WHEN suppressions LIKE '%pre_dip_vol%' THEN 1 ELSE 0 END)
                / COUNT(*), 2)                                                 AS fire_pct,
          ROUND(AVG(pre_dip_1m_usd_vol), 2)                                    AS avg_vol,
          ROUND(MIN(pre_dip_1m_usd_vol), 2)                                    AS min_vol,
          ROUND(MAX(pre_dip_1m_usd_vol), 2)                                    AS max_vol,
          SUM(CASE WHEN pre_dip_1m_usd_vol IS NULL THEN 1 ELSE 0 END)          AS null_count
        FROM base
        GROUP BY bucket
        ORDER BY bucket;
    """)

    con.close()
    print(f"\n{'=' * 70}")
    print("  Done. Paste output back to continue the audit.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()

"""
holder_filter.py — Ghost Filter: scam-detection signals on Tier 1 holder snapshots.

Signals:
  1. Funding collision — wallets sharing identical SOL balances (round to 4 dp).
     Threshold: 6+ wallets across all clusters → flagged.
  2. Low SOL clustering — user wallets with < 0.1 SOL.
     Threshold: 30+ wallets → flagged.

Shadow mode — logs results, does not block alerts.

Cross-table token column reference:
  tokens / alerts   → "address"
  holder_snapshots  → "token_mint"
  holder_filter_log → "token_address"
All three refer to the same Solana mint. Alias when joining.
"""

import json
import logging
import os
import time
from typing import Optional

import aiosqlite

logger = logging.getLogger("phoenix.holder_filter")

DB_PATH = "data/bot.db"

# ── Thresholds ───────────────────────────────────────────────────────────────

COLLISION_THRESHOLD = 6       # wallets in funding clusters to flag
LOW_SOL_THRESHOLD = 30        # wallets with < 0.1 SOL to flag
LOW_SOL_CUTOFF = 0.1          # SOL balance below this = "low SOL"

# v2 thresholds — drive the 3-mode `verdict` field. Independent of v1
# above (which still drives would_have_blocked for historical compat).
GHOST_BLOCK_WALLETS    = 8
GHOST_BLOCK_CLUSTERS   = 4
GHOST_CAUTION_WALLETS  = 6
GHOST_CAUTION_CLUSTERS = 3


# ── Entrypoint ───────────────────────────────────────────────────────────────


def evaluate_holder_filter(snapshot: dict) -> dict:
    """
    Run ghost filter signals on a holder snapshot dict.

    Args:
        snapshot: dict returned by snapshot_top_holders().

    Returns:
        Result dict with collision/low-SOL counts, flags, and block reasoning.
    """
    holders = snapshot.get("holders", [])

    # Filter to user_wallet holders only
    # Relies on classifier invariant: user_wallet → exclude_from_wallet_stats=False.
    # If classification rules change, add explicit exclude check here.
    user_wallets = [
        h for h in holders
        if h.get("holder_type") == "user_wallet"
    ]
    user_wallet_count = len(user_wallets)

    # ── Signal 1: Funding collision detection ────────────────────────────
    balance_groups: dict[float, int] = {}
    for h in user_wallets:
        sol = h.get("sol_balance")
        if sol is None or sol == 0.0:
            continue
        key = round(sol, 4)
        balance_groups[key] = balance_groups.get(key, 0) + 1

    # A cluster = any balance bucket with 2+ wallets
    clusters = [
        {"sol": sol, "wallets": count}
        for sol, count in balance_groups.items()
        if count >= 2
    ]
    clusters.sort(key=lambda c: c["wallets"], reverse=True)

    funding_collision_count = sum(c["wallets"] for c in clusters)
    flagged_collisions = funding_collision_count >= COLLISION_THRESHOLD

    # ── Signal 2: Low SOL clustering ─────────────────────────────────────
    low_sol_count = sum(
        1 for h in user_wallets
        if h.get("sol_balance") is not None and h["sol_balance"] < LOW_SOL_CUTOFF
    )
    flagged_low_sol = low_sol_count >= LOW_SOL_THRESHOLD

    # ── Verdict ──────────────────────────────────────────────────────────
    would_block = flagged_collisions or flagged_low_sol

    block_reason: str | None = None
    if flagged_collisions and flagged_low_sol:
        block_reason = "both"
    elif flagged_collisions:
        block_reason = "funding_collisions"
    elif flagged_low_sol:
        block_reason = "low_sol_cluster"

    # ── 3-mode verdict (v2, independent of v1 would_block) ───────────────
    cluster_count = len(clusters)
    if (
        funding_collision_count >= GHOST_BLOCK_WALLETS
        or cluster_count >= GHOST_BLOCK_CLUSTERS
    ):
        collision_verdict = "block"
    elif (
        funding_collision_count >= GHOST_CAUTION_WALLETS
        or cluster_count >= GHOST_CAUTION_CLUSTERS
    ):
        collision_verdict = "caution"
    else:
        collision_verdict = "pass"

    if flagged_low_sol or collision_verdict == "block":
        verdict = "block"
    elif collision_verdict == "caution":
        verdict = "caution"
    else:
        verdict = "pass"

    return {
        "funding_collision_count": funding_collision_count,
        "collision_clusters": clusters,
        "low_sol_count": low_sol_count,
        "user_wallet_count": user_wallet_count,
        "flagged_collisions": flagged_collisions,
        "flagged_low_sol": flagged_low_sol,
        "would_block": would_block,
        "block_reason": block_reason,
        "verdict": verdict,
        "checked_at": time.time(),
    }


# ── Persistence ──────────────────────────────────────────────────────────────


async def _ensure_table(db_conn: aiosqlite.Connection) -> None:
    """Create holder_filter_log table if it doesn't exist."""
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS holder_filter_log (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address            TEXT    NOT NULL,
            alert_time               REAL    NOT NULL,
            snapshot_id              INTEGER,
            funding_collision_count  INTEGER,
            low_sol_count            INTEGER,
            user_wallet_count        INTEGER,
            block_reason             TEXT,
            would_have_blocked       INTEGER NOT NULL,
            verdict                  TEXT,
            actually_blocked         INTEGER NOT NULL DEFAULT 0,
            payload_json             TEXT    NOT NULL
        )
    """)
    # Migration: add verdict column on existing DBs that predate the 3-mode rule.
    async with db_conn.execute("PRAGMA table_info(holder_filter_log)") as cur:
        hfl_cols = [row[1] async for row in cur]
    if "verdict" not in hfl_cols:
        await db_conn.execute("ALTER TABLE holder_filter_log ADD COLUMN verdict TEXT")
        await db_conn.commit()
        logger.info("Migrated holder_filter_log: added verdict column")
    await db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_hflog_token ON holder_filter_log(token_address)"
    )
    await db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_hflog_time ON holder_filter_log(alert_time)"
    )
    # Index for verdict — placed after migration so it works on
    # existing DBs that just got the column added above.
    await db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_hflog_verdict ON holder_filter_log(verdict)"
    )
    await db_conn.commit()


async def log_holder_filter(
    token_address: str,
    alert_time: float,
    snapshot_id: int | None,
    result: dict,
) -> int | None:
    """Insert a holder_filter_log row. Returns the inserted row id, or None on failure."""
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await _ensure_table(db)
            cursor = await db.execute(
                """
                INSERT INTO holder_filter_log (
                    token_address, alert_time, snapshot_id,
                    funding_collision_count, low_sol_count, user_wallet_count,
                    block_reason, would_have_blocked, verdict, actually_blocked,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    token_address,
                    alert_time,
                    snapshot_id,
                    result.get("funding_collision_count", 0),
                    result.get("low_sol_count", 0),
                    result.get("user_wallet_count", 0),
                    result.get("block_reason"),
                    1 if result.get("would_block") else 0,
                    result.get("verdict"),
                    json.dumps(result, default=str),
                ),
            )
            await db.commit()
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"holder_filter_log write failed for {token_address[:8]}: {e}")
        return None


async def mark_actually_blocked(row_id: int) -> None:
    """Set actually_blocked=1 on a holder_filter_log row by id."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE holder_filter_log SET actually_blocked = 1 WHERE id = ?",
                (row_id,),
            )
            await db.commit()
    except Exception as e:
        logger.error(f"mark_actually_blocked failed for row {row_id}: {e}")


async def get_recent_filter_result(
    token_address: str,
    max_age_seconds: int = 3600,
) -> dict | None:
    """
    Return the most recent ghost filter result for a token if fresh enough.

    Returns parsed payload_json dict when the newest holder_filter_log row
    is younger than max_age_seconds, otherwise None (caller should re-snapshot).
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """
                SELECT alert_time, payload_json
                FROM holder_filter_log
                WHERE token_address = ?
                ORDER BY id DESC LIMIT 1
                """,
                (token_address,),
            ) as cur:
                row = await cur.fetchone()
        if row is None:
            return None
        alert_time, payload_json = row
        if time.time() - alert_time >= max_age_seconds:
            return None
        return json.loads(payload_json)
    except Exception as e:
        logger.warning(f"get_recent_filter_result failed for {token_address[:8]}: {e}")
        return None

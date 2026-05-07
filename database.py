"""
database.py — SQLite persistence via aiosqlite.
Stores and retrieves TrackedToken objects.
Also stores alert history for performance tracking.
Also stores fee_gate_log, lp_floor_log, stillborn_log for shadow-mode filters.
"""

import aiosqlite
import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager

from models import TrackedToken, TokenStatus

logger = logging.getLogger(__name__)

DB_PATH = "data/bot.db"


@asynccontextmanager
async def db_connect(db_path: str = DB_PATH):
    """Standard aiosqlite connection with concurrency PRAGMAs.
    Use this everywhere instead of bare aiosqlite.connect() so every
    connection gets busy_timeout (waits out brief writer locks instead
    of failing) and synchronous=NORMAL (cheaper fsync, safe under WAL).
    The persistent file-level pragmas (journal_mode=WAL,
    journal_size_limit) are seeded once by init_db()."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA busy_timeout = 5000")
        await db.execute("PRAGMA synchronous = NORMAL")
        yield db


async def init_db(db_path: str = DB_PATH):
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        # Concurrency + size caps (prevents the WAL/lock blowup of
        # 2026-05-02, where pumpswap_fees grew to 39 GB with a 19 GB
        # WAL that never checkpointed). journal_mode + journal_size_limit
        # are persistent on the DB file; busy_timeout + synchronous +
        # wal_autocheckpoint are per-connection but seeding them here
        # ensures the schema-creating connection is configured too —
        # every other connection layers them on via db_connect().
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA synchronous = NORMAL")
        await db.execute("PRAGMA busy_timeout = 5000")
        await db.execute("PRAGMA wal_autocheckpoint = 1000")
        await db.execute("PRAGMA journal_size_limit = 1073741824")  # 1 GB hard cap
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                address         TEXT PRIMARY KEY,
                symbol          TEXT,
                pool_address    TEXT,
                status          TEXT,
                migration_price REAL,
                migration_mcap  REAL,
                current_price   REAL,
                current_mcap    REAL,
                liquidity_usd   REAL,
                ath_price       REAL,
                ath_mcap        REAL,
                ath_time        REAL,
                volume_1h       REAL,
                volume_6h       REAL,
                volume_24h      REAL,
                migration_time  REAL,
                last_price_update REAL,
                last_alerted_tier INTEGER,
                ath_source      TEXT DEFAULT 'unseeded',
                pool_orientation TEXT DEFAULT NULL,
                token_decimals  INTEGER DEFAULT NULL,
                phantom_cooldown_until REAL DEFAULT 0,
                ghost_cooldown_until REAL DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pumpswap_fees (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                signature       TEXT NOT NULL,
                slot            INTEGER NOT NULL,
                block_time      REAL,
                pool_address    TEXT NOT NULL,
                token_address   TEXT,
                event_type      TEXT NOT NULL,
                lp_fee          INTEGER NOT NULL,
                protocol_fee    INTEGER NOT NULL,
                creator_fee     INTEGER NOT NULL DEFAULT 0,
                total_fee       INTEGER NOT NULL,
                received_at     REAL NOT NULL
            )
        """)
        await db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pumpswap_fees_signature
            ON pumpswap_fees(signature, event_type)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_pumpswap_fees_pool
            ON pumpswap_fees(pool_address)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_pumpswap_fees_token
            ON pumpswap_fees(token_address)
        """)
        # Composite index for (token_address, block_time) range scans.
        # Used by Ante 5m window and fee-gate 5m pace queries.
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_pumpswap_fees_token_time
            ON pumpswap_fees(token_address, block_time)
        """)
        # Composite index for Ante v2 priority_fee lookup: pool + event_type +
        # block_time DESC lets get_median_priority_fee() use a bounded index
        # scan instead of a full-pool scan + sort.
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_pumpswap_fees_pool_buy_time
            ON pumpswap_fees(pool_address, event_type, block_time DESC)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                address         TEXT NOT NULL,
                symbol          TEXT,
                tier_index      INTEGER NOT NULL,
                tier_name       TEXT,
                alert_price     REAL NOT NULL,
                alert_mcap      REAL NOT NULL,
                ath_price       REAL,
                ath_mcap        REAL,
                peak_price_after REAL DEFAULT 0,
                peak_mcap_after  REAL DEFAULT 0,
                alert_time      REAL NOT NULL,
                FOREIGN KEY (address) REFERENCES tokens(address)
            )
        """)
        # ── Fee Gate shadow log ────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS fee_gate_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address   TEXT NOT NULL,
                symbol          TEXT,
                alert_tier      INTEGER NOT NULL,
                tier_name       TEXT,
                alert_time      REAL NOT NULL,
                total_fee       REAL,
                lp_fee          REAL,
                proto_fee       REAL,
                creator_fee     REAL,
                rate            REAL,
                events          INTEGER,
                creator_share   REAL,
                proto_share     REAL,
                fee_per_event   REAL,
                proto_to_lp     REAL,
                score           INTEGER,
                flags           TEXT,
                label           TEXT,
                manual_verdict  TEXT,
                reviewed_at     REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fgl_token ON fee_gate_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fgl_label ON fee_gate_log(label)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fgl_time  ON fee_gate_log(alert_time)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fgl_tier  ON fee_gate_log(alert_tier)")

        # ── Alert Block log (hard-block decisions) ────────────────────────
        # retry_count / last_retry_at support the UPSERT in log_alert_block:
        # first block writes retry_count=1, subsequent blocks on the same
        # (token, tier, reason) key bump the count instead of inserting new rows.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alert_block_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address    TEXT NOT NULL,
                symbol           TEXT,
                would_have_tier  INTEGER NOT NULL,
                tier_name        TEXT,
                block_time       REAL NOT NULL,
                block_reason     TEXT NOT NULL,
                fee_gate_log_id  INTEGER,
                no_fee_data      INTEGER NOT NULL DEFAULT 0,
                retry_count      INTEGER NOT NULL DEFAULT 1,
                last_retry_at    REAL,
                FOREIGN KEY (token_address) REFERENCES tokens(address),
                FOREIGN KEY (fee_gate_log_id) REFERENCES fee_gate_log(id)
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_abl_token  ON alert_block_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_abl_time   ON alert_block_log(block_time)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_abl_reason ON alert_block_log(block_reason)")
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_block_dedup "
            "ON alert_block_log(token_address, would_have_tier, block_reason)"
        )

        # ── LP Floor shadow log ────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lp_floor_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address   TEXT NOT NULL,
                symbol          TEXT,
                alert_tier      INTEGER NOT NULL,
                tier_name       TEXT,
                alert_time      REAL NOT NULL,
                liquidity_usd   REAL,
                label           TEXT,
                reason          TEXT,
                manual_verdict  TEXT,
                reviewed_at     REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_lpf_token ON lp_floor_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_lpf_label ON lp_floor_log(label)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_lpf_time  ON lp_floor_log(alert_time)")

        # ── Stillborn shadow log ───────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stillborn_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address   TEXT NOT NULL,
                symbol          TEXT,
                alert_tier      INTEGER NOT NULL,
                tier_name       TEXT,
                alert_time      REAL NOT NULL,
                events          INTEGER,
                total_fee_sol   REAL,
                label           TEXT,
                reason          TEXT,
                manual_verdict  TEXT,
                reviewed_at     REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sb_token ON stillborn_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sb_label ON stillborn_log(label)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sb_time  ON stillborn_log(alert_time)")

        # ── Ante shadow log ────────────────────────────────────────────────
        # Observe-only Phase 1 primitive: per-swap fee burn rolling stats,
        # logged once per fired dip alert. Never gates.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ante_log (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address         TEXT NOT NULL,
                symbol                TEXT,
                alert_tier            INTEGER,
                tier_name             TEXT,
                alert_time            REAL NOT NULL,
                -- Last-20-swaps window (distinct-signature samples)
                ante_n20_count        INTEGER,
                ante_n20_median_sol   REAL,
                ante_n20_p25_sol      REAL,
                ante_n20_p75_sol      REAL,
                ante_n20_width_ratio  REAL,
                -- Last-5-minutes window
                ante_5m_count         INTEGER,
                ante_5m_median_sol    REAL,
                ante_5m_p25_sol       REAL,
                ante_5m_p75_sol       REAL,
                ante_5m_width_ratio   REAL,
                -- Diagnostics
                base_fee_coverage     REAL,
                -- Taxonomy labels (V1) — categorical Ante classification per window
                label_5m              TEXT,
                rule_hit_5m           INTEGER,
                label_20sw            TEXT,
                rule_hit_20sw         INTEGER,
                manual_verdict        TEXT,
                reviewed_at           REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ante_token ON ante_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ante_time  ON ante_log(alert_time)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ante_tier  ON ante_log(alert_tier)")

        # ── Inspection Gate shadow log ─────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS inspection_gate_log (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address           TEXT NOT NULL,
                symbol                  TEXT,
                inception_slot          INTEGER,
                inception_block_time    INTEGER,
                window_end_slot         INTEGER,
                buy_count               INTEGER DEFAULT 0,
                sell_count              INTEGER DEFAULT 0,
                buy_sol                 REAL DEFAULT 0,
                sell_sol                REAL DEFAULT 0,
                gross_sol               REAL DEFAULT 0,
                net_sol                 REAL DEFAULT 0,
                sol_price_usd           REAL,
                buy_usd                 REAL DEFAULT 0,
                sell_usd                REAL DEFAULT 0,
                sell_to_buy_ratio       REAL DEFAULT 0,
                label                   TEXT,
                threshold_version       TEXT DEFAULT 'v1_10000usd_0.05ratio',
                check_started_at        INTEGER,
                check_completed_at      INTEGER,
                check_latency_ms        INTEGER,
                rpc_calls_made          INTEGER DEFAULT 0,
                retry_attempted         INTEGER DEFAULT 0,
                error_reason            TEXT,
                alert_id                INTEGER,
                FOREIGN KEY (alert_id) REFERENCES alerts(id)
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bgl_token ON inspection_gate_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bgl_label ON inspection_gate_log(label)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bgl_inception ON inspection_gate_log(inception_block_time)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bgl_alert ON inspection_gate_log(alert_id)")

        # ── Phantom Validator log (BLICKY phantom-dip detector) ───────────
        # Logs every validator invocation (both phantom-positive and -negative)
        # so the week-1 review can compute the true-positive / true-negative
        # split and the birdeye_to_ath_ratio distribution.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS phantom_abort_log (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address            TEXT NOT NULL,
                symbol                   TEXT,
                ath_update_time          REAL NOT NULL,
                ath_price                REAL NOT NULL,
                ath_mcap                 REAL,
                ath_source               TEXT,
                birdeye_current_price    REAL,
                birdeye_current_mcap     REAL,
                dex_current_price        REAL,
                dex_current_mcap         REAL,
                birdeye_to_ath_ratio     REAL,
                dex_to_ath_ratio         REAL,
                is_phantom               INTEGER NOT NULL,
                cooldown_until           REAL,
                birdeye_error            TEXT,
                created_at               REAL NOT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pal_token ON phantom_abort_log(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pal_time  ON phantom_abort_log(ath_update_time)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pal_phantom ON phantom_abort_log(is_phantom)")

        # ── Fast-Dip Detector shadow log (Stage 1: trigger only) ──────────
        # One row per dip episode for a token. Stage 1 fills the trigger
        # block + dip_end columns. Stage 2 will UPDATE the decision block
        # at trigger_block_time + 10s. Stage 3 will UPDATE the outcome
        # block with post-dip peak prices. All time math uses block_time.
        # Prices are stored in SOL units (per-token, derived from
        # quote_amount/base_amount); USD conversion is intentionally
        # deferred to analysis time so the table is invariant to a
        # SOL/USD constant choice.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS fast_dip_shadow (
                id                          INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Trigger (Stage 1) -----------------------------------------
                token_address               TEXT NOT NULL,
                pool_address                TEXT,
                symbol                      TEXT,
                trigger_block_time          REAL NOT NULL,
                trigger_wall_time           REAL NOT NULL,
                trigger_signature           TEXT,
                trigger_event_type          TEXT,
                trigger_price_sol           REAL NOT NULL,
                rolling_max_price_sol       REAL NOT NULL,
                rolling_max_block_time      REAL NOT NULL,
                drop_pct                    REAL NOT NULL,
                dip_end_block_time          REAL,
                dip_end_reason              TEXT,

                -- Decision (Stage 2 — populated by UPDATE later) ------------
                decision_block_time         REAL,
                decision_wall_time          REAL,
                depth_at_10s                REAL,
                depth_velocity_10s          REAL,
                swap_count_10s              INTEGER,
                buy_sell_ratio_10s          REAL,
                pre_dip_1m_usd_vol          REAL,
                trigger_lag_seconds         REAL,
                suppressions                TEXT,
                would_alert                 INTEGER,

                -- Outcome (Stage 3 — populated by UPDATE later) -------------
                post_60s_peak_price_sol     REAL,
                post_5m_peak_price_sol      REAL,
                post_60m_peak_price_sol     REAL,

                -- Manual review (any stage) ---------------------------------
                manual_verdict              TEXT,
                reviewed_at                 REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fds_token        ON fast_dip_shadow(token_address)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fds_trigger_time ON fast_dip_shadow(trigger_block_time)")

        # ── ATH retry queue persistence ──────────────────────────────────
        # Mirrors the in-memory _ath_retry_queue in modules.ath_seeder so
        # in-flight Birdeye retries survive restarts. first_success_at is
        # nullable: NULL means "not yet seeded by Birdeye"; non-NULL marks
        # reseed mode. Address is PK — INSERT OR REPLACE handles transitions
        # from the 2-field shape to the 3-field shape on first success.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ath_retry_queue (
                address          TEXT PRIMARY KEY,
                queued_at        REAL NOT NULL,
                last_attempt     REAL NOT NULL,
                first_success_at REAL
            )
        """)

        # ── Migration: Stage 2 +10s decision gate columns ────────────────
        # swap_density_5s persisted at trigger time (was discarded in
        # Stage 1). pre_dip_1m_swap_count populated by the +10s decision
        # UPDATE. Both nullable. Stage 1 rows stay NULL — no backfill.
        for col_sql in [
            "ALTER TABLE fast_dip_shadow ADD COLUMN swap_density_5s REAL",
            "ALTER TABLE fast_dip_shadow ADD COLUMN pre_dip_1m_swap_count INTEGER",
        ]:
            try:
                await db.execute(col_sql)
            except Exception:
                pass

        await db.commit()

        # ── Migration: add creator_fee column to pumpswap_fees if upgrading ─
        async with db.execute("PRAGMA table_info(pumpswap_fees)") as cur:
            cols = [row[1] async for row in cur]
        if "creator_fee" not in cols:
            await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN creator_fee INTEGER NOT NULL DEFAULT 0")
            await db.commit()
            logger.info("Migrated pumpswap_fees: added creator_fee column")

        # ── Migration: Ante Phase 1 — add base_fee + signature_count ─────
        # Both nullable (forward-only capture). Historical rows stay NULL and
        # roll up as "partial Ante" (priority + jito only) in analysis tools.
        async with db.execute("PRAGMA table_info(pumpswap_fees)") as cur:
            cols = [row[1] async for row in cur]
        if "base_fee" not in cols:
            await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN base_fee INTEGER")
            await db.commit()
            logger.info("Migrated pumpswap_fees: added base_fee column")
        if "signature_count" not in cols:
            await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN signature_count INTEGER")
            await db.commit()
            logger.info("Migrated pumpswap_fees: added signature_count column")

        # Schema drift recovery — columns added out-of-band on the live DB.
        # Guarded so fresh DBs get them and existing DBs silently skip.
        for col_sql in [
            "ALTER TABLE pumpswap_fees ADD COLUMN priority_fee INTEGER",
            "ALTER TABLE pumpswap_fees ADD COLUMN jito_tip INTEGER",
            "ALTER TABLE pumpswap_fees ADD COLUMN compute_units_consumed INTEGER",
            "ALTER TABLE pumpswap_fees ADD COLUMN quote_amount INTEGER DEFAULT 0",
            "ALTER TABLE pumpswap_fees ADD COLUMN user_pubkey TEXT",
        ]:
            try:
                await db.execute(col_sql)
            except Exception:
                pass

        # ── Migration: base_amount — pool-perspective base-side amount from
        # BuyEvent/SellEvent offset 16. Forward-only capture; historical rows
        # stay NULL. Note: for "inverted" pools where WSOL is the base side,
        # this column holds the SOL amount instead of the meme-token amount —
        # price-reconstruction consumers must pair it with quote_amount and
        # the pool's base/quote orientation.
        async with db.execute("PRAGMA table_info(pumpswap_fees)") as cur:
            cols = [row[1] async for row in cur]
        if "base_amount" not in cols:
            await db.execute("ALTER TABLE pumpswap_fees ADD COLUMN base_amount INTEGER")
            await db.commit()
            logger.info("Migrated pumpswap_fees: added base_amount column")

        # ── Migration: Ante Phase 1.1 — width-ratio columns on ante_log ──
        # Both nullable. NULL means insufficient samples to compute (e.g. zero
        # swaps in window). Capped at 10000 in the producer to bound storage.
        async with db.execute("PRAGMA table_info(ante_log)") as cur:
            ante_cols = [row[1] async for row in cur]
        if "ante_n20_width_ratio" not in ante_cols:
            await db.execute("ALTER TABLE ante_log ADD COLUMN ante_n20_width_ratio REAL")
            await db.commit()
            logger.info("Migrated ante_log: added ante_n20_width_ratio column")
        if "ante_5m_width_ratio" not in ante_cols:
            await db.execute("ALTER TABLE ante_log ADD COLUMN ante_5m_width_ratio REAL")
            await db.commit()
            logger.info("Migrated ante_log: added ante_5m_width_ratio column")

        # ── Migration: Ante v2 Session 2 — priority_fee observe-only columns
        # Both nullable (forward-only capture). median_priority_fee is lamports;
        # priority_fee_n is the non-NULL sample count (0–20).
        async with db.execute("PRAGMA table_info(ante_log)") as cur:
            ante_cols = [row[1] async for row in cur]
        if "median_priority_fee" not in ante_cols:
            await db.execute("ALTER TABLE ante_log ADD COLUMN median_priority_fee REAL")
            await db.commit()
            logger.info("Migrated ante_log: added median_priority_fee column")
        if "priority_fee_n" not in ante_cols:
            await db.execute("ALTER TABLE ante_log ADD COLUMN priority_fee_n INTEGER")
            await db.commit()
            logger.info("Migrated ante_log: added priority_fee_n column")

        # ── Migration: ath_source column on tokens ────────────────────────
        # Tracks provenance of ath_price so the seed chain can't silently
        # fall through to migration-era price without leaving a trace.
        # Values: 'unseeded' | 'birdeye' | 'fallback' | 'running_max'.
        async with db.execute("PRAGMA table_info(tokens)") as cur:
            tokens_cols = [row[1] async for row in cur]
        if "ath_source" not in tokens_cols:
            await db.execute("ALTER TABLE tokens ADD COLUMN ath_source TEXT DEFAULT 'unseeded'")
            # Backfill: provenance of pre-migration rows is unknowable.
            # 'running_max' is the safe default — it won't re-enter the
            # Birdeye retry loop on deploy (avoids a retry storm).
            await db.execute(
                "UPDATE tokens SET ath_source = 'running_max' WHERE ath_price > 0"
            )
            await db.execute(
                "UPDATE tokens SET ath_source = 'unseeded' "
                "WHERE ath_price IS NULL OR ath_price <= 0"
            )
            await db.commit()
            logger.info("Migrated tokens: added ath_source column (backfilled)")

        # ── Migration: pool_orientation + token_decimals on tokens ────────
        # Populated by migration_ws at migration time via getAccountInfo.
        # NULL means "not yet determined" — the gRPC indexer silently skips
        # price derivation for tokens without metadata. Forward-only; no
        # backfill for existing rows.
        async with db.execute("PRAGMA table_info(tokens)") as cur:
            tokens_cols = [row[1] async for row in cur]
        if "pool_orientation" not in tokens_cols:
            await db.execute("ALTER TABLE tokens ADD COLUMN pool_orientation TEXT DEFAULT NULL")
            await db.commit()
            logger.info("Migrated tokens: added pool_orientation column")
        if "token_decimals" not in tokens_cols:
            await db.execute("ALTER TABLE tokens ADD COLUMN token_decimals INTEGER DEFAULT NULL")
            await db.commit()
            logger.info("Migrated tokens: added token_decimals column")

        # ── Migration: phantom_cooldown_until on tokens ───────────────────
        # Set by phantom_validator when Birdeye-current is at-or-near a
        # freshly written ATH. alert_trigger reads this flag to suppress
        # tier evaluation during the cooldown window. Default 0 = no
        # cooldown active. Forward-only, no backfill.
        async with db.execute("PRAGMA table_info(tokens)") as cur:
            tokens_cols = [row[1] async for row in cur]
        if "phantom_cooldown_until" not in tokens_cols:
            await db.execute(
                "ALTER TABLE tokens ADD COLUMN phantom_cooldown_until REAL DEFAULT 0"
            )
            await db.commit()
            logger.info("Migrated tokens: added phantom_cooldown_until column")

        # ── Migration: ghost_cooldown_until on tokens ─────────────────────
        # Set in main.py when the holder filter returns verdict=block on
        # any tier. alert_trigger reads this flag to suppress tier
        # evaluation during the cooldown window. Default 0 = no cooldown
        # active. Forward-only, no backfill.
        async with db.execute("PRAGMA table_info(tokens)") as cur:
            tokens_cols = [row[1] async for row in cur]
        if "ghost_cooldown_until" not in tokens_cols:
            await db.execute(
                "ALTER TABLE tokens ADD COLUMN ghost_cooldown_until REAL DEFAULT 0"
            )
            await db.commit()
            logger.info("Migrated tokens: added ghost_cooldown_until column")

        # ── Migration: drawdown + timing columns on alerts ────────────────
        # Post-alert outcome tracking. Forward-only: historical alerts stay
        # NULL on all six columns — NULL means "we weren't tracking at the
        # time," not "zero drawdown." Do not backfill alert_price as a
        # synthetic floor; that would fabricate outcomes for rows we did
        # not observe. Column-existence is the idempotency guard — do not
        # use "any non-NULL trough_price_after" since the price loop
        # populates it within seconds of deploy.
        async with db.execute("PRAGMA table_info(alerts)") as cur:
            alerts_cols = [row[1] async for row in cur]
        if "trough_price_after" not in alerts_cols:
            await db.execute("ALTER TABLE alerts ADD COLUMN trough_price_after REAL")
            await db.execute("ALTER TABLE alerts ADD COLUMN trough_mcap_after REAL")
            await db.execute("ALTER TABLE alerts ADD COLUMN trough_time REAL")
            await db.execute("ALTER TABLE alerts ADD COLUMN peak_time REAL")
            await db.execute("ALTER TABLE alerts ADD COLUMN time_to_peak_minutes REAL")
            await db.execute("ALTER TABLE alerts ADD COLUMN max_drawdown_pct REAL")
            await db.commit()
            logger.info("Migrated alerts: added drawdown/timing columns")

    logger.info(f"Database initialised at {db_path}")


async def save_token(token: TrackedToken, db_path: str = DB_PATH):
    async with db_connect(db_path) as db:
        await db.execute("""
            INSERT OR REPLACE INTO tokens VALUES (
                :address, :symbol, :pool_address, :status,
                :migration_price, :migration_mcap,
                :current_price, :current_mcap, :liquidity_usd,
                :ath_price, :ath_mcap, :ath_time,
                :volume_1h, :volume_6h, :volume_24h,
                :migration_time, :last_price_update,
                :last_alerted_tier, :ath_source,
                :pool_orientation, :token_decimals,
                :phantom_cooldown_until,
                :ghost_cooldown_until
            )
        """, {
            "address":           token.address,
            "symbol":            token.symbol,
            "pool_address":      token.pool_address,
            "status":            token.status.value,
            "migration_price":   token.migration_price,
            "migration_mcap":    token.migration_mcap,
            "current_price":     token.current_price,
            "current_mcap":      token.current_mcap,
            "liquidity_usd":     token.liquidity_usd,
            "ath_price":         token.ath_price,
            "ath_mcap":          token.ath_mcap,
            "ath_time":          token.ath_time,
            "volume_1h":         token.volume_1h,
            "volume_6h":         token.volume_6h,
            "volume_24h":        token.volume_24h,
            "migration_time":    token.migration_time,
            "last_price_update": token.last_price_update,
            "last_alerted_tier": token.last_alerted_tier,
            "ath_source":        token.ath_source,
            "pool_orientation":  token.pool_orientation,
            "token_decimals":    token.token_decimals,
            "phantom_cooldown_until": token.phantom_cooldown_until,
            "ghost_cooldown_until": token.ghost_cooldown_until,
        })
        await db.commit()


async def load_all_tokens(db_path: str = DB_PATH) -> list[TrackedToken]:
    """Load all active tokens (excludes EXPIRED and BLOCKED — both terminal states)."""
    if not os.path.exists(db_path):
        return []
    tokens = []
    async with db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tokens WHERE status NOT IN ('expired', 'blocked')"
        ) as cursor:
            async for row in cursor:
                tokens.append(_row_to_token(row))
    return tokens


async def get_token(address: str, db_path: str = DB_PATH) -> TrackedToken | None:
    if not os.path.exists(db_path):
        return None
    async with db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tokens WHERE address = ?", (address,)
        ) as cursor:
            row = await cursor.fetchone()
            return _row_to_token(row) if row else None


async def token_exists(address: str, db_path: str = DB_PATH) -> bool:
    return await get_token(address, db_path) is not None


# ── Alert History ─────────────────────────────────────────────────────────────

async def save_alert(
    address: str,
    symbol: str,
    tier_index: int,
    tier_name: str,
    alert_price: float,
    alert_mcap: float,
    ath_price: float,
    ath_mcap: float,
    alert_time: float | None = None,
    db_path: str = DB_PATH,
):
    """Save an alert record when a dip alert fires."""
    ts = alert_time if alert_time is not None else time.time()
    async with db_connect(db_path) as db:
        await db.execute("""
            INSERT INTO alerts (
                address, symbol, tier_index, tier_name,
                alert_price, alert_mcap, ath_price, ath_mcap,
                peak_price_after, peak_mcap_after, alert_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            address, symbol, tier_index, tier_name,
            alert_price, alert_mcap, ath_price, ath_mcap,
            alert_price, alert_mcap, ts,
        ))
        await db.commit()


async def update_peak_after_alert(
    address: str,
    current_price: float,
    current_mcap: float,
    current_time: float | None = None,
    db_path: str = DB_PATH,
):
    """Update peak_price_after for all alerts on this token if current
    price is higher. Also records peak_time and time_to_peak_minutes
    when current_time is provided. current_time defaults to now() so
    callers that predate the timing fields keep working."""
    ts = current_time if current_time is not None else time.time()
    async with db_connect(db_path) as db:
        # alert_time is a column ref, not a param — each row computes its
        # own duration from its own alert_time (different tiers on the
        # same token fire at different times).
        await db.execute("""
            UPDATE alerts
            SET peak_price_after     = ?,
                peak_mcap_after      = ?,
                peak_time            = ?,
                time_to_peak_minutes = (? - alert_time) / 60.0
            WHERE address = ?
              AND peak_price_after < ?
        """, (
            current_price, current_mcap, ts, ts,
            address, current_price,
        ))
        await db.commit()


async def update_trough_after_alert(
    address: str,
    current_price: float,
    current_mcap: float,
    current_time: float,
    db_path: str = DB_PATH,
):
    """Update trough fields for all alerts on this token if current price
    is lower than recorded trough (or trough is NULL — first write after
    migration). max_drawdown_pct uses the row's alert_price column, not
    a bound param, so different-tier alerts on the same token compute
    their own drawdown."""
    async with db_connect(db_path) as db:
        await db.execute("""
            UPDATE alerts
            SET trough_price_after = ?,
                trough_mcap_after  = ?,
                trough_time        = ?,
                max_drawdown_pct   = 1.0 - (? / alert_price)
            WHERE address = ?
              AND (trough_price_after IS NULL OR trough_price_after > ?)
        """, (
            current_price, current_mcap, current_time,
            current_price, address, current_price,
        ))
        await db.commit()


async def get_alerts_since(
    since_time: float,
    db_path: str = DB_PATH,
) -> list[dict]:
    """Get all alerts fired since a given timestamp."""
    if not os.path.exists(db_path):
        return []
    async with db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts WHERE alert_time >= ? ORDER BY alert_time DESC",
            (since_time,),
        ) as cursor:
            return [dict(row) async for row in cursor]


async def get_all_alerts(db_path: str = DB_PATH) -> list[dict]:
    """Get all alert records."""
    if not os.path.exists(db_path):
        return []
    async with db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts ORDER BY alert_time DESC"
        ) as cursor:
            return [dict(row) async for row in cursor]


async def get_alerts_for_token(
    address: str,
    db_path: str = DB_PATH,
) -> list[dict]:
    """Get all alerts for a specific token."""
    if not os.path.exists(db_path):
        return []
    async with db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts WHERE address = ? ORDER BY alert_time ASC",
            (address,),
        ) as cursor:
            return [dict(row) async for row in cursor]


def _row_to_token(row) -> TrackedToken:
    # ath_source may be missing on very old rows that predate the migration
    # (shouldn't happen since init_db always runs first, but defensive).
    try:
        ath_source = row["ath_source"] or "unseeded"
    except (IndexError, KeyError):
        ath_source = "unseeded"

    # pool_orientation / token_decimals were added after ath_source; defend
    # against rows loaded via legacy codepaths that predate the migration.
    try:
        pool_orientation = row["pool_orientation"]
    except (IndexError, KeyError):
        pool_orientation = None

    try:
        token_decimals = row["token_decimals"]
    except (IndexError, KeyError):
        token_decimals = None

    # phantom_cooldown_until added in Phase-2 phantom-dip fix; defend
    # against rows loaded before the migration ran (edge case: tests
    # using legacy fixtures).
    try:
        phantom_cooldown_until = row["phantom_cooldown_until"] or 0.0
    except (IndexError, KeyError):
        phantom_cooldown_until = 0.0

    # ghost_cooldown_until added with the ghost-block alert-loop fix;
    # defend against rows loaded before the migration ran.
    try:
        ghost_cooldown_until = row["ghost_cooldown_until"] or 0.0
    except (IndexError, KeyError):
        ghost_cooldown_until = 0.0

    return TrackedToken(
        address           = row["address"],
        symbol            = row["symbol"] or "???",
        pool_address      = row["pool_address"] or "",
        status            = TokenStatus(row["status"]),
        migration_price   = row["migration_price"] or 0.0,
        migration_mcap    = row["migration_mcap"] or 0.0,
        current_price     = row["current_price"] or 0.0,
        current_mcap      = row["current_mcap"] or 0.0,
        liquidity_usd     = row["liquidity_usd"] or 0.0,
        ath_price         = row["ath_price"] or 0.0,
        ath_mcap          = row["ath_mcap"] or 0.0,
        ath_time          = row["ath_time"] or 0.0,
        volume_1h         = row["volume_1h"] or 0.0,
        volume_6h         = row["volume_6h"] or 0.0,
        volume_24h        = row["volume_24h"] or 0.0,
        migration_time    = row["migration_time"] or 0.0,
        last_price_update = row["last_price_update"] or 0.0,
        last_alerted_tier = row["last_alerted_tier"] if row["last_alerted_tier"] is not None else -1,
        ath_source        = ath_source,
        pool_orientation  = pool_orientation,
        token_decimals    = token_decimals,
        phantom_cooldown_until = phantom_cooldown_until,
        ghost_cooldown_until = ghost_cooldown_until,
    )


# ── PumpSwap Fees ────────────────────────────────────────────────────────────

async def save_pumpswap_fee(
    signature: str,
    slot: int,
    block_time: float | None,
    pool_address: str,
    token_address: str | None,
    event_type: str,
    lp_fee: int,
    protocol_fee: int,
    creator_fee: int = 0,
    db_path: str = DB_PATH,
) -> bool:
    """
    Save a decoded PumpSwap BuyEvent/SellEvent fee record.
    Returns True if inserted, False if it was a duplicate signature.
    """
    import time as _time
    total_fee = lp_fee + protocol_fee + creator_fee
    try:
        async with db_connect(db_path) as db:
            await db.execute("""
                INSERT INTO pumpswap_fees (
                    signature, slot, block_time, pool_address, token_address,
                    event_type, lp_fee, protocol_fee, creator_fee, total_fee, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signature, slot, block_time, pool_address, token_address,
                event_type, lp_fee, protocol_fee, creator_fee, total_fee, _time.time(),
            ))
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False


async def get_pool_fees_total(
    pool_address: str,
    db_path: str = DB_PATH,
) -> dict:
    """
    Sum all fees recorded for a given pool.
    Returns dict with lp, proto, creator, total (lamports), event_count, total_sol.
    """
    if not os.path.exists(db_path):
        return {
            "total_lp_fee": 0, "total_protocol_fee": 0, "total_creator_fee": 0,
            "total_fee": 0, "event_count": 0, "total_fee_sol": 0.0,
        }
    async with db_connect(db_path) as db:
        async with db.execute("""
            SELECT
                COALESCE(SUM(lp_fee), 0)       AS lp,
                COALESCE(SUM(protocol_fee), 0) AS proto,
                COALESCE(SUM(creator_fee), 0)  AS creator,
                COALESCE(SUM(total_fee), 0)    AS total,
                COUNT(*)                        AS cnt
            FROM pumpswap_fees
            WHERE pool_address = ?
        """, (pool_address,)) as cursor:
            row = await cursor.fetchone()
    return {
        "total_lp_fee":       row[0],
        "total_protocol_fee": row[1],
        "total_creator_fee":  row[2],
        "total_fee":          row[3],
        "event_count":        row[4],
        "total_fee_sol":      row[3] / 1_000_000_000,
    }


async def get_median_priority_fee(
    pool_address: str,
    before_time: float,
    n: int = 20,
    db_path: str = DB_PATH,
) -> tuple[float | None, int]:
    """
    Median priority_fee (lamports) over the last `n` buy-side swaps for
    `pool_address` with block_time <= `before_time`. Returns
    (median, count_of_non_null_rows). Returns (None, 0) if no data.

    Observe-only Ante v2 feature — logged, not gated.
    """
    if not os.path.exists(db_path):
        return (None, 0)
    async with db_connect(db_path) as db:
        async with db.execute(
            """
            SELECT priority_fee
            FROM pumpswap_fees
            WHERE pool_address = ?
              AND event_type = 'buy'
              AND block_time <= ?
              AND priority_fee IS NOT NULL
            ORDER BY block_time DESC
            LIMIT ?
            """,
            (pool_address, before_time, n),
        ) as cursor:
            rows = await cursor.fetchall()
    if not rows:
        return (None, 0)
    values = sorted(r[0] for r in rows)
    count = len(values)
    mid = count // 2
    if count % 2 == 1:
        median = float(values[mid])
    else:
        median = (values[mid - 1] + values[mid]) / 2.0
    return (median, count)


async def save_pumpswap_fees_batch(
    records: list[dict],
    db_path: str = DB_PATH,
) -> int:
    """
    Insert a batch of pumpswap_fees rows in a single transaction.
    Returns the number of rows actually inserted.
    """
    if not records:
        return 0
    import time as _time
    now = _time.time()
    rows = [
        (
            r["signature"],
            r["slot"],
            r.get("block_time"),
            r["pool_address"],
            r.get("token_address"),
            r["event_type"],
            r.get("quote_amount", 0),
            r["lp_fee"],
            r["protocol_fee"],
            r.get("creator_fee", 0),
            r["lp_fee"] + r["protocol_fee"] + r.get("creator_fee", 0),
            now,
            r.get("priority_fee"),
            r.get("jito_tip"),
            r.get("compute_units_consumed"),
            r.get("user_pubkey"),
            r.get("base_fee"),         # Ante Phase 1: tx-level, first-event-row only
            r.get("signature_count"),  # Ante Phase 1: tx-level, first-event-row only
            r.get("base_amount"),
        )
        for r in records
    ]
    async with db_connect(db_path) as db:
        cursor = await db.executemany("""
            INSERT OR IGNORE INTO pumpswap_fees (
                signature, slot, block_time, pool_address, token_address,
                event_type, quote_amount, lp_fee, protocol_fee, creator_fee, total_fee, received_at,
                priority_fee, jito_tip, compute_units_consumed, user_pubkey,
                base_fee, signature_count, base_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        await db.commit()
        return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0


async def prune_old_pumpswap_fees(
    retention_hours: int = 48,
    db_path: str = DB_PATH,
    chunk_size: int = 10000,
) -> int:
    """Delete pumpswap_fees rows older than retention_hours, in
    chunks to avoid long exclusive locks that starve concurrent
    writers. Returns total rows deleted."""
    cutoff = time.time() - (retention_hours * 3600)
    total_deleted = 0
    MAX_CHUNKS = 200  # 2M-row ceiling per prune cycle
    for _ in range(MAX_CHUNKS):
        async with db_connect(db_path) as db:
            cursor = await db.execute(
                "DELETE FROM pumpswap_fees "
                "WHERE id IN ("
                "  SELECT id FROM pumpswap_fees "
                "  WHERE received_at < ? LIMIT ?"
                ")",
                (cutoff, chunk_size),
            )
            await db.commit()
            deleted = cursor.rowcount or 0
        total_deleted += deleted
        logger.debug(
            f"prune chunk: deleted={deleted} "
            f"cumulative={total_deleted}"
        )
        if deleted < chunk_size:
            break
        await asyncio.sleep(0.1)  # yield to concurrent writers
    else:
        logger.warning(
            f"prune hit MAX_CHUNKS={MAX_CHUNKS}; "
            f"partial: {total_deleted}"
        )
    return total_deleted


# ── Fee Gate shadow log ──────────────────────────────────────────────────────

async def log_fee_gate(
    token_address: str,
    symbol: str,
    alert_tier: int,
    tier_name: str,
    alert_time: float,
    total_fee: float,
    lp_fee: float,
    proto_fee: float,
    creator_fee: float,
    rate: float,
    events: int,
    creator_share: float,
    proto_share: float,
    fee_per_event: float,
    proto_to_lp: float,
    score: int,
    flags: list,
    label: str,
    db_path: str = DB_PATH,
) -> int:
    """Insert one fee_gate_log row. Returns the inserted row's id (lastrowid)."""
    async with db_connect(db_path) as db:
        cursor = await db.execute("""
            INSERT INTO fee_gate_log (
                token_address, symbol, alert_tier, tier_name, alert_time,
                total_fee, lp_fee, proto_fee, creator_fee, rate, events,
                creator_share, proto_share, fee_per_event, proto_to_lp,
                score, flags, label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_address, symbol, alert_tier, tier_name, alert_time,
            total_fee, lp_fee, proto_fee, creator_fee, rate, events,
            creator_share, proto_share, fee_per_event, proto_to_lp,
            score, ",".join(flags) if flags else "", label,
        ))
        await db.commit()
        return cursor.lastrowid


async def log_alert_block(
    token_address: str,
    symbol: str,
    would_have_tier: int,
    tier_name: str,
    block_time: float,
    block_reason: str,
    fee_gate_log_id: int | None = None,
    no_fee_data: bool = False,
    db_path: str = DB_PATH,
) -> int:
    """UPSERT one alert_block_log row, keyed on (token_address,
    would_have_tier, block_reason). First block creates the row; subsequent
    retries bump retry_count and last_retry_at while preserving the original
    block_time as the first-seen timestamp."""
    async with db_connect(db_path) as db:
        cursor = await db.execute("""
            INSERT INTO alert_block_log (
                token_address, symbol, would_have_tier, tier_name, block_time,
                block_reason, fee_gate_log_id, no_fee_data,
                retry_count, last_retry_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(token_address, would_have_tier, block_reason)
            DO UPDATE SET
                retry_count  = retry_count + 1,
                last_retry_at = excluded.last_retry_at
        """, (
            token_address, symbol, would_have_tier, tier_name, block_time,
            block_reason, fee_gate_log_id, 1 if no_fee_data else 0,
            block_time,
        ))
        await db.commit()
        return cursor.lastrowid


async def mark_token_blocked(address: str, db_path: str = DB_PATH):
    """Set status=BLOCKED for a token (terminal state). Bumps last_price_update."""
    async with db_connect(db_path) as db:
        await db.execute(
            "UPDATE tokens SET status = ?, last_price_update = ? WHERE address = ?",
            (TokenStatus.BLOCKED.value, time.time(), address),
        )
        await db.commit()


# ── LP Floor shadow log ──────────────────────────────────────────────────────

async def log_lp_floor(
    token_address: str,
    symbol: str,
    alert_tier: int,
    tier_name: str,
    alert_time: float,
    liquidity_usd: float,
    label: str,
    reason: str,
    db_path: str = DB_PATH,
):
    async with db_connect(db_path) as db:
        await db.execute("""
            INSERT INTO lp_floor_log (
                token_address, symbol, alert_tier, tier_name, alert_time,
                liquidity_usd, label, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_address, symbol, alert_tier, tier_name, alert_time,
            liquidity_usd, label, reason,
        ))
        await db.commit()

# ── Ante shadow log ──────────────────────────────────────────────────────────

async def log_ante(
    token_address: str,
    symbol: str,
    alert_tier: int,
    tier_name: str,
    alert_time: float,
    n20_count: int,
    n20_median: float | None,
    n20_p25: float | None,
    n20_p75: float | None,
    n20_width: float | None,
    m5_count: int,
    m5_median: float | None,
    m5_p25: float | None,
    m5_p75: float | None,
    m5_width: float | None,
    base_fee_coverage: float,
    label_5m: str | None = None,
    rule_hit_5m: int | None = None,
    label_20sw: str | None = None,
    rule_hit_20sw: int | None = None,
    median_priority_fee: float | None = None,
    priority_fee_n: int | None = None,
    db_path: str = DB_PATH,
):
    """Insert one ante_log row. Observe-only shadow mode — never gates."""
    async with db_connect(db_path) as db:
        await db.execute("""
            INSERT INTO ante_log (
                token_address, symbol, alert_tier, tier_name, alert_time,
                ante_n20_count, ante_n20_median_sol, ante_n20_p25_sol, ante_n20_p75_sol,
                ante_n20_width_ratio,
                ante_5m_count, ante_5m_median_sol, ante_5m_p25_sol, ante_5m_p75_sol,
                ante_5m_width_ratio,
                base_fee_coverage,
                label_5m, rule_hit_5m, label_20sw, rule_hit_20sw,
                median_priority_fee, priority_fee_n
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_address, symbol, alert_tier, tier_name, alert_time,
            n20_count, n20_median, n20_p25, n20_p75, n20_width,
            m5_count, m5_median, m5_p25, m5_p75, m5_width,
            base_fee_coverage,
            label_5m, rule_hit_5m, label_20sw, rule_hit_20sw,
            median_priority_fee, priority_fee_n,
        ))
        await db.commit()


# ── Phantom Validator log ────────────────────────────────────────────────────

async def log_phantom_validation(
    log_data: dict,
    db_path: str = DB_PATH,
) -> int:
    """Insert one phantom_abort_log row.

    `log_data` is the dict returned by phantom_validator.validate_*. Both
    phantom-positive AND phantom-negative validations must be logged so the
    week-1 review can compute the true-positive / true-negative ratio.
    Returns the inserted row's id (lastrowid)."""
    async with db_connect(db_path) as db:
        cursor = await db.execute("""
            INSERT INTO phantom_abort_log (
                token_address, symbol, ath_update_time,
                ath_price, ath_mcap, ath_source,
                birdeye_current_price, birdeye_current_mcap,
                dex_current_price, dex_current_mcap,
                birdeye_to_ath_ratio, dex_to_ath_ratio,
                is_phantom, cooldown_until, birdeye_error, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            log_data.get("token_address"),
            log_data.get("symbol"),
            log_data.get("ath_update_time"),
            log_data.get("ath_price"),
            log_data.get("ath_mcap"),
            log_data.get("ath_source"),
            log_data.get("birdeye_current_price"),
            log_data.get("birdeye_current_mcap"),
            log_data.get("dex_current_price"),
            log_data.get("dex_current_mcap"),
            log_data.get("birdeye_to_ath_ratio"),
            log_data.get("dex_to_ath_ratio"),
            int(bool(log_data.get("is_phantom"))),
            log_data.get("cooldown_until"),
            log_data.get("birdeye_error"),
            log_data.get("created_at") or time.time(),
        ))
        await db.commit()
        return cursor.lastrowid


async def get_worst_fee_gate_label(
    token_address: str,
    db_path: str = DB_PATH,
) -> tuple[int, str]:
    """
    Return the highest (worst) fee gate score ever recorded for this token.
    Returns (max_score, worst_label). Returns (0, 'Normal') if no history.
    """
    if not os.path.exists(db_path):
        return 0, "Normal"
    async with db_connect(db_path) as db:
        async with db.execute(
            "SELECT MAX(score), label FROM fee_gate_log WHERE token_address = ? ORDER BY score DESC LIMIT 1",
            (token_address,),
        ) as cursor:
            row = await cursor.fetchone()
    if row and row[0] is not None:
        return row[0], row[1]
    return 0, "Normal"


# ── Fast-Dip Detector shadow log ─────────────────────────────────────────────

async def log_fast_dip_trigger(
    token_address: str,
    pool_address: str | None,
    symbol: str | None,
    trigger_block_time: float,
    trigger_wall_time: float,
    trigger_signature: str | None,
    trigger_event_type: str | None,
    trigger_price_sol: float,
    rolling_max_price_sol: float,
    rolling_max_block_time: float,
    drop_pct: float,
    swap_density_5s: int,
    trigger_lag_seconds: float,
    db_path: str = DB_PATH,
) -> int:
    """Insert one fast_dip_shadow row at trigger time (Stage 1+2).

    Returns the inserted row's id so the caller can hand it to
    update_fast_dip_dip_end() when the dip episode resolves and to
    update_fast_dip_decision() when the +10s decision fires.

    `trigger_lag_seconds` (= wall_clock - block_time at trigger) is set
    once at INSERT and never modified by the +10s decision UPDATE.
    """
    async with db_connect(db_path) as db:
        cursor = await db.execute("""
            INSERT INTO fast_dip_shadow (
                token_address, pool_address, symbol,
                trigger_block_time, trigger_wall_time,
                trigger_signature, trigger_event_type,
                trigger_price_sol, rolling_max_price_sol,
                rolling_max_block_time, drop_pct,
                swap_density_5s, trigger_lag_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_address, pool_address, symbol,
            trigger_block_time, trigger_wall_time,
            trigger_signature, trigger_event_type,
            trigger_price_sol, rolling_max_price_sol,
            rolling_max_block_time, drop_pct,
            swap_density_5s, trigger_lag_seconds,
        ))
        await db.commit()
        return cursor.lastrowid


async def update_fast_dip_dip_end(
    row_id: int,
    dip_end_block_time: float,
    dip_end_reason: str,
    db_path: str = DB_PATH,
):
    """Stamp the dip-end columns on an existing fast_dip_shadow row.

    `dip_end_reason` is one of: 'recovered' (live drop fell below 0.20),
    'gap' (no swaps for 60s), 'evicted' (token bumped out of memory cap).
    """
    async with db_connect(db_path) as db:
        await db.execute(
            "UPDATE fast_dip_shadow SET dip_end_block_time = ?, dip_end_reason = ? "
            "WHERE id = ?",
            (dip_end_block_time, dip_end_reason, row_id),
        )
        await db.commit()


async def update_fast_dip_decision(
    row_id: int,
    decision_block_time: float,
    decision_wall_time: float,
    depth_at_10s: float | None,
    depth_velocity_10s: float | None,
    swap_count_10s: int,
    buy_sell_ratio_10s: float | None,
    pre_dip_1m_usd_vol: float | None,
    pre_dip_1m_swap_count: int,
    suppressions: str | None,
    would_alert: int,
    db_path: str = DB_PATH,
):
    """Stamp the +10s decision-side columns on a fast_dip_shadow row (Stage 2).

    Does NOT touch trigger_lag_seconds (set at INSERT and immutable here).
    `suppressions` is a comma-separated list of fired rule ids, or NULL
    if no rules fired. `would_alert` is 1 when zero rules fired, else 0.
    """
    async with db_connect(db_path) as db:
        await db.execute(
            """
            UPDATE fast_dip_shadow
            SET decision_block_time   = ?,
                decision_wall_time    = ?,
                depth_at_10s          = ?,
                depth_velocity_10s    = ?,
                swap_count_10s        = ?,
                buy_sell_ratio_10s    = ?,
                pre_dip_1m_usd_vol    = ?,
                pre_dip_1m_swap_count = ?,
                suppressions          = ?,
                would_alert           = ?
            WHERE id = ?
            """,
            (
                decision_block_time, decision_wall_time,
                depth_at_10s, depth_velocity_10s,
                swap_count_10s, buy_sell_ratio_10s,
                pre_dip_1m_usd_vol, pre_dip_1m_swap_count,
                suppressions, would_alert,
                row_id,
            ),
        )
        await db.commit()


# ── ATH retry queue persistence ──────────────────────────────────────────────

async def upsert_ath_retry(
    address: str,
    queued_at: float,
    last_attempt: float,
    first_success_at: float | None = None,
    db_path: str = DB_PATH,
):
    """Mirror one in-memory _ath_retry_queue entry to disk. INSERT OR
    REPLACE so the 2-field → 3-field transition (NULL → first_success_at
    on first Birdeye success) is a single atomic write."""
    async with db_connect(db_path) as db:
        await db.execute(
            "INSERT OR REPLACE INTO ath_retry_queue "
            "(address, queued_at, last_attempt, first_success_at) "
            "VALUES (?, ?, ?, ?)",
            (address, queued_at, last_attempt, first_success_at),
        )
        await db.commit()


async def delete_ath_retry(address: str, db_path: str = DB_PATH):
    """Remove one address from the persisted retry queue."""
    async with db_connect(db_path) as db:
        await db.execute(
            "DELETE FROM ath_retry_queue WHERE address = ?",
            (address,),
        )
        await db.commit()


async def load_ath_retry_queue(db_path: str = DB_PATH) -> dict[str, dict]:
    """Load all persisted retry-queue rows into the in-memory shape used
    by modules.ath_seeder._ath_retry_queue. Polymorphic on first_success_at:
    rows with first_success_at IS NULL come back as 2-field dicts (no key
    at all), matching the read site at ath_seeder which uses .get() and
    treats absence as "not in reseed mode."""
    if not os.path.exists(db_path):
        return {}
    out: dict[str, dict] = {}
    async with db_connect(db_path) as db:
        async with db.execute(
            "SELECT address, queued_at, last_attempt, first_success_at "
            "FROM ath_retry_queue"
        ) as cursor:
            async for row in cursor:
                address, queued_at, last_attempt, first_success_at = row
                entry: dict = {
                    "queued_at": queued_at,
                    "last_attempt": last_attempt,
                }
                if first_success_at is not None:
                    entry["first_success_at"] = first_success_at
                out[address] = entry
    return out

"""
modules/telegram_sender.py
Formats and sends dip alert messages to Telegram.
Also handles daily performance recap and command polling.
Also runs Fee Gate shadow-mode scoring and appends label to alerts.
"""

import aiohttp
import asyncio
import logging
import sqlite3
import aiosqlite
import time
from dataclasses import dataclass, field
from models import TrackedToken
from filters.fee_gate import score_fees, enforce_sticky
from filters.lp_floor import score_lp
from modules.ante_taxonomy import classify_both_windows
from holder_filter import GHOST_CAUTION_WALLETS, GHOST_CAUTION_CLUSTERS
import database

DB_PATH = "data/bot.db"
LAMPORTS_PER_SOL = 1_000_000_000

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}"


@dataclass
class FeeEvaluation:
    """
    Single source of truth for one fee evaluation pass.
    Produced by TelegramSender.evaluate_fees(); consumed by _format_alert (render)
    and by alert_gate (block decision in Step 4).
    """
    score: int = 0
    flags: list = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    label: str = ""
    log_id: int | None = None
    is_empty: bool = True            # True when no fee data OR fee_gate disabled OR scoring failed
    raw_fee: dict | None = None      # raw _fetch_fee_stats() result; needed by Participation render


# ── Ante Phase 1.1 width-ratio constants ────────────────────────────────────
# Floor used as denominator when p25 is dust (or zero) — 1 lamport in SOL.
# Cap bounds outliers so a single huge swap can't push the stored ratio into
# the millions.  Both numbers are "policy" — change them in one place.
ANTE_WIDTH_FLOOR_SOL = 1e-9
ANTE_WIDTH_CAP = 10000.0


def _compute_width_ratio(p25: float | None, p75: float | None) -> float | None:
    """
    Width ratio = p75 / max(p25, FLOOR), capped at ANTE_WIDTH_CAP.
    Returns None if either input is None (no samples in window).
    A flat distribution (p25 == p75) returns 1.0; a token where every sampled
    swap pays the same dust amount returns 0.0 (when both p25 and p75 are zero).
    """
    if p25 is None or p75 is None:
        return None
    denom = max(p25, ANTE_WIDTH_FLOOR_SOL)
    ratio = p75 / denom
    if ratio > ANTE_WIDTH_CAP:
        return ANTE_WIDTH_CAP
    return ratio


def fmt_mcap(v: float) -> str:
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v / 1_000:.0f}k"
    else:
        return f"${v:.0f}"


def _get_block_stats(since: float) -> dict:
    """
    Block counts + resolution stats for alert_block_log entries since `since`.
    Returns keys: total, scam_likely, no_fee_data, resolved, pending.

    resolved = no_fee_data blocks whose token later produced a fee_gate_log row
               (i.e. fee data arrived post-block — the transient block was correct).
    pending  = no_fee_data blocks still awaiting any fee_gate_log entry for that token.
    """
    empty = {"total": 0, "scam_likely": 0, "no_fee_data": 0, "resolved": 0, "pending": 0}
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
        by_reason = {
            row[0]: row[1]
            for row in conn.execute("""
                SELECT block_reason, COUNT(*) AS cnt
                FROM alert_block_log
                WHERE block_time > ?
                GROUP BY block_reason
            """, (since,)).fetchall()
        }
        resolution = conn.execute("""
            SELECT
                SUM(CASE WHEN EXISTS (
                    SELECT 1 FROM fee_gate_log fgl
                    WHERE fgl.token_address = abl.token_address
                      AND fgl.alert_time > abl.block_time
                ) THEN 1 ELSE 0 END) AS resolved,
                SUM(CASE WHEN NOT EXISTS (
                    SELECT 1 FROM fee_gate_log fgl
                    WHERE fgl.token_address = abl.token_address
                      AND fgl.alert_time > abl.block_time
                ) THEN 1 ELSE 0 END) AS pending
            FROM alert_block_log abl
            WHERE abl.no_fee_data = 1
              AND abl.block_time > ?
        """, (since,)).fetchone()
        conn.close()
        return {
            "total":       sum(by_reason.values()),
            "scam_likely": by_reason.get("SCAM Likely", 0),
            "no_fee_data": by_reason.get("no_fee_data", 0),
            "resolved":    (resolution[0] if resolution else 0) or 0,
            "pending":     (resolution[1] if resolution else 0) or 0,
        }
    except Exception as e:
        logger.warning(f"_get_block_stats query failed: {e}")
        return empty


def _get_blocked_addrs(token_addresses: set) -> set:
    """
    Query tokens table for addresses currently in status='blocked'.
    Returns set of blocked addresses. Used by the daily recap to exclude
    blocked tokens from bounce stats (alongside the SCAM-label exclusion).
    """
    if not token_addresses:
        return set()
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
        placeholders = ",".join("?" for _ in token_addresses)
        rows = conn.execute(
            f"SELECT address FROM tokens WHERE status = 'blocked' AND address IN ({placeholders})",
            list(token_addresses),
        ).fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception as e:
        logger.warning(f"_get_blocked_addrs query failed: {e}")
        return set()


def _get_fee_gate_labels(token_addresses: set, since: float) -> dict:
    """
    Query fee_gate_log for the worst label per token address.
    Returns {address: worst_label} e.g. {'abc...': 'SCAM Likely', 'def...': 'Suspicious'}
    Only looks at tokens in the given set and alerts since the given timestamp.
    """
    if not token_addresses:
        return {}
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
        conn.row_factory = sqlite3.Row
        placeholders = ",".join("?" for _ in token_addresses)
        rows = conn.execute(f"""
            SELECT token_address, MAX(score) as max_score, label
            FROM fee_gate_log
            WHERE token_address IN ({placeholders})
            GROUP BY token_address
            ORDER BY max_score DESC
        """, list(token_addresses)).fetchall()
        conn.close()

        result = {}
        if rows:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
            for row in rows:
                addr = row["token_address"]
                max_score = row["max_score"]
                label_row = conn.execute(
                    "SELECT label FROM fee_gate_log WHERE token_address = ? AND score = ? LIMIT 1",
                    (addr, max_score),
                ).fetchone()
                if label_row:
                    result[addr] = label_row[0]
            conn.close()
        return result
    except Exception as e:
        logger.warning(f"fee_gate_log query failed: {e}")
        return {}


def _volume_label(vol_1h: float, vol_6h: float) -> str:
    avg_1h = vol_6h / 6 if vol_6h > 0 else 0
    if avg_1h > 0 and vol_1h > avg_1h * 2.0:
        return "Spiking"
    if avg_1h > 0 and vol_1h > avg_1h * 1.3:
        return "Active"
    if vol_1h > 0:
        return "Cooling"
    return "Quiet"


def _lp_state(liquidity_usd: float, cfg: dict | None = None) -> str:
    cfg = cfg or {}
    min_lp = cfg.get("min_liquidity_usd", 8_000)
    warn_lp = cfg.get("warn_liquidity_usd", 15_000)
    if liquidity_usd < min_lp:
        return "Low"
    if liquidity_usd < warn_lp:
        return "Thin"
    if liquidity_usd < 40_000:
        return "Tradable"
    return "Healthy"


def _builder_read(tier_name: str, vol_label: str, fee_label: str, lp_state: str,
                  sol_per_min: float = 0.0, ev_per_min: float = 0.0) -> list:
    lines = []

    # ── Line 1: sequence position ──────────────────────────────
    seq = {
        "Tier 1": "Early-sequence stress state",
        "Tier 2": "Mid-sequence stress state",
        "Tier 3": "Late-sequence stress state",
    }
    lines.append(seq.get(tier_name, "Stress state"))

    # ── Normalize inputs ───────────────────────────────────────
    fl = (fee_label or "").upper()
    lps = (lp_state or "").lower()

    if "SCAM" in fl:
        fee_phrase = "fee structure is extractive"
    elif "SUSPICIOUS" in fl:
        fee_phrase = "structure is compromised"
    elif "ELEVATED" in fl:
        fee_phrase = "structure requires caution"
    else:
        fee_phrase = "clean fee structure"

    if vol_label in ("Spiking", "Exploding"):
        attn_phrase = "Attention present"
    elif vol_label in ("Active", "Cooling"):
        attn_phrase = "Participation still present"
    else:
        attn_phrase = "Attention is weak"

    # ── Line 2: attention × structure ──────────────────────────
    if "SCAM" in fl or "SUSPICIOUS" in fl:
        lines.append(f"{attn_phrase}, but {fee_phrase}")
    elif "ELEVATED" in fl:
        lines.append(f"{attn_phrase}, {fee_phrase}")
    else:
        # Clean fee structure — never imply safety alone
        if attn_phrase == "Attention is weak":
            lines.append(f"{fee_phrase}, but attention is weak")
        else:
            lines.append(f"{attn_phrase} with {fee_phrase}")

    # ── Line 3: fragility / risk (always different dimension) ──
    normal_fees = not ("SCAM" in fl or "SUSPICIOUS" in fl or "ELEVATED" in fl)

    if normal_fees and lps == "low":
        # CRITICAL RULE
        lines.append("Pool is highly fragile, failure risk elevated")
    elif lps == "low":
        lines.append("Pool is highly fragile, collapse risk is severe")
    elif lps == "thin":
        if "SCAM" in fl or "SUSPICIOUS" in fl:
            lines.append("Pool fragility increases collapse risk")
        else:
            lines.append("Pool fragility is elevated")
    elif lps == "tradable":
        if "SCAM" in fl:
            lines.append("Reflex bounce possible, durability likely low")
        else:
            lines.append("Liquidity tradable, watch follow-through")
    else:  # healthy or unknown
        if "SCAM" in fl:
            lines.append("Reflex bounce possible, durability likely low")
        elif sol_per_min >= 1.0 or ev_per_min >= 30:
            lines.append("High activity velocity, attention is real-time")
        else:
            lines.append("Structure holding, watch follow-through")

    return lines


class TelegramSender:
    def __init__(self, config: dict):
        self.bot_token = config["telegram"]["bot_token"]
        self.chat_id = config["telegram"]["chat_id"]
        self.fee_gate_cfg = config.get("fee_gate", {"enabled": False})
        self.lp_floor_cfg = config.get("lp_floor", {"enabled": False})
        # Ante Phase 1: observe-only. mode=="observe" is the only supported
        # value today — anything else is treated as disabled to forbid gating.
        self.ante_cfg = config.get("ante", {"enabled": False, "mode": "observe"})
        # Ante Taxonomy (ghost mode): categorical labels over numeric Ante.
        # Absent block → empty dict → classification silently skipped.
        self.ante_taxonomy_cfg = config.get("ante_taxonomy", {})
        self._last_update_id = 0  # for command polling

    async def send_dip_alert(
        self,
        token: TrackedToken,
        tier: dict,
        session: aiohttp.ClientSession,
        ghost_filter_result: dict | None = None,
        fee_eval: FeeEvaluation | None = None,
    ) -> bool:
        """Format and send a dip alert. Returns True only when Telegram accepts it."""
        msg = await self._format_alert(
            token, tier,
            ghost_filter_result=ghost_filter_result,
            fee_eval=fee_eval,
        )
        return await self._send(msg, session)

    def _fetch_fee_stats(self, address: str, migration_time: float | None) -> dict | None:
        """Pull lifetime AMM + bribe aggregates for a token from pumpswap_fees."""
        import sqlite3
        try:
            conn = sqlite3.connect(
                f"file:{database.DB_PATH}?mode=ro",
                uri=True,
                timeout=2.0,
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    COALESCE(SUM(lp_fee), 0)        / 1e9  AS lp_sol,
                    COALESCE(SUM(protocol_fee), 0)  / 1e9  AS proto_sol,
                    COALESCE(SUM(creator_fee), 0)   / 1e9  AS creator_sol,
                    COALESCE(SUM(total_fee), 0)     / 1e9  AS total_sol,
                    COALESCE(SUM(priority_fee), 0)  / 1e9  AS priority_sol,
                    COALESCE(SUM(jito_tip), 0)      / 1e9  AS jito_sol,
                    COUNT(*)                               AS events,
                    COUNT(DISTINCT signature)              AS tx_count,
                    MIN(block_time)                        AS first_seen,
                    MAX(block_time)                        AS last_seen
                FROM pumpswap_fees
                WHERE token_address = ?
            """, (address,))
            row = cur.fetchone()

            import time as _t
            cur.execute("""
                SELECT COALESCE(SUM(total_fee), 0)/1e9, COUNT(*)
                FROM pumpswap_fees
                WHERE token_address = ? AND block_time >= ?
            """, (address, _t.time() - 300))
            pace = cur.fetchone()
            conn.close()

            if not row or row[6] == 0:
                return None

            return {
                "lp_sol": row[0],
                "proto_sol": row[1],
                "creator_sol": row[2],
                "total_sol": row[3],
                "priority_sol": row[4],
                "jito_sol": row[5],
                "bribes_sol": row[4] + row[5],
                "events": row[6],
                "tx_count": row[7],
                "first_seen": row[8],
                "last_seen": row[9],
                "sol_per_min": pace[0] / 5.0,
                "ev_per_min":  pace[1] / 5.0,
            }
        except Exception as e:
            logger.warning(f"_fetch_fee_stats failed for {address[:8]}: {e}")
            return None

    def _compute_ante_stats(self, token_address: str) -> dict | None:
        """
        Ante Phase 1 — rolling per-swap fee-burn stats.

        Observe-only: reads pumpswap_fees (priority_fee IS NOT NULL filter ⇒
        one row per distinct tx). Computes median/p25/p75 of
        (base_fee + priority_fee + jito_tip) in SOL over:
            - last 20 distinct-signature swaps
            - last 5 minutes

        Resilient to a DB where the base_fee column hasn't been added yet:
        if the migration hasn't run, the function falls back to partial Ante
        (priority + jito) and reports base_fee_coverage = 0. This means the
        code can ship before the migration without breaking alerts.

        Read-only SQLite connection. Returns None if no data — caller should
        silently skip.
        """
        import sqlite3
        import statistics

        window_n = int(self.ante_cfg.get("window_n", 20))
        window_s = int(self.ante_cfg.get("window_seconds", 300))
        # Cache schema detection across calls — pumpswap_fees columns don't change
        # at runtime once the migration has run.
        if not hasattr(self, "_ante_has_base_fee"):
            try:
                _probe = sqlite3.connect(
                    f"file:{database.DB_PATH}?mode=ro", uri=True, timeout=2.0
                )
                _cols = {r[1] for r in _probe.execute("PRAGMA table_info(pumpswap_fees)")}
                _probe.close()
                self._ante_has_base_fee = "base_fee" in _cols
            except Exception:
                self._ante_has_base_fee = False
        has_base_col = self._ante_has_base_fee

        # Build the ante-sum expression conditionally
        if has_base_col:
            sum_expr = "COALESCE(base_fee,0) + COALESCE(priority_fee,0) + COALESCE(jito_tip,0)"
            has_expr = "CASE WHEN base_fee IS NOT NULL THEN 1 ELSE 0 END"
        else:
            sum_expr = "COALESCE(priority_fee,0) + COALESCE(jito_tip,0)"
            has_expr = "0"

        try:
            conn = sqlite3.connect(f"file:{database.DB_PATH}?mode=ro", uri=True, timeout=2.0)
            cur = conn.cursor()

            # Last-N window — most recent distinct-signature rows
            cur.execute(f"""
                SELECT
                    {sum_expr} AS ante,
                    {has_expr} AS has_base
                FROM pumpswap_fees
                WHERE token_address = ?
                  AND priority_fee IS NOT NULL
                ORDER BY block_time DESC
                LIMIT {window_n}
            """, (token_address,))
            n_rows = cur.fetchall()

            # Last-5-minutes window
            cur.execute(f"""
                SELECT
                    {sum_expr} AS ante,
                    {has_expr} AS has_base
                FROM pumpswap_fees
                WHERE token_address = ?
                  AND priority_fee IS NOT NULL
                  AND block_time >= ?
            """, (token_address, time.time() - window_s))
            m_rows = cur.fetchall()
            conn.close()
        except Exception as e:
            logger.warning(f"_compute_ante_stats query failed for {token_address[:8]}: {e}")
            return None

        if not n_rows and not m_rows:
            return None

        def _p25_median_p75(values):
            if not values:
                return (None, None, None)
            if len(values) == 1:
                v = values[0]
                return (v, v, v)
            srt = sorted(values)
            med = statistics.median(srt)
            try:
                q = statistics.quantiles(srt, n=4, method="inclusive")
                return (q[0], med, q[2])
            except statistics.StatisticsError:
                return (srt[0], med, srt[-1])

        LAMPORTS = 1_000_000_000.0
        n_vals = [r[0] / LAMPORTS for r in n_rows]
        m_vals = [r[0] / LAMPORTS for r in m_rows]
        n_p25, n_med, n_p75 = _p25_median_p75(n_vals)
        m_p25, m_med, m_p75 = _p25_median_p75(m_vals)
        n_width = _compute_width_ratio(n_p25, n_p75)
        m_width = _compute_width_ratio(m_p25, m_p75)

        # base_fee coverage across the union of samples — informational only
        union_has_base = sum(r[1] for r in n_rows) + sum(r[1] for r in m_rows)
        union_count = len(n_rows) + len(m_rows)
        base_fee_coverage = (union_has_base / union_count) if union_count > 0 else 0.0

        return {
            "n20_count": len(n_rows),
            "n20_median": n_med,
            "n20_p25": n_p25,
            "n20_p75": n_p75,
            "n20_width": n_width,
            "m5_count": len(m_rows),
            "m5_median": m_med,
            "m5_p25": m_p25,
            "m5_p75": m_p75,
            "m5_width": m_width,
            "base_fee_coverage": base_fee_coverage,
        }

    async def evaluate_fees(
        self,
        token: TrackedToken,
        tier: dict,
        alert_time: float | None = None,
    ) -> FeeEvaluation:
        """
        Single fee evaluation pass: fetch raw stats, score, enforce sticky, log row.
        Returns a FeeEvaluation for use by both _format_alert (render) and the gate
        (block decision). Every call writes a new fee_gate_log row — callers must
        not double-invoke for the same alert.

        Empty-eval cases (is_empty=True, alert proceeds unblocked):
          - no fee data fetched
          - fee_gate disabled in config
          - scoring threw (logged warning)
        """
        if alert_time is None:
            alert_time = time.time()

        fee = self._fetch_fee_stats(token.address, token.migration_time)
        if not fee:
            return FeeEvaluation(is_empty=True, raw_fee=None)
        if not self.fee_gate_cfg.get("enabled"):
            return FeeEvaluation(is_empty=True, raw_fee=fee)

        try:
            score, flags, metrics, label = score_fees(
                total_fee=fee["total_sol"],
                lp=fee["lp_sol"],
                proto=fee["proto_sol"],
                creator=fee["creator_sol"],
                bribes=fee["bribes_sol"],
                tx_count=fee["tx_count"],
                events=fee["events"],
                cfg=self.fee_gate_cfg,
            )
            hist_score, hist_label = await database.get_worst_fee_gate_label(token.address)
            score, label, flags = enforce_sticky(
                score, label, flags, metrics,
                hist_score, hist_label,
            )
        except Exception as e:
            logger.warning(f"fee_gate scoring failed for {token.address[:8]}: {e}")
            return FeeEvaluation(is_empty=True, raw_fee=fee)

        log_id = None
        try:
            log_id = await database.log_fee_gate(
                token_address=token.address,
                symbol=token.symbol,
                alert_tier=tier.get("index", tier.get("tier_index", -1)),
                tier_name=tier.get("name", "Dip"),
                alert_time=alert_time,
                total_fee=fee["total_sol"],
                lp_fee=fee["lp_sol"],
                proto_fee=fee["proto_sol"],
                creator_fee=fee["creator_sol"],
                rate=fee["sol_per_min"],
                events=fee["events"],
                creator_share=metrics["creator_share"],
                proto_share=metrics["proto_share"],
                fee_per_event=metrics["bribes_pct_of_amm"],  # REPURPOSED: stores bribes_pct_of_amm
                proto_to_lp=metrics["bribes_per_tx_sol"],    # REPURPOSED: stores bribes_per_tx_sol
                score=score,
                flags=flags,
                label=label,
            )
        except Exception as e:
            logger.warning(f"fee_gate log failed for {token.address[:8]}: {e}")

        return FeeEvaluation(
            score=score,
            flags=flags,
            metrics=metrics,
            label=label,
            log_id=log_id,
            is_empty=False,
            raw_fee=fee,
        )

    async def _format_alert(
        self,
        token: TrackedToken,
        tier: dict,
        ghost_filter_result: dict | None = None,
        fee_eval: FeeEvaluation | None = None,
    ) -> str:
        drop_pct = token.drop_from_ath * 100
        mcap = token.current_mcap
        ath_mcap = token.ath_mcap or token.ath_price
        mig_mcap = token.migration_mcap
        age_h = token.age_hours
        tier_name = tier.get("name", "Dip")

        vol_label = _volume_label(token.volume_1h, token.volume_6h)
        lp_state = _lp_state(token.liquidity_usd, self.lp_floor_cfg)
        # Compute fee evaluation if caller didn't pre-compute (single source of truth).
        # When called from the alert gate (Step 4+), fee_eval is passed in and we
        # neither re-fetch nor re-log.
        if fee_eval is None:
            fee_eval = await self.evaluate_fees(token, tier, alert_time=time.time())
        fee = fee_eval.raw_fee

        # Header + State
        lines = [
            f"🔥 PHOENIX — <b>{token.symbol}</b> [{tier_name}]",
            f"<code>{token.address}</code>",
            "",
            "🧠 <b>State</b>",
            f"• MC: {fmt_mcap(mcap)} | ATH: {fmt_mcap(ath_mcap)} | Mig: {fmt_mcap(mig_mcap)}",
            f"• Floor: -{drop_pct:.0f}% | Age: {age_h:.1f}h | Vol: {vol_label}",
            f"• LP: {fmt_mcap(token.liquidity_usd)} ({lp_state})",
        ]

        # Participation
        if fee:
            lines.extend([
                "",
                "💰 <b>Participation</b>",
                f"• Total: {fee['total_sol']:.2f} SOL",
                f"• Split: LP {fee['lp_sol']:.2f} | Proto {fee['proto_sol']:.2f} | Creator {fee['creator_sol']:.2f}",
                f"• Pace: {fee['sol_per_min']:.2f} SOL/min | {fee['ev_per_min']:.0f} ev/min | Events: {fee['events']}",
            ])

        # Structure (Fee Gate) — render from pre-computed FeeEvaluation
        fee_label_for_read = "Normal"
        if not fee_eval.is_empty:
            try:
                score = fee_eval.score
                flags = fee_eval.flags
                metrics = fee_eval.metrics
                label = fee_eval.label
                fee_label_for_read = label

                if flags:
                    flags_str = ", ".join(flags)
                else:
                    if metrics["bribes_pct_of_amm"] >= 8.0 and metrics["creator_share"] < 0.35:
                        flags_str = "balanced_fees"
                    elif metrics["bribes_pct_of_amm"] >= 5.0:
                        flags_str = "healthy_bribes"
                    else:
                        flags_str = "clean"

                bribes_pct = metrics["bribes_pct_of_amm"]
                creator_pct = metrics["creator_share"] * 100
                bribes_per_tx_lamports = metrics["bribes_per_tx_sol"] * 1e9

                lines.extend([
                    "",
                    "🛑 <b>Structure</b>",
                    f"• Fee Gate: {label} ({score})",
                    f"• Bribes/AMM: {bribes_pct:.2f}% | Creator: {creator_pct:.1f}% | Bribes/tx: {bribes_per_tx_lamports:,.0f} lamports",
                    f"• Read: {flags_str}",
                ])
            except Exception as e:
                logger.warning(f"fee_gate render failed for {token.address[:8]}: {e}")

        # Secondary (LP Floor)
        lp_floor_label = "—"
        if self.lp_floor_cfg.get("enabled"):
            try:
                lp_label, lp_reason = score_lp(token.liquidity_usd, self.lp_floor_cfg)
                lp_floor_label = lp_label

                tier_index = tier.get("index", tier.get("tier_index", -1))
                await database.log_lp_floor(
                    token_address=token.address,
                    symbol=token.symbol,
                    alert_tier=tier_index,
                    tier_name=tier_name,
                    alert_time=time.time(),
                    liquidity_usd=token.liquidity_usd,
                    label=lp_label,
                    reason=lp_reason,
                )
            except Exception as e:
                logger.warning(f"lp_floor scoring failed for {token.address[:8]}: {e}")

        # Ante (Phase 1 — observe-only, ghost mode, never gates)
        # By design: adds 2 lines to the alert body and writes one ante_log row.
        # Does not read or modify any gating state anywhere else in the system.
        if self.ante_cfg.get("enabled") and self.ante_cfg.get("mode", "observe") == "observe":
            try:
                ante = self._compute_ante_stats(token.address)
                if ante is not None:
                    n_med = ante["n20_median"]
                    n_p25 = ante["n20_p25"]
                    n_p75 = ante["n20_p75"]
                    n_w   = ante["n20_width"]
                    m_med = ante["m5_median"]
                    m_w   = ante["m5_width"]
                    cov = ante["base_fee_coverage"] * 100.0

                    # Ante v2 Session 2 — observe-only priority_fee collection.
                    # Single captured timestamp so the fee-window and the
                    # ante_log row share the same alert_time.
                    ante_alert_time = time.time()
                    try:
                        pf_median, pf_count = await database.get_median_priority_fee(
                            token.pool_address, ante_alert_time
                        )
                    except Exception as e:
                        logger.warning(
                            f"priority_fee query failed for {token.address[:8]}: {e}"
                        )
                        pf_median, pf_count = None, 0

                    def _w_str(v):
                        return f"{v:.1f}×" if v is not None else "n/a"

                    if pf_median is None or pf_count == 0:
                        pf_line = "⛽ Priority Fee: n/a"
                    else:
                        pf_line = (
                            f"⛽ Priority Fee: {int(pf_median):,} lam "
                            f"(n={pf_count})"
                        )

                    lines.extend([
                        "",
                        "🎲 <b>Ante</b> <i>(observe-only)</i>",
                        (
                            f"• Median last-{ante['n20_count']}sw: "
                            f"{n_med:.6f} SOL | p25/p75: "
                            f"{n_p25:.6f} / {n_p75:.6f}"
                            if n_med is not None else
                            f"• Median last-{ante['n20_count']}sw: n/a"
                        ),
                        (
                            f"• Median 5m: {m_med:.6f} SOL "
                            f"(n={ante['m5_count']}) | base_fee cov: {cov:.0f}%"
                            if m_med is not None else
                            f"• Median 5m: n/a (n={ante['m5_count']}) | base_fee cov: {cov:.0f}%"
                        ),
                        (
                            f"• Width: last20 {_w_str(n_w)} | 5m {_w_str(m_w)} "
                            f"<i>(wider = more heterogeneous fee payers)</i>"
                        ),
                        pf_line,
                    ])

                    # Ante Taxonomy (ghost mode): categorical labels on top
                    # of numeric Ante. Disagreement between windows is itself
                    # signal — render both regardless.
                    taxonomy = None
                    if self.ante_taxonomy_cfg:
                        try:
                            stats_5m = {
                                "count":       ante["m5_count"],
                                "median":      ante["m5_median"],
                                "p25":         ante["m5_p25"] or 0.0,
                                "p75":         ante["m5_p75"] or 0.0,
                                "width_ratio": ante["m5_width"] if ante["m5_width"] is not None else 1.0,
                            }
                            stats_20sw = {
                                "count":       ante["n20_count"],
                                "median":      ante["n20_median"],
                                "p25":         ante["n20_p25"] or 0.0,
                                "p75":         ante["n20_p75"] or 0.0,
                                "width_ratio": ante["n20_width"] if ante["n20_width"] is not None else 1.0,
                            }
                            taxonomy = classify_both_windows(
                                stats_5m, stats_20sw, self.ante_taxonomy_cfg
                            )
                            lines.extend([
                                f"• Shape (5m):   {taxonomy['label_5m']} (rule {taxonomy['rule_hit_5m']})",
                                f"• Shape (20sw): {taxonomy['label_20sw']} (rule {taxonomy['rule_hit_20sw']})",
                            ])
                        except Exception as e:
                            logger.warning(f"ante_taxonomy classify failed for {token.address[:8]}: {e}")

                    tier_index = tier.get("index", tier.get("tier_index", -1))
                    await database.log_ante(
                        token_address=token.address,
                        symbol=token.symbol,
                        alert_tier=tier_index,
                        tier_name=tier_name,
                        alert_time=ante_alert_time,
                        n20_count=ante["n20_count"],
                        n20_median=ante["n20_median"],
                        n20_p25=ante["n20_p25"],
                        n20_p75=ante["n20_p75"],
                        n20_width=ante["n20_width"],
                        m5_count=ante["m5_count"],
                        m5_median=ante["m5_median"],
                        m5_p25=ante["m5_p25"],
                        m5_p75=ante["m5_p75"],
                        m5_width=ante["m5_width"],
                        base_fee_coverage=ante["base_fee_coverage"],
                        label_5m=taxonomy["label_5m"] if taxonomy else None,
                        rule_hit_5m=taxonomy["rule_hit_5m"] if taxonomy else None,
                        label_20sw=taxonomy["label_20sw"] if taxonomy else None,
                        rule_hit_20sw=taxonomy["rule_hit_20sw"] if taxonomy else None,
                        median_priority_fee=pf_median,
                        priority_fee_n=pf_count,
                    )
            except Exception as e:
                # Ante failure must never break the alert pipeline.
                logger.warning(f"ante stats failed for {token.address[:8]}: {e}")

        # Ghost Filter (Tier 1 only, shadow mode)
        if ghost_filter_result is not None:
            gf = ghost_filter_result
            uwc = int(gf.get("user_wallet_count") or 0)
            low_sol_count = int(gf.get("low_sol_count") or 0)
            funding_collision_count = int(gf.get("funding_collision_count") or 0)
            collision_clusters = gf.get("collision_clusters") or []
            block_reason = gf.get("block_reason") or ""
            low_pct = (low_sol_count / uwc * 100) if uwc > 0 else 0
            # Cached payloads from before the 3-mode rollout lack `verdict`;
            # derive a binary-equivalent value from would_block so they still
            # render correctly during the 1-hour cache TTL after deploy.
            verdict = gf.get("verdict") or ("block" if gf.get("would_block") else "pass")

            if verdict == "block":
                if block_reason == "both":
                    gf_read = "bundle suspected + low-sol cluster"
                elif block_reason == "funding_collisions":
                    gf_read = "bundle suspected"
                else:
                    gf_read = "low-sol cluster"

                col_line = f"• Collisions: {funding_collision_count} wallets"
                if collision_clusters:
                    col_line += f" in {len(collision_clusters)} clusters"

                lines.extend(["", "🕵️ <b>Ghost Filter: 🔴 WOULD BLOCK</b>", col_line])
                for cl in collision_clusters[:4]:
                    lines.append(f"  └ {float(cl.get('sol') or 0):.4f} SOL × {cl.get('wallets') or 0}")
                lines.append(f"• Low SOL (&lt;0.1): {low_sol_count}/{uwc} ({low_pct:.0f}%)")
                lines.append(f"• Read: {gf_read}")
            elif verdict == "caution":
                fund = funding_collision_count
                clust = len(collision_clusters)
                fund_caution = fund >= GHOST_CAUTION_WALLETS
                clust_caution = clust >= GHOST_CAUTION_CLUSTERS
                if fund_caution and clust_caution:
                    gf_read = "moderate funding collisions; elevated cluster count"
                elif fund_caution:
                    gf_read = "moderate funding collisions"
                else:
                    gf_read = "elevated cluster count"

                col_line = f"• Collisions: {fund} wallets"
                if collision_clusters:
                    col_line += f" in {clust} clusters"

                lines.extend(["", "🕵️ <b>Ghost Filter: ⚠️ CAUTION</b>", col_line])
                for cl in collision_clusters[:4]:
                    lines.append(f"  └ {float(cl.get('sol') or 0):.4f} SOL × {cl.get('wallets') or 0}")
                lines.append(f"• Low SOL (&lt;0.1): {low_sol_count}/{uwc} ({low_pct:.0f}%)")
                lines.append(f"• Read: {gf_read}")
            else:
                lines.extend([
                    "",
                    "🕵️ <b>Ghost Filter: 🟢 PASS</b>",
                    f"• Collisions: {funding_collision_count} wallets",
                    f"• Low SOL (&lt;0.1): {low_sol_count}/{uwc} ({low_pct:.0f}%)",
                ])

        # Builder Read
        reads = _builder_read(
            tier_name, vol_label, fee_label_for_read, lp_state,
            sol_per_min=fee["sol_per_min"] if fee else 0.0,
            ev_per_min=fee["ev_per_min"] if fee else 0.0,
        )
        lines.extend([
            "",
            "🏗️ <b>Builder Read</b>",
            f"• {reads[0]}",
            f"• {reads[1]}",
            f"• {reads[2]}",
        ])

        return "\n".join(lines)

    async def send_daily_recap(
        self,
        alerts: list,
        tokens_lookup: dict,
        session: aiohttp.ClientSession,
    ):
        """Format and send the daily performance recap."""
        msg = self._format_daily_recap(alerts, tokens_lookup)
        if msg:
            await self._send(msg, session, parse_mode=None)

    def _format_daily_recap(
        self,
        alerts: list,
        tokens_lookup: dict,
    ) -> str:
        """Return the plain-text daily recap, or '' if no alerts today.

        All recap composition now lives in stats.build_daily_recap — this
        wrapper just opens a read-only sqlite connection and delegates.
        The alerts / tokens_lookup args are kept for caller compatibility
        but are no longer used (build_daily_recap queries the DB directly).
        """
        if not alerts:
            return ""
        from stats import build_daily_recap
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5.0)
        try:
            return build_daily_recap(conn)
        finally:
            conn.close()
    def format_prim_report(self, days: int = 7) -> str:
        """Build /prim primitive report. Sync sqlite, read-only."""
        cutoff = time.time() - days * 86400
        SCAM = "SCAM Likely"
        FEE_STATES = ("Normal", "Elevated", "Suspicious")

        try:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5.0)
            conn.row_factory = sqlite3.Row

            tokens = [dict(r) for r in conn.execute("""
                SELECT t.address, t.symbol,
                       MIN(a.alert_mcap) AS min_alert,
                       MAX(a.peak_mcap_after) AS max_peak,
                       COUNT(a.id) AS n_alerts
                FROM tokens t JOIN alerts a ON a.address = t.address
                WHERE a.alert_time >= ?
                GROUP BY t.address;
            """, (cutoff,)).fetchall()]

            scam_set = {r[0] for r in conn.execute(
                "SELECT DISTINCT token_address FROM fee_gate_log WHERE label = ? AND alert_time >= ?",
                (SCAM, cutoff)).fetchall()}

            fee_map = {}
            for r in conn.execute("""
                SELECT token_address, label FROM fee_gate_log
                WHERE alert_time >= ? AND label != ?
                ORDER BY alert_time DESC
            """, (cutoff, SCAM)).fetchall():
                fee_map.setdefault(r[0], r[1])

            alerts = [dict(r) for r in conn.execute("""
                SELECT address, symbol, tier_index, alert_mcap, peak_mcap_after
                FROM alerts WHERE alert_time >= ?
            """, (cutoff,)).fetchall()]
            conn.close()
        except Exception as e:
            logger.warning(f"/prim query failed: {e}")
            return f"❌ /prim query failed: {e}"

        def tmult(t):
            return (t["max_peak"]/t["min_alert"]) if t["min_alert"] and t["max_peak"] else 0
        def amult(a):
            return (a["peak_mcap_after"]/a["alert_mcap"]) if a["alert_mcap"] and a["peak_mcap_after"] else 0
        def is_dead(t):
            return tmult(t) > 0 and tmult(t) < 1.2
        def pct(n, d):
            return f"{(100*n/d):.0f}%" if d else "0%"

        total = len(tokens)
        alerts_fired = sum(t["n_alerts"] for t in tokens)
        scam_tokens = [t for t in tokens if t["address"] in scam_set]
        nonscam = [t for t in tokens if t["address"] not in scam_set]
        twox = [t for t in tokens if tmult(t) >= 2]
        deaths = [t for t in tokens if is_dead(t)]
        top = max(nonscam, key=tmult, default=None) if nonscam else None

        L = []
        L.append(f"🔥 <b>PHOENIX — PRIMITIVE REPORT</b>\nWindow: Last {days} Days\n")
        L.append("━━━━━━━━━━━━━━━━━━━━━━━━\n<b>1. OVERVIEW</b>")
        L.append(f"Tokens Called: {total}")
        L.append(f"Alerts Fired: {alerts_fired}")
        L.append(f"Unique Tokens Removed as Scam: {len(scam_tokens)}")
        L.append(f"Non-Scam Tokens Remaining: {len(nonscam)}")
        L.append(f"\n2x+ Tokens: {len(twox)} / {total} ({pct(len(twox), total)})")
        L.append(f"Deaths (Never Bounced): {len(deaths)} / {total} ({pct(len(deaths), total)})")
        if top and tmult(top) > 0:
            L.append(f"\nTop Bounce:\n${top['symbol']} — peaked {fmt_mcap(top['max_peak'])} ({tmult(top):.1f}x)")

        ns_2x = [t for t in nonscam if tmult(t) >= 2]
        ns_d = [t for t in nonscam if is_dead(t)]
        L.append("\n━━━━━━━━━━━━━━━━━━━━━━━━\n<b>2. SELECTION QUALITY</b>")
        L.append(f"Scam Removed: {len(scam_tokens)}")
        L.append(f"Scam Removed Rate: {pct(len(scam_tokens), total)}")
        L.append(f"\nNon-Scam Tokens: {len(nonscam)}")
        L.append(f"Non-Scam 2x+: {len(ns_2x)} / {len(nonscam)} ({pct(len(ns_2x), len(nonscam))})")
        L.append(f"Non-Scam Deaths: {len(ns_d)} / {len(nonscam)} ({pct(len(ns_d), len(nonscam))})")

        L.append("\n━━━━━━━━━━━━━━━━━━━━━━━━\n<b>3. TIER BEHAVIOR</b>")
        tier_stats = {}
        for ti in (0, 1, 2):
            rows = [a for a in alerts if a["tier_index"] == ti]
            n = len(rows)
            x2 = sum(1 for a in rows if amult(a) >= 2)
            x3 = sum(1 for a in rows if amult(a) >= 3)
            x5 = sum(1 for a in rows if amult(a) >= 5)
            d = sum(1 for a in rows if 0 < amult(a) < 1.2)
            tier_stats[ti] = (n, x2, x3, x5, d)
            L.append(f"\n<b>Tier {ti+1}</b>")
            L.append(f"• Calls: {n} | 2x+: {x2} ({pct(x2,n)}) | 3x+: {x3} ({pct(x3,n)})")
            L.append(f"• 5x+: {x5} ({pct(x5,n)}) | Deaths: {d} ({pct(d,n)})")
        best2 = max(tier_stats, key=lambda k: tier_stats[k][1]/tier_stats[k][0] if tier_stats[k][0] else 0)
        best5 = max(tier_stats, key=lambda k: tier_stats[k][3]/tier_stats[k][0] if tier_stats[k][0] else 0)
        worstd = max(tier_stats, key=lambda k: tier_stats[k][4]/tier_stats[k][0] if tier_stats[k][0] else 0)
        L.append(f"\nBest by 2x+: Tier {best2+1} | Best by 5x+: Tier {best5+1} | Worst Death: Tier {worstd+1}")

        L.append("\n━━━━━━━━━━━━━━━━━━━━━━━━\n<b>4. FEE STATE BEHAVIOR</b>")
        fee_stats = {}
        for fs in FEE_STATES:
            toks = [t for t in nonscam if fee_map.get(t["address"]) == fs]
            n = len(toks)
            x2 = sum(1 for t in toks if tmult(t) >= 2)
            x3 = sum(1 for t in toks if tmult(t) >= 3)
            x5 = sum(1 for t in toks if tmult(t) >= 5)
            d = sum(1 for t in toks if is_dead(t))
            fee_stats[fs] = (n, x2, x3, x5, d)
            L.append(f"\n<b>{fs}</b>")
            L.append(f"• Count: {n} | 2x+: {x2} ({pct(x2,n)}) | 3x+: {x3} ({pct(x3,n)})")
            L.append(f"• 5x+: {x5} ({pct(x5,n)}) | Deaths: {d} ({pct(d,n)})")
        best_fs = max(fee_stats, key=lambda k: fee_stats[k][1]/fee_stats[k][0] if fee_stats[k][0] else 0)
        worst_fs = max(fee_stats, key=lambda k: fee_stats[k][4]/fee_stats[k][0] if fee_stats[k][0] else 0)
        L.append(f"\nBest by 2x+: {best_fs} | Worst Death: {worst_fs}")

        L.append("\n━━━━━━━━━━━━━━━━━━━━━━━━\n<b>5. TIER × FEE MATRIX</b>")
        tier_addrs = {ti: {a["address"] for a in alerts if a["tier_index"] == ti} for ti in (0,1,2)}
        combos = []
        for ti in (0, 1, 2):
            for fs in FEE_STATES:
                toks = [t for t in nonscam if fee_map.get(t["address"]) == fs and t["address"] in tier_addrs[ti]]
                n = len(toks)
                x2 = sum(1 for t in toks if tmult(t) >= 2)
                d = sum(1 for t in toks if is_dead(t))
                combos.append({"k": f"T{ti+1} × {fs}", "n": n, "x2": x2, "d": d,
                               "r2": (x2/n if n else 0), "rd": (d/n if n else 1)})
        ranked = sorted([c for c in combos if c["n"] > 0], key=lambda c: (-c["r2"], c["rd"]))
        winners = {c["k"] for c in ranked[:3]}
        for c in combos:
            mark = " 🏆" if c["k"] in winners else ""
            L.append(f"\n<b>{c['k']}</b>{mark}")
            L.append(f"• Count: {c['n']} | 2x+: {c['x2']} ({pct(c['x2'],c['n'])}) | Deaths: {c['d']} ({pct(c['d'],c['n'])})")

        L.append("\n━━━━━━━━━━━━━━━━━━━━━━━━\n<b>6. OUTCOME PROFILE</b>")
        b5 = sum(1 for t in tokens if tmult(t) >= 5)
        b45 = sum(1 for t in tokens if 4 <= tmult(t) < 5)
        b34 = sum(1 for t in tokens if 3 <= tmult(t) < 4)
        b23 = sum(1 for t in tokens if 2 <= tmult(t) < 3)
        bu2 = sum(1 for t in tokens if 0 < tmult(t) < 2)
        L.append(f"5x+: {b5} | 4x–5x: {b45} | 3x–4x: {b34}")
        L.append(f"2x–3x: {b23} | &lt;2x: {bu2} | Deaths: {len(deaths)}")

        L.append("\n━━━━━━━━━━━━━━━━━━━━━━━━\n<b>7. INTERPRETATION</b>")
        L.append(f"• Scam filter removed {len(scam_tokens)} ({pct(len(scam_tokens), total)}) of called tokens")
        L.append(f"• Best tier by 2x+: Tier {best2+1} | Worst by death: Tier {worstd+1}")
        L.append(f"• Best fee state: {best_fs} | Worst fee state: {worst_fs}")
        if ranked:
            L.append(f"• Strongest combo: {ranked[0]['k']} ({pct(ranked[0]['x2'], ranked[0]['n'])} 2x+)")

        return "\n".join(L)

    async def send_prim_report(self, session: aiohttp.ClientSession, days: int = 7):
        msg = self.format_prim_report(days)
        # Telegram 4096 char limit
        for i in range(0, len(msg), 3900):
            await self._send(msg[i:i+3900], session)

    async def poll_commands(self, session: aiohttp.ClientSession) -> list:
        """Poll Telegram for new commands. Returns list of parsed commands."""
        url = f"{TELEGRAM_API.format(token=self.bot_token)}/getUpdates"
        params = {
            "offset": self._last_update_id + 1,
            "timeout": 0,
            "allowed_updates": '["message"]',
        }
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

            if not data.get("ok"):
                return []

            commands = []
            for update in data.get("result", []):
                update_id = update.get("update_id", 0)
                if update_id > self._last_update_id:
                    self._last_update_id = update_id

                message = update.get("message", {})
                text = message.get("text", "").strip()
                chat_id = str(message.get("chat", {}).get("id", ""))

                if chat_id != self.chat_id:
                    continue

                if text.startswith("/"):
                    commands.append({"command": text.lower(), "chat_id": chat_id})

            return commands

        except Exception as e:
            logger.debug(f"Command poll error: {e}")
            return []

    async def _send(self, text: str, session: aiohttp.ClientSession, parse_mode: str | None = "HTML") -> bool:
        url = f"{TELEGRAM_API.format(token=self.bot_token)}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode is not None:
            payload["parse_mode"] = parse_mode
        # Retry policy: 3 attempts total. 2xx = success, 429 = honour retry_after,
        # 5xx/network = exponential backoff (1s, 2s, 4s), other 4xx = permanent
        # (do not retry — prevents infinite loop on malformed messages).
        max_attempts = 3
        backoff = [1, 2, 4]
        for attempt in range(1, max_attempts + 1):
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    status = resp.status
                    if 200 <= status < 300:
                        logger.info("✅ Telegram alert sent")
                        return True
                    if status == 429:
                        # Prefer Telegram's suggested retry_after if present.
                        wait = 2
                        try:
                            body_json = await resp.json(content_type=None)
                            wait = int(body_json.get("parameters", {}).get("retry_after", wait))
                        except Exception:
                            pass
                        if attempt >= max_attempts:
                            logger.error(f"Giving up after {max_attempts} Telegram retries")
                            return False
                        logger.warning(
                            f"Telegram 429 rate limit — waiting {wait}s "
                            f"(attempt {attempt}/{max_attempts})"
                        )
                        await asyncio.sleep(wait)
                        continue
                    if 500 <= status < 600:
                        body = await resp.text()
                        if attempt >= max_attempts:
                            logger.error(
                                f"Giving up after {max_attempts} Telegram retries "
                                f"(last status {status}: {body})"
                            )
                            return False
                        wait = backoff[attempt - 1]
                        logger.warning(
                            f"Telegram {status} — retrying in {wait}s "
                            f"(attempt {attempt}/{max_attempts}): {body}"
                        )
                        await asyncio.sleep(wait)
                        continue
                    # Any other 4xx — permanent, do not retry.
                    body = await resp.text()
                    logger.error(f"Permanent Telegram error (4xx) {status}: {body}")
                    return False
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt >= max_attempts:
                    logger.error(f"Giving up after {max_attempts} Telegram retries: {e}")
                    return False
                wait = backoff[attempt - 1]
                logger.warning(
                    f"Telegram network error — retrying in {wait}s "
                    f"(attempt {attempt}/{max_attempts}): {e}"
                )
                await asyncio.sleep(wait)
                continue
        return False

    async def send_startup_message(self, session: aiohttp.ClientSession):
        """Send a simple startup ping so you know the bot is alive."""
        await self._send("🤖 <b>Phoenix Bot started</b> — watching for migrations...", session)

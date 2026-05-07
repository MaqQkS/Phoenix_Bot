"""
modules/alert_trigger.py
Checks each token's drop from ATH and fires Telegram alerts at dip tiers.

Tiers (from config):
  Tier 1: 50-60% drop
  Tier 2: 62-80% drop
  Tier 3: 82-95% drop

Requirements before alerting:
  - Token must be ATH_CONFIRMED (pump multiple threshold reached; see config.tracking.min_pump_multiple)
  - Drop must be within the tier range
  - That tier must not have already been alerted
    (re-alerts on next tier if it drops further)
  - Price must have been updated recently (config.tracking.max_price_age_seconds)
"""

import logging
import time
import database as db
from models import TrackedToken, TokenStatus
from modules import ath_refresh_shadow

logger = logging.getLogger(__name__)

# Default freshness ceiling when config does not provide one.
DEFAULT_MAX_PRICE_AGE_SECONDS = 300


class AlertTrigger:
    def __init__(self, config: dict):
        tracking_cfg = config.get("tracking", {}) or {}
        self.max_price_age_seconds = tracking_cfg.get(
            "max_price_age_seconds",
            DEFAULT_MAX_PRICE_AGE_SECONDS,
        )
        raw_tiers = config.get("dip_tiers", [
            {"name": "Tier 1", "min_drop": 0.50, "max_drop": 0.60},
            {"name": "Tier 2", "min_drop": 0.62, "max_drop": 0.80},
            {"name": "Tier 3", "min_drop": 0.82, "max_drop": 0.95},
        ])
        self.tiers = []
        for i, tier_cfg in enumerate(raw_tiers):
            tier = dict(tier_cfg)
            tier["index"] = i
            self.tiers.append(tier)

    def check_tokens(self, tokens: list[TrackedToken]) -> list[tuple[TrackedToken, dict]]:
        """
        Check all tokens for dip tier triggers.
        Returns list of (token, tier) pairs that need an alert sent.
        """
        to_alert = []

        for token in tokens:
            # Only alert on ATH_CONFIRMED or already ALERTED tokens
            # BLOCKED tokens naturally excluded — not in this set
            if token.status not in (TokenStatus.ATH_CONFIRMED, TokenStatus.ALERTED):
                continue

            if token.ath_price <= 0 or token.current_price <= 0:
                continue

            # ── Stale price guard ──────────────────────────────────────
            # Don't fire alerts if the last price update is too old.
            # This prevents firing on stale/cached data from Dexscreener.
            if token.last_price_update <= 0:
                logger.warning(
                    f"⚠️ ${token.symbol} has no recorded price timestamp — "
                    f"skipping alert check (unknown freshness)"
                )
                continue

            price_age = time.time() - token.last_price_update
            if price_age > self.max_price_age_seconds:
                logger.warning(
                    f"⚠️ ${token.symbol} price is {price_age:.0f}s old — "
                    f"skipping alert check (stale data)"
                )
                continue

            # ── Phantom-dip cooldown ───────────────────────────────────
            # Suppress tier eval when phantom_validator flagged this token
            # for cooldown after a Birdeye ATH update. Window is short
            # (~120s) — long enough for Dexscreener cache to refresh.
            now_ts = time.time()
            if token.phantom_cooldown_until and now_ts < token.phantom_cooldown_until:
                remaining = token.phantom_cooldown_until - now_ts
                # Determine which tier this drop would have hit so the log
                # captures what the fix actually prevented.
                drop_preview = token.drop_from_ath
                preview_tier_idx = -1
                for i, tier in enumerate(self.tiers):
                    if tier["min_drop"] <= drop_preview < tier["max_drop"]:
                        preview_tier_idx = i
                        break
                if preview_tier_idx > token.last_alerted_tier:
                    logger.info(
                        f"⏸️  ${token.symbol} would-have-alerted Tier {preview_tier_idx + 1} "
                        f"but phantom cooldown active (expires in {remaining:.0f}s)"
                    )
                else:
                    logger.info(
                        f"⏸️  ${token.symbol} tier eval suppressed (phantom cooldown, "
                        f"{remaining:.0f}s remaining)"
                    )
                continue

            # ── Ghost-block cooldown ───────────────────────────────────
            # Suppress tier eval when the holder filter returned
            # verdict=block on a recent tier evaluation. Window is the
            # holder-filter cache TTL + 60s (~3660s) so the next eval
            # after expiry takes a fresh snapshot rather than re-using
            # the cached verdict that triggered the cooldown. Token-level
            # (not tier-level): a T1 ghost-block suppresses T2/T3 eval
            # for the same token until expiry.
            if token.ghost_cooldown_until and now_ts < token.ghost_cooldown_until:
                remaining = token.ghost_cooldown_until - now_ts
                drop_preview = token.drop_from_ath
                preview_tier_idx = -1
                for i, tier in enumerate(self.tiers):
                    if tier["min_drop"] <= drop_preview < tier["max_drop"]:
                        preview_tier_idx = i
                        break
                if preview_tier_idx > token.last_alerted_tier:
                    logger.info(
                        f"⏸️  ${token.symbol} would-have-alerted Tier {preview_tier_idx + 1} "
                        f"but ghost cooldown active (expires in {remaining:.0f}s)"
                    )
                else:
                    logger.info(
                        f"⏸️  ${token.symbol} tier eval suppressed (ghost cooldown, "
                        f"{remaining:.0f}s remaining)"
                    )
                continue

            drop = token.drop_from_ath  # e.g. 0.72 = 72% drop

            # Find which tier this drop falls into
            triggered_tier = None
            triggered_tier_index = -1

            for i, tier in enumerate(self.tiers):
                if tier["min_drop"] <= drop < tier["max_drop"]:
                    triggered_tier = tier
                    triggered_tier_index = i
                    break

            if triggered_tier is None:
                continue

            # Only alert if this is a new (deeper) tier
            if triggered_tier_index <= token.last_alerted_tier:
                continue

            # Log what we're about to alert on for debugging
            logger.info(
                f"📋 ${token.symbol} alert check: mcap=${token.current_mcap:,.0f} | "
                f"ath=${token.ath_mcap:,.0f} | drop={drop*100:.0f}% | "
                f"tier={triggered_tier['name']} | price_age={price_age:.0f}s"
            )

            to_alert.append((token, triggered_tier))

        return to_alert

    async def mark_alerted(self, token: TrackedToken, tier_index: int, alert_time: float | None = None):
        """Update token status, log the alert to history, and save to DB."""
        tier_name = self.tiers[tier_index]["name"] if tier_index < len(self.tiers) else f"Tier {tier_index + 1}"

        # Save alert record for performance tracking
        await db.save_alert(
            address=token.address,
            symbol=token.symbol,
            tier_index=tier_index,
            tier_name=tier_name,
            alert_price=token.current_price,
            alert_mcap=token.current_mcap,
            ath_price=token.ath_price,
            ath_mcap=token.ath_mcap,
            alert_time=alert_time,
        )

        old_status = token.status.value
        token.last_alerted_tier = tier_index
        token.status = TokenStatus.ALERTED
        await db.save_token(token)
        ath_refresh_shadow.log_status_transition(
            token.address, old_status, "alerted", token.migration_time, token.symbol
        )

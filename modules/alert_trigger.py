"""
modules/alert_trigger.py
Checks each token's drop from ATH and fires Telegram alerts at dip tiers.

Tiers (from config):
  Tier 1: 55-65% drop
  Tier 2: 65-80% drop
  Tier 3: 80-93% drop

Requirements before alerting:
  - Token must be ATH_CONFIRMED (1.2x from migration)
  - Drop must be within the tier range
  - That tier must not have already been alerted
    (re-alerts on next tier if it drops further)
"""

import logging
import time
import database as db
from models import TrackedToken, TokenStatus

logger = logging.getLogger(__name__)


class AlertTrigger:
    def __init__(self, config: dict):
        self.tiers = config.get("dip_tiers", [
            {"name": "Tier 1", "min_drop": 0.55, "max_drop": 0.65},
            {"name": "Tier 2", "min_drop": 0.65, "max_drop": 0.80},
            {"name": "Tier 3", "min_drop": 0.80, "max_drop": 0.93},
        ])

    def check_tokens(self, tokens: list[TrackedToken]) -> list[tuple[TrackedToken, dict]]:
        """
        Check all tokens for dip tier triggers.
        Returns list of (token, tier) pairs that need an alert sent.
        """
        to_alert = []

        for token in tokens:
            # Only alert on ATH_CONFIRMED or already ALERTED tokens
            if token.status not in (TokenStatus.ATH_CONFIRMED, TokenStatus.ALERTED):
                continue

            if token.ath_price <= 0 or token.current_price <= 0:
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

            to_alert.append((token, triggered_tier))

        return to_alert

    async def mark_alerted(self, token: TrackedToken, tier_index: int):
        """Update token status and save to DB after alert is sent."""
        token.last_alerted_tier = tier_index
        token.status = TokenStatus.ALERTED
        await db.save_token(token)
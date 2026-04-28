"""
models.py — Data models for the dip bot.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class TokenStatus(str, Enum):
    TRACKING      = "tracking"       # Watching for ATH + dips
    ATH_CONFIRMED = "ath_confirmed"  # Pump multiple threshold reached (see config.tracking.min_pump_multiple); dip alerts active
    ALERTED       = "alerted"        # At least one dip alert sent
    EXPIRED       = "expired"        # Too old, stop tracking
    BLOCKED       = "blocked"        # Hard-blocked from alerts (e.g. SCAM Likely). Terminal state.


@dataclass
class TrackedToken:
    # ── Identity ──────────────────────────────────────────────────────────
    address: str
    symbol: str = "???"
    pool_address: str = ""

    # ── Status ────────────────────────────────────────────────────────────
    status: TokenStatus = TokenStatus.TRACKING

    # ── Prices ────────────────────────────────────────────────────────────
    migration_price: float = 0.0      # price at migration moment
    migration_mcap: float = 0.0       # mcap at migration moment
    current_price: float = 0.0
    current_mcap: float = 0.0
    liquidity_usd: float = 0.0

    # ── ATH ───────────────────────────────────────────────────────────────
    ath_price: float = 0.0
    ath_mcap: float = 0.0
    ath_time: float = 0.0
    # Provenance of ath_price. Values:
    #   'unseeded'            — no Birdeye data yet
    #   'birdeye'             — initial Birdeye seed accepted
    #   'birdeye_reseeded'    — re-query found higher peak than initial seed
    #   'birdeye_corrected'   — T+15m correction found higher peak
    #   'birdeye_running_max' — running-max exceeded any Birdeye value
    #   'running_max'         — Birdeye never succeeded, timeout
    #   'fallback'            — legacy historical rows
    ath_source: str = "unseeded"

    # ── Volume ────────────────────────────────────────────────────────────
    volume_1h: float = 0.0
    volume_6h: float = 0.0
    volume_24h: float = 0.0

    # ── Timing ────────────────────────────────────────────────────────────
    migration_time: float = 0.0       # unix timestamp of migration
    last_price_update: float = 0.0

    # ── Alert tracking ────────────────────────────────────────────────────
    # Which tier index (0/1/2) was last alerted. -1 = none yet.
    last_alerted_tier: int = -1

    # ── Phantom-dip cooldown ──────────────────────────────────────────────
    # Set by phantom_validator when Birdeye-current is at-or-near a freshly
    # written ATH (BLICKY-class phantom). While time.time() < this value,
    # alert_trigger.check_tokens() suppresses tier evaluation for the token.
    # 0.0 = no cooldown active.
    phantom_cooldown_until: float = 0.0

    # ── Pool metadata (fetched at migration time by migration_ws) ─────────
    # Set once from getAccountInfo on the PumpSwap pool + SPL mint. Immutable
    # after first write. NULL means "not yet determined" — the gRPC indexer
    # skips price derivation for tokens without metadata.
    #   pool_orientation: 'normal' (base=meme, quote=WSOL) |
    #                     'inverted' (base=WSOL, quote=meme) | None
    #   token_decimals:   SPL mint decimals (typically 6 for pump.fun)
    pool_orientation: str | None = None
    token_decimals: int | None = None

    # ── Computed properties ───────────────────────────────────────────────
    @property
    def pump_multiple(self) -> float:
        """How much the token pumped from migration price to ATH."""
        if self.migration_price <= 0:
            return 0.0
        return self.ath_price / self.migration_price

    @property
    def drop_from_ath(self) -> float:
        """Current drop from ATH as a fraction (0.0 - 1.0). 0.76 = 76% drop."""
        if self.ath_price <= 0:
            return 0.0
        return 1.0 - (self.current_price / self.ath_price)

    @property
    def age_hours(self) -> float:
        """Age of token since migration in hours."""
        import time
        if self.migration_time <= 0:
            return 0.0
        return (time.time() - self.migration_time) / 3600
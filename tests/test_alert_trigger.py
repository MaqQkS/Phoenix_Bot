"""Tests for the phantom-cooldown gate in modules.alert_trigger.

We only exercise the new behaviour added by the phantom-dip fix:
  - When token.phantom_cooldown_until is in the future, check_tokens()
    suppresses tier evaluation for that token.
  - When the cooldown is in the past, normal tier-eval resumes.

The tier-ladder logic itself is explicitly out of scope for this PR
(see the spec's "Out of scope" section).
"""

import time

import pytest

from models import TokenStatus, TrackedToken
from modules.alert_trigger import AlertTrigger


def _config() -> dict:
    """Match the shipped config tiers so tests don't drift."""
    return {
        "tracking": {"max_price_age_seconds": 300},
        "dip_tiers": [
            {"name": "Tier 1", "min_drop": 0.50, "max_drop": 0.60},
            {"name": "Tier 2", "min_drop": 0.62, "max_drop": 0.80},
            {"name": "Tier 3", "min_drop": 0.82, "max_drop": 0.95},
        ],
    }


def _ath_confirmed_token(
    *,
    address: str = "TOK_pump",
    symbol: str = "TOK",
    ath_price: float = 1.0,
    current_price: float,
    phantom_cooldown_until: float = 0.0,
    last_alerted_tier: int = -1,
) -> TrackedToken:
    return TrackedToken(
        address=address,
        symbol=symbol,
        status=TokenStatus.ATH_CONFIRMED,
        ath_price=ath_price,
        ath_mcap=ath_price * 1_000_000_000,
        current_price=current_price,
        current_mcap=current_price * 1_000_000_000,
        last_price_update=time.time(),
        phantom_cooldown_until=phantom_cooldown_until,
        last_alerted_tier=last_alerted_tier,
    )


# ── Sanity: baseline behaviour without cooldown ──────────────────────

def test_baseline_tier_eval_fires_at_minus_83_percent():
    """Without phantom cooldown, a -83% drop fires Tier 3."""
    trigger = AlertTrigger(_config())
    # ath=1.0, current=0.17 => drop=0.83 => Tier 3 (0.82 <= 0.83 < 0.95)
    token = _ath_confirmed_token(current_price=0.17)
    to_alert = trigger.check_tokens([token])
    assert len(to_alert) == 1
    fired_token, fired_tier = to_alert[0]
    assert fired_token is token
    assert fired_tier["index"] == 2
    assert fired_tier["name"] == "Tier 3"


# ── Cooldown active: suppression ─────────────────────────────────────

def test_phantom_cooldown_suppresses_tier_eval():
    """The BLICKY scenario: -83% drop on Dex side, but phantom cooldown
    is in the future. check_tokens() must return no alerts for this token."""
    trigger = AlertTrigger(_config())
    token = _ath_confirmed_token(
        current_price=0.17,                              # would-be Tier 3
        phantom_cooldown_until=time.time() + 60,         # 60s remaining
    )
    to_alert = trigger.check_tokens([token])
    assert to_alert == []


def test_phantom_cooldown_suppresses_tier_1_too():
    """Cooldown applies to all tier bands, not just Tier 3."""
    trigger = AlertTrigger(_config())
    # -55% drop => Tier 1 band
    token = _ath_confirmed_token(
        current_price=0.45,
        phantom_cooldown_until=time.time() + 60,
    )
    assert trigger.check_tokens([token]) == []


def test_phantom_cooldown_does_not_affect_other_tokens():
    """Cooldown is per-token: a phantom on token A must not suppress
    a real dip on token B."""
    trigger = AlertTrigger(_config())
    cooled = _ath_confirmed_token(
        address="A_pump", symbol="A",
        current_price=0.17,
        phantom_cooldown_until=time.time() + 60,
    )
    fresh = _ath_confirmed_token(
        address="B_pump", symbol="B",
        current_price=0.17,
        phantom_cooldown_until=0.0,
    )
    fired = trigger.check_tokens([cooled, fresh])
    assert len(fired) == 1
    assert fired[0][0].address == "B_pump"


# ── Cooldown expired: tier eval resumes ─────────────────────────────

def test_expired_cooldown_allows_tier_eval():
    """Cooldown timestamp in the past => normal tier evaluation resumes."""
    trigger = AlertTrigger(_config())
    token = _ath_confirmed_token(
        current_price=0.17,
        phantom_cooldown_until=time.time() - 60,  # expired 60s ago
    )
    to_alert = trigger.check_tokens([token])
    assert len(to_alert) == 1
    assert to_alert[0][1]["index"] == 2  # Tier 3


def test_zero_cooldown_is_treated_as_no_cooldown():
    """Default field value (0.0) means 'never set' — must not suppress."""
    trigger = AlertTrigger(_config())
    token = _ath_confirmed_token(
        current_price=0.17,
        phantom_cooldown_until=0.0,
    )
    to_alert = trigger.check_tokens([token])
    assert len(to_alert) == 1


def test_missing_price_timestamp_suppresses_alert():
    """Unknown price freshness must fail closed for tier evaluation."""
    trigger = AlertTrigger(_config())
    token = _ath_confirmed_token(current_price=0.17)
    token.last_price_update = 0.0

    assert trigger.check_tokens([token]) == []


def test_configured_stale_price_window_suppresses_alert():
    """The stale-price ceiling is read from config.tracking."""
    cfg = _config()
    cfg["tracking"]["max_price_age_seconds"] = 30
    trigger = AlertTrigger(cfg)
    token = _ath_confirmed_token(current_price=0.17)
    token.last_price_update = time.time() - 31

    assert trigger.check_tokens([token]) == []

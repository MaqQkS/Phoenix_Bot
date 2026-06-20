"""Tests for modules.phantom_validator.validate_current_after_ath_update.

The validator's job is narrow:
  1. Hit Birdeye /defi/price for the token.
  2. Compare the response to token.ath_price.
  3. Return (is_phantom, log_data) — phantom when Birdeye-current is
     within `phantom_threshold_pct` of ATH (defaults to 60% of ATH).
  4. Fail OPEN on any error — no exceptions propagate, is_phantom=False
     when Birdeye is unreachable / malformed.

Tests use aioresponses to stub the HTTP layer.
"""

import asyncio
import re
import time

import aiohttp
import pytest
from aioresponses import aioresponses

from models import TokenStatus, TrackedToken
from modules.phantom_validator import (
    BIRDEYE_PRICE_URL,
    validate_current_after_ath_update,
)


# aioresponses matches on full URL including query string. Our validator
# appends ?address=<mint>, so use a regex that matches the base URL with
# any query string.
_BIRDEYE_URL_RE = re.compile(re.escape(BIRDEYE_PRICE_URL) + r"\?.*")


def _token(
    *,
    address: str = "TEST_MINT_pump",
    symbol: str = "TEST",
    ath_price: float = 1e-7,
    ath_mcap: float = 100_000.0,
    current_price: float = 1.7e-8,
    current_mcap: float = 17_000.0,
    ath_source: str = "birdeye_reseeded",
) -> TrackedToken:
    """Build a phantom-shaped TrackedToken: low Dex current vs high ATH.
    Caller can override individual fields for boundary cases."""
    return TrackedToken(
        address=address,
        symbol=symbol,
        status=TokenStatus.ATH_CONFIRMED,
        ath_price=ath_price,
        ath_mcap=ath_mcap,
        current_price=current_price,
        current_mcap=current_mcap,
        ath_source=ath_source,
        last_price_update=time.time(),
    )


def _config(threshold: float = 0.40, cooldown: float = 120.0) -> dict:
    return {
        "birdeye": {"api_key": "test-key"},
        "phantom_validator": {
            "enabled": True,
            "phantom_threshold_pct": threshold,
            "cooldown_seconds": cooldown,
            "birdeye_timeout_sec": 5,
            "fail_open_on_error": True,
        },
    }


# ── Phantom detection ─────────────────────────────────────────────────

async def test_phantom_detected_when_birdeye_near_ath():
    """Birdeye-current at 95% of ATH => phantom."""
    token = _token()  # ath=1e-7
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(
            _BIRDEYE_URL_RE,
            payload={"data": {"value": 0.95e-7}, "success": True},
        )
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is True
    assert log["birdeye_current_price"] == pytest.approx(0.95e-7)
    assert log["birdeye_to_ath_ratio"] == pytest.approx(0.95)
    assert log["dex_to_ath_ratio"] == pytest.approx(0.17)
    assert log["is_phantom"] == 1
    assert log["cooldown_until"] is not None
    assert log["birdeye_error"] is None
    # Birdeye-mcap derived from ath_mcap * ratio
    assert log["birdeye_current_mcap"] == pytest.approx(100_000 * 0.95)


async def test_real_dip_when_birdeye_far_from_ath():
    """Birdeye-current at 30% of ATH => real dip (not phantom)."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(
            _BIRDEYE_URL_RE,
            payload={"data": {"value": 0.30e-7}, "success": True},
        )
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_to_ath_ratio"] == pytest.approx(0.30)
    assert log["is_phantom"] == 0
    assert log["cooldown_until"] is None
    assert log["birdeye_error"] is None


# ── Threshold boundary ────────────────────────────────────────────────

async def test_threshold_boundary_inclusive():
    """At exactly threshold (Birdeye/ATH == 1 - threshold = 0.60),
    boundary is INCLUSIVE — classifies as phantom (>= 0.60).

    Documented behavior: the safer side (suppress one extra alert
    rather than let an edge-case phantom fire)."""
    token = _token(ath_price=1.0, ath_mcap=100.0)  # round numbers for exactness
    cfg = _config(threshold=0.40)

    with aioresponses() as mocked:
        # Birdeye returns exactly 0.60 — at the boundary
        mocked.get(_BIRDEYE_URL_RE, payload={"data": {"value": 0.60}})
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    # 0.60 >= (1 - 0.40) is True => phantom
    assert is_phantom is True
    assert log["birdeye_to_ath_ratio"] == pytest.approx(0.60)


async def test_just_below_threshold_is_not_phantom():
    """0.59 (just under 0.60 = 1 - threshold) classifies as a real dip."""
    token = _token(ath_price=1.0, ath_mcap=100.0)
    cfg = _config(threshold=0.40)

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, payload={"data": {"value": 0.59}})
        async with aiohttp.ClientSession() as session:
            is_phantom, _ = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False


# ── Fail-open paths ───────────────────────────────────────────────────

async def test_birdeye_500_fails_open():
    """A 500 from Birdeye must NOT block alerts (fail-open contract)."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, status=500, body="server error")
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] == "http_500"
    assert log["is_phantom"] == 0
    assert log["birdeye_current_price"] is None


async def test_birdeye_429_fails_open():
    """Rate limit also fails open."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, status=429, body="rate limited")
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] == "http_429"


async def test_birdeye_client_error_fails_open():
    """Transport-level aiohttp error fails open."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(
            _BIRDEYE_URL_RE,
            exception=aiohttp.ClientConnectionError("network down"),
        )
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] is not None
    assert "client_error" in log["birdeye_error"]


async def test_birdeye_timeout_fails_open():
    """asyncio.TimeoutError from the HTTP layer fails open."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, exception=asyncio.TimeoutError())
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] == "timeout"


async def test_birdeye_missing_value_field_fails_open():
    """200 response with malformed JSON (no data.value) fails open."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(
            _BIRDEYE_URL_RE,
            payload={"data": {"unrelated_key": 42}, "success": True},
        )
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] == "missing_value_field"


async def test_birdeye_non_positive_value_fails_open():
    """A zero or negative price fails open — Birdeye sometimes returns
    0 for tokens it has no data for yet."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, payload={"data": {"value": 0}})
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] == "non_positive_value"


async def test_birdeye_non_numeric_value_fails_open():
    """A string in `value` (rare schema drift) fails open without raising."""
    token = _token()
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(
            _BIRDEYE_URL_RE,
            payload={"data": {"value": "not_a_number"}},
        )
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is False
    assert log["birdeye_error"] is not None
    assert "non_numeric_value" in log["birdeye_error"]


# ── Defensive: validator called with no ATH yet ──────────────────────

async def test_validator_with_zero_ath_fails_open():
    """If ATH is somehow 0 when validator is called (caller bug), fail
    open — never blocks alerts."""
    token = _token(ath_price=0.0, ath_mcap=0.0)
    cfg = _config()

    async with aiohttp.ClientSession() as session:
        is_phantom, log = await validate_current_after_ath_update(
            token, session, cfg
        )

    assert is_phantom is False
    assert log["birdeye_error"] == "ath_price_non_positive"


# ── Logged data contract (week-1 review depends on this) ─────────────

async def test_log_data_captures_dex_prices_at_validation_time():
    """log_data must hold the Dex current_price/mcap as-of the validation
    call — those are the values that would have been used for the
    drawdown calculation. Verifies dex_to_ath_ratio is also computed."""
    token = _token(
        ath_price=1e-6,
        ath_mcap=500_000.0,
        current_price=2e-7,    # 20% of ATH on the Dex side
        current_mcap=100_000.0,
    )
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, payload={"data": {"value": 9.5e-7}})
        async with aiohttp.ClientSession() as session:
            is_phantom, log = await validate_current_after_ath_update(
                token, session, cfg
            )

    assert is_phantom is True
    # The Dex side numbers must be captured AT validation time
    assert log["dex_current_price"] == pytest.approx(2e-7)
    assert log["dex_current_mcap"] == pytest.approx(100_000.0)
    assert log["dex_to_ath_ratio"] == pytest.approx(0.20)
    # Birdeye/ATH ratio
    assert log["birdeye_to_ath_ratio"] == pytest.approx(0.95)
    # Source captured
    assert log["ath_source"] == "birdeye_reseeded"


async def test_log_data_always_has_token_address_and_symbol():
    """Even on Birdeye errors, the row must be insertable (token_address
    is NOT NULL in the table)."""
    token = _token(address="ADDR_pump", symbol="ABCD")
    cfg = _config()

    with aioresponses() as mocked:
        mocked.get(_BIRDEYE_URL_RE, status=503)
        async with aiohttp.ClientSession() as session:
            _, log = await validate_current_after_ath_update(token, session, cfg)

    assert log["token_address"] == "ADDR_pump"
    assert log["symbol"] == "ABCD"
    assert log["ath_price"] > 0  # NOT NULL constraint
    assert log["created_at"] > 0  # NOT NULL constraint

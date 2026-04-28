"""
modules/phantom_validator.py

Cross-checks Birdeye's current price for a token immediately after Birdeye
seeds or reseeds its ATH. If Birdeye-current is close to the new ATH while
the bot's Dexscreener-derived current is not, that is the BLICKY-class
phantom signature: Dex's cache is serving a stale post-migration price
while Birdeye sees the live peak. Returns (is_phantom, log_data); the
caller is expected to apply a cooldown when is_phantom is True.

Fail-open: any Birdeye 4xx/5xx, timeout, or malformed response returns
(False, log_data_with_error). Never block alerts on Birdeye failure —
better to risk a phantom than miss a real dip.

Birdeye endpoint: GET /defi/price?address=<mint>
Response shape: {"data": {"value": <usd_per_token>, ...}, "success": true}

Configuration (config['phantom_validator']):
  enabled                : true                      — kill switch
  phantom_threshold_pct  : 0.40 (default)            — Birdeye-current >=
                                                       (1 - 0.40) * ath_price
                                                       => phantom
  cooldown_seconds       : 120                       — caller-driven; this
                                                       module just records it
  birdeye_timeout_sec    : 5
  fail_open_on_error     : true
"""

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from models import TrackedToken

logger = logging.getLogger(__name__)

BIRDEYE_PRICE_URL = "https://public-api.birdeye.so/defi/price"


def _ratio(numerator: float, denominator: float) -> Optional[float]:
    """Safe ratio; returns None when denominator <= 0."""
    if denominator and denominator > 0:
        return numerator / denominator
    return None


async def validate_current_after_ath_update(
    token: TrackedToken,
    session: aiohttp.ClientSession,
    config: dict,
) -> tuple[bool, dict]:
    """
    Called immediately after a Birdeye ATH update for `token`.

    Fetches Birdeye's current price for `token.address`, compares to
    `token.ath_price`. Returns (is_phantom, log_data) where:

      is_phantom = True iff Birdeye-current is within phantom_threshold_pct
                   of the new ATH (i.e. token is at-or-near peak; any
                   Dex-derived "drawdown" is the BLICKY phantom).

      log_data   = dict with all phantom_abort_log fields populated where
                   possible (caller passes to db.log_phantom_validation).

    Threshold semantics: with phantom_threshold_pct=0.40, a Birdeye-current
    >= 0.60 * ath_price is a phantom. Boundary is INCLUSIVE (>=), so
    "exactly 60% of ATH" classifies as phantom — the safer choice
    (treats edge case as a phantom rather than letting it fire).

    Failure modes:
      - HTTP non-200, timeout, transport error: (False, log_data) with
        birdeye_error populated.
      - 200 but missing/zero/non-numeric `data.value`: same.
      - All errors logged at WARNING. Caller is expected to pass through
        without setting cooldown.
    """
    pv_cfg = (config or {}).get("phantom_validator", {}) or {}
    threshold_pct  = float(pv_cfg.get("phantom_threshold_pct", 0.40))
    cooldown_secs  = float(pv_cfg.get("cooldown_seconds", 120))
    timeout_sec    = float(pv_cfg.get("birdeye_timeout_sec", 5))
    api_key        = (config.get("birdeye") or {}).get("api_key", "")

    now = time.time()

    log_data: dict = {
        "token_address":         token.address,
        "symbol":                token.symbol,
        "ath_update_time":       now,
        "ath_price":             token.ath_price,
        "ath_mcap":              token.ath_mcap,
        "ath_source":            token.ath_source,
        "birdeye_current_price": None,
        "birdeye_current_mcap":  None,
        "dex_current_price":     token.current_price,
        "dex_current_mcap":      token.current_mcap,
        "birdeye_to_ath_ratio":  None,
        "dex_to_ath_ratio":      _ratio(token.current_price, token.ath_price),
        "is_phantom":            0,
        "cooldown_until":        None,
        "birdeye_error":         None,
        "created_at":            now,
    }

    # Defensive: caller should have a positive ATH after a Birdeye write,
    # but a malformed call shouldn't break the loop.
    if token.ath_price <= 0:
        log_data["birdeye_error"] = "ath_price_non_positive"
        logger.warning(
            f"phantom_validator: ${token.symbol} called with ath_price={token.ath_price}; "
            f"failing open"
        )
        return False, log_data

    headers = {
        "X-API-KEY": api_key,
        "x-chain":   "solana",
        "accept":    "application/json",
    }
    params = {"address": token.address}

    birdeye_price: Optional[float] = None
    try:
        async with session.get(
            BIRDEYE_PRICE_URL,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout_sec),
        ) as resp:
            if resp.status != 200:
                log_data["birdeye_error"] = f"http_{resp.status}"
                logger.warning(
                    f"phantom_validator: ${token.symbol} Birdeye HTTP {resp.status} — "
                    f"failing open"
                )
                return False, log_data
            try:
                payload = await resp.json()
            except Exception as e:
                log_data["birdeye_error"] = f"json_decode:{e!r}"
                logger.warning(
                    f"phantom_validator: ${token.symbol} JSON decode error — failing open: {e!r}"
                )
                return False, log_data

        # Tolerate either {"data": {"value": x}} (current Birdeye shape) or
        # {"value": x} (defensive against minor schema drift).
        data_field = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data_field, dict):
            raw = data_field.get("value")
        else:
            raw = payload.get("value") if isinstance(payload, dict) else None

        if raw is None:
            log_data["birdeye_error"] = "missing_value_field"
            logger.warning(
                f"phantom_validator: ${token.symbol} Birdeye response missing "
                f"data.value — failing open"
            )
            return False, log_data

        try:
            birdeye_price = float(raw)
        except (TypeError, ValueError) as e:
            log_data["birdeye_error"] = f"non_numeric_value:{e!r}"
            logger.warning(
                f"phantom_validator: ${token.symbol} non-numeric Birdeye price "
                f"({raw!r}) — failing open"
            )
            return False, log_data

        if birdeye_price <= 0:
            log_data["birdeye_error"] = "non_positive_value"
            logger.warning(
                f"phantom_validator: ${token.symbol} Birdeye price <= 0 — failing open"
            )
            return False, log_data

    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        log_data["birdeye_error"] = "timeout"
        logger.warning(
            f"phantom_validator: ${token.symbol} Birdeye timeout after {timeout_sec}s — "
            f"failing open"
        )
        return False, log_data
    except aiohttp.ClientError as e:
        log_data["birdeye_error"] = f"client_error:{e!r}"
        logger.warning(
            f"phantom_validator: ${token.symbol} Birdeye client error — failing open: {e!r}"
        )
        return False, log_data
    except Exception as e:
        # Catch-all to honour the fail-open contract. Anything we missed
        # above must NOT propagate up into the price loop.
        log_data["birdeye_error"] = f"unexpected:{e!r}"
        logger.warning(
            f"phantom_validator: ${token.symbol} unexpected error — failing open: {e!r}"
        )
        return False, log_data

    # ── Compare Birdeye-current to new ATH ────────────────────────────
    birdeye_to_ath_ratio = birdeye_price / token.ath_price
    # Derive Birdeye-current-mcap from ath_mcap and the price ratio so
    # the figure shares the same supply basis as the ATH that just
    # triggered this validation. Falls back to None when ath_mcap is
    # unset (rare; defensive).
    if token.ath_mcap and token.ath_mcap > 0:
        birdeye_mcap = token.ath_mcap * birdeye_to_ath_ratio
    else:
        birdeye_mcap = None

    log_data["birdeye_current_price"] = birdeye_price
    log_data["birdeye_current_mcap"]  = birdeye_mcap
    log_data["birdeye_to_ath_ratio"]  = birdeye_to_ath_ratio

    # phantom = Birdeye sees price within `threshold_pct` of ATH
    # (>= is the deliberate inclusive boundary; see docstring)
    is_phantom = birdeye_to_ath_ratio >= (1.0 - threshold_pct)
    log_data["is_phantom"] = 1 if is_phantom else 0

    if is_phantom:
        cooldown_until = now + cooldown_secs
        log_data["cooldown_until"] = cooldown_until
        dex_ratio = log_data["dex_to_ath_ratio"]
        dex_pct = (dex_ratio * 100) if dex_ratio is not None else float("nan")
        logger.info(
            f"🚫 Phantom detected for ${token.symbol}: "
            f"ATH=${token.ath_mcap:,.0f}, "
            f"Birdeye-current=${(birdeye_mcap or 0):,.0f} "
            f"({birdeye_to_ath_ratio*100:.0f}% of ATH), "
            f"Dex-current=${token.current_mcap:,.0f} "
            f"({dex_pct:.0f}% of ATH). "
            f"Suppressing tier eval for {cooldown_secs:.0f}s."
        )
    else:
        logger.debug(
            f"phantom_validator: ${token.symbol} OK — "
            f"birdeye/ath={birdeye_to_ath_ratio*100:.0f}% (threshold for phantom: "
            f">= {(1.0-threshold_pct)*100:.0f}%)"
        )

    return is_phantom, log_data

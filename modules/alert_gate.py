"""
modules/alert_gate.py
Hard-block decision layer between AlertTrigger and TelegramSender.

Calls sender.evaluate_fees() once, then decides allow/block based on:
  - config.fee_gate.blocking_enabled (kill switch — false ⇒ always allow)
  - fee_eval.is_empty                 (transient block: no fee data yet)
  - fee_eval.label in block_labels    (permanent block — sets status=BLOCKED)

On permanent block: marks token status=BLOCKED so future ticks skip it.
On transient block: leaves status alone; token re-evaluated next tier.
Fails open: any exception allows the alert through (logged ERROR).

The returned GateResult carries fee_eval forward so the alert formatter
reuses it without a second fetch/score/log pass.
"""

import logging
from dataclasses import dataclass

from models import TrackedToken
from modules.telegram_sender import (
    FeeEvaluation,
    TelegramSender,
)
from modules import ath_refresh_shadow

logger = logging.getLogger(__name__)


@dataclass
class GateResult:
    allow: bool
    fee_eval: FeeEvaluation | None
    block_reason: str | None


async def evaluate(
    token: TrackedToken,
    tier: dict,
    alert_time: float,
    sender: TelegramSender,
    db,                # database module (imported as `import database as db`)
    config: dict,
) -> GateResult:
    """Decide whether to allow or block one dip alert. See module docstring."""
    try:
        fee_eval = await sender.evaluate_fees(token, tier, alert_time)

        fg_config = config.get("fee_gate", {})
        blocking_enabled = fg_config.get("blocking_enabled", False)
        block_labels = fg_config.get("block_labels", [])

        # Kill switch: if blocking disabled, allow everything through
        if not blocking_enabled:
            gate_result = GateResult(allow=True, fee_eval=fee_eval, block_reason=None)

        # No fee data: block temporarily (do NOT set status to BLOCKED — transient)
        elif fee_eval.is_empty:
            await db.log_alert_block(
                token_address=token.address,
                symbol=token.symbol,
                would_have_tier=tier.get("index", 0),
                tier_name=tier.get("name", ""),
                block_time=alert_time,
                block_reason="no_fee_data",
                fee_gate_log_id=None,
                no_fee_data=True,
            )
            gate_result = GateResult(allow=False, fee_eval=fee_eval, block_reason="no_fee_data")

        # Known-bad label: permanent block, set status
        elif fee_eval.label in block_labels:
            await db.log_alert_block(
                token_address=token.address,
                symbol=token.symbol,
                would_have_tier=tier.get("index", 0),
                tier_name=tier.get("name", ""),
                block_time=alert_time,
                block_reason=fee_eval.label,
                fee_gate_log_id=fee_eval.log_id,
                no_fee_data=False,
            )
            old_status = token.status.value if hasattr(token.status, "value") else str(token.status)
            await db.mark_token_blocked(token.address)
            ath_refresh_shadow.log_status_transition(
                token.address, old_status, "blocked", token.migration_time, token.symbol
            )
            gate_result = GateResult(allow=False, fee_eval=fee_eval, block_reason=fee_eval.label)

        # Passed all checks
        else:
            gate_result = GateResult(allow=True, fee_eval=fee_eval, block_reason=None)

        return gate_result

    except Exception as e:
        logger.error(
            f"alert_gate.evaluate() failed for {token.symbol}/{token.address}: {e}",
            exc_info=True,
        )
        # Fail-open: gate bug shouldn't eat signals
        return GateResult(allow=True, fee_eval=None, block_reason=None)

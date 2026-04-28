"""
filters/lp_floor.py — Thesis 3: LP Floor filter
Flags tokens with too-thin liquidity to trade. Label-only in shadow mode.
"""
from typing import Tuple

LABEL_EMOJI = {
    "OK":   "🟢",
    "THIN": "🟡",
    "LOW":  "🔴",
}


def score_lp(liquidity_usd: float, cfg: dict) -> Tuple[str, str]:
    """
    Returns (label, reason).
    Labels: 'OK' | 'THIN' | 'LOW'
    """
    if liquidity_usd is None or liquidity_usd <= 0:
        return "LOW", "no_lp_data"

    min_lp = cfg.get("min_liquidity_usd", 8000)
    warn_lp = cfg.get("warn_liquidity_usd", 15000)

    if liquidity_usd < min_lp:
        return "LOW", f"lp_under_{int(min_lp/1000)}k"
    if liquidity_usd < warn_lp:
        return "THIN", f"lp_under_{int(warn_lp/1000)}k"
    return "OK", ""
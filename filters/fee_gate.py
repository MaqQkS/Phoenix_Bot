"""
Fee gate v2 — bribe-intensity rubric.

Derived from 198-token manual labeling analysis (Apr 2026).
Primary signal: bribes_pct_of_amm >= 2.81 separates noise from organic with
100% organic recall and 81% noise rejection in the calibration sample.

Keeps score/flags/label/sticky structure from v1.
"""
from typing import Dict, List, Tuple

EPS = 1e-9


def compute_metrics(
    total_fee: float,
    lp: float,
    proto: float,
    creator: float,
    bribes: float,
    tx_count: int,
    events: int,
) -> Dict[str, float]:
    """All AMM/bribe totals in SOL. tx_count = distinct signatures."""
    return {
        "creator_share": creator / (total_fee + EPS),
        "proto_share":   proto   / (total_fee + EPS),
        "lp_share":      lp      / (total_fee + EPS),
        "bribes_pct_of_amm":  (bribes / (total_fee + EPS)) * 100.0,
        "bribes_per_tx_sol":  bribes / (tx_count + EPS),
        "amm_per_tx_sol":     total_fee / (tx_count + EPS),
        "events_per_tx":      events / (tx_count + EPS),
    }


def score_fees(
    total_fee: float,
    lp: float,
    proto: float,
    creator: float,
    bribes: float,
    tx_count: int,
    events: int,
    cfg: dict,
) -> Tuple[int, List[str], Dict[str, float], str]:
    """
    Returns (score, flags, metrics, label).

    Rubric (higher score = more noise-like):
      R1  bribes_pct_of_amm  < 2.81%    +3   primary separator
      R2  bribes_pct_of_amm  < 5.0%     +1   soft zone
      R3  creator_share     >= 0.70     +2   only combined with low bribes
      R4  tx_count          < 500       +1   tiny-sample hedge
    """
    # Guard: no data
    if total_fee is None or total_fee <= 0:
        return 0, ["insufficient_data"], _zero_metrics(), "Normal"
    if tx_count is None or tx_count <= 0 or events is None or events <= 0:
        return 0, ["insufficient_data"], _zero_metrics(), "Normal"

    t = cfg.get("thresholds", {})
    bribes_hard  = t.get("bribes_pct_hard", 2.81)
    bribes_soft  = t.get("bribes_pct_soft", 5.0)
    creator_hi   = t.get("creator_dom_hi", 0.70)
    tx_min       = t.get("tx_count_min", 500)

    m = compute_metrics(total_fee, lp, proto, creator, bribes, tx_count, events)
    flags: List[str] = []
    score = 0

    # R1: primary bribe-intensity gate
    if m["bribes_pct_of_amm"] < bribes_hard:
        score += 3
        flags.append("bribes_starved")
    elif m["bribes_pct_of_amm"] < bribes_soft:
        score += 1
        flags.append("bribes_low")

    # R3: creator dominance only penalized when bribes also low
    # (Heavily Manufactured tokens have high creator_share but pay real bribes)
    if m["creator_share"] >= creator_hi and m["bribes_pct_of_amm"] < bribes_soft:
        score += 2
        flags.append("creator_dom_hi")

    # R4: thin sample hedge
    if tx_count < tx_min:
        score += 1
        flags.append("thin_sample")

    # Label
    labels = cfg.get("labels", {})
    scam_min = labels.get("scam_likely_min", 4)
    susp_min = labels.get("suspicious_min", 3)

    if score >= scam_min:
        label = "SCAM Likely"
    elif score >= susp_min:
        label = "Suspicious"
    elif score >= 1:
        label = "Elevated"
    else:
        label = "Normal"

    return score, flags, m, label


def enforce_sticky(
    current_score: int,
    current_label: str,
    current_flags: list,
    current_metrics: dict,
    historical_score: int,
    historical_label: str,
) -> Tuple[int, str, list]:
    """Never downgrade. If history was worse, keep historical verdict."""
    if historical_score > current_score:
        flags = list(current_flags) + ["sticky_lock"]
        return historical_score, historical_label, flags
    return current_score, current_label, current_flags


def _zero_metrics() -> Dict[str, float]:
    return {
        "creator_share": 0.0, "proto_share": 0.0, "lp_share": 0.0,
        "bribes_pct_of_amm": 0.0, "bribes_per_tx_sol": 0.0,
        "amm_per_tx_sol": 0.0, "events_per_tx": 0.0,
    }
"""
Ante Taxonomy — categorical labels for Ante filter output.
Ghost mode: labels are recorded + displayed, never gate alerts.
"""

# Buckets
ANTE_ORGANIC       = "ORGANIC"
ANTE_WASH_UNIFORM  = "WASH_UNIFORM"
ANTE_BIMODAL       = "BIMODAL"
ANTE_COORDINATED   = "COORDINATED"
ANTE_AMBIGUOUS     = "AMBIGUOUS"
ANTE_INSUFFICIENT  = "INSUFFICIENT"


def classify(stats: dict, config: dict) -> tuple[str, int]:
    """
    Classify a single window's Ante distribution.

    stats: {count, median, p25, p75, width_ratio}  (all SOL, not lamports)
    config: ante_taxonomy block from config.yaml

    Returns: (label, rule_hit)  rule_hit ∈ {1..6}
    """
    floor       = config["floor_sol"]
    real_fee    = config["real_fee_sol"]
    min_samples = config["min_samples"]
    wash_w_max  = config["wash_width_max"]
    bimodal_w   = config["bimodal_width_min"]
    coord_w_max = config["coordinated_width_max"]

    n     = stats.get("count", 0)
    p25   = stats.get("p25", 0.0)
    p75   = stats.get("p75", 0.0)
    width = stats.get("width_ratio", 1.0)

    # Rule 1: insufficient sample
    if n < min_samples:
        return ANTE_INSUFFICIENT, 1

    # Rule 2: bimodal (floored p25 + wide spread) — checked BEFORE uniform
    if p25 <= floor and width >= bimodal_w:
        return ANTE_BIMODAL, 2

    # Rule 3: uniform wash — tight width, hasn't reached real-fee zone
    # (fee level doesn't define wash; tight distribution + sub-competitive fees does)
    if width < wash_w_max and p75 < real_fee:
        return ANTE_WASH_UNIFORM, 3

    # Rule 4: coordinated (tight spread, real fees)
    if width < coord_w_max and p25 >= floor and p75 >= real_fee:
        return ANTE_COORDINATED, 4

    # Rule 5: organic (real quartiles, wide but not floored)
    if p25 >= floor * 2 and p75 >= real_fee:
        return ANTE_ORGANIC, 5

    # Rule 6: didn't match cleanly
    return ANTE_AMBIGUOUS, 6


def classify_both_windows(stats_5m: dict, stats_20sw: dict, config: dict) -> dict:
    """
    Returns both labels independently for drift comparison.
    """
    label_5m,   rule_5m   = classify(stats_5m,   config)
    label_20sw, rule_20sw = classify(stats_20sw, config)
    return {
        "label_5m":      label_5m,
        "rule_hit_5m":   rule_5m,
        "label_20sw":    label_20sw,
        "rule_hit_20sw": rule_20sw,
    }

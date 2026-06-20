"""
Build a shadow-only composite scam-risk research export.

This script does not change Phoenix runtime behavior:
  - SQLite is opened read-only with PRAGMA query_only.
  - No DB tables are created or modified.
  - Composite risk is computed only for CSV research exports.

Important legacy column mapping:
  fee_gate_log.fee_per_event stores bribes_pct_of_amm.
  fee_gate_log.proto_to_lp stores bribes_per_tx_sol.

Inspection Gate status:
  Deprecated. It is intentionally excluded from composite scoring.
  Historical compatibility columns are emitted as empty/deprecated fields only.

Outputs:
  research_exports/composite_risk_research.csv
  research_exports/composite_risk_rule_summary.csv
  research_exports/composite_risk_rule_examples.csv
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = REPO_ROOT / "data" / "bot.db"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "research_exports"

FEE_ABNORMAL = {"Elevated", "Suspicious", "SCAM Likely"}
ANTE_ABNORMAL = {"WASH_UNIFORM", "BIMODAL", "COORDINATED"}
ANTE_WASH = {"WASH_UNIFORM"}
LP_WEAK = {"THIN", "LOW"}
INSPECTION_GATE_STATUS = "DEPRECATED"


@dataclass(frozen=True)
class Rule:
    key: str
    description: str
    predicate: Callable[[dict], bool]
    recommendation: str


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def fetch_rows(conn: sqlite3.Connection, sql: str, params: Iterable[object] = ()) -> list[dict]:
    return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def index_by_token(rows: list[dict], token_key: str = "token_address") -> dict[str, list[dict]]:
    indexed: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        token = row.get(token_key)
        if token:
            indexed[token].append(row)
    for token_rows in indexed.values():
        token_rows.sort(key=lambda row: row.get("alert_time") or 0)
    return indexed


def nearest_by_time(
    indexed: dict[str, list[dict]],
    token: str,
    timestamp: float | None,
    *,
    max_delta_seconds: float,
) -> tuple[dict | None, float | None]:
    if not token or timestamp is None:
        return None, None
    rows = indexed.get(token) or []
    if not rows:
        return None, None
    times = [row.get("alert_time") or 0 for row in rows]
    pos = bisect.bisect_left(times, timestamp)
    candidates = []
    if pos < len(rows):
        candidates.append(rows[pos])
    if pos > 0:
        candidates.append(rows[pos - 1])
    if not candidates:
        return None, None
    best = min(candidates, key=lambda row: abs((row.get("alert_time") or 0) - timestamp))
    delta = abs((best.get("alert_time") or 0) - timestamp)
    if delta <= max_delta_seconds:
        return best, delta
    return None, delta


def safe_ratio(num, den) -> float | None:
    try:
        if den and float(den) > 0:
            return float(num) / float(den)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return None


def minutes_between(later, earlier) -> float | None:
    if later is None or earlier is None:
        return None
    try:
        return (float(later) - float(earlier)) / 60.0
    except (TypeError, ValueError):
        return None


def hours_between(later, earlier) -> float | None:
    mins = minutes_between(later, earlier)
    return mins / 60.0 if mins is not None else None


def fmt_ts(ts) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OSError):
        return ""


def label_from_score(score: int) -> str:
    if score >= 6:
        return "BLOCK_CANDIDATE"
    if score >= 4:
        return "SUSPICIOUS"
    if score >= 2:
        return "CAUTION"
    return "CLEAN"


def score_composite(row: dict) -> tuple[int, str, list[str]]:
    score = 0
    reasons: list[str] = []

    fee_label = row.get("fee_gate_label") or "Normal"
    if fee_label == "SCAM Likely":
        score += 6
        reasons.append("fee_gate_scam_likely(+6)")
    elif fee_label == "Suspicious":
        score += 3
        reasons.append("fee_gate_suspicious(+3)")
    elif fee_label == "Elevated":
        score += 1
        reasons.append("fee_gate_elevated(+1)")

    lp_label = row.get("lp_floor_label")
    if lp_label == "LOW":
        score += 2
        reasons.append("lp_low(+2)")
    elif lp_label == "THIN":
        score += 1
        reasons.append("lp_thin(+1)")

    ante_labels = {row.get("ante_label_5m"), row.get("ante_label_20sw")}
    if "WASH_UNIFORM" in ante_labels:
        score += 2
        reasons.append("ante_wash_uniform(+2)")
    elif "BIMODAL" in ante_labels:
        score += 2
        reasons.append("ante_bimodal(+2)")
    elif "COORDINATED" in ante_labels:
        score += 1
        reasons.append("ante_coordinated(+1)")

    ghost_verdict = row.get("ghost_verdict")
    if ghost_verdict == "block":
        score += 4
        reasons.append("ghost_block(+4)")
    elif ghost_verdict == "caution":
        score += 2
        reasons.append("ghost_caution(+2)")

    if row.get("rule_fast_collapse_sequence"):
        score += 2
        reasons.append("fast_collapse_sequence(+2)")

    return score, label_from_score(score), reasons or ["no_shadow_risk_reasons"]


def build_research_rows(
    conn: sqlite3.Connection,
    *,
    weak_ath_multiple: float,
    quick_collapse_minutes: float,
) -> list[dict]:
    alerts = fetch_rows(
        conn,
        """
        SELECT a.*, t.migration_time, t.migration_mcap, t.ath_time,
               t.liquidity_usd AS token_liquidity_usd, t.status AS token_status,
               t.pool_address, t.ath_source
        FROM alerts a
        LEFT JOIN tokens t ON t.address = a.address
        ORDER BY a.alert_time ASC
        """,
    )
    fee_logs = fetch_rows(conn, "SELECT * FROM fee_gate_log ORDER BY alert_time ASC")
    lp_logs = fetch_rows(conn, "SELECT * FROM lp_floor_log ORDER BY alert_time ASC")
    ante_logs = fetch_rows(conn, "SELECT * FROM ante_log ORDER BY alert_time ASC")
    ghost_logs = fetch_rows(conn, "SELECT * FROM holder_filter_log ORDER BY alert_time ASC")

    fee_by_token = index_by_token(fee_logs)
    lp_by_token = index_by_token(lp_logs)
    ante_by_token = index_by_token(ante_logs)
    ghost_by_token = index_by_token(ghost_logs)

    out: list[dict] = []
    for alert in alerts:
        token = alert.get("address")
        alert_time = alert.get("alert_time")

        fee, fee_delta = nearest_by_time(
            fee_by_token, token, alert_time, max_delta_seconds=120
        )
        lp, lp_delta = nearest_by_time(
            lp_by_token, token, alert_time, max_delta_seconds=120
        )
        ante, ante_delta = nearest_by_time(
            ante_by_token, token, alert_time, max_delta_seconds=120
        )
        ghost, ghost_delta = nearest_by_time(
            ghost_by_token, token, alert_time, max_delta_seconds=3600
        )

        post_peak_multiple = safe_ratio(alert.get("peak_mcap_after"), alert.get("alert_mcap"))
        drawdown_pct = None
        if alert.get("ath_mcap") and alert.get("alert_mcap"):
            drawdown_pct = (1.0 - float(alert["alert_mcap"]) / float(alert["ath_mcap"])) * 100.0
        ath_multiple = safe_ratio(alert.get("ath_mcap"), alert.get("migration_mcap"))
        migration_age_hours = hours_between(alert_time, alert.get("migration_time"))
        migration_to_ath_minutes = minutes_between(alert.get("ath_time"), alert.get("migration_time"))
        ath_to_alert_minutes = minutes_between(alert_time, alert.get("ath_time"))

        fee_label = fee.get("label") if fee else ""
        lp_label = lp.get("label") if lp else ""
        ante_5m = ante.get("label_5m") if ante else ""
        ante_20sw = ante.get("label_20sw") if ante else ""

        rule_fast_collapse = (
            ath_multiple is not None
            and ath_multiple < weak_ath_multiple
            and ath_to_alert_minutes is not None
            and 0 <= ath_to_alert_minutes <= quick_collapse_minutes
            and alert.get("tier_index") in (0, 1)
            and lp_label in LP_WEAK
        )

        row = {
            "row_type": "alert",
            "token_address": token,
            "symbol": alert.get("symbol") or "",
            "alert_id": alert.get("id"),
            "alert_time": alert_time,
            "alert_time_utc": fmt_ts(alert_time),
            "tier_index": alert.get("tier_index"),
            "tier_name": alert.get("tier_name") or "",
            "fee_gate_label": fee_label,
            "fee_gate_score": fee.get("score") if fee else "",
            "fee_gate_flags": fee.get("flags") if fee else "",
            "fee_gate_log_id": fee.get("id") if fee else "",
            "fee_log_delta_seconds": round(fee_delta, 3) if fee_delta is not None else "",
            "total_fee_sol": fee.get("total_fee") if fee else "",
            "lp_fee_sol": fee.get("lp_fee") if fee else "",
            "protocol_fee_sol": fee.get("proto_fee") if fee else "",
            "creator_fee_sol": fee.get("creator_fee") if fee else "",
            "creator_share": fee.get("creator_share") if fee else "",
            "proto_share": fee.get("proto_share") if fee else "",
            "bribes_pct_of_amm": fee.get("fee_per_event") if fee else "",
            "bribes_per_tx_sol": fee.get("proto_to_lp") if fee else "",
            "fee_events": fee.get("events") if fee else "",
            "fee_rate_sol_per_min": fee.get("rate") if fee else "",
            "lp_floor_label": lp_label,
            "lp_floor_reason": lp.get("reason") if lp else "",
            "lp_log_delta_seconds": round(lp_delta, 3) if lp_delta is not None else "",
            "liquidity_usd": lp.get("liquidity_usd") if lp else alert.get("token_liquidity_usd"),
            "ante_label_5m": ante_5m,
            "ante_label_20sw": ante_20sw,
            "ante_5m_count": ante.get("ante_5m_count") if ante else "",
            "ante_20sw_count": ante.get("ante_n20_count") if ante else "",
            "ante_5m_median_sol": ante.get("ante_5m_median_sol") if ante else "",
            "ante_20sw_median_sol": ante.get("ante_n20_median_sol") if ante else "",
            "ante_5m_width_ratio": ante.get("ante_5m_width_ratio") if ante else "",
            "ante_20sw_width_ratio": ante.get("ante_n20_width_ratio") if ante else "",
            "ante_log_delta_seconds": round(ante_delta, 3) if ante_delta is not None else "",
            "ghost_verdict": ghost.get("verdict") if ghost else "",
            "ghost_block_reason": ghost.get("block_reason") if ghost else "",
            "ghost_would_have_blocked": ghost.get("would_have_blocked") if ghost else "",
            "ghost_funding_collision_count": ghost.get("funding_collision_count") if ghost else "",
            "ghost_low_sol_count": ghost.get("low_sol_count") if ghost else "",
            "ghost_user_wallet_count": ghost.get("user_wallet_count") if ghost else "",
            "ghost_log_delta_seconds": round(ghost_delta, 3) if ghost_delta is not None else "",
            "inspection_gate_status": INSPECTION_GATE_STATUS,
            "inspection_label": "",
            "inspection_error_reason": "DEPRECATED: intentionally disabled; excluded_from_composite",
            "inspection_buy_usd": "",
            "inspection_sell_to_buy_ratio": "",
            "inspection_threshold_version": "",
            "migration_age_hours": migration_age_hours,
            "ath_multiple": ath_multiple,
            "time_migration_to_ath_minutes": migration_to_ath_minutes,
            "time_ath_to_alert_minutes": ath_to_alert_minutes,
            "drawdown_pct": drawdown_pct,
            "post_alert_peak_multiple": post_peak_multiple,
            "outcome_2x_plus": int(post_peak_multiple >= 2.0) if post_peak_multiple is not None else "",
            "outcome_death_lt_1_2x": int(post_peak_multiple < 1.2) if post_peak_multiple is not None else "",
            "rule_fee_elev_susp_lp_low": fee_label in {"Elevated", "Suspicious"} and lp_label == "LOW",
            "rule_fee_susp_lp_thin_low": fee_label == "Suspicious" and lp_label in LP_WEAK,
            "rule_creator_hi_bribes_low_lp_weak": (
                numeric_ge(fee.get("creator_share") if fee else None, 0.70)
                and numeric_lt(fee.get("fee_per_event") if fee else None, 5.0)
                and lp_label in LP_WEAK
            ),
            "rule_bribes_starved_lp_weak": (
                numeric_lt(fee.get("fee_per_event") if fee else None, 2.81)
                and lp_label in LP_WEAK
            ),
            "rule_ante_wash_fee_abnormal": (
                (ante_5m in ANTE_WASH or ante_20sw in ANTE_WASH)
                and fee_label in FEE_ABNORMAL
            ),
            "rule_fast_collapse_sequence": rule_fast_collapse,
            "manual_label": "",
            "manual_confidence": "",
            "manual_notes": "",
        }
        score, label, reasons = score_composite(row)
        row["composite_risk_score"] = score
        row["composite_risk_label"] = label
        row["composite_reasons"] = ";".join(reasons)
        out.append(row)
    return out


def numeric_lt(value, threshold: float) -> bool:
    try:
        return value is not None and value != "" and float(value) < threshold
    except (TypeError, ValueError):
        return False


def numeric_ge(value, threshold: float) -> bool:
    try:
        return value is not None and value != "" and float(value) >= threshold
    except (TypeError, ValueError):
        return False


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def summarize_rule(rows: list[dict], rule: Rule) -> dict:
    matches = [row for row in rows if rule.predicate(row)]
    outcomes = [
        float(row["post_alert_peak_multiple"])
        for row in matches
        if row.get("post_alert_peak_multiple") not in ("", None)
    ]
    two_x = sum(1 for value in outcomes if value >= 2.0)
    deaths = sum(1 for value in outcomes if value < 1.2)
    return {
        "rule_key": rule.key,
        "description": rule.description,
        "count": len(matches),
        "token_count": len({row["token_address"] for row in matches}),
        "outcome_count": len(outcomes),
        "two_x_count": two_x,
        "two_x_rate": pct(two_x, len(outcomes)),
        "death_lt_1_2x_count": deaths,
        "death_lt_1_2x_rate": pct(deaths, len(outcomes)),
        "median_post_alert_peak_multiple": median(outcomes),
        "recommendation": rule.recommendation,
    }


def pct(num: int, den: int) -> float | None:
    if not den:
        return None
    return round((num / den) * 100.0, 2)


def build_rules() -> list[Rule]:
    return [
        Rule(
            "A_fee_elev_susp_lp_low",
            "Fee Elevated/Suspicious + LP LOW",
            lambda row: bool(row.get("rule_fee_elev_susp_lp_low")),
            "shadow; consider later only with manual labels",
        ),
        Rule(
            "B_fee_susp_lp_thin_low",
            "Fee Suspicious + LP THIN/LOW",
            lambda row: bool(row.get("rule_fee_susp_lp_thin_low")),
            "shadow; not safe for hard block from proxy data",
        ),
        Rule(
            "C_creator_hi_bribes_low_lp_weak",
            "creator_share >= 0.70 + bribes_pct_of_amm < 5% + LP THIN/LOW",
            lambda row: bool(row.get("rule_creator_hi_bribes_low_lp_weak")),
            "shadow; high attacker cost but sample is small",
        ),
        Rule(
            "D_bribes_starved_lp_weak",
            "bribes_pct_of_amm < 2.81% + LP THIN/LOW",
            lambda row: bool(row.get("rule_bribes_starved_lp_weak")),
            "shadow; high attacker cost but needs labels",
        ),
        Rule(
            "E_ante_wash_fee_abnormal",
            "Ante WASH_UNIFORM + any Fee Gate abnormality",
            lambda row: bool(row.get("rule_ante_wash_fee_abnormal")),
            "shadow; reject as standalone blocker until sample grows",
        ),
        Rule(
            "F_fast_collapse_sequence",
            "weak ATH multiple + quick T1/T2 collapse + LP THIN/LOW",
            lambda row: bool(row.get("rule_fast_collapse_sequence")),
            "shadow; thresholds are exploratory and configurable",
        ),
    ]


def examples_for_rule(rows: list[dict], rule: Rule, *, limit_each: int = 5) -> list[dict]:
    matches = [row for row in rows if rule.predicate(row)]
    with_outcome = [
        row for row in matches
        if row.get("post_alert_peak_multiple") not in ("", None)
    ]
    winners = sorted(
        [row for row in with_outcome if float(row["post_alert_peak_multiple"]) >= 2.0],
        key=lambda row: float(row["post_alert_peak_multiple"]),
        reverse=True,
    )[:limit_each]
    deaths = sorted(
        [row for row in with_outcome if float(row["post_alert_peak_multiple"]) < 1.2],
        key=lambda row: float(row["post_alert_peak_multiple"]),
    )[:limit_each]
    out = []
    for kind, sample_rows in (("winner_2x_plus", winners), ("death_lt_1_2x", deaths)):
        for row in sample_rows:
            out.append(
                {
                    "rule_key": rule.key,
                    "example_type": kind,
                    "token_address": row["token_address"],
                    "symbol": row["symbol"],
                    "alert_time_utc": row["alert_time_utc"],
                    "tier_name": row["tier_name"],
                    "fee_gate_label": row["fee_gate_label"],
                    "lp_floor_label": row["lp_floor_label"],
                    "ante_label_5m": row["ante_label_5m"],
                    "ante_label_20sw": row["ante_label_20sw"],
                    "creator_share": row["creator_share"],
                    "bribes_pct_of_amm": row["bribes_pct_of_amm"],
                    "liquidity_usd": row["liquidity_usd"],
                    "ath_multiple": row["ath_multiple"],
                    "time_ath_to_alert_minutes": row["time_ath_to_alert_minutes"],
                    "post_alert_peak_multiple": row["post_alert_peak_multiple"],
                    "composite_risk_label": row["composite_risk_label"],
                    "composite_reasons": row["composite_reasons"],
                }
            )
    return out


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def print_summary(summary_rows: list[dict], output_paths: list[Path]) -> None:
    print("Composite risk research export complete.")
    for path in output_paths:
        print(f"  wrote {path}")
    print()
    print("Rule summary:")
    for row in summary_rows:
        print(
            f"  {row['rule_key']}: n={row['count']} tokens={row['token_count']} "
            f"2x={row['two_x_rate']}% death<1.2x={row['death_lt_1_2x_rate']}% "
            f"median={fmt_float(row['median_post_alert_peak_multiple'])} "
            f"recommendation={row['recommendation']}"
        )
    print()
    print("Legacy Fee Gate mapping documented in export:")
    print("  fee_gate_log.fee_per_event -> bribes_pct_of_amm")
    print("  fee_gate_log.proto_to_lp   -> bribes_per_tx_sol")
    print()
    print("Inspection Gate status:")
    print("  DEPRECATED; excluded from composite scoring and emitted as empty compatibility columns.")


def fmt_float(value) -> str:
    if value is None or value == "":
        return "n/a"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--weak-ath-multiple", type=float, default=2.0)
    parser.add_argument("--quick-collapse-minutes", type=float, default=60.0)
    parser.add_argument("--examples-per-rule", type=int, default=5)
    args = parser.parse_args()

    conn = connect_readonly(args.db)
    try:
        rows = build_research_rows(
            conn,
            weak_ath_multiple=args.weak_ath_multiple,
            quick_collapse_minutes=args.quick_collapse_minutes,
        )
    finally:
        conn.close()

    rules = build_rules()
    summary_rows = [summarize_rule(rows, rule) for rule in rules]
    example_rows = []
    for rule in rules:
        example_rows.extend(
            examples_for_rule(rows, rule, limit_each=args.examples_per_rule)
        )

    research_path = args.out_dir / "composite_risk_research.csv"
    summary_path = args.out_dir / "composite_risk_rule_summary.csv"
    examples_path = args.out_dir / "composite_risk_rule_examples.csv"
    metadata_path = args.out_dir / "composite_risk_export_metadata.json"

    write_csv(research_path, rows)
    write_csv(summary_path, summary_rows)
    write_csv(examples_path, example_rows)
    metadata_path.write_text(
        json.dumps(
            {
                "db_path": str(args.db),
                "row_count": len(rows),
                "weak_ath_multiple": args.weak_ath_multiple,
                "quick_collapse_minutes": args.quick_collapse_minutes,
                "shadow_only": True,
                "inspection_gate": {
                    "status": INSPECTION_GATE_STATUS,
                    "excluded_from_scoring": True,
                    "note": "Intentionally retired; export keeps empty compatibility columns only.",
                },
                "legacy_fee_gate_mapping": {
                    "fee_gate_log.fee_per_event": "bribes_pct_of_amm",
                    "fee_gate_log.proto_to_lp": "bribes_per_tx_sol",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print_summary(summary_rows, [research_path, summary_path, examples_path, metadata_path])


if __name__ == "__main__":
    main()

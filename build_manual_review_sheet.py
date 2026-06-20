import pandas as pd
from pathlib import Path


ALERTS_CSV = Path(r"db_exports\alerts.csv")
FEE_LOG_CSV = Path(r"db_exports\fee_gate_log.csv")
OUTPUT_CSV = Path("token_manual_review.csv")

START_DATE = pd.Timestamp("2026-04-10")


def main() -> None:
    if not ALERTS_CSV.exists():
        raise FileNotFoundError(f"Missing alerts CSV: {ALERTS_CSV.resolve()}")

    alerts = pd.read_csv(ALERTS_CSV)

    if alerts.empty:
        raise ValueError("alerts.csv is empty.")

    required_alert_cols = ["address", "symbol", "alert_time"]
    missing_alert_cols = [c for c in required_alert_cols if c not in alerts.columns]
    if missing_alert_cols:
        raise ValueError(
            f"alerts.csv is missing required columns: {missing_alert_cols}\n"
            f"Found columns: {list(alerts.columns)}"
        )

    # Parse unix timestamps in SECONDS
    alerts["alert_time"] = pd.to_datetime(alerts["alert_time"], unit="s", errors="coerce")
    alerts = alerts[alerts["alert_time"].notna()].copy()

    print("alerts min:", alerts["alert_time"].min())
    print("alerts max:", alerts["alert_time"].max())

    alerts = alerts[alerts["alert_time"] >= START_DATE].copy()

    if alerts.empty:
        raise ValueError(
            f"No alerts found on or after {START_DATE.date()} in {ALERTS_CSV}"
        )

    alerts = alerts.sort_values("alert_time", ascending=False)
    alerts_unique = alerts.drop_duplicates(subset=["address"], keep="first").copy()

    review = pd.DataFrame({
        "ca": alerts_unique["address"],
        "name": alerts_unique["symbol"],
    })

    optional_alert_cols = [
        "tier_index",
        "tier_name",
        "alert_price",
        "alert_mcap",
        "ath_price",
        "ath_mcap",
        "peak_price_after",
        "peak_mcap_after",
        "alert_time",
    ]
    for col in optional_alert_cols:
        if col in alerts_unique.columns:
            review[col] = alerts_unique[col]

    if FEE_LOG_CSV.exists():
        fee_log = pd.read_csv(FEE_LOG_CSV)

        if not fee_log.empty and "token_address" in fee_log.columns:
            if "alert_time" in fee_log.columns:
                fee_log["alert_time"] = pd.to_datetime(fee_log["alert_time"], unit="s", errors="coerce")
                fee_log = fee_log[fee_log["alert_time"].notna()].copy()
                fee_log = fee_log[fee_log["alert_time"] >= START_DATE].copy()
                fee_log = fee_log.sort_values("alert_time", ascending=False)

            fee_unique = fee_log.drop_duplicates(subset=["token_address"], keep="first").copy()

            keep_cols = ["token_address"]
            rename_map = {"token_address": "ca"}

            optional_fee_cols = [
                "symbol",
                "alert_tier",
                "tier_name",
                "total_fee",
                "lp_fee",
                "proto_fee",
                "creator_fee",
                "rate",
                "events",
                "creator_share",
                "proto_share",
                "fee_per_event",
                "proto_to_lp",
                "score",
                "flags",
                "label",
                "manual_verdict",
                "reviewed_at",
            ]

            for col in optional_fee_cols:
                if col in fee_unique.columns:
                    keep_cols.append(col)

            fee_unique = fee_unique[keep_cols].copy()

            rename_map.update({
                "symbol": "fee_log_symbol",
                "alert_tier": "fee_alert_tier",
                "tier_name": "fee_tier_name",
                "score": "model_score",
                "flags": "model_flags",
                "label": "model_rating",
                "manual_verdict": "existing_manual_verdict",
            })

            fee_unique = fee_unique.rename(columns=rename_map)
            review = review.merge(fee_unique, on="ca", how="left")

    review["manual_class"] = ""
    review["manual_rating"] = ""
    review["is_true_wash_scam"] = ""
    review["why_model_flagged"] = ""
    review["actual_issue"] = ""
    review["notes"] = ""

    preferred_order = [
        "ca",
        "name",
        "model_rating",
        "model_score",
        "model_flags",
        "fee_alert_tier",
        "fee_tier_name",
        "tier_index",
        "tier_name",
        "alert_price",
        "alert_mcap",
        "ath_price",
        "ath_mcap",
        "peak_price_after",
        "peak_mcap_after",
        "total_fee",
        "lp_fee",
        "proto_fee",
        "creator_fee",
        "rate",
        "events",
        "creator_share",
        "proto_share",
        "fee_per_event",
        "proto_to_lp",
        "alert_time",
        "existing_manual_verdict",
        "manual_class",
        "manual_rating",
        "is_true_wash_scam",
        "why_model_flagged",
        "actual_issue",
        "notes",
    ]

    final_cols = [c for c in preferred_order if c in review.columns]
    review = review[final_cols]

    sort_cols = [c for c in ["model_rating", "name"] if c in review.columns]
    if sort_cols:
        review = review.sort_values(sort_cols, ascending=True, na_position="last")

    review.to_csv(OUTPUT_CSV, index=False)

    print(f"Saved: {OUTPUT_CSV.resolve()}")
    print(f"Start date filter: {START_DATE.date()}")
    print(f"Unique tokens in review sheet: {len(review)}")


if __name__ == "__main__":
    main()
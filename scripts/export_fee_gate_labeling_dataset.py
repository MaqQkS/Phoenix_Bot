"""
Build manual-labeling exports for Fee Gate research.

Read-only with respect to Phoenix state:
  - opens SQLite with mode=ro
  - enables PRAGMA query_only
  - writes only CSV/XLSX/README artifacts under research_exports/

Usage:
    python scripts/export_fee_gate_labeling_dataset.py
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = REPO_ROOT / "data" / "bot.db"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "research_exports"

TARGET_LABELS = ["Suspicious", "Elevated", "Normal"]
SEVERITY_RANK = {"suspicious": 1, "elevated": 2, "normal": 3}
MANUAL_COLUMNS = [
    "manual_label",
    "manual_confidence",
    "manual_notes",
    "chart_reviewed",
    "reviewer",
]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def discover_schema(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    schema: dict[str, list[str]] = {}
    for row in rows:
        table = row["name"]
        cols = [
            col["name"]
            for col in conn.execute(f"PRAGMA table_info({quote_ident(table)})")
        ]
        schema[table] = cols
    return schema


def print_schema(schema: dict[str, list[str]]) -> None:
    print("Available tables/columns:")
    for table, cols in schema.items():
        print(f"  {table}: {', '.join(cols)}")
    print()


def first_existing(cols: Iterable[str], candidates: Iterable[str]) -> str | None:
    available = set(cols)
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


def select_existing(
    table_cols: Iterable[str],
    desired: Iterable[tuple[str, str]],
    table: str,
    where_sql: str = "",
    params: Iterable[object] = (),
) -> tuple[str, list[object]]:
    cols = set(table_cols)
    select_parts = [
        f"{quote_ident(source)} AS {quote_ident(alias)}"
        for source, alias in desired
        if source in cols
    ]
    if not select_parts:
        raise ValueError(f"No requested columns exist on table {table}")

    sql = f"SELECT {', '.join(select_parts)} FROM {quote_ident(table)}"
    if where_sql:
        sql = f"{sql} {where_sql}"
    return sql, list(params)


def read_fee_gate_rows(
    conn: sqlite3.Connection,
    schema: dict[str, list[str]],
    labels: list[str],
) -> tuple[pd.DataFrame, str]:
    if "fee_gate_log" not in schema:
        raise RuntimeError("fee_gate_log table is missing")

    fgl_cols = schema["fee_gate_log"]
    label_col = first_existing(
        fgl_cols,
        ["label", "fee_gate_label", "classification", "rating", "model_rating"],
    )
    if not label_col:
        raise RuntimeError(
            "Could not find a Fee Gate label column on fee_gate_log. "
            f"Available columns: {fgl_cols}"
        )

    token_col = first_existing(fgl_cols, ["token_address", "address", "mint"])
    if not token_col:
        raise RuntimeError(
            "Could not find a token address column on fee_gate_log. "
            f"Available columns: {fgl_cols}"
        )

    desired = [
        ("id", "fee_gate_log_id"),
        (token_col, "token_address"),
        ("symbol", "fee_gate_symbol"),
        ("alert_tier", "alert_tier"),
        ("tier_index", "alert_tier"),
        ("tier_name", "tier_name"),
        ("alert_time", "alert_time"),
        ("total_fee", "total_fee"),
        ("lp_fee", "lp_fee"),
        ("proto_fee", "proto_fee"),
        ("protocol_fee", "proto_fee"),
        ("creator_fee", "creator_fee"),
        ("rate", "rate"),
        ("events", "events"),
        ("creator_share", "creator_share"),
        ("proto_share", "proto_share"),
        ("lp_share", "lp_share"),
        ("fee_per_event", "fee_per_event"),
        ("proto_to_lp", "proto_to_lp"),
        ("score", "score"),
        ("flags", "flags"),
        (label_col, "fee_gate_label"),
        ("manual_verdict", "existing_manual_verdict"),
        ("reviewed_at", "existing_reviewed_at"),
    ]

    lower_labels = [label.lower() for label in labels]
    placeholders = ", ".join("?" for _ in lower_labels)
    where_sql = (
        f"WHERE LOWER(TRIM({quote_ident(label_col)})) IN ({placeholders})"
    )
    sql, params = select_existing(
        fgl_cols,
        desired,
        "fee_gate_log",
        where_sql=where_sql,
        params=lower_labels,
    )
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df, label_col

    df["fee_gate_label"] = df["fee_gate_label"].astype("string").str.strip()
    df["fee_gate_label_norm"] = df["fee_gate_label"].str.lower()
    df["fee_gate_severity_rank"] = (
        df["fee_gate_label_norm"].map(SEVERITY_RANK).fillna(99).astype("int64")
    )
    return df, label_col


def read_tokens(
    conn: sqlite3.Connection,
    schema: dict[str, list[str]],
    token_addresses: list[str],
) -> pd.DataFrame:
    if "tokens" not in schema or not token_addresses:
        return pd.DataFrame()

    token_cols = schema["tokens"]
    address_col = first_existing(token_cols, ["address", "token_address", "mint"])
    if not address_col:
        return pd.DataFrame()

    desired = [
        (address_col, "token_address"),
        ("symbol", "token_symbol"),
        ("pool_address", "pool_address"),
        ("status", "token_status"),
        ("migration_price", "migration_price"),
        ("migration_mcap", "migration_mcap"),
        ("current_price", "current_price"),
        ("current_mcap", "current_mcap"),
        ("liquidity_usd", "liquidity_usd"),
        ("ath_price", "token_ath_price"),
        ("ath_mcap", "token_ath_mcap"),
        ("ath_time", "ath_time"),
        ("volume_1h", "volume_1h"),
        ("volume_6h", "volume_6h"),
        ("volume_24h", "volume_24h"),
        ("migration_time", "migration_time"),
        ("migrated_at", "migration_time"),
        ("last_price_update", "last_price_update"),
        ("last_alerted_tier", "last_alerted_tier"),
        ("ath_source", "ath_source"),
        ("ath_confirmed", "ath_confirmed"),
        ("ath_confirmed_at", "ath_confirmed_at"),
        ("pool_orientation", "pool_orientation"),
    ]
    placeholders = ", ".join("?" for _ in token_addresses)
    where_sql = f"WHERE {quote_ident(address_col)} IN ({placeholders})"
    sql, params = select_existing(
        token_cols,
        desired,
        "tokens",
        where_sql=where_sql,
        params=token_addresses,
    )
    return pd.read_sql_query(sql, conn, params=params)


def read_alerts(
    conn: sqlite3.Connection,
    schema: dict[str, list[str]],
    token_addresses: list[str],
) -> pd.DataFrame:
    if "alerts" not in schema or not token_addresses:
        return pd.DataFrame()

    alert_cols = schema["alerts"]
    address_col = first_existing(alert_cols, ["address", "token_address", "mint"])
    tier_col = first_existing(alert_cols, ["tier_index", "alert_tier"])
    time_col = first_existing(alert_cols, ["alert_time", "created_at", "time"])
    if not address_col or not tier_col or not time_col:
        return pd.DataFrame()

    desired = [
        ("id", "alert_id"),
        (address_col, "token_address"),
        (tier_col, "alert_tier"),
        (time_col, "alert_time"),
        ("tier_name", "alert_tier_name"),
        ("alert_price", "price_at_alert"),
        ("price_at_alert", "price_at_alert"),
        ("alert_mcap", "market_cap_at_alert"),
        ("market_cap_at_alert", "market_cap_at_alert"),
        ("ath_price", "ath_at_alert"),
        ("ath_at_alert", "ath_at_alert"),
        ("ath_mcap", "ath_market_cap_at_alert"),
        ("ath_market_cap", "ath_market_cap_at_alert"),
        ("peak_price_after", "peak_price_after"),
        ("peak_mcap_after", "peak_mcap_after"),
        ("trough_price_after", "trough_price_after"),
        ("trough_mcap_after", "trough_mcap_after"),
        ("trough_time", "trough_time"),
        ("peak_time", "peak_time"),
        ("time_to_peak_minutes", "time_to_peak_minutes"),
        ("max_drawdown_pct", "max_drawdown_pct_after_alert"),
    ]
    placeholders = ", ".join("?" for _ in token_addresses)
    where_sql = f"WHERE {quote_ident(address_col)} IN ({placeholders})"
    sql, params = select_existing(
        alert_cols,
        desired,
        "alerts",
        where_sql=where_sql,
        params=token_addresses,
    )
    return pd.read_sql_query(sql, conn, params=params)


def add_time_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            continue
        utc_col = f"{col}_utc"
        df[utc_col] = pd.to_datetime(df[col], unit="s", utc=True, errors="coerce")
        df[utc_col] = df[utc_col].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        df[utc_col] = df[utc_col].fillna("")
    return df


def combine_symbol(df: pd.DataFrame) -> pd.DataFrame:
    if "fee_gate_symbol" in df.columns and "token_symbol" in df.columns:
        df["symbol"] = df["fee_gate_symbol"].combine_first(df["token_symbol"])
    elif "fee_gate_symbol" in df.columns:
        df["symbol"] = df["fee_gate_symbol"]
    elif "token_symbol" in df.columns:
        df["symbol"] = df["token_symbol"]
    return df


def merge_alerts(
    fee_rows: pd.DataFrame,
    alerts: pd.DataFrame,
    tolerance_sec: float,
) -> pd.DataFrame:
    if alerts.empty:
        fee_rows["alert_join_delta_sec"] = pd.NA
        return fee_rows

    required = {"token_address", "alert_tier", "alert_time"}
    if not required.issubset(fee_rows.columns) or not required.issubset(alerts.columns):
        fee_rows["alert_join_delta_sec"] = pd.NA
        return fee_rows

    fee_rows = fee_rows.copy()
    alerts = alerts.copy()
    fee_rows["_source_order"] = range(len(fee_rows))
    alerts["_alert_time_for_delta"] = alerts["alert_time"]

    merged_parts = []
    for key, group in fee_rows.groupby(["token_address", "alert_tier"], dropna=False):
        token_address, alert_tier = key
        candidates = alerts[
            (alerts["token_address"] == token_address)
            & (alerts["alert_tier"] == alert_tier)
        ].sort_values("alert_time")

        if candidates.empty:
            merged = group.copy()
            for col in alerts.columns:
                if col not in required and col not in merged.columns:
                    merged[col] = pd.NA
            merged["alert_join_delta_sec"] = pd.NA
            merged_parts.append(merged)
            continue

        merged = pd.merge_asof(
            group.sort_values("alert_time"),
            candidates,
            on="alert_time",
            by=["token_address", "alert_tier"],
            direction="nearest",
            tolerance=tolerance_sec,
            suffixes=("", "_from_alert"),
        )
        if "_alert_time_for_delta" in merged.columns:
            merged["alert_join_delta_sec"] = (
                merged["alert_time"] - merged["_alert_time_for_delta"]
            ).abs()
        else:
            merged["alert_join_delta_sec"] = pd.NA
        merged_parts.append(merged)

    out = pd.concat(merged_parts, ignore_index=True)
    out = out.sort_values("_source_order").drop(columns=["_source_order"])
    drop_cols = [
        col
        for col in ["_alert_time_for_delta", "_alert_time_for_delta_from_alert"]
        if col in out.columns
    ]
    if drop_cols:
        out = out.drop(columns=drop_cols)
    return out


def enrich_rows(
    fee_rows: pd.DataFrame,
    tokens: pd.DataFrame,
    alerts: pd.DataFrame,
    alert_tolerance_sec: float,
) -> pd.DataFrame:
    df = merge_alerts(fee_rows, alerts, alert_tolerance_sec)

    if not tokens.empty and "token_address" in tokens.columns:
        df = df.merge(tokens, on="token_address", how="left")

    df = combine_symbol(df)

    if "tier_name" in df.columns and "alert_tier_name" in df.columns:
        df["tier_name"] = df["tier_name"].combine_first(df["alert_tier_name"])

    if "lp_share" not in df.columns and {"lp_fee", "total_fee"}.issubset(df.columns):
        df["lp_share"] = df["lp_fee"] / df["total_fee"].replace(0, pd.NA)

    if "liquidity_usd" in df.columns:
        df["lp_usd"] = df["liquidity_usd"]

    if {"alert_time", "migration_time"}.issubset(df.columns):
        df["token_age_at_alert_hours"] = (
            df["alert_time"] - df["migration_time"]
        ) / 3600.0

    if "drawdown_pct" not in df.columns:
        if {"price_at_alert", "ath_at_alert"}.issubset(df.columns):
            df["drawdown_pct"] = (
                1.0 - (df["price_at_alert"] / df["ath_at_alert"].replace(0, pd.NA))
            ) * 100.0
        elif {"market_cap_at_alert", "ath_market_cap_at_alert"}.issubset(df.columns):
            df["drawdown_pct"] = (
                1.0
                - (
                    df["market_cap_at_alert"]
                    / df["ath_market_cap_at_alert"].replace(0, pd.NA)
                )
            ) * 100.0

    if "token_address" in df.columns:
        df["dexscreener_url"] = (
            "https://dexscreener.com/solana/" + df["token_address"].fillna("")
        )

    for col in MANUAL_COLUMNS:
        df[col] = ""

    time_cols = [
        "alert_time",
        "migration_time",
        "ath_time",
        "last_price_update",
        "existing_reviewed_at",
        "peak_time",
        "trough_time",
    ]
    df = add_time_columns(df, time_cols)
    return df


def ordered_columns(df: pd.DataFrame, by_token: bool = False) -> list[str]:
    base = [
        "token_address",
        "symbol",
        "fee_gate_label",
        "alert_tier",
        "tier_name",
        "alert_time",
        "alert_time_utc",
    ]

    token_only = [
        "earliest_alert_time",
        "earliest_alert_time_utc",
        "latest_alert_time",
        "latest_alert_time_utc",
        "fee_gate_log_count",
        "all_fee_gate_labels",
        "all_tiers_fired",
        "all_tier_names_fired",
    ]

    context = [
        "migration_time",
        "migration_time_utc",
        "token_age_at_alert_hours",
        "price_at_alert",
        "market_cap_at_alert",
        "ath_at_alert",
        "ath_market_cap_at_alert",
        "drawdown_pct",
        "max_drawdown_pct_after_alert",
        "peak_price_after",
        "peak_mcap_after",
        "trough_price_after",
        "trough_mcap_after",
        "time_to_peak_minutes",
        "total_fee",
        "lp_fee",
        "proto_fee",
        "creator_fee",
        "rate",
        "events",
        "creator_share",
        "proto_share",
        "lp_share",
        "fee_per_event",
        "proto_to_lp",
        "score",
        "flags",
        "liquidity_usd",
        "lp_usd",
        "ath_source",
        "ath_confirmed",
        "ath_confirmed_at",
        "pool_address",
        "token_status",
        "migration_price",
        "migration_mcap",
        "current_price",
        "current_mcap",
        "token_ath_price",
        "token_ath_mcap",
        "ath_time",
        "ath_time_utc",
        "volume_1h",
        "volume_6h",
        "volume_24h",
        "last_price_update",
        "last_price_update_utc",
        "last_alerted_tier",
        "pool_orientation",
        "dexscreener_url",
        "fee_gate_log_id",
        "alert_id",
        "alert_join_delta_sec",
        "existing_manual_verdict",
        "existing_reviewed_at",
        "existing_reviewed_at_utc",
    ]

    preferred = base + (token_only if by_token else []) + context + MANUAL_COLUMNS
    ordered = [col for col in preferred if col in df.columns]
    extras = [
        col
        for col in df.columns
        if col not in ordered
        and col not in {"fee_gate_label_norm", "fee_gate_severity_rank"}
    ]
    return ordered + extras


def sort_by_fee_gate(df: pd.DataFrame, time_col: str = "alert_time") -> pd.DataFrame:
    sort_cols = ["fee_gate_severity_rank"]
    ascending = [True]
    if time_col in df.columns:
        sort_cols.append(time_col)
        ascending.append(False)
    return df.sort_values(sort_cols, ascending=ascending, na_position="last")


def build_by_token(by_alert: pd.DataFrame) -> pd.DataFrame:
    if by_alert.empty:
        return by_alert.copy()

    sorted_rows = sort_by_fee_gate(by_alert, "alert_time")
    reps = sorted_rows.groupby("token_address", dropna=False).head(1).copy()

    grouped = by_alert.groupby("token_address", dropna=False)
    agg = grouped.agg(
        earliest_alert_time=("alert_time", "min"),
        latest_alert_time=("alert_time", "max"),
        fee_gate_log_count=("fee_gate_log_id", "count"),
    )

    def labels_for(group: pd.DataFrame) -> str:
        labels = (
            group[["fee_gate_label", "fee_gate_severity_rank"]]
            .dropna()
            .drop_duplicates()
            .sort_values("fee_gate_severity_rank")["fee_gate_label"]
            .tolist()
        )
        return ", ".join(str(label) for label in labels)

    def tiers_for(group: pd.DataFrame) -> str:
        if "alert_tier" not in group.columns:
            return ""
        tiers = sorted({int(t) for t in group["alert_tier"].dropna().tolist()})
        return ", ".join(f"T{tier + 1}" for tier in tiers)

    def tier_names_for(group: pd.DataFrame) -> str:
        if "tier_name" not in group.columns:
            return ""
        names = [str(v) for v in group["tier_name"].dropna().unique().tolist()]
        return ", ".join(names)

    agg["all_fee_gate_labels"] = grouped.apply(labels_for, include_groups=False)
    agg["all_tiers_fired"] = grouped.apply(tiers_for, include_groups=False)
    agg["all_tier_names_fired"] = grouped.apply(tier_names_for, include_groups=False)
    agg = agg.reset_index()

    out = reps.drop(
        columns=[
            col
            for col in [
                "earliest_alert_time",
                "latest_alert_time",
                "fee_gate_log_count",
                "all_fee_gate_labels",
                "all_tiers_fired",
                "all_tier_names_fired",
            ]
            if col in reps.columns
        ]
    ).merge(agg, on="token_address", how="left")

    for col in MANUAL_COLUMNS:
        out[col] = ""

    out = add_time_columns(out, ["earliest_alert_time", "latest_alert_time"])
    out = sort_by_fee_gate(out, "latest_alert_time")
    return out


def write_exports(df: pd.DataFrame, csv_path: Path, xlsx_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="manual_labels")
        worksheet = writer.sheets["manual_labels"]
        worksheet.freeze_panes = "A2"
        for idx, col in enumerate(df.columns, start=1):
            values = [str(value) for value in df[col].head(200).tolist()]
            max_len = max([len(str(col)), *(len(v) for v in values)], default=10)
            worksheet.column_dimensions[worksheet.cell(row=1, column=idx).column_letter].width = min(
                max(max_len + 2, 10),
                48,
            )


def write_readme(output_dir: Path) -> Path:
    readme_path = output_dir / "fee_gate_manual_labeling_README.md"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    readme_path.write_text(
        "\n".join(
            [
                "# Fee Gate Manual Labeling Export",
                "",
                f"Generated: {now}",
                "",
                "This export is for manual labeling only.",
                "",
                "Do not treat Fee Gate labels as ground truth. The `fee_gate_label` "
                "column is Phoenix's current model output.",
                "",
                "Fill `manual_label` with one of: `organic`, `wash`, `bundled`, `scam`.",
                "",
                "`manual_label` is the future ground truth for research. After labeling, "
                "compare it against Fee Gate metrics to determine whether better "
                "thresholds/features can improve Fee Gate, or whether Phoenix needs a "
                "separate filter.",
                "",
                "Suggested workflow:",
                "1. Review rows in severity order: Suspicious, Elevated, Normal.",
                "2. Open `dexscreener_url` for chart context.",
                "3. Fill `manual_label`, `manual_confidence`, `manual_notes`, "
                "`chart_reviewed`, and `reviewer`.",
            ]
        ),
        encoding="utf-8",
    )
    return readme_path


def print_summary(
    by_alert: pd.DataFrame,
    by_token: pd.DataFrame,
    output_paths: list[Path],
) -> None:
    print("Export summary:")
    print(f"  total rows by alert: {len(by_alert)}")
    print(f"  total unique tokens: {by_alert['token_address'].nunique(dropna=True) if 'token_address' in by_alert.columns else 0}")
    print("  count by fee_gate_label:")
    if "fee_gate_label" in by_alert.columns:
        counts = (
            by_alert.groupby(["fee_gate_label", "fee_gate_severity_rank"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["fee_gate_severity_rank", "fee_gate_label"])
        )
        for _, row in counts.iterrows():
            print(f"    {row['fee_gate_label']}: {row['count']}")

    tier_cols = [col for col in ["alert_tier", "tier_name"] if col in by_alert.columns]
    if tier_cols:
        print("  count by alert_tier / tier_name:")
        tier_counts = (
            by_alert.groupby(tier_cols, dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(tier_cols)
        )
        for _, row in tier_counts.iterrows():
            tier = row.get("alert_tier", "")
            tier_name = row.get("tier_name", "")
            print(f"    {tier} / {tier_name}: {row['count']}")

    missing_token = (
        int(by_alert["token_address"].isna().sum())
        + int((by_alert["token_address"].astype("string").str.strip() == "").sum())
        if "token_address" in by_alert.columns
        else len(by_alert)
    )
    missing_symbol = (
        int(by_alert["symbol"].isna().sum())
        + int((by_alert["symbol"].astype("string").str.strip() == "").sum())
        if "symbol" in by_alert.columns
        else len(by_alert)
    )
    print(f"  rows missing token_address: {missing_token}")
    print(f"  rows missing symbol: {missing_symbol}")
    print(f"  rows by token: {len(by_token)}")
    print("  output file paths:")
    for path in output_paths:
        print(f"    {path}")
    print()
    print("Manual labeling notes:")
    print("  This export is for manual labeling only.")
    print("  Fee Gate labels are model outputs, not ground truth.")
    print("  Fill manual_label with: organic, wash, bundled, scam.")
    print("  After labeling, compare manual_label against Fee Gate metrics/features.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Fee Gate rows for manual organic/wash/bundled/scam labeling."
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--alert-time-tolerance-sec",
        type=float,
        default=30.0,
        help="Max token+tier timestamp distance for alert enrichment only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = args.db_path.resolve()
    output_dir = args.output_dir.resolve()

    print(f"Database: {db_path}")
    print(f"Output directory: {output_dir}")
    print()

    with connect_readonly(db_path) as conn:
        schema = discover_schema(conn)
        print_schema(schema)

        fee_rows, label_col = read_fee_gate_rows(conn, schema, TARGET_LABELS)
        print(f"Fee Gate label column discovered: fee_gate_log.{label_col}")

        if fee_rows.empty:
            print("No Fee Gate rows matched Suspicious/Elevated/Normal.")
            return

        token_addresses = sorted(
            fee_rows["token_address"].dropna().astype(str).unique().tolist()
        )
        tokens = read_tokens(conn, schema, token_addresses)
        alerts = read_alerts(conn, schema, token_addresses)

    by_alert_full = enrich_rows(
        fee_rows,
        tokens,
        alerts,
        alert_tolerance_sec=args.alert_time_tolerance_sec,
    )
    by_alert_full = sort_by_fee_gate(by_alert_full, "alert_time")

    by_token_full = build_by_token(by_alert_full)

    by_alert = by_alert_full[ordered_columns(by_alert_full, by_token=False)]
    by_token = by_token_full[ordered_columns(by_token_full, by_token=True)]

    by_alert_csv = output_dir / "fee_gate_manual_labeling_by_alert.csv"
    by_alert_xlsx = output_dir / "fee_gate_manual_labeling_by_alert.xlsx"
    by_token_csv = output_dir / "fee_gate_manual_labeling_by_token.csv"
    by_token_xlsx = output_dir / "fee_gate_manual_labeling_by_token.xlsx"

    write_exports(by_alert, by_alert_csv, by_alert_xlsx)
    write_exports(by_token, by_token_csv, by_token_xlsx)
    readme_path = write_readme(output_dir)

    print_summary(
        by_alert_full,
        by_token_full,
        [by_alert_csv, by_alert_xlsx, by_token_csv, by_token_xlsx, readme_path],
    )


if __name__ == "__main__":
    main()

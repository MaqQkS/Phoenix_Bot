"""
Diagnostic: quantify ATH staleness and missed Tier alerts.
Read-only. Writes CSV + JSON outputs; prints aggregate summary.
Compares DB-recorded ATH against Birdeye 1m-OHLCV true-high across the
first 30 min post-migration, then simulates refresh strategies.
"""
import asyncio
import aiohttp
import csv
import json
import sqlite3
import statistics
import time
import yaml
from dataclasses import dataclass, asdict
from pathlib import Path

DB_PATH     = "data/bot.db"
API_KEY     = yaml.safe_load(open("config.yaml"))["birdeye"]["api_key"]
WINDOW_S    = 1800            # 30 minutes
OUT_DIR     = Path("diagnostics_out")
TIERS = [
    ("Tier 1", 0.50, 0.60),
    ("Tier 2", 0.62, 0.80),
    ("Tier 3", 0.82, 0.95),
]

@dataclass
class TokenRow:
    address: str
    symbol: str
    status: str
    migration_time: float
    migration_price: float
    ath_price: float
    ath_source: str
    ath_time: float | None

def classify_tier(drop_pct: float) -> str | None:
    for name, lo, hi in TIERS:
        if lo <= drop_pct <= hi:
            return name
    if drop_pct > 0.95:
        return "Beyond Tier 3"
    return None

def select_sample():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()
    # Stratified sample: emphasise terminal states where we can compare
    strata = {
        "alerted":       18,
        "ath_confirmed": 10,
        "expired":       10,
        "blocked":        5,
    }
    rows = []
    for status, n in strata.items():
        q = """
        SELECT address, symbol, status, migration_time, migration_price,
               ath_price, ath_source, ath_time
        FROM tokens
        WHERE status = ?
          AND migration_time > strftime('%s','now','-7 days')
          AND migration_time > 0
          AND ath_price > 0
        ORDER BY migration_time DESC
        LIMIT ?
        """
        for r in cur.execute(q, (status, n)):
            rows.append(TokenRow(**dict(r)))
    conn.close()
    return rows

def load_alerts_for(addrs: list[str]) -> dict[str, list[dict]]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" * len(addrs))
    q = f"""
    SELECT address, tier_index, tier_name, alert_price, ath_price as alert_ath,
           alert_time
    FROM alerts
    WHERE address IN ({placeholders})
    ORDER BY alert_time ASC
    """
    m: dict[str, list[dict]] = {}
    for r in conn.execute(q, addrs):
        m.setdefault(r["address"], []).append(dict(r))
    conn.close()
    return m

async def fetch_ohlcv(session, address: str, t_from: int, t_to: int,
                     resolution: str = "1m") -> list[dict]:
    url = "https://public-api.birdeye.so/defi/ohlcv"
    params = {"address": address, "type": resolution,
              "time_from": t_from, "time_to": t_to}
    headers = {"X-API-KEY": API_KEY, "x-chain": "solana"}
    async with session.get(url, params=params, headers=headers,
                           timeout=aiohttp.ClientTimeout(total=20)) as r:
        if r.status != 200:
            return []
        data = await r.json()
        return data.get("data", {}).get("items", []) or []

def compute_metrics(tok: TokenRow, candles: list[dict], alerts: list[dict]):
    """Returns dict of per-token metrics."""
    res = {
        "address":          tok.address,
        "symbol":           tok.symbol,
        "status":           tok.status,
        "ath_source":       tok.ath_source,
        "migration_time":   tok.migration_time,
        "migration_price":  tok.migration_price,
        "recorded_ath":     tok.ath_price,
        "ath_time_db":      tok.ath_time,
        "n_candles":        len(candles),
        "true_ath":         None,
        "true_ath_unix":    None,
        "true_min_post_ath": None,
        "true_min_post_ath_unix": None,
        "ath_gap_pct":      None,      # (true_ath - recorded_ath) / recorded_ath
        "true_drawdown_pct":None,      # from true_ath to true_min_post_ath
        "recorded_drawdown_pct": None, # from recorded_ath to true_min_post_ath
        "true_tier":        None,
        "recorded_tier":    None,
        "alerts_fired":     [a["tier_name"] for a in alerts],
        "missed_tier":      False,
        "wrong_tier":       False,
        "time_to_true_ath_s":  None,
        "time_after_ath_to_min_s": None,
        "peak_dwell_within_5pct_s": None,
        "velocity_pre_peak_pct_per_min": None,
    }
    if not candles:
        return res

    # Work in unix seconds; normalize highs/lows
    rows = []
    for c in candles:
        try:
            rows.append({
                "t": int(c.get("unixTime", 0)),
                "o": float(c.get("o", 0) or 0),
                "h": float(c.get("h", 0) or 0),
                "l": float(c.get("l", 0) or 0),
                "c": float(c.get("c", 0) or 0),
            })
        except (ValueError, TypeError):
            continue
    if not rows:
        return res

    # True ATH = max high across window
    peak = max(rows, key=lambda x: x["h"])
    true_ath = peak["h"]
    res["true_ath"]      = true_ath
    res["true_ath_unix"] = peak["t"]

    # True min AFTER the peak (drawdown measurement only makes sense post-peak)
    post = [r for r in rows if r["t"] >= peak["t"]]
    if post:
        trough = min(post, key=lambda x: x["l"])
        res["true_min_post_ath"]      = trough["l"]
        res["true_min_post_ath_unix"] = trough["t"]
        if true_ath > 0:
            res["true_drawdown_pct"] = 1.0 - (trough["l"] / true_ath)
        if tok.ath_price > 0:
            res["recorded_drawdown_pct"] = 1.0 - (trough["l"] / tok.ath_price)

    if tok.ath_price > 0 and true_ath > 0:
        res["ath_gap_pct"] = (true_ath - tok.ath_price) / tok.ath_price

    res["true_tier"]     = classify_tier(res["true_drawdown_pct"]) \
                           if res["true_drawdown_pct"] is not None else None
    res["recorded_tier"] = classify_tier(res["recorded_drawdown_pct"]) \
                           if res["recorded_drawdown_pct"] is not None else None

    # Timing features
    res["time_to_true_ath_s"] = peak["t"] - int(tok.migration_time)
    if res["true_min_post_ath_unix"]:
        res["time_after_ath_to_min_s"] = res["true_min_post_ath_unix"] - peak["t"]

    # Peak-dwell: seconds within 5% of true ATH
    near_peak = [r for r in rows if r["h"] >= true_ath * 0.95]
    if near_peak:
        res["peak_dwell_within_5pct_s"] = (max(r["t"] for r in near_peak)
                                           - min(r["t"] for r in near_peak)) + 60

    # Velocity: % change per min in 2 min before peak
    pre = [r for r in rows if peak["t"] - 120 <= r["t"] < peak["t"]]
    if pre and pre[0]["o"] > 0:
        res["velocity_pre_peak_pct_per_min"] = (
            (peak["h"] - pre[0]["o"]) / pre[0]["o"] * 100.0 / max(1, len(pre))
        )

    # Tier-miss logic:
    fired = set(a["tier_name"] for a in alerts)
    res["missed_tier"] = (
        res["true_tier"] in ("Tier 1", "Tier 2", "Tier 3") and
        res["true_tier"] not in fired
    )
    res["wrong_tier"] = bool(
        fired and res["true_tier"]
        and res["true_tier"] not in fired
        and any(t in fired for t in ("Tier 1","Tier 2","Tier 3"))
    )
    return res

def simulate_strategies(metrics: list[dict]) -> dict:
    """
    A: every 60s × 30 min  -> 30 calls/token
    B: every 30s × 10 min, then every 120s × 20 min -> 20+10 = 30 calls/token
    C: adaptive - refresh if price moved >15% since last Dex poll, OR if
       ath_source='fallback'. We approximate per-token refresh count by
       counting 1m candles whose high/close jumps >=15% from prior candle's
       close (proxy for significant moves).
    D: Dex-fast 10s cycles - approximate by assuming we would catch any
       minute-high within 10s; model it as always catches 1m-candle high.
    """
    out = {"A": {"calls_per_token":30}, "B": {"calls_per_token":30},
           "C": {"calls_per_token":[]},  "D": {"calls_per_token":0}}
    for m in metrics:
        # For Strategy C we need the candle data; we stored n_candles+metrics
        # but not raw rows. Re-use: estimate C calls via reconstructed proxy
        # from true_ath/time_to/velocity. Simpler: assume 1 refresh per
        # 15%-move event; estimate from velocity + peak time.
        # Better: count refreshes as ceil(time_to_peak/60) + 1 (one refresh at
        # peak detection) if velocity >= 15%/min, else 0 during run-up +
        # 1 confirmation at peak.
        v   = m.get("velocity_pre_peak_pct_per_min") or 0.0
        ttp = (m.get("time_to_true_ath_s") or 0) / 60.0
        if v >= 15.0:
            c_calls = int(ttp)
        else:
            c_calls = max(1, int(ttp / 3))
        out["C"]["calls_per_token"].append(c_calls)
    avg_c = statistics.mean(out["C"]["calls_per_token"]) if out["C"]["calls_per_token"] else 0
    out["C"]["avg_calls_per_token"] = avg_c
    # Hit-rate (catches true ATH -> correct tier):
    # A: 60s cadence inside 30min window. Miss rate proxied by peak_dwell<60s.
    # B: 30s cadence inside first 10min; 120s cadence after. Miss proxy by
    #    whether peak fell in first 10min and peak_dwell>=30s, else need <120s.
    # C: adaptive triggers on move; hit if velocity>=15%/min AND peak inside window.
    # D: 10s Dex. Hit if peak_dwell>=10s (nearly always).
    for m in metrics:
        dwell = m.get("peak_dwell_within_5pct_s") or 0
        ttp_s = m.get("time_to_true_ath_s") or 0
        within = (ttp_s <= WINDOW_S)
        m["_hit_A"] = bool(within and dwell >= 60)
        m["_hit_B"] = bool(within and (
            (ttp_s <= 600 and dwell >= 30) or
            (ttp_s >  600 and dwell >= 120)))
        m["_hit_C"] = bool(within and (
            (m.get("velocity_pre_peak_pct_per_min") or 0) >= 15
             or dwell >= 60))
        m["_hit_D"] = bool(within and dwell >= 10)
    return out

async def main():
    OUT_DIR.mkdir(exist_ok=True)
    sample = select_sample()
    addrs  = [t.address for t in sample]
    alerts_map = load_alerts_for(addrs)
    print(f"Sample: {len(sample)} tokens")
    status_counts: dict[str,int] = {}
    for t in sample:
        status_counts[t.status] = status_counts.get(t.status,0)+1
    print(f"By status: {status_counts}")

    results = []
    async with aiohttp.ClientSession() as s:
        for i, tok in enumerate(sample, 1):
            t_from = int(tok.migration_time)
            t_to   = t_from + WINDOW_S
            try:
                candles = await fetch_ohlcv(s, tok.address, t_from, t_to, "1m")
            except Exception as e:
                print(f"[{i:02d}/{len(sample)}] {tok.symbol[:10]:10s} ERR {e}")
                candles = []
            m = compute_metrics(tok, candles, alerts_map.get(tok.address, []))
            results.append(m)
            tag = ("MISS" if m["missed_tier"] else
                   "WRONG" if m["wrong_tier"] else "ok")
            gap = m["ath_gap_pct"]
            gap_str = f"{gap*100:+.1f}%" if gap is not None else "  n/a"
            safe_sym = (tok.symbol or "?").encode("ascii","replace").decode("ascii")
            line = (f"[{i:02d}/{len(sample)}] {safe_sym[:10]:10s} "
                    f"{tok.status:13s} cand={m['n_candles']:2d} "
                    f"gap={gap_str} true_dd={m['true_drawdown_pct']} "
                    f"true_tier={m['true_tier']} fired={m['alerts_fired']} "
                    f"[{tag}]")
            try:
                print(line)
            except UnicodeEncodeError:
                print(line.encode("ascii","replace").decode("ascii"))
            await asyncio.sleep(0.2)   # courteous pacing

    sim = simulate_strategies(results)

    # Save detail CSV
    csv_path = OUT_DIR / "ath_staleness_detail.csv"
    if results:
        fields = list(results[0].keys())
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in results:
                rr = {k: (v if not isinstance(v, list) else "|".join(v))
                      for k, v in r.items()}
                w.writerow(rr)
    # JSON dump
    with (OUT_DIR / "ath_staleness_detail.json").open("w", encoding="utf-8") as f:
        json.dump({"results": results, "sim": sim,
                   "window_s": WINDOW_S, "tiers": TIERS,
                   "run_unix": time.time()}, f, indent=2, default=str)

    # Aggregate summary
    gaps = [r["ath_gap_pct"] for r in results
            if r.get("ath_gap_pct") is not None]
    meaningful_gap = [g for g in gaps if g > 0.10]
    def dist(lst):
        if not lst: return {}
        lst = sorted(lst)
        return {
            "n": len(lst),
            "min": min(lst),
            "p25": lst[len(lst)//4],
            "median": statistics.median(lst),
            "p75": lst[3*len(lst)//4],
            "p90": lst[int(0.9*len(lst))],
            "max": max(lst),
            "mean": statistics.mean(lst),
        }
    missed_true_tier = {}
    for r in results:
        if r["missed_tier"]:
            missed_true_tier[r["true_tier"]] = \
                missed_true_tier.get(r["true_tier"], 0) + 1

    hits = {s: sum(1 for r in results if r.get(f"_hit_{s}")) for s in "ABCD"}
    total_eligible = sum(1 for r in results
                         if r.get("true_drawdown_pct") is not None and
                            r.get("true_tier") in ("Tier 1","Tier 2","Tier 3"))

    with (OUT_DIR / "ath_staleness_summary.txt").open("w", encoding="utf-8") as f:
        f.write("=== ATH STALENESS DIAGNOSTIC ===\n")
        f.write(f"sample_size={len(results)} status_counts={status_counts}\n\n")
        f.write(f"ath_gap_pct_distribution (n={len(gaps)}): {dist(gaps)}\n")
        f.write(f"meaningful_gap (>10%): {len(meaningful_gap)}/{len(gaps)}\n")
        f.write(f"tier-eligible (true_tier is a named tier): {total_eligible}\n")
        f.write(f"missed_tier_by_true_tier: {missed_true_tier}\n")
        f.write(f"wrong_tier_count: "
                f"{sum(1 for r in results if r['wrong_tier'])}\n\n")
        f.write("--- STRATEGY HITS ---\n")
        for s in "ABCD":
            rate = hits[s] / total_eligible if total_eligible else 0.0
            f.write(f" {s}: hits={hits[s]}/{total_eligible} ({rate*100:.0f}%)\n")
        f.write(f"\nStrategy C avg_calls/token: {sim['C']['avg_calls_per_token']:.1f}\n")

    print("\n=== SUMMARY ===")
    print(f"ath_gap_pct: {dist(gaps)}")
    print(f"meaningful_gap (>10%): {len(meaningful_gap)}/{len(gaps)}")
    print(f"tier-eligible: {total_eligible}")
    print(f"missed_tier_by_true_tier: {missed_true_tier}")
    print(f"hits per strategy: {hits} of {total_eligible} eligible")
    print(f"Strategy C avg calls/token: {sim['C']['avg_calls_per_token']:.1f}")
    print(f"\nOutputs: {csv_path}")

if __name__ == "__main__":
    asyncio.run(main())

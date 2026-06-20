"""
diagnostics.py — Phoenix Bot full system health check.
Run from project root:  python diagnostics.py
Read-only. Safe to run while bot is live.
"""

import os
import sys
import time
import sqlite3
import asyncio
from pathlib import Path

DB_PATH = "data/bot.db"
CONFIG_PATH = "config.yaml"

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"


def ok(msg): print(f"{GREEN}✅ {msg}{RESET}")
def warn(msg): print(f"{YELLOW}⚠️  {msg}{RESET}")
def fail(msg): print(f"{RED}❌ {msg}{RESET}")
def header(msg): print(f"\n{BOLD}── {msg} ──{RESET}")


# ──────────────────────────────────────────────────────────────
# 1. FILES & STRUCTURE
# ──────────────────────────────────────────────────────────────
def check_files():
    header("1. File Structure")
    required = [
        "main.py", "config.yaml", "database.py", "models.py", "stats.py",
        "modules/migration_ws.py",
        "modules/price_tracker.py",
        "modules/alert_trigger.py",
        "modules/telegram_sender.py",
        "modules/grpc_indexer.py",
        "filters/fee_gate.py",
        "filters/lp_floor.py",
        "utils/birdeye.py",
        "utils/dexscreener.py",
        "utils/grpc_decoder.py",
        "data/bot.db",
    ]
    missing = [f for f in required if not Path(f).exists()]
    if missing:
        for f in missing:
            fail(f"Missing: {f}")
    else:
        ok(f"All {len(required)} core files present")


# ──────────────────────────────────────────────────────────────
# 2. CONFIG
# ──────────────────────────────────────────────────────────────
def check_config():
    header("2. Config")
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        fail(f"Cannot load config: {e}")
        return None

    checks = [
        ("telegram.bot_token", cfg.get("telegram", {}).get("bot_token")),
        ("telegram.chat_id", cfg.get("telegram", {}).get("chat_id")),
        ("helius.api_key", cfg.get("helius", {}).get("api_key")),
        ("birdeye.api_key", cfg.get("birdeye", {}).get("api_key")),
        ("fee_gate.enabled", cfg.get("fee_gate", {}).get("enabled")),
        ("lp_floor.enabled", cfg.get("lp_floor", {}).get("enabled")),
    ]
    for key, val in checks:
        if val is None or val == "":
            fail(f"{key} missing/empty")
        else:
            display = "***" if "token" in key or "key" in key else val
            ok(f"{key} = {display}")

    grpc_enabled = os.environ.get("GRPC_INDEXER_ENABLED", "").lower() == "true"
    if grpc_enabled:
        ok("GRPC_INDEXER_ENABLED = true")
    else:
        warn("GRPC_INDEXER_ENABLED not set (fee indexer will not run)")

    return cfg


# ──────────────────────────────────────────────────────────────
# 3. DATABASE HEALTH
# ──────────────────────────────────────────────────────────────
def check_database():
    header("3. Database")
    if not Path(DB_PATH).exists():
        fail(f"{DB_PATH} not found")
        return

    size_mb = Path(DB_PATH).stat().st_size / (1024 * 1024)
    print(f"   DB size: {size_mb:.1f} MB")

    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5.0)
        cur = conn.cursor()

        tables = [r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        expected = ["tokens", "alerts", "pumpswap_fees", "fee_gate_log", "lp_floor_log"]
        for t in expected:
            if t in tables:
                count = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                ok(f"{t}: {count:,} rows")
            else:
                fail(f"Missing table: {t}")

        # Recent activity
        print()
        now = time.time()
        day_ago = now - 86400
        hour_ago = now - 3600

        recent_tokens = cur.execute(
            "SELECT COUNT(*) FROM tokens WHERE migration_time >= ?", (day_ago,)
        ).fetchone()[0]
        print(f"   Tokens migrated last 24h: {recent_tokens}")

        recent_alerts = cur.execute(
            "SELECT COUNT(*) FROM alerts WHERE alert_time >= ?", (day_ago,)
        ).fetchone()[0]
        print(f"   Alerts fired last 24h: {recent_alerts}")

        recent_fees = cur.execute(
            "SELECT COUNT(*) FROM pumpswap_fees WHERE block_time >= ?", (hour_ago,)
        ).fetchone()[0]
        if recent_fees > 0:
            ok(f"pumpswap_fees events last 1h: {recent_fees:,}")
        else:
            warn("pumpswap_fees: 0 events in last 1h (gRPC indexer may be down)")

        # Latest migration
        latest = cur.execute(
            "SELECT symbol, address, migration_time FROM tokens "
            "ORDER BY migration_time DESC LIMIT 1"
        ).fetchone()
        if latest:
            age_min = (now - latest[2]) / 60
            print(f"   Latest migration: {latest[0]} ({age_min:.1f} min ago)")

        # Latest alert
        latest_a = cur.execute(
            "SELECT symbol, tier_name, alert_time FROM alerts "
            "ORDER BY alert_time DESC LIMIT 1"
        ).fetchone()
        if latest_a:
            age_min = (now - latest_a[2]) / 60
            print(f"   Latest alert: {latest_a[0]} [{latest_a[1]}] ({age_min:.1f} min ago)")

        # Fee gate label distribution
        print()
        rows = cur.execute(
            "SELECT label, COUNT(*) FROM fee_gate_log "
            "WHERE alert_time >= ? GROUP BY label", (day_ago,)
        ).fetchall()
        if rows:
            print("   Fee gate labels (24h):")
            for label, count in rows:
                print(f"     {label}: {count}")

        conn.close()
    except Exception as e:
        fail(f"DB query failed: {e}")


# ──────────────────────────────────────────────────────────────
# 4. API REACHABILITY
# ──────────────────────────────────────────────────────────────
async def check_apis(cfg):
    header("4. API Reachability")
    if not cfg:
        fail("Skipping — no config")
        return

    try:
        import aiohttp
    except ImportError:
        fail("aiohttp not installed")
        return

    helius_key = cfg.get("helius", {}).get("api_key", "")
    birdeye_key = cfg.get("birdeye", {}).get("api_key", "")

    async with aiohttp.ClientSession() as session:
        # Helius RPC
        try:
            url = f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    ok("Helius RPC reachable")
                else:
                    fail(f"Helius RPC HTTP {r.status}")
        except Exception as e:
            fail(f"Helius: {e}")

        # Birdeye
        try:
            url = "https://public-api.birdeye.so/defi/price?address=So11111111111111111111111111111111111111112"
            headers = {"X-API-KEY": birdeye_key, "x-chain": "solana"}
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    ok("Birdeye reachable (auth OK)")
                elif r.status == 401:
                    fail("Birdeye 401 — API key invalid")
                else:
                    warn(f"Birdeye HTTP {r.status}")
        except Exception as e:
            fail(f"Birdeye: {e}")

        # Dexscreener
        try:
            url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    ok("Dexscreener reachable")
                else:
                    fail(f"Dexscreener HTTP {r.status}")
        except Exception as e:
            fail(f"Dexscreener: {e}")

        # Telegram
        try:
            bot_token = cfg.get("telegram", {}).get("bot_token", "")
            url = f"https://api.telegram.org/bot{bot_token}/getMe"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                if data.get("ok"):
                    ok(f"Telegram bot: @{data['result']['username']}")
                else:
                    fail(f"Telegram: {data}")
        except Exception as e:
            fail(f"Telegram: {e}")


# ──────────────────────────────────────────────────────────────
# 5. gRPC INDEXER HEALTH
# ──────────────────────────────────────────────────────────────
def check_grpc():
    header("5. gRPC Indexer")
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
        cur = conn.cursor()

        now = time.time()
        min5 = now - 300
        min15 = now - 900

        c5 = cur.execute(
            "SELECT COUNT(*) FROM pumpswap_fees WHERE received_at >= ?", (min5,)
        ).fetchone()[0]
        c15 = cur.execute(
            "SELECT COUNT(*) FROM pumpswap_fees WHERE received_at >= ?", (min15,)
        ).fetchone()[0]

        print(f"   Events last 5 min:  {c5:,}")
        print(f"   Events last 15 min: {c15:,}")

        if c5 == 0 and c15 == 0:
            fail("No gRPC events in last 15min — indexer likely DOWN")
        elif c5 == 0:
            warn("No events in last 5min — indexer may have stalled")
        else:
            rate = c5 / 5
            ok(f"Indexer live (~{rate:.0f} events/min)")

        # Unique pools active
        unique_pools = cur.execute(
            "SELECT COUNT(DISTINCT pool_address) FROM pumpswap_fees WHERE received_at >= ?",
            (min15,)
        ).fetchone()[0]
        print(f"   Unique pools active (15min): {unique_pools}")

        conn.close()
    except Exception as e:
        fail(f"gRPC check failed: {e}")


# ──────────────────────────────────────────────────────────────
# 6. ALERT PIPELINE SANITY
# ──────────────────────────────────────────────────────────────
def check_alert_pipeline():
    header("6. Alert Pipeline")
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
        cur = conn.cursor()

        now = time.time()
        day_ago = now - 86400

        # Tokens tracked → alerts fired ratio
        tracked = cur.execute(
            "SELECT COUNT(*) FROM tokens WHERE migration_time >= ?", (day_ago,)
        ).fetchone()[0]
        alerted = cur.execute(
            "SELECT COUNT(DISTINCT address) FROM alerts WHERE alert_time >= ?", (day_ago,)
        ).fetchone()[0]

        if tracked == 0:
            pass
        else:
            rate = (alerted / tracked) * 100
            print(f"   {alerted}/{tracked} tokens alerted ({rate:.0f}% hit rate)")
            if rate == 0:
                warn("No alerts fired — check alert_trigger / price_tracker")
            elif rate > 80:
                warn("Hit rate > 80% — filters may be too loose")
            else:
                ok("Hit rate in reasonable range")

        # ATH seeding health
        missing_ath = cur.execute(
            "SELECT COUNT(*) FROM tokens WHERE migration_time >= ? AND (ath_mcap IS NULL OR ath_mcap = 0)",
            (day_ago,)
        ).fetchone()[0]
        if missing_ath > 0:
            warn(f"{missing_ath} recent tokens missing ATH (Birdeye seeding issue)")
        else:
            ok("All recent tokens have ATH seeded")

        conn.close()
    except Exception as e:
        fail(f"Pipeline check failed: {e}")


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
async def main():
    print(f"\n{BOLD}🔥 Phoenix Bot — Full Diagnostic{RESET}")
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    check_files()
    cfg = check_config()
    check_database()
    await check_apis(cfg)
    check_grpc()
    check_alert_pipeline()

    print(f"\n{BOLD}── Done ──{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
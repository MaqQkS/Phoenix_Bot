"""
test.py — Verify alert metrics against live data.
Loads all tracked tokens from DB, fetches fresh data from Dexscreener + Birdeye,
and prints the formatted alert so you can compare against Axiom/Dexscreener manually.

Usage:
    python test.py                  # show all tracked tokens
    python test.py <mint_address>   # show specific token
"""

import asyncio
import sys
import time

import aiohttp
import yaml

import database as db
from models import TrackedToken
from utils.dexscreener import get_pumpswap_pair, extract_price_data, get_sol_price
from utils.birdeye import get_ath_since_migration


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def fmt_mcap(v: float) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v/1_000:.1f}k"
    else:
        return f"${v:.0f}"


def fmt_price(v: float) -> str:
    if v >= 0.01:
        return f"${v:.4f}"
    elif v >= 0.000001:
        return f"${v:.8f}"
    else:
        return f"${v:.12f}"


async def test_token(token: TrackedToken, session: aiohttp.ClientSession, config: dict):
    """Fetch live data and display formatted alert for verification."""
    birdeye_key = config["birdeye"]["api_key"]

    print(f"\n{'='*60}")
    print(f"  TOKEN: ${token.symbol}")
    print(f"  CA: {token.address}")
    print(f"{'='*60}")

    # ── Fetch live Dexscreener data ──────────────────────────────
    pair = await get_pumpswap_pair(token.address, session)
    if not pair:
        print("  ❌ No PumpSwap pair found on Dexscreener")
        print(f"     Check: https://dexscreener.com/solana/{token.address}")
        return

    data = extract_price_data(pair)
    live_price = data.get("price_usd", 0)
    live_mcap = data.get("mcap", 0)
    live_liq = data.get("liquidity_usd", 0)
    live_vol_1h = data.get("volume_1h", 0)
    live_vol_6h = data.get("volume_6h", 0)
    live_vol_24h = data.get("volume_24h", 0)

    # ── Fetch live Birdeye ATH ───────────────────────────────────
    birdeye_ath = await get_ath_since_migration(
        token_address=token.address,
        migration_time=token.migration_time,
        api_key=birdeye_key,
        session=session,
    )

    # ── Fetch SOL price ──────────────────────────────────────────
    sol_price = await get_sol_price(session)
    expected_mig_mcap = sol_price * 420

    # ── Calculate metrics ────────────────────────────────────────
    # Use birdeye ATH if available, otherwise DB ATH, otherwise live price
    ath_price = birdeye_ath or token.ath_price or live_price
    if live_price > ath_price:
        ath_price = live_price

    # ATH mcap estimate (rough: scale mcap by price ratio)
    if token.migration_price > 0 and ath_price > 0:
        ath_mcap = token.migration_mcap * (ath_price / token.migration_price)
    else:
        ath_mcap = live_mcap

    # Drop from ATH
    if ath_price > 0:
        drop = 1.0 - (live_price / ath_price)
    else:
        drop = 0.0

    # Age
    if token.migration_time > 0:
        age_hours = (time.time() - token.migration_time) / 3600
    else:
        age_hours = 0.0

    # Volume label
    avg_1h = live_vol_6h / 6 if live_vol_6h > 0 else 0
    if avg_1h > 0 and live_vol_1h > avg_1h * 1.5:
        vol_label = "Spiking ⚡"
    elif live_vol_1h > 0:
        vol_label = "Active 📊"
    else:
        vol_label = "Quiet 😴"

    # Pump multiple
    if token.migration_price > 0:
        pump_mult = ath_price / token.migration_price
    else:
        pump_mult = 0.0

    # ── Print formatted alert ────────────────────────────────────
    print()
    print(f"  👀 Floor Watch ⚠️ [Migration Dip]")
    print(f"  ${token.symbol} · {fmt_mcap(live_mcap)} MC · migrated at {fmt_mcap(token.migration_mcap)}")
    print(f"  ├ ATH: {fmt_mcap(ath_mcap)} · Floor: -{drop*100:.0f}% from peak")
    print(f"  ├ Age: {age_hours:.1f} hours")
    print(f"  ├ Vol: {vol_label}")
    print()

    # ── Raw data for manual verification ─────────────────────────
    print(f"  ── RAW DATA (compare with Dexscreener/Axiom) ──")
    print(f"  Live Price:      {fmt_price(live_price)}")
    print(f"  Live Mcap:       {fmt_mcap(live_mcap)}  (raw: ${live_mcap:,.0f})")
    print(f"  Migration Mcap:  {fmt_mcap(token.migration_mcap)}  (expected ~{fmt_mcap(expected_mig_mcap)} at SOL=${sol_price:.0f})")
    print(f"  ATH Price:       {fmt_price(ath_price)}  (source: {'birdeye' if birdeye_ath else 'db/live'})")
    print(f"  ATH Mcap:        {fmt_mcap(ath_mcap)}  (estimated)")
    print(f"  Drop from ATH:   {drop*100:.1f}%")
    print(f"  Pump Multiple:   {pump_mult:.2f}x from migration")
    print(f"  Age:             {age_hours:.1f} hours")
    print(f"  Liquidity:       {fmt_mcap(live_liq)}")
    print(f"  Vol 1h:          {fmt_mcap(live_vol_1h)}")
    print(f"  Vol 6h:          {fmt_mcap(live_vol_6h)}")
    print(f"  Vol 24h:         {fmt_mcap(live_vol_24h)}")
    print(f"  Vol Label:       {vol_label}")
    print(f"  Status:          {token.status.value}")
    print(f"  Last Alerted:    Tier {token.last_alerted_tier}")
    print()
    print(f"  🔗 Dexscreener:  https://dexscreener.com/solana/{token.address}")
    print(f"  🔗 Birdeye:      https://birdeye.so/token/{token.address}?chain=solana")
    print(f"  🔗 PumpSwap:     https://pump.fun/{token.address}")
    print(f"{'='*60}")


async def main():
    config = load_config()
    db_path = config.get("database", {}).get("path", "data/bot.db")
    await db.init_db(db_path)

    # Optional: filter by mint address
    target_mint = sys.argv[1] if len(sys.argv) > 1 else None

    async with aiohttp.ClientSession() as session:
        if target_mint:
            token = await db.get_token(target_mint, db_path)
            if not token:
                print(f"❌ Token {target_mint} not found in DB")
                return
            await test_token(token, session, config)
        else:
            tokens = await db.load_all_tokens(db_path)
            if not tokens:
                print("❌ No tokens in DB. Run the bot first to detect migrations.")
                return

            print(f"\n📊 Testing {len(tokens)} tracked token(s)...\n")
            for token in tokens:
                await test_token(token, session, config)


if __name__ == "__main__":
    asyncio.run(main())
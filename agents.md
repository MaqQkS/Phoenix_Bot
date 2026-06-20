# Phoenix Project — Agent Instructions

## Project Identity

Phoenix is a Solana Pump.fun / PumpSwap sequence engine. It is not just a dip-alert bot.

The system tracks a repeatable market sequence:

1. Pump.fun launch
2. PumpSwap migration
3. Initial expansion after migration
4. Failed reclaim of ATH
5. Sustained pullback
6. Tiered dip alerts
7. Outcome tracking after alerts

The goal is to turn noisy memecoin behavior into structured sequence labels with measurable state at each point. Phoenix should behave like infrastructure for strategy research: field, clock, score, state, and outcome.

Do not optimize for quick alert spam. Optimize for clean state, accurate sequence tracking, durable primitives, and reliable data capture.

---

## Core Design Principles

1. Shadow mode first  
   New filters should usually log labels and metrics before blocking alerts.

2. Sequence correctness over cleverness  
   A signal is only useful if migration state, ATH state, price freshness, tier state, and outcome tracking are correct.

3. Avoid hidden coupling  
   Modules should not depend on each other through unclear globals unless explicitly justified.

4. Data integrity matters  
   SQLite records are used for later research. Avoid changing schemas or field meaning without migration notes.

5. Do not invent thresholds without evidence  
   If adding filters, prefer configurable thresholds and logging over hard-coded assumptions.

6. Prefer explicit state transitions  
   Token lifecycle state should be easy to inspect and debug.

---

## Important System Concepts

### Migration Tracking

Phoenix detects Pump.fun to PumpSwap migrations and starts tracking tokens after migration.

Important files may include:

- `modules/migration_ws.py`
- `modules/migration_detector.py`
- `modules/backfill.py`

There may be legacy overlap between migration modules. Identify duplication or conflicting responsibility.

### Price Tracking

Phoenix tracks live price, ATH since migration, drawdown, and dip tier eligibility.

Important files may include:

- `modules/price_tracker.py`
- `utils/dexscreener.py`
- `utils/birdeye.py`

Important behavior:

- ATH should be since migration, not all-time before migration.
- Price data freshness matters.
- Alerts should not fire on stale price updates.
- A token should not refire the same tier unless explicitly designed to.

### Alert Triggering

Phoenix fires tiered dip alerts after a token has expanded enough from migration and then pulled back.

Important files may include:

- `modules/alert_trigger.py`
- `modules/telegram_sender.py`

Historical tier logic:

- Tier 1: roughly 55–65% drawdown
- Tier 2: roughly 65–80% drawdown
- Tier 3: roughly 80–93% drawdown

Historical activation logic:

- Previously min pump multiple was around 1.39x from migration.
- Later changed or considered changing toward 1.30x.
- Use config values where possible instead of hard-coded numbers.

### Fee / Scam Filtering

Phoenix parses PumpSwap fee structure and uses it for scam/wash-trade research.

Important files may include:

- `modules/grpc_indexer.py`
- `utils/grpc_decoder.py`
- `utils/onchain_fees.py`
- `modules/telegram_sender.py`

Important tables may include:

- `pumpswap_fees`
- `fee_gate_log`
- `alerts`
- `tokens`

Fee columns are meaningful:

- `lp_fee`
- `protocol_fee`
- `creator_fee`
- `total_fee`

Important invariant:

- `total_fee = lp_fee + protocol_fee + creator_fee`

Fee Gate should generally be treated as a shadow label unless the config explicitly says to block.

### LP Floor

LP Floor is another shadow label used to identify thin liquidity traps.

Do not silently enforce LP Floor unless the code/config explicitly intends that.

---

## Known Risk Areas to Inspect

When auditing Phoenix, pay special attention to:

1. ATH seeding and retry logic  
   Check whether retry queue ownership is clean. Avoid unclear ownership between `ath_seeder`, `migration_ws`, and `price_tracker`.

2. Migration module duplication  
   Determine whether `migration_ws.py` and `migration_detector.py` overlap or conflict.

3. Stale price handling  
   Ensure alert triggering respects max price age / freshness.

4. Tier refiring  
   Ensure the same token does not fire the same tier multiple times unless designed.

5. Config drift  
   Find hard-coded thresholds that should come from YAML/config.

6. DB write integrity  
   Check for missing commits, duplicate rows, inconsistent token addresses, bad timestamps, or unit mismatches.

7. Fee unit conversion  
   Make sure lamports/SOL conversions are explicit and not mixed.

8. Async lifecycle bugs  
   Look for un-awaited coroutines, swallowed exceptions, race conditions, session leaks, and reconnect loops.

9. Shadow filter behavior  
   Verify Fee Gate and LP Floor labels are logged/displayed correctly without accidentally blocking unless intended.

10. Telegram formatting  
   Make sure alert messages display useful state without breaking on missing fields.

---

## How to Work

Before editing code:

1. Read the relevant files.
2. Summarize the current architecture.
3. Identify concrete bugs/friction points.
4. Separate confirmed bugs from speculative risks.
5. Propose small fixes.
6. Only then edit.

When editing:

- Keep diffs small.
- Preserve existing architecture unless a refactor is clearly justified.
- Add tests or diagnostic scripts where practical.
- Do not rewrite the whole bot.
- Do not change public behavior silently.
- Prefer config-driven changes.
- Add logging where it helps future debugging.

After editing:

- Run available tests.
- Run linters/type checks if present.
- If tests do not exist, say so and propose the minimum useful test harness.
- Summarize files changed, why, and how to verify.

---

## Output Style

Use this structure in responses:

1. Architecture read
2. Confirmed issues
3. Friction points / maintainability risks
4. Suggested fixes
5. Changes made
6. Tests / verification
7. Remaining risks

Be direct. Do not flatter the project. Treat this like a serious trading/data system where bad state creates bad decisions.

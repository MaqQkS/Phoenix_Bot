# Phoenix Bot — Pre-Fix Systems Audit

| Field | Value |
|---|---|
| Audit date | 2026-04-28 (CDT) |
| Git commit SHA | `d90f3af9b8ac58134d983082f0f1a63efac03faf` |
| Branch | `main` |
| Working tree | dirty (~14 modified `.py`/`.yaml` files; 30+ untracked `.py` files) |
| Live DB | `data/bot.db` (9.93 GB, WAL mode) |
| Log window | `bot.log` covers 2026-04-26 23:12 → 2026-04-28 05:31 (≈30.3 h) |

## Table of contents

1. [ATH path trace](#1-ath-path-trace)
2. [Schema reality check](#2-schema-reality-check)
3. [Uncommitted work map](#3-uncommitted-work-map)
4. [Known-broken / debt inventory](#4-known-broken--debt-inventory)
5. [Live data state](#5-live-data-state)
6. [Consumer dependency map](#6-consumer-dependency-map)
7. [Birdeye credit profile](#7-birdeye-credit-profile)
8. [Top surprises](#8-top-surprises)

---

## 1. ATH path trace

The ATH on `TrackedToken` is written from three modules and read from five. There are **seven** documented `ath_source` provenance values ([models.py:39-47](models.py#L39-L47)) — six are produced; `fallback` is referenced as a possible state but never written. The seed chain runs in `migration_ws._seed_ath` (Birdeye OHLCV) → `process_ath_retry_queue` (retry + reseed + correction) → `price_tracker._process_token` (running max). The phantom validator runs after every Birdeye write to catch BLICKY-class stale-Dex phantoms.

### 1.1 Call graph (writes)

```
migration_ws.MigrationWebSocket.run                        (loop in main.py:410)
  └─ _handle_message → _build_token (no ATH yet, ath_price=0.0)
                    → _seed_ath                            [migration_ws.py:813]
                          ├─ get_ath_since_migration()     [utils/birdeye.py:38]
                          ├─ writes ath_price/ath_mcap/ath_time
                          ├─ writes ath_source = 'birdeye' OR 'unseeded'
                          └─ _run_phantom_validation(token)

migration_ws.MigrationWebSocket.process_retry_queue        (drained by main.py:198)
  └─ if Dex 404 retry → eventually _build_token + _seed_ath above

migration_ws.MigrationWebSocket.process_ath_retry_queue    (drained by main.py:203)
  ├─ if age>max_age and source∈{unseeded,fallback}
  │     → ath_source = 'running_max'                       [migration_ws.py:636]
  ├─ if Birdeye returns hit AND was already 'birdeye'/'birdeye_reseeded'
  │     → ath_source = 'birdeye_reseeded'                  [migration_ws.py:693]
  ├─ if Birdeye returns hit AND first-time
  │     → ath_source = 'birdeye'                           [migration_ws.py:718]
  └─ T+15m correction pass (15m candles)
        → ath_source = 'birdeye_corrected'                 [migration_ws.py:803]
              └─ each writer also fires _run_phantom_validation

price_tracker.PriceTracker.update_prices                   (loop in main.py:411)
  └─ _process_token
      └─ if price > token.ath_price                        [price_tracker.py:151]
          ├─ writes ath_price/ath_mcap/ath_time
          └─ if ath_source ∈ {birdeye, birdeye_reseeded, birdeye_corrected}
                → ath_source = 'birdeye_running_max'       [price_tracker.py:157]
            ELSE  ath_source LEFT UNCHANGED                ⚠ (drift — see §4)
```

### 1.2 Read/write site table

| File:Line | Role | Classification | Notes |
|---|---|---|---|
| [models.py:36-47](models.py#L36-L47) | declares `ath_*` fields + provenance | DECLARATION | docstring lists 7 source values; `fallback` is dead code |
| [models.py:88-92](models.py#L88-L92) | `drop_from_ath` derived property | DERIVED-PROPERTY | `1 - current/ath`; the only fan-out for tier eval |
| [database.py:45-54](database.py#L45-L54) | tokens schema | DECLARATION | `ath_source TEXT DEFAULT 'unseeded'` |
| [database.py:110-111](database.py#L110-L111) | alerts schema | DECLARATION | `ath_price`, `ath_mcap` snapshotted at fire time |
| [database.py:303-305](database.py#L303-L305) | phantom_abort_log schema | DECLARATION | persists ATH at validator time |
| [database.py:407-419](database.py#L407-L419) | ath_source migration | BACKFILL | sets `running_max` for `ath_price>0`, else `unseeded` |
| [database.py:482-501](database.py#L482-L501) | save_token | WRITE-PERSIST | full ATH snapshot per save |
| [database.py:565-573](database.py#L565-L573) | save_alert | WRITE-ALERT | snapshots `ath_price`/`ath_mcap` into alerts row |
| [database.py:719-721](database.py#L719-L721) | _row_to_token | READ-LOAD | hydrates from DB |
| [migration_ws.py:36-38](modules/migration_ws.py#L36-L38) | imports `validate_current_after_ath_update`, `check_inception_bundle` | IMPORT | `check_inception_bundle` is unused (§4) |
| [migration_ws.py:503](modules/migration_ws.py#L503) | new TrackedToken sets `ath_price=0.0` | WRITE-INIT | seed yet to run |
| [migration_ws.py:635-638](modules/migration_ws.py#L635-L638) | retry exhausted → `running_max` | WRITE-SOURCE | only triggers for `unseeded`/`fallback` |
| [migration_ws.py:644-706](modules/migration_ws.py#L644-L706) | reseed loop | WRITE-PEAK | `birdeye_reseeded`, runs phantom validator |
| [migration_ws.py:711-742](modules/migration_ws.py#L711-L742) | first-time success | WRITE-SEED | `birdeye`, runs phantom validator |
| [migration_ws.py:779-809](modules/migration_ws.py#L779-L809) | T+15m correction | WRITE-CORRECT | `birdeye_corrected`, 15m candles |
| [migration_ws.py:830-843](modules/migration_ws.py#L830-L843) | initial seed (`_seed_ath`) | WRITE-SEED | `birdeye` |
| [migration_ws.py:847](modules/migration_ws.py#L847) | initial seed miss | WRITE-INIT | `unseeded`; queued for retry |
| [price_tracker.py:151-157](modules/price_tracker.py#L151-L157) | running-max ATH | WRITE-PEAK | flips source to `birdeye_running_max` only for already-Birdeye sources |
| [price_tracker.py:166](modules/price_tracker.py#L166) | drop log | READ-LOG | informational |
| [price_tracker.py:198](modules/price_tracker.py#L198) | mcap >50× ATH glitch guard | READ-GUARD | suppresses peak-after-alert update |
| [alert_trigger.py:56](modules/alert_trigger.py#L56) | `if ath_price ≤ 0` skip | ALERT-PATH READ | gate before any tier eval |
| [alert_trigger.py:79](modules/alert_trigger.py#L79) | `drop_preview = drop_from_ath` | ALERT-PATH READ | logged during phantom cooldown only |
| [alert_trigger.py:97](modules/alert_trigger.py#L97) | `drop = drop_from_ath` | ALERT-PATH READ | THE tier-triggering decision |
| [alert_trigger.py:119](modules/alert_trigger.py#L119) | log of `ath_mcap` at fire time | ALERT-PATH LOG | informational |
| [alert_trigger.py:139-140](modules/alert_trigger.py#L139-L140) | passes `ath_price`/`ath_mcap` to `save_alert` | ALERT-PATH WRITE | persists snapshot |
| [phantom_validator.py:92-100](modules/phantom_validator.py#L92-L100) | log_data ATH dump | OBS WRITE | full trio + source |
| [phantom_validator.py:109](modules/phantom_validator.py#L109) | guard `if ath_price ≤ 0` | OBS GUARD | fail-open |
| [phantom_validator.py:206-212](modules/phantom_validator.py#L206-L212) | `birdeye_to_ath_ratio` | OBS DECISION | the phantom test; mcap derived from ath_mcap |
| [phantom_validator.py:222](modules/phantom_validator.py#L222) | `is_phantom = ratio ≥ 1-threshold` | OBS DECISION | drives 120 s cooldown |
| [telegram_sender.py:579-601](modules/telegram_sender.py#L579-L601) | alert message text | REPORTING | renders drop% + ATH mcap |
| [stats.py:348-377](stats.py#L348-L377) | recap rendering | REPORTING | reads from tokens row |
| [backfill.py:91, 293](modules/backfill.py#L91) | new token `ath_price=0.0` | WRITE-INIT | for backfilled tokens |
| [tests/test_phantom_validator.py](tests/test_phantom_validator.py) | unit tests | TEST | not in alert path |
| [tests/test_alert_trigger.py](tests/test_alert_trigger.py) | unit tests | TEST | not in alert path |

### 1.3 Risk to ATH high fix

- 🔴 **The decision boundary is `drop_from_ath` ≥ tier_min ([alert_trigger.py:97](modules/alert_trigger.py#L97))** — every fix that changes how/when ATH is written feeds straight into this single read. Any latency or accuracy change in ATH writes is observable here within one 10 s tick.
- 🔴 **Three independent writers each carry their own `ath_source` semantics** ([migration_ws.py:636,693,718,803](modules/migration_ws.py#L636), [price_tracker.py:157](modules/price_tracker.py#L157)). A new Birdeye-1m polling writer must declare its own provenance value to keep downstream attribution intact.
- 🟡 **Phantom validator's decision is divided directly by `ath_price` ([phantom_validator.py:206](modules/phantom_validator.py#L206))** — if the new path raises ATH faster than today, phantom rate goes up; if it raises slower, phantom misses real BLICKYs.
- 🟡 **`fallback` ath_source is referenced in 6 places ([migration_ws.py:16,579,635,710,721,818](modules/migration_ws.py#L16); [price_tracker.py:150](modules/price_tracker.py#L150)) but no writer produces it** — a fix that introduces this state will need to wire those branches.

---

## 2. Schema reality check

The live DB at `data/bot.db` carries 12 user tables; `database.init_db` in [database.py:21](database.py#L21) creates 11. `holder_filter_log` and `holder_snapshots` are created lazily by helper modules outside the canonical path. The legacy gRPC tables (`grpc_prices`, `grpc_ath_shadow`, `ath_refresh_shadow_log`) referenced in old diagnostics scripts and `.claude/settings.local.json` no longer exist — they were dropped (`.claude/settings.local.json:318-319`) and code stopped writing them. `pumpswap_fees` carries 8 columns added by `ALTER TABLE` migrations; the canonical CREATE only declares 12 of the live 20 columns.

### 2.1 Tables: live vs declared

```
sqlite> SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
alert_block_log      holder_filter_log     phantom_abort_log
alerts               holder_snapshots      pumpswap_fees
ante_log             inspection_gate_log   stillborn_log
fee_gate_log         lp_floor_log          tokens
                                           sqlite_sequence
```

| Table | Created by | In `init_db`? | Status |
|---|---|---|---|
| `tokens` | [database.py:35](database.py#L35) | ✓ | drift on `pool_orientation`/`token_decimals`/`phantom_cooldown_until` (added via `ALTER TABLE` guards at [database.py:429-449](database.py#L429-L449)) |
| `alerts` | [database.py:102](database.py#L102) | ✓ | drift on 6 drawdown columns (`trough_*`, `peak_time`, `time_to_peak_minutes`, `max_drawdown_pct`) added at [database.py:462-470](database.py#L462-L470) |
| `pumpswap_fees` | [database.py:60](database.py#L60) | ✓ | **major drift**: canonical schema declares 12 cols; live has 20. See §2.2. |
| `fee_gate_log` | [database.py:120](database.py#L120) | ✓ | matches |
| `alert_block_log` | [database.py:153](database.py#L153) | ✓ | matches; unique-index dedup at [database.py:174](database.py#L174) |
| `lp_floor_log` | [database.py:179](database.py#L179) | ✓ | matches |
| `stillborn_log` | [database.py:199](database.py#L199) | ✓ | **0 rows** — never populated |
| `ante_log` | [database.py:223](database.py#L223) | ✓ | drift: `ante_*_width_ratio`, `median_priority_fee`, `priority_fee_n` added by guards at [database.py:373-399](database.py#L373-L399) |
| `inspection_gate_log` | [database.py:259](database.py#L259) | ✓ | **0 rows** — orphan call site, see §4 |
| `phantom_abort_log` | [database.py:298](database.py#L298) | ✓ | matches; ✓ produced after Apr 26 |
| `holder_filter_log` | [holder_filter.py:148](holder_filter.py#L148) `_ensure_table` | ✗ | created lazily on first `log_holder_filter` call |
| `holder_snapshots` | [snapshot_holders.py:464](snapshot_holders.py#L464) `_ensure_table` | ✗ | created lazily on first snapshot persist |
| `grpc_prices` | NOT in init_db; was created by deleted `modules/migration_detector.py` | ✗ | dropped manually (`.claude/settings.local.json:318`) |
| `grpc_ath_shadow` | NOT in init_db | ✗ | dropped manually (`.claude/settings.local.json:319`) |
| `ath_refresh_shadow_log` | [ath_refresh_shadow.py:43](modules/ath_refresh_shadow.py#L43) | ✗ | only created if `ath_refresh_shadow.enabled=true` (currently `false` in [config.yaml:148](config.yaml#L148)) — table absent |

### 2.2 `pumpswap_fees` drift detail

Canonical declaration in [database.py:60-75](database.py#L60-L75):

```sql
CREATE TABLE pumpswap_fees (
    id, signature, slot, block_time, pool_address, token_address,
    event_type, lp_fee, protocol_fee, creator_fee, total_fee, received_at
)
```

Live `PRAGMA table_info(pumpswap_fees)` adds 8 columns out-of-band:

| Column | Added by | Reason |
|---|---|---|
| `creator_fee` (overlap) | guard [database.py:325-330](database.py#L325-L330) | column already in canonical CREATE; double-defined |
| `base_fee` | guard [database.py:335-340](database.py#L335-L340) | Ante Phase 1 |
| `signature_count` | guard [database.py:341-344](database.py#L341-L344) | Ante Phase 1 |
| `priority_fee` | try/except [database.py:349](database.py#L349) | "schema drift recovery" |
| `jito_tip` | try/except [database.py:350](database.py#L350) | "schema drift recovery" |
| `compute_units_consumed` | try/except [database.py:351](database.py#L351) | "schema drift recovery" |
| `quote_amount` | try/except [database.py:352](database.py#L352) | "schema drift recovery" |
| `user_pubkey` | try/except [database.py:353](database.py#L353) | "schema drift recovery" |
| `base_amount` | guard [database.py:367-371](database.py#L367-L371) | Ante Phase 1 |

Status: **drift**. Anyone reading `database.py` to understand the schema will be wrong by 8 columns. The `try/except: pass` block at [database.py:355-358](database.py#L355-L358) silently swallows column-already-exists errors.

### 2.3 Indexes — declared vs live

```
sqlite> SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, name;
```

Result (35 indexes; trimmed):

| Table | Live indexes | Match declared? |
|---|---|---|
| `tokens` | (none, PK only) | ✓ |
| `alerts` | (none, PK only) | ✓ — none declared either |
| `pumpswap_fees` | `idx_pumpswap_fees_pool`, `idx_pumpswap_fees_pool_buy_time`, `idx_pumpswap_fees_signature` (UNIQUE), `idx_pumpswap_fees_token`, `idx_pumpswap_fees_token_time` | ✓ |
| `alert_block_log` | `idx_abl_*` × 3 + `idx_alert_block_dedup` (UNIQUE) | ✓ |
| `holder_filter_log` | `idx_hflog_token`, `idx_hflog_time`, `idx_hflog_verdict` | ✓ (created in helper module) |
| `holder_snapshots` | `idx_hs_mint`, `idx_hs_ping` | ✓ |
| `inspection_gate_log` | `idx_bgl_token`, `idx_bgl_label`, `idx_bgl_inception`, `idx_bgl_alert` | ✓ — but table empty |

No index on `tokens(migration_time)` or `alerts(address, alert_time)`. With 271 tokens and 251 alerts that's fine today; once the cleanup script restocks data it could matter.

### 2.4 Risk to ATH high fix

- 🟡 **`tokens.ath_*` has no index.** A fix that joins ATH against the live `pumpswap_fees` (14.4 M rows) by `migration_time` will need to push the filter to the joined side — or accept full scans of `tokens`.
- ⚪ Schema drift on `pumpswap_fees` and `tokens` is recoverable via the existing guards; it's documentation-rot, not a functional bug.
- N/A The historical gRPC tables (`grpc_prices`, `grpc_ath_shadow`, `ath_refresh_shadow_log`) are gone and not load-bearing; they only matter if the operator re-enables shadow mode.

---

## 3. Uncommitted work map

`git status` reports 14 modified tracked files and 30+ untracked `.py` files. Running production modules (`modules/phantom_validator.py`, `modules/grpc_indexer.py`, `modules/inspection_gate.py`, `modules/ath_refresh_shadow.py`, `utils/grpc_decoder.py`, `utils/onchain_fees.py`, `snapshot_holders.py`) live entirely outside git — they have never been committed, only `holder_filter.py` was just rescued in commit `cf2c216`. Modified-file diff totals: ≈1,665 inserted / 581 deleted lines across `.py`/`.yaml` (excluding `bot.log`).

### 3.1 Modified files (tracked)

```
$ git diff --stat HEAD -- '*.py' '*.yaml' | grep -v bot.log
 check_db.py                   |  14 --
 config.yaml                   |  17 ++
 database.py                   | 112 ++++++++-
 main.py                       |  34 +++
 models.py                     |  26 +++
 modules/alert_trigger.py      |  98 +++++++-
 modules/backfill.py           | 148 +++++++++++-
 modules/migration_detector.py | 228 ------------------
 modules/migration_ws.py       | 527 +++++++++++++++++++++++++++++++++++++++++-
 modules/price_tracker.py      | 295 ++++++++++++++---------
 stats.py                      | 364 +++++++++++++++++++++++++++--
 test.py                       | 182 ---------------
 utils/birdeye.py              | 147 +++++++++++-
 utils/dexscreener.py          |  54 +++++
 14 files changed, 1665 insertions(+), 581 deletions(-)
```

| File | Area | +/− | Touches ATH? | Completion guess |
|---|---|---|---|---|
| `database.py` | adds `phantom_abort_log` table + schema, `phantom_cooldown_until` column on tokens, `log_phantom_validation`, drawdown columns on alerts, `journal_size_limit`/WAL pragmas | +112 / -0 | indirect (phantom log persists ATH) | complete |
| `main.py` | adds `periodic_checkpoint_loop`, `aiosqlite` import | +34 / -0 | no | complete |
| `models.py` | adds `ath_source`, `pool_orientation`, `token_decimals`, `phantom_cooldown_until` | +26 / -0 | yes — declares ATH provenance | complete |
| `config.yaml` | adds `phantom_validator` block | +17 / -0 | indirect | complete |
| `modules/alert_trigger.py` | tier ranges 50/60/62/80/82/95, stale-price guard (`MAX_PRICE_AGE_SECONDS=300`), phantom-cooldown skip, `index` enrichment on tier dicts | +98 / -10 | yes — gating logic | complete |
| `modules/backfill.py` | `migration_mcap = sol_price*410` (was 420), `migration_price = mcap/1e9`, `periodic_backfill_loop`, pool-metadata fetch | +148 / -0 | partial (sets migration_time correctly to `pairCreatedAt`) | complete |
| `modules/migration_ws.py` | rewrite: ATH retry queue cadence, T+15 m correction, pool metadata, phantom validation hook, `birdeye_reseeded`/`birdeye_corrected` writes | +527 / -16 | **yes — primary ATH writer** | complete |
| `modules/price_tracker.py` | rewrite: bulk Dex pulls, `birdeye_running_max` source flip, stale-mcap guard, peak/trough updates moved out of seed path | +295 / -123 | **yes — running-max writer** | complete |
| `stats.py` | renamed (`alerted_tokens.py` → `stats.py`), CLI subcommands, fee-gate buckets, taxonomy buckets | +364 / -49 | reads ATH for recap | complete |
| `utils/birdeye.py` | adds `1m` candle bracket for <20 min, optional `resolution` arg, plus an **append-only** block at L102+ adding `get_sol_price_at`/`get_sol_price_now` (with a stray triple-quoted instructions block at [utils/birdeye.py:102-108](utils/birdeye.py#L102-L108) that became a no-op string literal and a duplicate `logger = logging.getLogger(__name__)` at L110) | +147 / -3 | yes — primary Birdeye fetch | **WIP-looking** (see §4) |
| `utils/dexscreener.py` | adds `get_pumpswap_pairs_bulk` for batch lookups | +54 / -0 | indirect | complete |
| `check_db.py` (DELETED) | one-off DB query helper | -14 | no | abandoned |
| `test.py` (DELETED) | scratch test | -182 | no | abandoned |
| `modules/migration_detector.py` (DELETED) | the original migration WS predecessor (created `grpc_prices` on import, per old diagnostic notes) | -228 | yes (legacy) | abandoned |

### 3.2 Untracked files

The untracked set contains **production-path code** that is being executed on every run but never committed:

| Untracked path | Imported by (production) | Status |
|---|---|---|
| `modules/phantom_validator.py` | `modules/migration_ws.py:38` | **load-bearing** |
| `modules/grpc_indexer.py` | `main.py:345`, `modules/migration_ws.py:107` | load-bearing (but disabled by `GRPC_INDEXER_ENABLED` env var) |
| `modules/inspection_gate.py` | `modules/migration_ws.py:36` (imported, never called — see §4) | dead-import |
| `modules/ath_refresh_shadow.py` | `modules/migration_ws.py:37`, `modules/price_tracker.py:16`, `modules/backfill.py:18`, `modules/alert_trigger.py:22` | load-bearing (no-op when `enabled=false`) |
| `utils/grpc_decoder.py` | `modules/grpc_indexer.py:33` | load-bearing iff gRPC indexer enabled |
| `utils/onchain_fees.py` | `modules/grpc_indexer.py:45` | load-bearing iff gRPC indexer enabled |
| `snapshot_holders.py` | `main.py:31` | **load-bearing** (Tier-1 holder snapshot) |
| (root) `holder_filter.py` | `main.py:32` | now tracked at `cf2c216` — was previously untracked |
| `db_cleanup.py` | run manually | utility |
| `diagnostics.py`, `block_stats.py`, `ath_staleness_diagnostic.py`, etc. | none | one-off scripts |
| `data/bot.db`, `bot.log.*` | runtime | data |

### 3.3 Risk to ATH high fix

- 🔴 **Production modules are not in git.** A fix shipped in `modules/phantom_validator.py` or `snapshot_holders.py` cannot be reverted with `git checkout` and isn't covered by `git log`/`git blame`. If a regression appears, the only paper trail is the file's mtime.
- 🟡 **The 527-line diff in `migration_ws.py` and 295-line diff in `price_tracker.py` are the ATH-writer rewrites.** The fix scope intersects this code; new edits will stack on top of an already-large uncommitted change.
- 🟡 **`utils/birdeye.py` looks half-pasted** ([utils/birdeye.py:102-110](utils/birdeye.py#L102-L110) — see §4).
- ⚪ Deleted files (`migration_detector.py`, `test.py`, `check_db.py`) are clean drops.

---

## 4. Known-broken / debt inventory

### 4.1 `migration_time` semantic bug — confirmed and quantified

**Code site.** [migration_ws.py:504](modules/migration_ws.py#L504): `migration_time=time.time(),` runs after the Dexscreener fetch+retry block (up to ~9 s of wall clock). On-chain `blockTime` from the migration tx is never read. The backfill path uses `pairCreatedAt` instead at [backfill.py:92](modules/backfill.py#L92) and `[backfill.py:294](modules/backfill.py#L294) — three different semantics in three places.

**Quantification.** Comparing `tokens.migration_time` to `MIN(pumpswap_fees.block_time)` per token:

```sql
WITH first_seen AS (
  SELECT token_address, MIN(block_time) AS first_block_time
  FROM pumpswap_fees
  WHERE token_address IS NOT NULL AND block_time IS NOT NULL
  GROUP BY token_address
)
SELECT COUNT(*),
       AVG(t.migration_time - fs.first_block_time),
       MIN(t.migration_time - fs.first_block_time),
       MAX(t.migration_time - fs.first_block_time),
       SUM(CASE WHEN t.migration_time < fs.first_block_time THEN 1 ELSE 0 END),
       SUM(CASE WHEN t.migration_time >= fs.first_block_time THEN 1 ELSE 0 END)
FROM tokens t JOIN first_seen fs ON t.address = fs.token_address
WHERE t.migration_time IS NOT NULL AND t.migration_time > 0;

→ 270 | -13330.46 | -2689128.00 | 230.53 | 13 | 257
```

Of 270 tokens, **257** have `migration_time` *after* `first_block_time` and **13** have it before (those are backfill `pairCreatedAt` rows, which precede the first PumpSwap fee event). The 13 negative-skew rows pull the mean to −13,330 s but the median is in the +0–60 s bucket:

```
< -300s : 11   (backfilled, pairCreatedAt much earlier than first block)
-300..0 :  2
0..60   : 205  ← live WS-detected tokens, ~95% of WS cohort
60..300 : 52
≥ 300s  :  0
```

Status: **broken** (the WS path reports a fabricated time). Mitigations exist on the consume side — `inspection_gate` derives the slot from `MIN(slot)` in `pumpswap_fees` ([inspection_gate.py:91-96](modules/inspection_gate.py#L91-L96)), and `ath_retry` uses `migration_time` only as an age reference ([migration_ws.py:622-626](modules/migration_ws.py#L622-L626)). But anyone consuming `migration_time` directly (`age_hours`, `last 7-day cohort` queries) is reading a value that is on average ~30 s late.

### 4.2 `grpc_ath_shadow` table — no longer exists

```
sqlite> SELECT name FROM sqlite_master WHERE name='grpc_ath_shadow';
(empty)
```

There is no `grpc_ath_shadow` writer in current source — every reference is in `diagnostics_out/shadow_validation/*` or `.claude/settings.local.json` (which contains `DROP TABLE IF EXISTS grpc_ath_shadow;`). The legacy writer lived in `modules/migration_detector.py` (deleted, see §3).

Status: **deliberately dropped** rather than broken. The shadow_validation reports captured 1,906 rows / 179 tokens before the drop. New writers do not exist. The `diagnostics_out/shadow_validation/REPORT.md` notes "8 of 35 tokens had `MAX(grpc_prices.price_usd)` materially > `MAX(shadow.grpc_peak_price)` — worst MASCOTS +271%" — i.e., the shadow undercounted late-life peaks. Last writes were within `2026-04-23 23:37 → 2026-04-25 02:26 UTC` per the same report.

### 4.3 `ath_refresh_shadow` auto-disable — not the issue; kill switch is on

[config.yaml:148](config.yaml#L148): `enabled: false`. Module entry point at [ath_refresh_shadow.py:73-76](modules/ath_refresh_shadow.py#L73-L76) returns immediately when `enabled` is false:

```python
if not _cfg.get("enabled", False):
    logger.info("ATH refresh shadow: disabled via config")
    _enabled = False
    return False
```

So the time-based auto-disable at [ath_refresh_shadow.py:80-97](modules/ath_refresh_shadow.py#L80-L97) (`auto_disable_after_hours: 48`) is unreachable while the kill switch is set. The `ath_refresh_shadow_log` table doesn't exist in the live DB, confirming `_init_schema` has not been called this session. All `observe_*` / `check_delta` / `log_status_transition` callers are dead-letter no-ops.

The user's note "expired April 22, was supposed to auto-disable" is therefore moot under the **current** config — but the operator has presumably flipped the kill switch off; nothing in the current run would auto-disable if the kill switch were re-enabled.

Status: **disabled (config kill switch)**, not "still running."

### 4.4 Other broken / orphan items observed

- **`check_inception_bundle` is a dead import** ([migration_ws.py:36](modules/migration_ws.py#L36) imports `from modules.inspection_gate import check_inception_bundle`). `grep "check_inception_bundle("` returns only the function definition itself. `inspection_gate_log` has 0 rows. `inspection_gate.enabled: true` in [config.yaml:95](config.yaml#L95) is a lie. Status: **broken / abandoned mid-wiring**.
- **`stillborn_log`** has 0 rows. Schema and helper writer (`log_lp_floor`-shaped) exist; no production writer found. Status: **broken / unfinished feature**.
- **`fallback` ath_source value is referenced but never produced** ([models.py:46](models.py#L46), [migration_ws.py:16,635,710,721,818](modules/migration_ws.py#L16); [price_tracker.py:150](modules/price_tracker.py#L150)). DB query `SELECT ath_source, COUNT(*) FROM tokens GROUP BY ath_source` returns 0 rows for `fallback`. The migration backfill at [database.py:412-418](database.py#L412-L418) sets pre-migration rows to `running_max`, not `fallback`. Status: **drift** — comments and branches reference a state that no live code writes.
- **`utils/birdeye.py:102-108` is a no-op docstring** that says "ADD THIS TO utils/birdeye.py — do not replace the file, append to it." That instructional comment was pasted as a triple-quoted string at module scope and is now dead text. Followed by a duplicate `logger = logging.getLogger(__name__)` at line 110. The file *executes* fine, but it is structured as if mid-paste. Status: **WIP-looking; functionally fine, cosmetically broken**.
- **`bot.db` (root, 0 bytes)** sits next to the real DB at `data/bot.db` (9.93 GB). No code currently writes to root `bot.db` (every reference uses `data/bot.db`), so it's a stale stub. Status: **drift / harmless**.
- **`utils/dexscreener.py.bak`** is a 3.9 KB `.bak` of `dexscreener.py`. Status: **drift / harmless**.
- **`price_tracker._process_token` only updates `ath_source` for already-Birdeye sources** ([price_tracker.py:156-157](modules/price_tracker.py#L156-L157)). Tokens with `ath_source='unseeded'` whose ATH gets bumped by a Dex poll keep `ath_source='unseeded'` while their `ath_price` updates. Live evidence: 16/19 `unseeded` tokens have `ath_price > 0`. Status: **drift** — provenance flag desynced from `ath_price`.
- **17 alerts in the last 7 days fired on `ath_source='unseeded'` tokens** (confirmed via SQL — see §5.6). The alert path does not gate on `ath_source`, so this is by design, but it means the upcoming fix's correctness check cannot use `ath_source` alone to decide whether ATH was Birdeye-validated.
- **WAL safety pre-existing**: comments at [database.py:30-33](database.py#L30-L33) and [main.py:357-359](main.py#L357-L359) record WAL ballooning to 154 GB twice from a long-locking cleanup query. New `journal_size_limit = 1 GB` and 30-min TRUNCATE checkpoints mitigate.

### 4.5 Risk to ATH high fix

- 🔴 **`migration_time` is wrong by tens of seconds for the WS cohort** — any blind-window window calculation that uses it (e.g., "fire Birdeye-1m polls during the Dexscreener blind window") will start the clock at the wrong tick.
- 🔴 **Production modules are not in git** (cross-ref §3) — a fix landed without committing first will be invisible to `git log` review.
- 🟡 **`fallback` is dead code in 6 sites** — a new state introduced by the fix should pick a fresh value and not reuse `fallback` until those branches are pruned.
- ⚪ `inspection_gate`, `stillborn_log`, `ath_refresh_shadow` are all label-only / disabled paths and don't gate alerts; they are "noise" in the audit but won't surprise the fix.

---

## 5. Live data state

All queries below run against `data/bot.db` (read-only).

### 5.1 Token status distribution

```sql
SELECT status, COUNT(*) FROM tokens GROUP BY status;
```

```
alerted        | 99
ath_confirmed  | 46
blocked        | 36
expired        |  3
tracking       | 86
            (270 total)
```

### 5.2 Token ATH source distribution

```sql
SELECT ath_source, COUNT(*),
       SUM(CASE WHEN ath_price > 0 THEN 1 ELSE 0 END) AS with_pos_ath
FROM tokens GROUP BY ath_source;
```

```
birdeye             | 55 | 55
birdeye_corrected   | 11 | 11
birdeye_reseeded    | 94 | 94
birdeye_running_max | 92 | 92
unseeded            | 19 | 16   ← 16 of 19 have ath_price>0 despite source flag
```

`fallback` and `running_max` produce 0 rows.

### 5.3 ath_price coverage by status × source

```sql
SELECT status, ath_source, COUNT(*) AS n,
       SUM(CASE WHEN ath_price > 0 THEN 1 ELSE 0 END) AS with_ath
FROM tokens GROUP BY status, ath_source ORDER BY status, ath_source;
```

```
alerted        | birdeye             | 12 | 12
alerted        | birdeye_corrected   |  5 |  5
alerted        | birdeye_reseeded    | 36 | 36
alerted        | birdeye_running_max | 39 | 39
alerted        | unseeded            |  7 |  7   ← unseeded BUT ath_price>0
ath_confirmed  | birdeye             |  6 |  6
ath_confirmed  | birdeye_corrected   |  1 |  1
ath_confirmed  | birdeye_reseeded    | 16 | 16
ath_confirmed  | birdeye_running_max | 21 | 21
ath_confirmed  | unseeded            |  2 |  2
blocked        | birdeye             |  5 |  5
blocked        | birdeye_corrected   |  4 |  4
blocked        | birdeye_reseeded    |  7 |  7
blocked        | birdeye_running_max | 17 | 17
blocked        | unseeded            |  3 |  3
expired        | unseeded            |  3 |  0
tracking       | birdeye             | 33 | 33
tracking       | birdeye_corrected   |  1 |  1
tracking       | birdeye_reseeded    | 35 | 35
tracking       | birdeye_running_max | 13 | 13
tracking       | unseeded            |  4 |  4
```

### 5.4 `migration_time` NULL/zero rate

```sql
SELECT COUNT(*) AS total,
       SUM(CASE WHEN migration_time IS NULL THEN 1 ELSE 0 END) AS null_count,
       SUM(CASE WHEN migration_time = 0 THEN 1 ELSE 0 END) AS zero_count,
       SUM(CASE WHEN migration_time > 0 THEN 1 ELSE 0 END) AS positive
FROM tokens;
```

```
270 | 0 | 0 | 270   (all positive — every token has a migration_time)
```

### 5.5 `block_time` NULL rate on `pumpswap_fees`

```sql
SELECT COUNT(*) AS total, SUM(CASE WHEN block_time IS NULL THEN 1 ELSE 0 END) FROM pumpswap_fees;
→ 14407184 | 0   (zero NULLs)
```

### 5.6 7-day cohort behaviour

```sql
WITH c AS (SELECT * FROM tokens WHERE migration_time >= unixepoch('now','-7 days'))
SELECT COUNT(*),
       SUM(CASE WHEN status IN ('ath_confirmed','alerted','blocked') THEN 1 ELSE 0 END),
       SUM(CASE WHEN status='alerted' THEN 1 ELSE 0 END)
FROM c;

→ 269 new tokens | 181 reached post-tracking | 99 in alerted state
```

```sql
SELECT COUNT(*) AS alerts_total, SUM(CASE WHEN tier_index=0 THEN 1 ELSE 0 END) AS t1,
       SUM(CASE WHEN tier_index=1 THEN 1 ELSE 0 END) AS t2,
       SUM(CASE WHEN tier_index=2 THEN 1 ELSE 0 END) AS t3,
       COUNT(DISTINCT address)
FROM alerts WHERE alert_time >= unixepoch('now','-7 days');

→ 251 | 75 (T1) | 90 (T2) | 86 (T3) | 99 unique tokens
```

Cohort firing-rate: **99 / 269 = 36.8 %** of tokens ever fire any tier in the 7-day window.

### 5.7 Age × source distribution (median age by source, hours)

```sql
WITH ranked AS (
  SELECT ath_source, (unixepoch('now') - migration_time)/3600.0 AS age_h,
         ROW_NUMBER() OVER (PARTITION BY ath_source ORDER BY age_h) AS rn,
         COUNT(*) OVER (PARTITION BY ath_source) AS cnt
  FROM tokens WHERE migration_time > 0
) SELECT ath_source, age_h, cnt FROM ranked WHERE rn = (cnt+1)/2;
```

```
birdeye             | 13.4 h | n=55
birdeye_corrected   | 11.4 h | n=11
birdeye_reseeded    | 13.8 h | n=95
birdeye_running_max | 14.6 h | n=90
unseeded            | 30.5 h | n=19   ← significantly older
```

The `unseeded` cohort has notably-older medians, with a max age of 771.9 h (32 d). These are tokens that should have transitioned to `running_max` (after 30-min retry exhaustion) or `expired` (after 48 h `max_token_age_hours`) but didn't — see §4.4.

### 5.8 `unseeded` alerts in 7-day window

```sql
SELECT a.address, a.tier_index, t.ath_source
FROM alerts a JOIN tokens t ON t.address = a.address
WHERE t.ath_source IN ('unseeded','running_max','fallback')
  AND a.alert_time >= unixepoch('now','-7 days');
```

**17 rows.** All `unseeded`. T1/T2/T3 all represented. Examples include `EDASbUwX...stfu`, `BA8Pe9v...Nigslop`, `eogqSgF6...NEWSCAM`. Each fired on a Dex-running-max ATH that was never Birdeye-validated.

### 5.9 Phantom validator activity

```sql
SELECT COUNT(*), SUM(is_phantom),
       SUM(CASE WHEN birdeye_error IS NOT NULL THEN 1 ELSE 0 END)
FROM phantom_abort_log;
→ 698 | 624 | 0   (89.4% phantom-positive, 0 errors)
```

### 5.10 Blocking labels

```sql
SELECT block_reason, COUNT(*) FROM alert_block_log GROUP BY block_reason;
→ SCAM Likely | 36

SELECT label, COUNT(*) FROM fee_gate_log GROUP BY label;
→ Elevated   | 23
  Normal     | 203
  SCAM Likely| 36
  Suspicious | 31

SELECT verdict, COUNT(*) FROM holder_filter_log GROUP BY verdict;
→ block   |   6
  caution |   2
  pass    | 125
```

### 5.11 Risk to ATH high fix

- 🔴 **17 of 251 alerts in the last 7 days (6.8 %) fired on `unseeded` ath_source.** A fix that changes ATH writes during the blind window should narrow this — the operator should expect `unseeded` count to drop (or be replaced by a new `birdeye_blind_poll` provenance value).
- 🟡 **89 % phantom-positive rate over 698 invocations** is high. If the new ATH writer raises ATH faster than today, this rate goes up further; consider adjusting `phantom_threshold_pct` or the cooldown duration in lockstep with the fix.
- ⚪ `migration_time` is universally non-NULL and `block_time` is universally non-NULL on `pumpswap_fees`, so JOIN-based checks won't drop rows.

---

## 6. Consumer dependency map

Every read site for `ath_price`, `ath_mcap`, `ath_time`, or `drop_from_ath`. Sorted ALERT-PATH first.

| File:Line | Function/loop | Reads | Blast radius if ATH wrong | Criticality |
|---|---|---|---|---|
| [alert_trigger.py:97](modules/alert_trigger.py#L97) | `AlertTrigger.check_tokens` | `drop_from_ath` | Wrong tier fires (or no tier fires when one should). THE alert decision boundary. | **ALERT-PATH** |
| [alert_trigger.py:56](modules/alert_trigger.py#L56) | `AlertTrigger.check_tokens` | `ath_price ≤ 0` guard | Pre-tier gate — wrong ATH=0 silently skips alerts. | **ALERT-PATH** |
| [alert_trigger.py:79](modules/alert_trigger.py#L79) | `AlertTrigger.check_tokens` (phantom branch) | `drop_from_ath` | Logs the would-have-fired tier during phantom cooldown. Wrong ATH skews the would-have logs. | **ALERT-PATH** |
| [phantom_validator.py:206](modules/phantom_validator.py#L206) | `validate_current_after_ath_update` | `birdeye_price / ath_price` | Wrong ATH inflates/deflates the phantom ratio → wrong cooldown decisions → false suppression or false fire. | **ALERT-PATH** |
| [phantom_validator.py:109](modules/phantom_validator.py#L109) | `validate_current_after_ath_update` | `ath_price ≤ 0` guard | Disables phantom check; alert can fire even if Dex/Birdeye disagree. | **ALERT-PATH** |
| [alert_trigger.py:139-140](modules/alert_trigger.py#L139-L140) | `mark_alerted` | `ath_price`, `ath_mcap` | Snapshot persisted into `alerts.ath_price` / `alerts.ath_mcap` — wrong here pollutes recap forever. | **ALERT-PATH** (write-snapshot) |
| [price_tracker.py:198](modules/price_tracker.py#L198) | `_process_token` | `ath_mcap > 0 and mcap > ath_mcap*50` | Skips peak-after-alert update on >50× spike; wrong ATH (low) over-suppresses peak data. | ALERT-PATH (post-alert tracking) |
| [alert_trigger.py:119](modules/alert_trigger.py#L119) | `check_tokens` log | `ath_mcap` | Log line; not a decision input. | OBSERVABILITY |
| [phantom_validator.py:92-100](modules/phantom_validator.py#L92-L100) | `validate_current_after_ath_update` | `ath_price`, `ath_mcap`, `ath_source` | Persisted to `phantom_abort_log` — historical ATH provenance for week-1 review. | OBSERVABILITY |
| [phantom_validator.py:211-212,232](modules/phantom_validator.py#L211-L212) | `validate_current_after_ath_update` | `ath_mcap` | Log line + computed `birdeye_mcap`. | OBSERVABILITY |
| [price_tracker.py:166-170](modules/price_tracker.py#L166-L170) | `_process_token` debug log | `drop_from_ath`, `ath_mcap` | Log only. | OBSERVABILITY |
| [telegram_sender.py:579-601](modules/telegram_sender.py#L579-L601) | `_format_alert` | `drop_from_ath`, `ath_price`, `ath_mcap` | Telegram alert text — wrong drop% in user-visible message. | REPORTING |
| [stats.py:348-377](stats.py#L348-L377) | `show_alerted` recap | `ath_price`, `ath_mcap`, `current_price` | Daily/weekly recap UI; wrong ATH skews recap drop%. | REPORTING |
| [main.py:105](main.py#L105) | `price_alert_loop` | `drop_from_ath` (via log only) | Log line. | OBSERVABILITY |
| [database.py:565-573](database.py#L565-L573) | `save_alert` | passthrough write of `ath_price`, `ath_mcap` | Persisted. | ALERT-PATH (write-snapshot) |
| [database.py:1077-1090](database.py#L1077-L1090) | `log_phantom_validation` | passthrough write | Persisted. | OBSERVABILITY |
| [tests/test_alert_trigger.py](tests/test_alert_trigger.py), [tests/test_phantom_validator.py](tests/test_phantom_validator.py) | unit tests | sets ATH on fixtures | n/a (test) | TEST |

### 6.1 Risk to ATH high fix

- 🔴 **The alert-path reads are concentrated in `alert_trigger.check_tokens` and `phantom_validator.validate_current_after_ath_update`.** Any fix that changes how/when ATH gets written must be validated against both — phantom in particular reads `ath_price` *as the divisor*, so a sudden 1.5× ATH spike from a new Birdeye-1m polling path will spike phantom rates.
- 🔴 **`alerts.ath_price` / `alerts.ath_mcap` are snapshotted at fire time.** A fix that retroactively updates ATH after an alert won't update the alerts row — the recap still shows the original (lower) ATH and an inflated peak%.
- 🟡 **`stats.py:348` reads `tokens.ath_*` directly**, not `alerts.ath_*`, so historical recap is sensitive to mid-flight rewrites of the tokens row.
- ⚪ All other consumers are logging or display surfaces.

---

## 7. Birdeye credit profile

There are three Birdeye endpoints in use, each with different per-call cost classes. No plan ceiling is encoded anywhere in `config.yaml` or in any Python constant — `grep "credit\|monthly\|ceiling\|RATE_LIMIT"` on production code returns only references to `max_age` ceilings on the ATH retry queue and a `429 Birdeye rate limited` log line in [utils/birdeye.py:93](utils/birdeye.py#L93). The operator must size against an external dashboard.

### 7.1 Call sites

| File:Line | Function | Endpoint | Trigger |
|---|---|---|---|
| [migration_ws.py:822](modules/migration_ws.py#L822) | `_seed_ath` | `/defi/ohlcv` | per-migration (1 call per WS-detected migration) |
| [migration_ws.py:671](modules/migration_ws.py#L671) | `process_ath_retry_queue` | `/defi/ohlcv` | per-poll-interval, gated by `initial_interval_seconds=30` (hot) or `sustained_interval_seconds=120` (cold), max age 1800 s |
| [migration_ws.py:788](modules/migration_ws.py#L788) | T+15m correction pass | `/defi/ohlcv` (forced `15m`) | one-shot per token at age 900–960 s |
| [phantom_validator.py:127](modules/phantom_validator.py#L127) | `validate_current_after_ath_update` | `/defi/price` | per-Birdeye-ATH-write (after every `_seed_ath`/reseed/correction success) |
| [inspection_gate.py:147](modules/inspection_gate.py#L147) | `check_inception_bundle` (caller `inspection_bundle`) | `/defi/history_price` | per-alert (one per fire, dead in current run — see §4.4) |
| [inspection_gate.py:149](modules/inspection_gate.py#L149) | fallback | `/defi/price` | iff `history_price` returned None |

### 7.2 Observed rates (30.31 h log window)

Counts derived from `bot.log` (current session) and `phantom_abort_log` table.

| Source | Count | Per-hour rate | Endpoint |
|---|---|---|---|
| Successful OHLCV fetch (`Birdeye ATH for ...`) | 1,432 | ~47.2 / h | `/defi/ohlcv` |
| Failed OHLCV ("No Birdeye ATH yet") | 83 | ~2.7 / h | `/defi/ohlcv` |
| Phantom validator total (DB ground-truth, last 30 h) | 694 | ~22.9 / h | `/defi/price` |
| `inspection_gate` `get_sol_price_at` | 0 | 0 / h | `/defi/history_price` (call site dead) |
| `inspection_gate` `get_sol_price_now` | 0 | 0 / h | `/defi/price` (call site dead) |
| Migrations processed (denominator) | 270 | ~8.9 / h | n/a |

Cross-checks:
- Migrations that produce ATH writes are counted as ATH seeded/reseeded/corrected. `grep "ATH seeded\|ATH reseeded\|ATH corrected"` = **698** events over 30 h, matching the 694 phantom calls (one phantom per ATH write).
- "Birdeye rate limited" appearances: **0** in the current log (no 429s).

### 7.3 Roll-up

- **`/defi/ohlcv`**: 1,515 calls / 30 h → **~50 / h**, ~1,200/day. Driven mostly by reseed retries within the first 10 min of every migration (initial_interval=30 s).
- **`/defi/price`** (phantom validator): ~23 / h, ~550/day. Tracks ATH-write rate.
- **`/defi/history_price`**: 0 today, but if `inspection_gate` were re-wired the rate would equal the alert rate — last 30 h had 251 alerts → ~8 / h, ~200/day.

Total Birdeye rate today: **~75 calls/hour** (~1,800/day) across two endpoints.

### 7.4 Plan ceiling

- **Not discoverable from code.** No constant in `config.yaml`, no `BIRDEYE_PLAN_CU`, no `RATE_LIMIT` string, no rate-tracking table. The only failure-mode is the "rate limited" log line + `return None` at [utils/birdeye.py:93](utils/birdeye.py#L93). The diagnostics doc `diagnostics_out/BIRDEYE_BURN_GAP_INVESTIGATION.md` references a "Birdeye Standard" plan and per-endpoint CU costs (60 CU `/history_price`, 3 CU `/price`, 30 CU `/ohlcv`) but those numbers are commentary, not enforced anywhere.
- **Confidence**: medium. The OHLCV count comes from log-grepping success lines only (failed/throttled calls under-count); phantom count is DB ground-truth and is exact; inspection_gate is exactly zero only because the call site is unreachable.

### 7.5 Risk to ATH high fix

- 🔴 **No plan ceiling anywhere in code.** A fix that adds Birdeye-1m polling during the blind window will need an explicit budget — at 8.9 migrations/h × ~5 min blind window × poll_interval, the burn is easy to compute but nothing today stops a config typo from 10×-ing it.
- 🟡 **Today's `/defi/ohlcv` 50/h baseline is mostly retries.** A blind-window polling path that runs in parallel with the existing retry queue would double-count for the first ~10 min — collapse them or one of the two paths becomes wasteful.
- ⚪ `inspection_gate` is currently 0/h; if it gets re-wired, that adds ~8/h independent of the ATH change.

---

## 8. Top surprises

These are the items most likely to surprise an engineer who hasn't looked at the system in three weeks.

### 8.1 The four most load-bearing modules in the system are not in git

`modules/phantom_validator.py`, `modules/grpc_indexer.py`, `modules/inspection_gate.py`, `modules/ath_refresh_shadow.py`, plus `utils/grpc_decoder.py`, `utils/onchain_fees.py`, and `snapshot_holders.py` are all flagged as untracked by `git status`. They are imported by tracked code (`modules/migration_ws.py:36-38`, `main.py:31`, `main.py:345`, etc.) and execute on every run. The recent commit `cf2c216 holder_filter: track existing module in git` shows the operator already noticed this pattern for `holder_filter.py` and rescued it — but the same fix has not happened for the rest. A hostile `git checkout` or `git clean -fd` in a future session could detonate the production system in a single command.

### 8.2 The `inspection_gate` shadow filter is configured `enabled: true` but has zero call sites and zero rows

[config.yaml:95-100](config.yaml#L95-L100) declares `inspection_gate.enabled: true` with thresholds and a "v2_10000usd_0.07ratio" version. [migration_ws.py:36](modules/migration_ws.py#L36) imports `check_inception_bundle`. But `grep "check_inception_bundle("` returns **only the function definition** — no caller. The `inspection_gate_log` table is empty (0 rows). The function does its own config read at [inspection_gate.py:53-55](modules/inspection_gate.py#L53-L55), checks `enabled`, and would write rows if invoked — but nothing invokes it. This is a "fix that didn't actually fix what it claims to" — the wiring was removed but the import, config, and log table all stayed.

### 8.3 17 alerts in the last 7 days fired on tokens whose `ath_source='unseeded'`

By design, `alert_trigger.check_tokens` does NOT gate on `ath_source` — it only gates on `ath_price > 0` and `current_price > 0`. So tokens whose Birdeye seed never succeeded but whose Dexscreener running-max found a peak are valid alert candidates. Combined with the [price_tracker.py:156-157](modules/price_tracker.py#L156-L157) bug where `ath_source` only flips for already-Birdeye sources, this means the bot is firing alerts on Dex-only-peak ATHs while still labeling them `unseeded` in the DB. Anyone using `ath_source` as a quality filter for analysis will under-count these.

### 8.4 The same migration timestamp is set three different ways across three writers

[migration_ws.py:504](modules/migration_ws.py#L504) writes `migration_time=time.time()` — the WS-handler wall clock, ~30 s after the actual migration block. [backfill.py:92](modules/backfill.py#L92) writes `migration_time=created_at` from Dexscreener's `pairCreatedAt` — closer to truth. [backfill.py:294](modules/backfill.py#L294) hedges with `migration_time=created_at if created_at > 0 else time.time()`. The 11 tokens with `migration_time` more than 5 minutes BEFORE first `block_time` are all from the backfill paths; the 257 tokens AFTER first block_time are all from the WS path. The system has been running for weeks with three different definitions of "migration_time" coexisting in the same column.

### 8.5 `utils/birdeye.py` looks like an in-progress copy-paste

[utils/birdeye.py:102-108](utils/birdeye.py#L102-L108) is a triple-quoted string at module scope:
```python
"""
ADD THIS TO utils/birdeye.py — do not replace the file, append to it.

New function: get_sol_price_at(timestamp, ...) with in-memory minute-bucket cache.

Used by bundle_gate to convert SOL → USD at inception time.
"""

logger = logging.getLogger(__name__)
```
That is the *instructions to the developer*, pasted as if it were code. The duplicate `logger = logging.getLogger(__name__)` at L110 (already declared at L17) is the giveaway. Functionally the file works — a bare string at module scope is a no-op — but the cosmetics of the file say "this was finished mid-paste and never cleaned up." It is the strongest signal that the operator's recent ATH-related work shipped without a code-review pass.

---

# Phoenix Bot — Project State

Last updated: 2026-05-03

This file is the single source of truth for project state across 
chats. Update it after each major milestone.

---

## Current state

**Phase:** Phase 1 (target May 31, 2026 soft deadline)

**Live in production:**
- gRPC indexer (Chainstack Yellowstone, Growth plan, 1 stream)
- Birdeye ATH seeding (95% seed rate, 18% T+15m correction rate)
- Ghost Filter (3-mode verdict, hard suppression on 'block')
- Ante V1 classifier (shadow mode only)
- Fee Gate v2 (active)
- Fast-Dip Detector Stage 1 (shadow mode, commit 2ac220b, 
  2026-05-02)

**Detector Stage 1 audit (2026-05-03 morning, ~18h live):**
- 196 triggers, 84 unique tokens
- Top-1 share 6.6% (DickButt 13 — verified hot-token cluster)
- 192 recovered / 2 gap / 0 evicted / 2 still_open
- Trigger rate ~10.5/hr matches Pass-1-scaled expectation
- All trigger drops ≥0.40, density gate working (no rows below 5)

**DB:**
- bot.db: ~40GB main + ~17GB WAL (cleanup pending)
- Last DB nuke: pre-2026-04-29 (Maq's call)

---

## Active workstreams

### Fast-Dip Detector (Phase 1)

Architecture locked from Pass 1.5 (2026-05-01). 4-stage 
implementation, each stage in its own chat:

- ✅ Stage 1 — rolling-max tracker + trigger + shadow table 
  (commit 2ac220b, live 2026-05-02)
- ⏳ Stage 2 — +10s decision gate + suppression rules 
  (pending, next chat)
- ⏳ Stage 3 — circuit breakers + outcome updater
- ⏳ Stage 4 — Telegram dry-run formatting

**Locked decisions (do not re-derive):**
- Trigger: 40% drop / 60s rolling max + 5 swaps in trailing 5s
- Decision checkpoint: +10s (NOT +5s, NOT +15s)
- Suppression rules (OR-composed):
  - buy_sell_ratio_10s < 0.25
  - depth_velocity_10s ≥ 0.02
  - pre_dip_1m_usd_vol < 5_000
  - swap_count_10s < 5
  - trigger_lag > 5s
- Pre-dip 1m USD volume cliff at 5k (NOT 30k — measured)
- 4-week shadow period before promotion to live alerts
- Promotion gate: ≥100 would_alert events, ≥80% manually 
  labeled tradeable, ≤15% rugs

**Pass 1.5 artifacts:**
- diagnostics_out/fast_dip_2026_04_29/REPORT.md (Pass 1)
- diagnostics_out/fast_dip_2026_04_29/06_pass15_summary.md
- diagnostics_out/fast_dip_2026_04_29/06_pass15_examples.md
- diagnostics_out/fast_dip_2026_04_29/06_pass15_features.csv

### Phase 2 prep (NOT active yet)

- SCAM rework — `scam_bundle` gap is core open problem. Closes 
  after Phase 1.
- Ante V2 rework — V1 misses scam_bundle entirely. Separate chat.

---

## Cleanup items (not blocking, file when natural)

- **DB cleanup:** PRAGMA wal_checkpoint(TRUNCATE) + VACUUM. 
  Schedule during Stage 2 → Stage 3 transition (clean shutdown 
  window).
- **price_tracker.py:151-157 provenance overwrite bug** — 
  ath_source='birdeye_corrected' silently overwritten with 
  'birdeye_running_max' on Dex live-poll exceeding stored ATH. 
  ~75% undercount of actual correction activity in DB. Use 
  bot.log for ground truth, not column counts.
- **phantom_validator** — vestigial-but-harmless. Detune option 
  documented or remove entirely. Not blocking.
- **tokens.migration_time semantic bug** — partial fix in 
  06c4d08. Verify residual need before further action. Correct 
  t0 anchor is MIN(pumpswap_fees.block_time).
- **Telegram /fastdip command** — manual labeling helper for 
  Stage 2/3 outputs. Build only if labeling friction blocks 
  promotion review.

---

## Don't bring up unless I ask

These are debunked hypotheses, dead ends, and sensitive items. 
Future chats: do not resurface unless Maq specifically asks.

- **Buyer-pubkey concentration filter** — KILLED 2026-04-26 
  after three CC investigations. Best AUC was 0.659 
  (unique_buyers), none crossed 0.70 bar. Don't revisit without 
  fundamentally different signal.
- **Buyer-pubkey primitives anti-rank bundle_family** (AUC < 
  0.40). Wallet-count primitives actively hurt scam_bundle 
  detection.
- **gRPC-as-primary-price** — failed offline validation 
  (n=141, ~1% median edge, flat across buckets). Right-tail 
  wins were dust trades. gRPC indexer stays load-bearing for 
  Ante + fee gate + Phase 2. Candidate replacement (untested): 
  Birdeye-1m polling vs Dexscreener.
- **30k pre-dip 1m USD volume threshold** — eyeballed from live 
  watching, wrong by ~6x. Real cliff at 5k. Live observations 
  produce direction; measurement produces thresholds. Don't 
  re-propose 30k.
- **Ante V2** — fully removed (commits 62a0c91 + cd6e350). 
  Dormant V2 columns and 141 historical rows preserved as 
  anti-examples. Rework is a separate future chat.
- **Pre-rewrite Gooner/SAM canonical-failure data** — DB nuke 
  destroyed it. The 18%-undershoot framing for those tokens 
  cannot be reproduced from local data. Don't cite as a current 
  verifiable failure case.
- **grpc_prices table** — does NOT exist in bot.db. All gRPC 
  data lives in pumpswap_fees. Any script referencing 
  grpc_prices is wrong.
- **Faster Dex polling as a P1 fix** — discarded. P1 was about 
  Dex blindness; faster polling solves nothing.
- **Volume-anomaly detection alone as fast-dip primary path** 
  — discarded. Interesting complement, not the primary signal.
- **Two-stage decision (+5s prelim, +10s confirm)** — proposed 
  during Stage 1 architecture, rejected. +5s overlaps too much 
  to be a reliable preliminary signal.

---

## Workflow conventions

- **Three-chat pattern:** architecture chat → CC investigation 
  → CC implementation. Architecture decisions in Claude chat, 
  file-level work in Claude Code.
- **Diff-gate discipline:** CC must print full git diff and 
  wait for literal "yes" before committing.
- **Shadow mode before hard gates.** ≥90% precision on 25+ 
  manual labels before promotion.
- **Additive, non-restructuring changes only** during Phase 1.
- **Verification method:** SQLite queries + PowerShell log 
  greps, not formal test suites.

---

## Tools & resources

- **Runtime:** Windows/PowerShell, Python 3.11 venv, SQLite
- **gRPC:** Chainstack Yellowstone, Growth plan, 1 stream
- **Price sources:** Dexscreener (primary), Birdeye (ATH), 
  CoinGecko (SOL/USD)
- **Alerts:** Telegram
- **Implementation:** Claude Code v2.1.119 (with 
  --dangerously-skip-permissions)
- **Key tables:** pumpswap_fees, tokens, alerts, 
  fast_dip_shadow, fee_gate_log, ante_log


  COMPLETED 2026-05-02 to 2026-05-05:
- DB locking fix (commit 6027d0b): WAL pragmas, db_connect 
  wrapper migrated 7 files, 168h pumpswap_fees prune loop
- Unseeded provenance fix (commit 56abcaf): running_max
  now promotes ath_source unseeded → running_max on bump.
  Verified: 0 unseeded alerts post-restart, was 11 before.
- Retention tightened 168h → 48h (in commit 56abcaf)
- Bloated DB at data/bloated_db_2026_05_02/ (delete after 5/5)
- Backup at data/bot.db.backup_2026_05_02 (keep — cold storage)

OPEN BACKLOG (highest priority first):
1. Prune-induced lock contention (added 2026-05-05)
   ~108 lock errors / 44h. DELETE of 300-500K rows holds
   exclusive lock ~30s, starves price_tracker writes.
   Fix: chunk DELETE into 10K batches with 100ms sleeps,
   or run every 5min instead of hourly. Small CC session.

2. SPIRIT-class premature Tier 1 (added 2026-05-03)
   Fast-movers where Birdeye first-seed is wrong, T1 fires
   bad, reseed corrects, T2 fires good. ~3-5 of 150 alerts.
   Fix: gate alerts on candle count >= 3 of seed source.

3. Polling-gap on volatile spikes (Phase 2 candidate)
   NYAN/DickButt/GAY: bot's 30s polling cadence misses
   sub-minute peaks. Real ATHs go untracked entirely.
   Bigger work — revisit gRPC-as-price-source decision.
   Validation must specifically test peak-capture, not
   median accuracy.

4. journal_size_limit didn't persist as -1 instead of 1GB
   despite init_db PRAGMA (lower priority, harmless now)

5. Find checkpoint blocker (lower priority, fix is holding)

6. Stage/commit untracked migration scripts or delete them

7. Delete bloated_db_2026_05_02/ folder (do today, ~58GB)


# ATH Seeding Bug — Investigation Findings (2026-05-05)

## TL;DR

Phoenix has a structural gap in ATH seeding: **only tokens created via 
`migration_ws._build_token` get a Birdeye OHLCV seed.** Tokens created 
via any other path (backfill, WS-saw-migration-but-Dexscreener-failed) 
skip Birdeye entirely. Their `ath_source` defaults to `'unseeded'` and 
gets promoted to `'running_max'` on first price_tracker poll. ATH then 
tracks only post-creation peaks, missing anything that happened during 
the gap.

Prevalence: 8 of 157 T1 alerts (5.1%) in last 14d had `ath_source ∈ 
{running_max, fallback}`. Of those, 5 have meaningfully recoverable 
ATH (10-40% undershoot vs Birdeye-visible peak). 3 are currently still 
tracking with wrong stored ATH right now: JOHN, BioHash, YIPPEE.

This is **NOT** the systemic 66% undershoot the prior $180-SOL 
investigation suggested — that was an SOL anchoring artifact. The 
real problem is small but real and structural.

---

## What we ruled out (and why)

### "Birdeye is unreliable for memecoins"
False. Morsecoin verification: Phoenix's stored migration_mcap 
($34,450) matched gRPC first-tick mcap to within 0.06% (Formula B, 
$86 SOL). For BioHash: Birdeye 1m high at 16:15 UTC was $126,369, 
matching axiom.trade chart truth. Birdeye captured the spike correctly. 
Birdeye is fine.

### "Phoenix has a math bug (decimals or formula)"
False. `part_a.py:96-99` already applies the correct decimal 
adjustment. Formula B (`(quote_amount/1e9 × SOL_USD) / 
(base_amount/1e6)`) is what Phoenix uses, and it's correct.

### "Birdeye retry logic exhausts before getting data"
False. None of the 8 candidates ever had `_seed_ath` called. The 
retry exhaustion path was never exercised. The bug is upstream of 
retries entirely.

### "gRPC missed the peak"
N/A for the gap window. For BioHash specifically, gRPC had no data 
during 16:02-16:22 UTC (pool was quiet pre-fee_t0). Post-fee_t0, 
gRPC peak ($77,293 at 17:04) matched Birdeye 1m high ($76,576) to 
within 1%. The chart's $125K peak occurred during the gap and only 
Birdeye saw it.

### "Phoenix needs to be Birdeye-everywhere"
False — and would be expensive (180-360× current Birdeye load). The 
fix is to make the existing single Birdeye seed call from 
`_seed_ath` actually run for all token-creation paths.

---

## The actual bug

**One root cause, three manifestations:**

1. **Bot offline during migration.** WS can't see migration. Backfill 
   catches token after restart. backfill.py creates token with 
   `ath_price=0.0`, never calls `_seed_ath`.
   - BioHash (24-min outage on May 5)
   - EAGLE (same outage)

2. **WS sees migration but `_build_token` fails.** Dexscreener 
   hadn't indexed the new pool yet. WS retries 5min, gives up. 
   Backfill catches token later, again skipping `_seed_ath`.
   - Wild, trollhouse, AverageGuy, 熊猫 (xiongmao)
   - JOHN, YIPPEE (also backfill-detected, this category)

3. **Both classes converge on the same code path:** 
   `modules.backfill.periodic_backfill_loop` (or startup backfill) 
   creates tokens without seeding. `ath_source='unseeded'` defaults 
   to `'running_max'` on first price_tracker poll.

In all 8 cases, **Birdeye had data within 0.0-0.6 minutes of 
migration_time**. Phoenix just didn't ask.

---

## Outage profile (operating reality)

13 process boundaries in 9 days of bot.log:
- 5 graceful shutdowns (manual stops, deploys)
- 8 hard kills (deploys without graceful shutdown, 2 OS crashes)
- 5 outages > 15 min — these are the BioHash-class generators

Two unpreventable Windows OS crashes (Kernel-Power Event 41) on 
Apr 30 and May 5. Phoenix needs to be robust to *its own restarts* 
because they will keep happening.

The architecture currently treats backfill as a recovery path. In 
practice it's a steady-state path responsible for ~10-15% of 
detection events. Fix accordingly.

---

## Currently-tracking impact

3 tokens are alerted with stored ATH measurably below Birdeye-visible 
peak right now:

| Token   | Stored ATH | Birdeye peak | Undershoot |
|---------|-----------|--------------|------------|
| JOHN    | $76,683   | $88,447      | 13.3%      |
| BioHash | $76,778   | $126,369     | 39.2%      |
| YIPPEE  | $53,519   | $59,214      | 9.6%       |

Decision needed before any SQL UPDATE: bumping these values triggers 
an immediate `price_alert_loop` tier check. If the higher ATH puts 
current price past a tier threshold, that tier will fire even though 
the original token already alerted at a lower tier. Three options:

- (a) Leave alone. Old alerts are ledger entries.
- (b) Update silently. Suppress further alert fires for these 3 specifically.
- (c) Update and let alerts fire normally. Could result in T2/T3 fires for tokens 
      that already T1'd.

Decision deferred to fix implementation phase.

---

## The fix (architecture)

### Part 1 — Shared seeder module (must-do)

Extract `_seed_ath` from `migration_ws.py` into 
`modules/ath_seeder.py`:

- `seed_ath_for_token(token, http_session, config, ath_retry_queue=None)`
- Move `_ath_retry_queue` + `process_ath_retry_queue` here
- Make queue optional so callers can opt into retry behavior

Call sites:
- `migration_ws._build_token` success path (existing)
- `backfill.py:91` (startup backfill)
- `backfill.py:283-298` (periodic backfill loop)

### Part 2 — price_tracker promotion guard (defense-in-depth)

In `price_tracker._process_token`, when promoting `unseeded → 
running_max`:
- If token age < 30min: enqueue for Birdeye seed instead of promoting
- Only promote to running_max after seed retry window exhausts

This catches future code paths that might create tokens without 
explicit seeding.

### Out of scope for this fix

- Lifting the 5-min Dexscreener retry ceiling in WS path (separate 
  problem; backfill seeding makes it irrelevant)
- Backfill-correcting historical alerts (ledger-level, leave alone)
- Cloud VM migration (Phase 2/3 question)

---

## Risks during implementation

### Birdeye API rate
Backfill startup can catch ~30 tokens at once. 30 sequential Birdeye 
calls = ~6 seconds at 5/sec rate limit. Within budget but worth an 
explicit rate limit in the seeder module.

### Birdeye OHLCV resolution for old backfill catches
`_pick_resolution` picks 1m for <20min, 15m for 20m-2h, longer beyond. 
A 4-hour-old token caught by backfill gets 15m candles → short pumps 
get smoothed. Acceptable degraded behavior; document it.

### Provenance overwrite bug (pre-existing, separate)
`price_tracker.py:151-157` overwrites `birdeye_corrected` with 
`birdeye_running_max`. This affects analysis queries against 
`ath_source` column counts but not the actual ATH values. Cleanup 
item, not blocking this fix.

---

## What this means for Phase 1 closure

**Phase 1 P1 closure stands.** The 95% Birdeye seed rate metric was 
correct as measured — for tokens that went through `_seed_ath`. The 
investigation discovered a *different* population (tokens that bypass 
seeding entirely) which the original P1 framing didn't account for.

This is a follow-up workstream within Phase 1's problem space, not a 
P1 reopening. Treat as a high-leverage, well-scoped patch.

---

## Investigation methodology learnings

For future reference:

1. **The first investigation (Sonnet 4.6) returned headline findings 
   that were upstream-wrong.** $180 SOL constant produced a fictitious 
   "66% systemic undershoot." Correcting to $86 flipped the sign 
   entirely. Always verify constants before trusting cross-population 
   statistics.

2. **The second investigation (Opus, math verification) was decisive.** 
   Single token, three formulas, side-by-side comparison against 
   migration_mcap as ground truth. Took 15 minutes and resolved 
   ambiguity that had spawned 3 prior investigations.

3. **bot.log + DB cross-reference is required.** SQL alone showed 
   `ath_source='running_max'` and we inferred "Birdeye retries 
   exhausted." Bot.log showed `_seed_ath` was never called. Two 
   different stories from the same data.

4. **CC narratives can be coherent and wrong.** When a model returns 
   a confident-sounding multi-paragraph diagnosis, check whether the 
   narrative's load-bearing claims are inferences or verified facts.

---

## Open follow-ups (not blocking this fix)

- The May 2 17:32 hang (52 min of `database is locked` errors) is a 
  separate bug class. Worth investigating before Phase 2.
- The two Windows OS crashes (Apr 30, May 5) suggest local hardware/
  thermal/uptime issues. Cloud VM migration is the long-term answer; 
  for now, accept and engineer for it.
- `price_tracker.py:151-157` provenance overwrite: still unfixed. 
  Affects `ath_source` column reliability for diagnostics. Pre-existing.
- `phantom_validator` detune/removal: still pending from prior 
  Phase 1 work. Pre-existing.

---

*Investigation chats: 6 sequential investigations on 2026-05-05.  
Originating observation: BioHash alert showed $73K ATH; axiom chart 
showed $125K peak. Resolution: backfill creates tokens without 
calling Birdeye seed.*

# Strategy Analysis — 2026-07-11

Data window: **2026-03-01 → 2026-07-10** (4.3 months), production DB.
Population: 2,997 signals, 11,398 limits, 673 real execution outcomes (36 MT5 accounts), 176 excursion rows, 54 trailing simulations.

Everything below states its sample size and which data was cut. R-multiples use the **Total-sizing unit**: 1R = the signal's full-fill risk (sum of |limit − SL| across all its limits), so a full stop-out = exactly −1R.

---

## 1. Data quality — what was kept, what was cut, and why

**Core set (n=349)** — the only signals with a trustworthy, bot-determined outcome:
- `profit` + `closed_reason='automatic'` (313) and `stop_loss` + `automatic` (36), each with ≥1 hit limit.
- Exit price: real `tp_price` where recorded (all June+; 87 signals), otherwise modeled as last-hit-limit ± TP threshold from `tp_configuration.json`. The model was validated against the 87 real exits: **mean absolute error 0.043R**, median 0.004R.

**Cut from the profitability core (with counts):**
| Excluded population | n | Reason |
|---|---|---|
| Cancelled without any hit | 2,353 | Never a trade (expiry 864, manual 1,116, near-miss 387, spread/news 52, etc.) |
| Hit, then cancelled | 90 | **Unknown outcome** — position entered, then manually/news/expiry-cancelled with no exit price recorded |
| Manual profits with hits | 102 | Outcome real but exit price mostly unrecorded; user judgment (incl. 12 ex-stop-losses re-marked profit, 27 ex-cancels) |
| Manual profits with 0 hits | 21 | Bot never saw a fill; not reconstructable |
| Breakeven (manual) | 39 | Treated as 0R in the extended set |
| stop_loss → cancelled corrections | 56 | Typo-limit signals, correctly excluded (status now cancelled) |
| Still active | 34 | Open |

**The single biggest transparency issue:** of 582 signals that entered a position, only 349 (60%) have a bot-determined outcome. 90 (15%) are hit-then-cancelled with *no exit information at all*, and 141 more are manual profit/breakeven decisions. If the unrecorded cancels skew negative (people tend to cancel losers), true expectancy is lower than reported. This is the #1 fix in NEXT_STEPS.md.

**Known model bias:** toll-metals TP was $4 until 2026-06-06, then raised to $5. Pre-June modeled exits use today's $5. Re-modeling the 121 affected signals at $4 shrinks the headline from **+29.3R to +19.7R** — treat the truth as somewhere in that band.

**Execution data caveat:** the 673 execution-bot "final" rows collapse to only **58 unique signals** replicated across accounts (e.g., all 21 `pa` rows are one USDCAD signal). Per-row cuts overweight popular signals; signal-level numbers are given where it matters.

---

## 2. Headline profitability (core, n=349)

| Metric | Value |
|---|---|
| Win rate | 88.5% |
| Sum R (Total sizing) | **+29.3R** (sensitivity band +19.7…+29.3R) |
| Avg R / signal | +0.084 |
| Avg win / avg loss | **+0.21R / −0.90R** (≈ 10.7 wins pay for 1 loss) |
| Throughput | ~18.8 auto-closed signals/week |
| Weekly pace | ~+1.5R/week Mar–May, ≈ 0 since mid-June |

Extended set (+ breakeven as 0R, + the 103 reconstructable manual outcomes): n=490, +51.0R, 81.6% win rate — directionally the same.

The strategy premise is confirmed by timing: **winners resolve in a median of 3 minutes** after first fill (toll: 0.8 min); losers take a median of 9 minutes and a mean of ~5 hours. The edge is a fast bounce off a level; if it hasn't bounced quickly, it usually isn't going to.

**The equity curve stalled in mid-June.** Weekly R: steady +0.4…+8.6 every week Mar 2 → Jun 14, then −2.7, −1.1, +0.0, +1.4 since. The execution-bot data (which only covers Jun 15 → Jul 10) independently shows toll at −0.10 avg R per signal in that window. Part of the March-vs-June gap is the TP-config artifact above, but the recent flatness is real and worth watching before scaling anything up.

---

## 3. Where the edge is — and where it leaks

**By signal type (core):**
| type | n | win% | sum R | avg R |
|---|---|---|---|---|
| toll | 161 | 88.2 | **+26.9** | +0.167 |
| standard | 136 | 86.8 | **−3.2** | −0.024 |
| scalp | 30 | 90.0 | +1.8 | +0.059 |
| risky | 5 | 100 | +1.7 | +0.340 |
| pa / swing / 1-1 | 17 | 100 | +2.2 | small n |

Toll (gold levels) **is** the strategy: XAUUSD alone is +26.4R of the +29.3R total (48% of trades, ~90% of profit). `standard` has a *higher* limit count (4.6 avg vs 3.2) and negative expectancy — the same conclusion as the June analysis, now on 136 signals. Execution data agrees at the signal level: standard +0.03 avg R (n=33 signals), toll −0.10 (n=7, June-only), risky +0.62 (n=8), scalp +0.19 (n=9).

**Consistent losers, worth cutting or down-weighting:**
- **Stocks**: −2.8R over 8 core signals (50% win rate), and −8.9R over 52 execution rows. No evidence this asset class works.
- **Cross pairs**: AUDNZD −1.8R (n=5), GBPAUD −1.6R (n=7), GBPCAD −0.8R (n=6). Small n each, but all negative while majors are all positive.
- **JP225 / NAS100**: ≈ 0 combined over 18.

**By time (NY, hour of first fill):**
- **8–9 AM ET is the worst window**: −3.6R over 47 signals (US news hours). 6 AM also negative.
- Evening/overnight (17:00–02:00 ET) is the best: +18.9R over 113 at ~92% win rate.
- **Thursday is the only negative day**: −5.1R over 82 (79% win vs 86–98% other days); Friday +11.8R (98% win, n=55).

**By depth (limits hit before close):** expectancy decays as price eats through levels — 1 hit: +0.172 avg R (99.3% win!); 2: +0.052; 3: +0.156; 4: −0.135; 5+: −0.35 or worse. Same picture in real executions by deepest level filled (levels 1–2 positive, 3+ negative, level 4 −0.22 avg R).

**Win-size compression (the mechanism behind everything above):** win *rate* barely moves with limit count, but win *size* collapses because TP is a fixed distance off the last limit while every extra limit extends the risk:
| limits on signal | n | win% | avg win R | avg R |
|---|---|---|---|---|
| 1–2 | 68 | 79.4 | 0.439 | +0.142 |
| 3 | 83 | 92.8 | 0.265 | +0.185 |
| 4–5 | 149 | 89.3 | 0.136 | +0.034 |
| 6+ | 49 | 91.8 | 0.072 | **−0.016** |

6+ limit signals are net losers *despite a 92% win rate*.

---

## 4. Real execution vs. server tracking (tp_outcomes, Jun 15 – Jul 10)

Clean finals: n=673 rows / 58 signals / 36 accounts (no rows needed outlier removal; |R| ≤ 3.6 everywhere — user-glitch screen at |R|>5 or R<−2 caught nothing).

- Real-world results are **much rougher than the alert stream suggests**: 59% of rows profitable (65.5% of signals at median-user level) vs the server's 88.5% — driven by spread-adjusted entries, broker SLs firing on wicks, and per-user trailing exits. **29% of all real positions ended at stop-loss** vs ~10% server-side.
- Still net positive: +33.7R across rows; +0.117 avg R per signal (median user). Server-model R and median-user R correlate 0.73 on shared auto-closed signals — the tracking model is a fair proxy, just optimistic in level.
- **Trailing beats fixed TP, clearly**: trailing_stop exits averaged **+0.253R** (n=237) vs tp_full **+0.127R** (n=238); stop_loss −0.285 (n=198). Winners captured a median **50% of their MFE** — half the favorable excursion is being left on the table by fixed-threshold exits. All 54 server-side trailing simulations (tight/medium/loose) also ended profitable.
- Loser MFE is small (median 0.11R, 75th pct 0.23R): most losers never got meaningfully green, so a move-to-BE rule would save only the ~25% of losers that reached +0.23R — and winners' MAE (median 0.24R) means an early BE-move would also stop out real winners. Sequencing data (does MFE come before MAE?) isn't recorded yet — needed before recommending a BE rule (see NEXT_STEPS).

---

## 5. Stop-loss: don't tighten it globally

From `signal_excursions` (small n — 33 winners with MAE data, collect more before acting):
- Winners routinely draw down **0.7–2.0 ATR** (median 0.74, 75th pct 1.96) before hitting TP.
- Actual SLs sit at ~3–4 ATR from entry (median 3.0 on winners).
- Re-sim: an SL at 1.0×ATR keeps only 45% of winners; even 2.0×ATR keeps 58%. With avg win +0.21R, halving SL distance doubles R-per-win but the survival math doesn't come close to compensating at this win-size profile.

The 90%-win/deep-SL structure is load-bearing. The better risk lever is **exposure shape** (section 6), not SL distance. Revisit ATR-based stops when excursion data reaches a few hundred entered signals (~2–3 more months).

---

## 6. Lot sizing recommendation

Same-1R comparison on the core set, including new hybrid rules (dd = worst peak-to-trough drawdown in R):

| scheme | sum R | sharpe | worst signal | max dd |
|---|---|---|---|---|
| Total (1 lot / signal, split) | 29.3 | 0.200 | −1.0 | −5.1 |
| Fixed (1 lot / limit) | 76.0 | 0.149 | −8.0 | −17.3 |
| Fixed capped at 3 lots | 69.8 | 0.188 | −3.0 | −13.5 |
| **Total, skip 6+ limit signals** | 30.1 | **0.213** | −1.0 | −5.5 |
| Cap-3 + skip 6+ | 72.0 | 0.205 | −3.0 | −14.8 |

Risk-matched (scaled to equal volatility), Fixed delivers **25% less** return than Total (21.9 vs 29.3). The June finding holds with a month more data.

**Recommended sizing policy:**
1. **Total sizing** — fix risk per signal, split across limits (worst case −1R).
2. **Skip or halve 6+ limit signals** — strictly better in return *and* risk; they're negative-expectancy.
3. **Per-type risk multipliers**: full size on toll/gold; reduced (or zero for auto-execution) on `standard`, stocks, and cross pairs until they prove out. Dropping `standard` alone lifts portfolio Sharpe from 0.20 to 0.36. Skipping Thursdays + the 8–9 AM ET window does about the same (0.36) — these filters overlap, so apply one or the other, not assume both stack.
4. If a user insists on Fixed-style sizing for the bigger absolute returns, **cap lots at 3 per signal** — nearly all of Fixed's return at 25% less drawdown.

---

## 7. TP recommendation

1. **Make trailing the default exit** in the execution bot (evidence: +0.253 vs +0.127 avg R, and 50% median MFE capture on fixed TP). The server's auto-TP threshold then acts as the trailing *arm* point, which is how the sim data was collected.
2. Fixed thresholds look roughly right in *placement* (winners' median MFE ≈ 0.74× threshold from first entry; losers rarely reach 0.35×) — the money is not in moving the threshold, it's in trailing beyond it.
3. Per-type: no evidence the swing 3× multiplier or 1-1 $10 setting are wrong — too few closes to judge (8 and 1 in core). Flagged as insufficient data.

---

## 8. What there wasn't enough data to answer

- **SL re-optimization** — 33 winners with MAE, 2 tracked real SLs. Need ~10× more (collecting since Jun 26).
- **pa / swing / 1-1 / risky types** — 5–17 closes each. `risky` looks excellent in both datasets (+0.34 server, +0.62/signal execution) but n≤8.
- **Oil** — feed added late; 6 closes, ≈ 0R. Excursions exclude oil entirely (no MT5 bars).
- **News windows / volatility guard effectiveness** — cancels are recorded, but nothing tracks what price did *after* a news/NM/spread cancel, so their benefit is unmeasurable.
- **Entry-context indicators** (RSI, HTF trend, volume spike, wick rejection): on 46 entered signals, none separates winners from losers robustly yet. `htf_aligned` and volume-spike differences are within noise. `approach_velocity`/`pre_hit_mae` contain unit-scale outliers (values in the tens of thousands) — bug, see NEXT_STEPS.
- **June regime shift** — can't distinguish "market changed" from "signal provider changed" (avg limits/signal rose 3.6 → 4.7 in June) with current metadata.

Detailed instrumentation to close these gaps is in **NEXT_STEPS.md**.

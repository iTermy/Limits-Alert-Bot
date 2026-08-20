# Tick backtest — findings

Run 2026-08-20 against 656M archived ICMarkets ticks covering 2026-04-01 → 2026-08-20.
2,995 signals replayed; 1,431 entered; 1,383 in the analysis universe after excluding
unmeasurable broker basis, instant-entry signals and corrupt price levels.

Method, calibration and validation live in `backtest/README.md`. Reproduce with
`python -m backtest.build_master` then the scripts under `backtest/study/`.

---

## 1. The bottom line

Under an execution model taken from the MT5 execution bot's own code, the strategy
returns **−0.019R per trade, 95% CI [−0.049, +0.011]** over 1,383 trades. That is not
distinguishable from zero, and it is not distinguishable from zero after testing 42
exit rules, 6 stop placements, 5 breakeven rules, 8 lot-weighting schemes and 8
fill-depth/ladder policies. **Nothing tested produced a confidently positive edge.**

| universe | n | total | mean R | 95% CI | win |
|---|---|---|---|---|---|
| `live` — the TM bot's own frame | 1,415 | +1.3R | +0.0009 | [−0.027, +0.028] | 81.0% |
| `real` — what the execution bot places | 1,383 | −26.2R | −0.0189 | [−0.049, +0.011] | 80.0% |
| `bare` — order at the bare level | 1,366 | −36.3R | −0.0266 | [−0.055, +0.002] | 79.2% |
| `held` — cancels honoured (achievable) | 533 | −20.2R | −0.0379 | [−0.085, +0.008] | 72.8% |

The arithmetic is unforgiving and explains everything below:

```
avg win  +0.231R      win rate needed to break even   81.55%
avg loss −1.023R      win rate achieved               80.04%
```

The strategy is **1.5 percentage points of win rate short of break-even.** Every
lever tested moves win rate and average win in opposite directions along almost
exactly this line, which is why none of them changes the answer.

**The result is not stable in time.** Split at the median date:

| half | n | total | mean R | 95% CI |
|---|---|---|---|---|
| first | 692 | +7.2R | +0.0104 | [−0.032, +0.054] |
| second | 691 | −33.4R | **−0.0483** | **[−0.088, −0.007]** |

The second half is significantly negative. `toll` — historically the profit engine —
goes +0.038R to −0.049R across the split. Whatever edge existed has decayed.

---

## 2. The questions asked

### Trailing or fixed take-profit, and at what configuration?

**Trailing is directionally better and not statistically separable from fixed.** On
the achievable universe, paired per-signal against the current rule:

| rule | mean R | paired diff vs current | wins on | verdict |
|---|---|---|---|---|
| fixed x1.0 (current) | −0.0034 | — | — | — |
| fixed x0.8 | +0.0079 | +0.0113 [−0.010, +0.035] | 4.3% | not separable |
| trail arm0.5 gap0.75 | +0.0126 | +0.0161 [−0.016, +0.051] | 24.8% | not separable |
| trail arm0.75 gap0.5 | +0.0166 | +0.0201 [−0.006, +0.049] | 26.3% | not separable |
| trail arm0.75 gap1.0 | +0.0166 | +0.0200 [−0.013, +0.059] | 28.0% | not separable |
| trail arm0.75 gap1.5 | +0.0199 | +0.0233 [−0.015, +0.067] | 30.6% | not separable |

Every trailing variant beats fixed on the mean and every confidence interval
straddles zero. Note trailing **wins on only a quarter of signals** — it loses more
often and wins bigger, which is why the mean moves but significance does not.

If trailing is used anyway (it is the execution bot default, and the sign agrees
with the live `tp_outcomes` evidence), the data favours **arming near 0.75× the TP
threshold with a gap of 0.5–1.0×**. Arming at 1.5× is clearly worse (−0.037R to
−0.058R); gaps of 2.0× are clearly worse.

**Fixed TP should not be widened.** The sweep is monotone against it: x1.25 through
x3.0 all land between −0.04R and −0.06R. Slightly tighter (x0.8) is marginally the
best fixed setting. This contradicts the earlier "risky wants a wider TP" read.

### Should the stop-loss change?

**No, and it barely matters.** All six placements are negative and every paired
interval straddles zero:

| stop | mean R | paired vs current | win rate | avg loss |
|---|---|---|---|---|
| x0.5 (half as far) | −0.0121 | +0.0054 [−0.034, +0.045] | 66.9% | −1.144 |
| x0.75 | −0.0102 | +0.0087 [−0.009, +0.026] | 75.7% | −1.057 |
| **x1.0 (current)** | −0.0189 | — | 80.0% | −1.023 |
| x1.25 | −0.0089 | +0.0090 [−0.005, +0.024] | 83.9% | −0.991 |
| x1.5 | −0.0171 | +0.0008 [−0.016, +0.018] | 85.2% | −0.965 |
| x2.0 | −0.0117 | +0.0062 [−0.015, +0.028] | 88.1% | −0.906 |

Tightening buys a smaller loss and pays for it in win rate; widening does the
reverse. The trade is close to exactly fair, which is what "no edge" looks like.

Breakeven stops (armed after the trade runs 0.25×–1.5× TP in favour) are all
negative too, and arming early enough to matter (0.25×) converts 57% of trades into
flat exits for no gain.

Supporting fact: **winners routinely go against you first.** A stop at 1.0× the TP
threshold would cut 23% of the winners, at 2.0× still 7.9%. The current stop sits at
a median 2.72× TP. There is no room to tighten it without destroying the win rate
the strategy depends on.

### What lot sizing works — first limits or last?

**Neither. Weighting across the ladder is not a lever.** All eight schemes,
normalised so a full-fill stop-out costs exactly 1R:

| scheme | mean R | paired vs fixed-lot | wins on |
|---|---|---|---|
| first only | −0.0120 | +0.0013 [−0.008, +0.011] | 48.7% |
| front x2 | −0.0123 | +0.0010 [−0.004, +0.006] | 54.0% |
| front linear | −0.0128 | +0.0005 [−0.003, +0.004] | 58.7% |
| **fixed lot (current)** | −0.0133 | — | — |
| back linear | −0.0144 | −0.0011 [−0.005, +0.003] | 37.7% |
| back x2 | −0.0147 | −0.0014 [−0.008, +0.005] | 34.0% |
| last only | −0.0166 | −0.0033 [−0.020, +0.015] | 29.5% |

There is a consistent *sign* — front-loading beats back-loading — but the entire
spread from best to worst is 0.005R, well inside noise. Keep the current fixed lot.

### More limits or fewer?

**Do not cap fill depth.** This is the most seductive trap in the data. Conditional
on how deep price dragged the position, results look devastating:

| fills | n | mean R | win rate |
|---|---|---|---|
| 1 | 420 | **+0.147** | 96.4% |
| 2 | 386 | +0.043 | 82.1% |
| 3 | 287 | −0.064 | 74.9% |
| 4 | 161 | −0.236 | 62.1% |
| 5 | 84 | −0.380 | 53.6% |
| 6 | 45 | −0.356 | 55.6% |

But fill depth is an *outcome of price*, not a choice — you cannot select depth-1
trades in advance. Replaying an actual cap makes things **worse**:

| policy | mean R |
|---|---|
| cap at 1 fill | −0.0313 |
| cap at 2 fills | −0.0268 |
| cap at 3 fills | −0.0140 |
| cap at 4 fills | −0.0129 |
| **take everything (current)** | −0.0189 |

Capping keeps every trade where price ran deep, but with a smaller position and a
target set off a shallower limit. The deep fills are *averaging into recoveries*,
not causing the losses.

Skipping wide ladders at the signal level is the one variant that turns positive —
`skip ladders > 3` gives **+0.0038R (n=603), CI [−0.049, +0.057]** — but it is not
significant and it discards 56% of the trades.

### Which signal types work?

None significantly. Ranked, with the caveat that every interval contains zero:

| type | n | total | mean R | 95% CI |
|---|---|---|---|---|
| risky | 129 | +2.0R | +0.015 | [−0.084, +0.113] |
| toll | 599 | +5.1R | +0.008 | [−0.039, +0.053] |
| swing | 32 | −0.7R | −0.023 | [−0.149, +0.086] |
| standard | 524 | −22.7R | −0.043 | [−0.091, +0.002] |
| pa | 12 | −1.4R | −0.117 | [−0.374, +0.064] |
| scalp | 81 | −10.9R | **−0.134** | [−0.287, +0.004] |

By asset, only the negatives are conclusive: **index −0.113R, CI [−0.220, −0.008]**
and **forex −0.055R, CI [−0.104, −0.007]**. Gold is +0.011R [−0.029, +0.052] on
n=765 — the largest sample in the set and still indistinguishable from zero.

`scalp` is the clearest candidate for removal: worst mean, and no exit rule tested
rescues it (best is −0.104R). Index signals are negative at every threshold tested,
consistent with the earlier M1 study.

---

## 3. What the cancels are worth — first measurement

Cancelled signals have never been scored, because once a signal is cancelled nothing
records what price did next. Replaying with orders left resting answers it: the
cancels prevented 850 trades that would have returned **−24.3R**, so cancelling was
worth **+24.3R**.

| cancel reason | n | mean R avoided | total avoided |
|---|---|---|---|
| cancelled via alert reply (human) | 232 | −0.041 | +9.5R |
| cancelled via signal reply (human) | 154 | −0.049 | +7.6R |
| **near-miss auto-cancel** | **293** | **−0.022** | **+6.3R** |
| bulk cancel (human) | 51 | −0.068 | +3.5R |
| overlap cancel | 40 | −0.065 | +2.6R |
| user cancelled | 72 | +0.053 | −3.8R |

**The human cancels carry it.** The near-miss guard is the single largest automated
contributor by count but the weakest per trade (−0.022R avoided), and the news and
spread-hour guards fired too rarely in this window to measure. The cancel machinery
is doing real work — it is the difference between the full universe and a smaller,
less-bad one — but it is discretion, not automation, that is adding the value.

---

## 4. Can it be compounded? No.

This is the question that settles it independently of statistical significance.

The return distribution is exactly the shape that resists sizing: 80% of trades land
between +0.05R and +0.5R, and 19% land at −1R or worse. Excess kurtosis +9.3.

- **Concentration:** the top 1% of trades (14 of them) contribute +15.9R; the top 5%
  contribute +53.5R against a −26.2R total. The worst 5% contribute −87.6R.
- **Concurrency:** median 3 positions open simultaneously, max 13. Risk per trade is
  not risk per account — any per-trade fraction must be divided by ~3.
- **Growth rate:** with a negative mean, the Kelly-optimal fraction is ~0 and every
  positive fraction compounds downward.

Even taking the *optimistic* pre-correction figure of +0.0072R/trade, the sizing
maths was already prohibitive: Kelly-optimal f = 1.9% of equity per 1R, buying
**+0.7% per 100 trades** at the cost of a **median 38% drawdown** (62% at the 95th
percentile, 84% worst case). Growth turned negative above f = 3.8% and hit ruin at
25%. That is not a strategy that can be levered into a living; the ergodicity cost
consumes the edge long before the edge is statistically real.

---

## 5. Bugs and data-quality problems found

These matter to the running bot, not just the backtest.

1. **~2.4% of signals could never be traded.** 28 signals carry a stop-loss on the
   wrong side of their own limits; 15 more carry corrupt price levels (gold at
   154,524, EURJPY at 0.00214, USDCHF limits at 1.79 against a 0.80 market); 25 have
   no placeable limit at signal time. The TM bot accepts, tracks and alerts on all of
   them. MT5 rejects them, so the execution bot silently skips — but the TM bot's
   own records count them as live signals. **A sanity check on parsed levels against
   the live price would catch every one.**
2. **Seven instruments are parser typos** — `EURBGBG`, `GBPCFH`, `GPBNZD`, `JPYUSD`,
   `XOM.NAS`, `GOOGL.NAS`, `SNDK.NAS`. These never matched any feed, so they were
   tracked but never priced.
3. **The TM bot's recorded fill price is optimistic by one spread.** It records the
   level as the fill; a real order transacts a spread worse. The execution bot
   already pays this, so live P&L reporting overstates results slightly. (Its
   compensating order shift is *protective* overall — see README.)
4. **The broker basis drifts far more than assumed.** JP225's monthly basis swings
   207 points and BTCUSDT's 119 — both wider than those symbols' own TP thresholds.
   Any analysis using a constant per-symbol offset on indices or crypto is invalid.

---

## 6. Honest limits

- **Coverage starts 2026-04-01.** 701 March signals (151 entered) are permanently
  unreachable; the terminal's tick window is rolling and recedes.
- **TP thresholds before 2026-07-14 are today's config**, not config-at-time. Sweeps
  therefore answer "today's config scaled by k". Checked and it does not rescue the
  result: the config-at-time subset is *worse* (−0.082R, n=288), not better.
- **The `real` universe includes trades the bot cancelled.** It is the right basis
  for comparing exit rules (larger n, policy-independent), but the achievable figure
  is `held`, and every headline conclusion was re-checked there.
- **Fill count matches history 75.9%** (84.8% where the threshold is config-at-time).
  This biases reproducing history, not comparing policies.
- **n is the binding constraint on every per-type conclusion.** Only `toll` (599),
  `standard` (524) and gold (765) have samples where a 0.02R effect could ever be
  resolved, and even those cannot resolve one.

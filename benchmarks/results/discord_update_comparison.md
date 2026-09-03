# Discord live-update benchmark

Run on 2026-09-03 from `master` at `93cd0a1` with the bounded-pass freeze fix.

## Workload

- 90 simulated seconds with continuously changing prices
- 5 or 10 active signals sharing one Discord channel
- Discord bucket: 5 requests per 5 seconds
- Network outage: second 27 through second 55
- Critical hit/SL-style event: second 40
- Legacy policy: refresh every 15 seconds, up to 5 concurrent edits
- New policy: one sequential snapshot pass, drop remaining cosmetic work when
  a critical event arrives, then wait 30 seconds after the pass completes

Run with:

```powershell
py benchmarks/discord_update_benchmark.py --scale 0.02
```

## Results

| Signals | Policy | Cosmetic requests | Max app in flight | Max Discord queue | Mean payload age | Critical latency | Bucket wait time |
|---:|---|---:|---:|---:|---:|---:|---:|
| 5 | Legacy | 20 | 5 | 5 | 6.02 s | 20.13 s | 5.00 s |
| 5 | Bounded pass | 6 | 1 | 1 | 4.26 s | 15.49 s | 0.00 s |
| 10 | Legacy | 30 | 5 | 5 | 6.00 s | 20.32 s | 19.98 s |
| 10 | Bounded pass | 11 | 1 | 1 | 2.82 s | 15.53 s | 5.00 s |

## Interpretation

The bounded-pass policy cut cosmetic request volume by 70% at five signals and
63% at ten signals. It reduced the maximum Discord queue from five requests to
one and critical-event latency by roughly 23–24% during the outage. At ten
signals, modeled bucket wait time fell by 75% and mean payload age by 53%.

This workload intentionally leaves the simulated in-flight request waiting for
the outage, so its payload-age number is conservative. Production additionally
cancels an individual cosmetic edit after 8 seconds. A running pass is never
refilled, and failed cosmetic snapshots wait until the next pass.

`rate_limit_waits` and bucket wait time represent modeled client-side
Retry-After waits, not real Discord 429 responses. This benchmark deliberately
does not call Discord. Exact production limits are dynamic, so the staging
follow-up should observe Discord's real response headers at normal cadence
rather than intentionally provoking a 429.

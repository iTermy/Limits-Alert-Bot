# Discord live-update benchmark

Run on 2026-09-03 after updating `master` to `a568c03` and applying the
coalesced live-update changes.

## Workload

- 90 simulated seconds with continuously changing prices
- 5 or 10 active signals sharing one Discord channel
- Discord bucket: 5 requests per 5 seconds
- Network outage: second 27 through second 55
- Critical hit/SL-style event: second 40
- Legacy policy: refresh every 15 seconds, up to 5 concurrent edits
- New policy: refresh every 30 seconds, one sequential worker, one pending key
  per signal

Note on the baseline: the "legacy" policy here is the pre-`fa2e923` fan-out, which
edited up to five embeds concurrently regardless of channel. `fa2e923` had already
grouped edits by channel, so for this single-channel workload the shipped code was
closer to the new policy than the legacy column suggests. The measured win that
survives that correction is the coalescing (one pending key per signal) and the
30-second cadence, not the serialization. The shipped implementation keeps
`fa2e923`'s cross-channel parallelism on top of the coalesced queue: channels drain
in parallel, signals within a channel drain sequentially.

Run with:

```powershell
py benchmarks/discord_update_benchmark.py --scale 0.02
```

## Results

| Signals | Policy | Cosmetic requests | Max app in flight | Max Discord queue | Mean payload age | Critical latency | Bucket wait time |
|---:|---|---:|---:|---:|---:|---:|---:|
| 5 | Legacy | 20 | 5 | 5 | 6.22 s | 21.05 s | 5.00 s |
| 5 | Coalesced | 10 | 1 | 1 | 3.49 s | 14.72 s | 8.80 s |
| 10 | Legacy | 30 | 5 | 5 | 6.02 s | 20.26 s | 19.97 s |
| 10 | Coalesced | 15 | 1 | 1 | 2.70 s | 14.77 s | 13.25 s |

## Interpretation

For both loads, the new policy cut cosmetic request volume by 50%, reduced the
maximum Discord queue from five requests to one, and reduced critical-event
latency by roughly 27–30% during the outage. At 10 signals, total bucket wait
time fell by about 34% and mean payload age fell by about 55%.

The worst individual cosmetic payload can still span the whole outage because
an already in-flight HTTP request cannot be replaced. The improvement is that
only that one request can be stale; later signals are rendered when their turn
arrives, and repeated scheduler passes do not append stale batches.

`rate_limit_waits` and bucket wait time represent modeled client-side
Retry-After waits, not real Discord 429 responses. This benchmark deliberately
does not call Discord. Exact production limits are dynamic, so the staging
follow-up should observe Discord's real response headers at normal cadence
rather than intentionally provoking a 429.

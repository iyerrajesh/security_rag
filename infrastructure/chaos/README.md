# Chaos tests: backpressure and failure recovery

These are repeatable scripts that stress the Kafka pipeline and check the
outcome. Each one prints PASS/FAIL per check and exits non-zero on failure.

```bash
infrastructure/chaos/run-all.sh          # all tests, about 6 minutes
infrastructure/chaos/broker-pause.sh     # or one test at a time
```

> **The scripts wipe local Kafka data.** Each test deletes and recreates the
> `vehicle.*`, `validated.vehicle.*` and `quarantine.vehicle` topics so its counts aren't polluted by earlier runs. Kafka must
> be running (`docker compose --profile m1 up -d kafka`). The scripts create
> `.venv/` if it's missing.

Results go to `output/chaos/<test>/`:
- `result.txt`: the pass/fail lines
- `run/manifest.json`: the simulator's backpressure metrics and a per-second timeline
- `consumer.json`: the verifier's counts
- `lag.csv` (slow-consumer test only)

## Tests

| Script | What it does | Pass means |
|---|---|---|
| `broker-pause.sh` | Pauses the broker for 20 s partway through a 1,000-vehicle run | The queue fills, the simulator blocks, and after unpause every event is delivered, in order |
| `producer-timeout.sh` | Pauses the broker for longer than `message.timeout.ms` (5 s) | Loud failure: the simulator exits non-zero, failed messages are counted, and delivered + failed = produced |
| `small-queue.sh` | Caps the producer queue at 1,000 messages, then pauses the broker for 5 s | Queue depth never exceeds the cap (memory stays bounded), the simulator blocks, nothing is lost |
| `broker-throttle.sh` | Broker quota `producer_byte_rate=256 KB/s`, compared against an unthrottled run | Run is >1.5× slower, the client sees broker throttle time, nothing is lost |
| `slow-consumer.sh` | Real-time producer against a consumer sleeping 2 ms per message | Lag builds and then returns to 0. The producer is never blocked: consumers don't push back on producers |
| `consumer-restart.sh` | SIGKILLs a consumer mid-backlog, then restarts it in the same group | Restart resumes exactly at the committed offsets and drains the backlog |

Tune a test with environment variables, for example
`PAUSE_SECONDS=60 VEHICLES=2000 infrastructure/chaos/broker-pause.sh`.
The defaults are at the top of each script.

## Results (Apple Silicon laptop, single broker)

| Test | Key numbers |
|---|---|
| broker-pause | Queue hit 100,000 messages; the simulator was blocked 16.4 s of the 20 s pause; 451,468 / 451,468 consumed |
| producer-timeout | 211,057 messages expired (`_MSG_TIMED_OUT`); 240,411 delivered; exit 1 |
| small-queue | Max queue 1,000–1,001 (cap 1,000); blocked 404 times for 6.0 s in total; 217,285 / 217,285 consumed |
| broker-throttle | 5.2 s → 36.4 s (26.1k → 3.7k events/s); max broker throttle 1,301 ms |
| slow-consumer | Lag peaked at 9,830 messages, drained to 0 about 25 s after production ended |
| consumer-restart | Resumed at the committed offset (62,649); full recovery took 45.7 s, about the 45 s session timeout |

## What we learned

- **Producer backpressure blocks; it never drops.** A full local queue raises
  `BufferError`. `KafkaSink` waits, so memory stays bounded by
  `queue.buffering.max.messages` / `queue.buffering.max.kbytes`.
- **The real data-loss risk is `message.timeout.ms`.** When the broker is
  unreachable for longer than that, messages expire in the client. The sink
  makes this loud (exit 1, counts in the manifest), but those events are gone
  unless the producer re-sends them. Production producers need either a timeout
  longer than any expected outage or a persistent local buffer.
- **A "failed" message may still have been written.** If a timed-out request
  was already in flight, the broker can still store it. That is why
  producer-timeout checks `delivered ≤ consumed ≤ produced`, not equality.
  Downstream deduplication on `event_id` is what makes this safe.
- **Consumers never slow producers.** A slow consumer shows up only as lag,
  which is why consumer lag is the metric to alert on (Phase 12).
- **A crashed consumer stalls its partitions for `session.timeout.ms`.**
  A SIGKILLed member keeps its partitions until the broker evicts it (45 s by
  default). For faster failover, lower the session timeout, at the cost of
  rebalancing more often on GC pauses.
- **Restarts are at-least-once.** The restart resumes from the last *committed*
  offset (auto-commit every 5 s). Anything the dead consumer processed after
  that commit is delivered again. Sinks must be idempotent.

## Measurements

`simulator --sink kafka` writes these to `manifest.json` under `kafka`:

| Field | Meaning |
|---|---|
| `produced` / `delivered` / `failed` | Messages handed to the producer / acknowledged by the broker / failed with an error |
| `first_error` | The first delivery error, if any |
| `queue_full` | Times `produce()` hit a full local queue |
| `blocked_seconds` | Total time the simulator waited for queue space |
| `max_queue_depth` | Peak `len(producer)`: queued messages plus in-flight requests, so it can read slightly above the message cap |
| `max_broker_rtt_ms`, `max_broker_throttle_ms` | Peak broker round-trip and throttle time, from librdkafka statistics |
| `timeline` | Per-second samples of produced, delivered, failed, queue depth and blocked time |

Any librdkafka producer setting can be passed with `--producer-config KEY=VALUE`
(repeatable). `ingestion.consume` takes `--delay-ms` (simulates a slow consumer)
and `--summary-json` (machine-readable results).

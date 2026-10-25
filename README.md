# Kafka Client Benchmark

A small, honest benchmark harness for Node.js Kafka clients, built as the
companion to the article *"KafkaJS Hasn't Shipped Since 2023. Should You
Move On?"* (kodedmo.dev).

It exists because most published client benchmarks share the same quiet
flaws: clients measured against differently durable settings, consumers
replaying whatever an old topic happens to hold, counters that accept a
batch overshoot as an exact total, and latency percentiles that are not
measuring what their labels claim. This harness is designed so those
flaws cannot happen, and it prints its full configuration next to every
number it produces.

**All results are local lab measurements, not production guarantees.**

## Run it

### Option 1: GitHub Codespaces (no local setup)

Open this repository in Codespaces (Code -> Codespaces -> Create). The
devcontainer starts a Kafka broker and a runner environment with
dependencies installed. Then:

```bash
npm run bench
```

### Option 2: Docker Compose (one command)

Requires Docker. Starts Apache Kafka 4.1.2 (KRaft, single node) and runs
the benchmark against it:

```bash
docker compose up --build
```

Choose clients or scale the workload through the environment:

```bash
CLIENTS=kafkajs docker compose up --build
MESSAGES=50000 RUNS=5 docker compose up --build
```

### Option 3: Your own broker

Point `BROKERS` at any reachable Kafka broker (3.5+ recommended) and run:

```bash
npm ci
BROKERS=127.0.0.1:9092 BROKER_VERSION="Apache Kafka 4.1.2" npm run bench
```

## Clients

| Client | Version | Notes |
| --- | --- | --- |
| kafkajs | 2.2.4 | Pure JS. The article's subject. |
| @platformatic/kafka | 2.14.0 | Pure JS/TS, no native addon required. |
| @confluentinc/kafka-javascript | 1.11.0 | librdkafka-backed, via its KafkaJS compatibility API. See the acks difference below. |
| node-rdkafka | not included | Requires a node-gyp source build at install time; it is welcome as a contribution where that build works. |

Select clients with `CLIENTS=kafkajs,platformatic,confluent`.

## How to read the output

The harness prints the full configuration first (client and version,
Node version, broker version, message count and size, partitions,
acks, batching shape, consumer mode). A run is only comparable to
another run whose configuration block matches.

- **Producer, serial scenario**: one send call in flight at a time,
  every send awaited. This is a *latency-focused* workload. It is
  deliberately **not** a maximum-throughput test; pipelining clients
  will look worse here than they are.
- **Producer, bounded scenario** (`PRODUCER_SCENARIO=bounded` or
  `both`): up to `CONCURRENCY` send calls in flight, with backpressure.
  Every send is still awaited before the run ends, so every number is
  broker-confirmed.
- **sendCallP50/P95/P99Ms** are latencies *per send call*. One call
  carries up to `BATCH_SIZE` messages. They are never divided by the
  batch size to fake a per-message latency.
- **Headline result** is the run with the median throughput. Its
  latency percentiles and CPU/RSS figures are *that run's own*, not
  pooled across runs, and the output says so. All individual runs are
  printed above the headline.
- **Consumer** numbers are *fetch/count-only*: the handler validates
  and counts messages and does no application processing. They are a
  raw consumption rate, not your application's throughput.
- **CPU/RSS**: consumer timing and resource baselines start when the
  first message arrives, so group join and assignment are excluded.
  RSS is a before/after delta, not a peak measurement.

## Methodology

### Dataset isolation

Every producer run writes to a fresh topic created for that client and
that run. Every consumer run reads a fresh topic seeded for that client
and that run, with a fresh consumer group. Topics are deleted
afterwards (`KEEP_TOPICS=1` to keep them for inspection). No run can
replay, pollute or double-count another run's data.

### Deterministic data

The dataset is a pure function of `(SEED, MESSAGES, PAYLOAD_BYTES)`.
Each message carries its sequence number in its first four bytes and a
PRNG fill derived from `(seed, seq)` only, so any message can be
regenerated independently and datasets are byte-identical across
machines. Check yours:

```bash
npm run seed:check   # prints the dataset checksum and message digests
```

### Exact counting

Before a consumer run, the harness verifies by partition offsets that
the seeded topic holds *exactly* the dataset. During the run, a
validator checks the consumed set, not just the total: every sequence
number in range seen exactly once, no foreign data, and an
order-independent checksum equal to the seeded dataset's checksum. A
run that fails validation throws and reports no result.

### Offsets and commits

Auto-commit is off everywhere. Only offsets of messages that were
actually counted are ever resolved or committed, and the commit happens
before the run is declared finished. A message that was not counted is
never committed, so this harness can neither skip it silently nor count
it twice. Per client:

- **KafkaJS, eachBatch** (default): offsets are resolved per counted
  message inside the batch (a mid-batch stop leaves the rest
  unresolved) and committed with `commitOffsetsIfNecessary()` at the
  target count.
- **KafkaJS, eachMessage** (`CONSUMER_MODE=eachMessage`): KafkaJS
  resolves each message as its handler completes; at the target the
  harness explicitly commits `last counted offset + 1` per partition.
  Same committed-offset policy as eachBatch, so the two modes are
  comparable.
- **Platformatic**: each counted message is committed with
  `message.commit()` immediately after counting; the stream is closed
  at the target.
- **Confluent**: eachMessage plus an explicit `commitOffsets()` of the
  counted set at the target, mirroring the KafkaJS eachMessage policy.

### Durability alignment

The default is `ACKS=-1` ("all in-sync replicas") for every client, and
the value used is printed in the configuration block and in each
client's adapter note. Known per-client differences, printed at runtime:

- **KafkaJS**: acks is a native per-send setting; used exactly as
  configured.
- **Platformatic**: acks is a native per-send setting with the same
  -1/0/1 semantics; used exactly as configured.
- **Confluent**: the compatibility API has **no per-send acks**. It is
  set once at producer creation (`'acks': '-1'`), the closest
  documented equivalent, and applies to every send. Compression is set
  to `none` for all clients.

## Exercises

1. **Durability costs something. Measure it.** Run with `ACKS=1` and
   with `ACKS=-1` (for example `ACKS=1 npm run bench:kafkajs`) and
   compare. Explain the tradeoff in terms of what the broker must do
   before it may acknowledge.
2. **Batch size is a latency dial.** Compare `BATCH_SIZE=1` and
   `BATCH_SIZE=100` in the serial producer scenario. Watch what happens
   to sendCallP99 and to throughput, and explain why both move.
3. **eachMessage vs eachBatch.** With KafkaJS, run
   `CONSUMER_MODE=eachMessage npm run bench:kafkajs`, then the default
   eachBatch run. The dataset, handler work and committed-offset policy
   are identical; only the delivery shape changes. Where does the
   difference show up?
4. **Optional: bounded concurrency.** Run `PRODUCER_SCENARIO=bounded`
   and compare with the serial run. Explain what the serial scenario
   was hiding, and why neither number alone is "the client's speed".

## Configuration

All knobs are environment variables: `BROKERS`, `BROKER_VERSION`,
`CLIENTS`, `MESSAGES` (20000), `PAYLOAD_BYTES` (256), `BATCH_SIZE`
(200), `RUNS` (3), `ACKS` (-1), `PARTITIONS` (3),
`REPLICATION_FACTOR` (1), `PRODUCER_SCENARIO` (serial | bounded |
both), `CONCURRENCY` (10), `CONSUMER_MODE` (eachBatch | eachMessage),
`SEED`, `TIMEOUT_MS` (120000), `KEEP_TOPICS`.

## Pinned versions

Node `>=22.22.0 || >=24.6.0` (engines field), kafkajs 2.2.4,
@platformatic/kafka 2.14.0, @confluentinc/kafka-javascript 1.11.0,
Apache Kafka 4.1.2 and node:24.22.1-bookworm-slim in the Compose and
devcontainer setup. `package-lock.json` locks the full tree.

## Repository layout

```
src/bench.mjs          orchestration: isolation, warm-up, runs, reporting
src/config.mjs         environment config + the printed configuration block
src/payloads.mjs       deterministic dataset generation and checksums
src/validator.mjs      exact-set consumption validation
src/stats.mjs          percentile / median definitions (read the comments)
src/topic-admin.mjs    topic setup/teardown and offset verification
src/seed.mjs           dataset determinism check (npm run seed:check)
src/adapters/          one file per client: kafkajs, platformatic, confluent
test/logic.test.mjs    broker-free tests: npm run test:logic
sample-output/         captured output from real runs of this repo's checks
```

## A note on this repo's own test evidence

The broker-free checks in `sample-output/` (logic tests, seed
determinism, adapter loading) were run and captured on the machine that
built this repository. A live broker run could not be completed on that
machine because its platform blocks TCP connections initiated by JVM
processes, which prevents any JVM Kafka broker from starting there;
`sample-output/environment-blocker.md` documents the evidence in full,
and `sample-output/attempted-live-run.txt` shows the actual attempt.
Run any of the three routes above in a normal environment to produce
live numbers; nothing in this repository presents a number that was not
measured.

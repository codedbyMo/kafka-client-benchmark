// Benchmark orchestrator.
//
// Isolation contract (this is what makes the numbers worth anything):
//   - Producer runs write to a fresh topic created for that client and
//     that run, and deleted afterwards. No client or run ever writes to
//     another one's topic.
//   - Consumer runs read a fresh topic seeded for that client and that
//     run with exactly the deterministic dataset, verified by offset
//     counts before the consumer starts. The consumer group is also
//     fresh per run. There is no shared state between runs to replay,
//     leak or double-count.
//   - Seeding always uses the KafkaJS adapter with acks=-1. Seeding is
//     infrastructure, not a measurement; the dataset bytes are identical
//     by construction and verified again on the way out by the
//     ConsumptionValidator (exact set + checksum).

import { loadConfig, printConfig } from './config.mjs';
import { generateDataset } from './payloads.mjs';
import { ConsumptionValidator } from './validator.mjs';
import { summarizeProducerRun, summarizeConsumerRun, medianRun } from './stats.mjs';
import { createAdmin, ensureTopic, removeTopic, topicMessageCount } from './topic-admin.mjs';
import * as kafkajsAdapter from './adapters/kafkajs.mjs';

const ADAPTERS = {
  kafkajs: kafkajsAdapter,
  platformatic: () => import('./adapters/platformatic.mjs'),
  confluent: () => import('./adapters/confluent.mjs'),
};

async function loadAdapter(clientName) {
  const entry = ADAPTERS[clientName];
  if (!entry) {
    throw new Error(`Unknown client "${clientName}". Known: ${Object.keys(ADAPTERS).join(', ')}`);
  }
  return typeof entry === 'function' ? entry() : entry;
}

function chunked(messages, size) {
  const chunks = [];
  for (let i = 0; i < messages.length; i += size) {
    chunks.push(messages.slice(i, i + size));
  }
  return chunks;
}

// One producer measurement over a full dataset. Returns the raw material
// for summarizeProducerRun: per-send-call latencies and totals.
async function produceOnce(adapter, config, topic, messages, scenario) {
  const producer = await adapter.createProducer(config);
  const sendLatenciesMs = [];
  const chunks = chunked(messages, config.batchSize);
  const cpu0 = process.cpuUsage();
  const rss0 = process.memoryUsage().rss;
  const startAll = process.hrtime.bigint();
  try {
    if (scenario === 'bounded') {
      // Bounded concurrency: at most config.concurrency send calls in
      // flight. Every chunk's send is still awaited before the run ends,
      // so every number here is broker-confirmed, with backpressure
      // applied by simply not starting new sends while at the limit.
      let next = 0;
      const workers = Array.from({ length: Math.min(config.concurrency, chunks.length) }, async () => {
        while (true) {
          const index = next++;
          if (index >= chunks.length) return;
          const t0 = process.hrtime.bigint();
          await producer.sendBatch(topic, chunks[index]);
          sendLatenciesMs.push(Number(process.hrtime.bigint() - t0) / 1e6);
        }
      });
      await Promise.all(workers);
    } else {
      // Serial acknowledged sends: one send call in flight at a time.
      // This is a latency-focused workload, NOT a maximum-throughput
      // test; the README says so in as many words.
      for (const chunk of chunks) {
        const t0 = process.hrtime.bigint();
        await producer.sendBatch(topic, chunk);
        sendLatenciesMs.push(Number(process.hrtime.bigint() - t0) / 1e6);
      }
    }
  } finally {
    await producer.close();
  }
  const wallMs = Number(process.hrtime.bigint() - startAll) / 1e6;
  const cpu = process.cpuUsage(cpu0);
  return {
    sendLatenciesMs,
    totalMessages: messages.length,
    wallMs,
    cpuMs: (cpu.user + cpu.system) / 1000,
    rssDeltaMb: (process.memoryUsage().rss - rss0) / 1e6,
  };
}

// Seeding verification polls the offset read until the count matches or
// the deadline passes. A single read taken immediately after topic
// creation and seeding can lag reality on a fresh topic (leader election
// and metadata propagation are concurrent with the check), so one read
// is not trusted on its own; this is a bounded poll, not a fixed sleep,
// and when the deadline expires the failure is the same error as a
// single-read check would have raised, with the last observed count.
const SEED_VERIFY_DEADLINE_MS = 30_000;
const SEED_VERIFY_POLL_MS = 250;

async function seedTopic(admin, config, topic, messages) {
  await ensureTopic(admin, topic, config.partitions, config.replicationFactor);
  const seeder = await kafkajsAdapter.createProducer(config);
  try {
    for (const chunk of chunked(messages, 500)) {
      await seeder.sendBatch(topic, chunk);
    }
  } finally {
    await seeder.close();
  }
  const deadline = Date.now() + SEED_VERIFY_DEADLINE_MS;
  let stored = await topicMessageCount(admin, topic);
  while (stored !== messages.length && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, SEED_VERIFY_POLL_MS));
    stored = await topicMessageCount(admin, topic);
  }
  if (stored !== messages.length) {
    throw new Error(`Seeding verification failed for ${topic}: expected ${messages.length} messages, broker holds ${stored}`);
  }
}

async function benchProducerScenario(admin, adapter, config, messages, runId, scenario) {
  // Warm-up: 10% of the workload against its own throwaway topic, never
  // reported, so first-run connection and metadata costs do not pollute
  // the measured runs.
  const warmTopic = `bench-${adapter.name}-warmup-${runId}`;
  await ensureTopic(admin, warmTopic, config.partitions, config.replicationFactor);
  try {
    const producer = await adapter.createProducer(config);
    try {
      await producer.sendBatch(warmTopic, messages.slice(0, Math.floor(messages.length / 10)));
    } finally {
      await producer.close();
    }
  } finally {
    if (!config.keepTopics) await removeTopic(admin, warmTopic);
  }

  const runs = [];
  for (let run = 0; run < config.runs; run++) {
    const topic = `bench-${adapter.name}-producer-${scenario}-r${run}-${runId}`;
    await ensureTopic(admin, topic, config.partitions, config.replicationFactor);
    try {
      runs.push(summarizeProducerRun(await produceOnce(adapter, config, topic, messages, scenario)));
    } finally {
      if (!config.keepTopics) await removeTopic(admin, topic);
    }
  }
  return runs;
}

async function benchConsumer(admin, adapter, config, messages, runId) {
  const runs = [];
  for (let run = 0; run < config.runs; run++) {
    const topic = `bench-${adapter.name}-consumer-r${run}-${runId}`;
    const groupId = `bench-${adapter.name}-r${run}-${runId}`;
    await seedTopic(admin, config, topic, messages);
    try {
      const validator = new ConsumptionValidator(messages);
      const raw = await adapter.consumeOnce({ config, topic, groupId, validator });
      const validation = validator.result();
      if (!validation.ok) {
        throw new Error(
          `Consumer validation failed for ${adapter.name} run ${run}: ${JSON.stringify(validation)}`
        );
      }
      runs.push({ ...summarizeConsumerRun({ received: validator.count, ...raw }), validation });
    } finally {
      if (!config.keepTopics) await removeTopic(admin, topic);
    }
  }
  return runs;
}

// One client's failure must not abort the whole run: a client that
// cannot talk to the broker at all (e.g. a client/broker version
// incompatibility in its metadata step) would otherwise hide every
// other client's results and the summary. Failures are therefore
// isolated per client and per phase, recorded in the results in place
// of that phase's runs as { status: 'FAILED', error }, and the process
// exit code is non-zero if anything failed (see README, "Failure
// handling and exit codes").
function failureRecord(error) {
  const message = error && error.message ? error.message : String(error);
  const code = error && error.code && !message.includes(String(error.code)) ? ` [${error.code}]` : '';
  return { status: 'FAILED', error: message + code };
}

async function main() {
  const config = loadConfig();
  const messages = generateDataset(config.messages, config.payloadBytes, config.seed);
  const runId = Date.now().toString(36);
  const admin = createAdmin(config.brokers);
  await admin.connect();
  const allResults = {};
  let anyFailure = false;
  try {
    for (const clientName of config.clients) {
      let adapter;
      try {
        adapter = await loadAdapter(clientName);
      } catch (error) {
        anyFailure = true;
        const failure = failureRecord(error);
        allResults[clientName] = failure;
        console.log(`\nClient ${clientName} FAILED to load: ${failure.error}`);
        console.log('\n==================================================\n');
        continue;
      }
      printConfig(config, adapter.name, adapter.version);
      console.log(adapter.acksNote(config));

      const scenarios =
        config.producerScenario === 'both' ? ['serial', 'bounded'] : [config.producerScenario];
      const producerResults = {};
      for (const scenario of scenarios) {
        try {
          const runs = await benchProducerScenario(admin, adapter, config, messages, runId, scenario);
          producerResults[scenario] = runs;
          console.log(`\nProducer (${scenario}) runs for ${adapter.name}:`);
          for (const [i, r] of runs.entries()) console.log(`  run ${i}:`, JSON.stringify(r));
          console.log(
            `Producer (${scenario}) headline (median-throughput run; its latency percentiles and CPU/RSS are that run's own):`,
            JSON.stringify(medianRun(runs))
          );
        } catch (error) {
          anyFailure = true;
          const failure = failureRecord(error);
          producerResults[scenario] = failure;
          console.log(`\nProducer (${scenario}) FAILED for ${adapter.name}: ${failure.error}`);
        }
      }

      let consumerResult;
      try {
        const consumerRuns = await benchConsumer(admin, adapter, config, messages, runId);
        consumerResult = consumerRuns;
        console.log(`\nConsumer (fetch/count-only) runs for ${adapter.name}:`);
        for (const [i, r] of consumerRuns.entries()) console.log(`  run ${i}:`, JSON.stringify(r));
        console.log(
          'Consumer headline (median-throughput run):',
          JSON.stringify(medianRun(consumerRuns))
        );
      } catch (error) {
        anyFailure = true;
        const failure = failureRecord(error);
        consumerResult = failure;
        console.log(`\nConsumer FAILED for ${adapter.name}: ${failure.error}`);
      }

      allResults[adapter.name] = { producer: producerResults, consumer: consumerResult };
      console.log('\n==================================================\n');
    }
  } finally {
    await admin.disconnect();
  }
  console.log('RESULTS_JSON_BEGIN');
  console.log(JSON.stringify(allResults, null, 2));
  console.log('RESULTS_JSON_END');
  // exitCode (not exit()) so the summary above is flushed first. A fatal
  // setup failure (bad config, broker unreachable) never reaches this
  // point: it throws out of main() and exits 1 with no summary, because
  // nothing was measured.
  if (anyFailure) process.exitCode = 1;
}

main().catch((error) => {
  console.error('Benchmark failed:', error);
  process.exit(1);
});

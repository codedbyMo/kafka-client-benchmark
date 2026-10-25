// @confluentinc/kafka-javascript 1.11.0 adapter, via its KafkaJS
// compatibility API (backed by librdkafka).
//
// Documented differences from KafkaJS that this adapter has to respect
// (see the package's MIGRATION.md):
//   - Configuration is librdkafka-style and lives at client CREATION:
//     acks and compression are producer-creation settings, not per-send;
//     'auto.offset.reset' and 'enable.auto.commit' are consumer-creation
//     settings, not subscribe()/run() arguments.
//   - subscribe() takes { topics: [...] }, not { topic }.
//   - There is no heartbeat() to call in handlers; librdkafka manages
//     heartbeats itself.
//   - eachBatch semantics differ from KafkaJS under the hood, so this
//     adapter consumes with eachMessage and commits explicitly.

import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const require = createRequire(import.meta.url);
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

export const name = 'confluent';
export const version = JSON.parse(
  readFileSync(
    new URL('../../node_modules/@confluentinc/kafka-javascript/package.json', import.meta.url),
    'utf8'
  )
).version;

export function acksNote(config) {
  return (
    `confluent: DIFFERENCE - acks is not a per-send option in the compatibility API; ` +
    `it is set once at producer creation ('acks': '${config.acks}'), which is the closest ` +
    `documented equivalent and applies to every send this producer makes.`
  );
}

export async function createProducer(config) {
  const kafka = new Kafka();
  const producer = kafka.producer({
    'bootstrap.servers': config.brokers.join(','),
    'client.id': 'bench',
    acks: String(config.acks),
    'compression.type': 'none',
    'enable.idempotence': false,
  });
  await producer.connect();
  return {
    // send() resolves with delivery reports, i.e. only after the broker
    // has acknowledged with the creation-time acks setting.
    async sendBatch(topic, chunk) {
      await producer.send({ topic, messages: chunk });
    },
    async close() {
      await producer.disconnect();
    },
  };
}

// Fetch/count-only consumer. Manual commit policy mirrors the other
// adapters: 'enable.auto.commit' is false, and exactly the counted set is
// committed explicitly once the target is reached.
export async function consumeOnce({ config, topic, groupId, validator }) {
  const kafka = new Kafka();
  const consumer = kafka.consumer({
    'bootstrap.servers': config.brokers.join(','),
    'client.id': 'bench',
    'group.id': groupId,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': false,
  });
  await consumer.connect();
  await consumer.subscribe({ topics: [topic] });

  let start;
  let cpu0;
  let rss0;
  const markStart = () => {
    if (start === undefined) {
      start = process.hrtime.bigint();
      cpu0 = process.cpuUsage();
      rss0 = process.memoryUsage().rss;
    }
  };

  let resolveDone;
  let rejectTimeout;
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectTimeout = reject;
  });
  const timer = setTimeout(
    () => rejectTimeout(new Error(`Consumer timed out at ${validator.count}/${validator.target} messages`)),
    config.timeoutMs
  );

  const lastCountedByPartition = new Map();
  try {
    const runPromise = consumer.run({
      eachMessage: async ({ partition, message }) => {
        markStart();
        const before = validator.count;
        const more = validator.observe(message.value);
        if (validator.count > before) {
          lastCountedByPartition.set(partition, message.offset);
        }
        if (!more) {
          const offsets = [...lastCountedByPartition.entries()].map(([p, offset]) => ({
            topic,
            partition: p,
            offset: (BigInt(offset) + 1n).toString(),
          }));
          if (offsets.length > 0) await consumer.commitOffsets(offsets);
          resolveDone();
        }
      },
    });
    runPromise.catch(rejectTimeout);
    await done;
  } finally {
    clearTimeout(timer);
    await consumer.disconnect().catch(() => {});
  }

  const wallMs = Number(process.hrtime.bigint() - start) / 1e6;
  const cpu = process.cpuUsage(cpu0);
  return {
    wallMs,
    cpuMs: (cpu.user + cpu.system) / 1000,
    rssDeltaMb: (process.memoryUsage().rss - rss0) / 1e6,
  };
}

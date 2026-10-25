// KafkaJS 2.2.4 adapter.

import { Kafka, CompressionTypes } from 'kafkajs';
import { readFileSync } from 'node:fs';

export const name = 'kafkajs';
export const version = JSON.parse(
  readFileSync(new URL('../../node_modules/kafkajs/package.json', import.meta.url), 'utf8')
).version;

export function acksNote(config) {
  return `kafkajs: acks is a native per-send setting; using acks=${config.acks} exactly as configured.`;
}

function createKafka(config) {
  return new Kafka({ clientId: 'bench', brokers: config.brokers });
}

export async function createProducer(config) {
  const kafka = createKafka(config);
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  return {
    // Resolves only when the broker has acknowledged the batch with the
    // configured durability (acks=-1: all in-sync replicas).
    async sendBatch(topic, chunk) {
      await producer.send({
        topic,
        acks: config.acks,
        compression: CompressionTypes.None,
        messages: chunk,
      });
    },
    async close() {
      await producer.disconnect();
    },
  };
}

// Fetch/count-only consumer: the handler validates and counts messages; it
// deliberately does no application processing, so the number is a raw
// consumption rate, not an application throughput rate.
//
// Timing and CPU/RSS baselines are taken when the FIRST batch/message
// arrives, so group join and assignment are excluded from every figure.
//
// Offset policy (both modes):
//   - autoCommit is off. Nothing is committed on a timer.
//   - Only offsets of messages that were actually counted are resolved /
//     committed, and the final commit happens before the run is declared
//     done. A message that was not counted is never committed, so it can
//     neither be skipped silently nor counted twice by this harness.
export async function consumeOnce({ config, topic, groupId, validator }) {
  const kafka = createKafka(config);
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

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

  let rejectTimeout;
  let resolveDone;
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectTimeout = reject;
  });
  const timer = setTimeout(() => {
    rejectTimeout(
      new Error(`Consumer timed out at ${validator.count}/${validator.target} messages`)
    );
  }, config.timeoutMs);

  const finish = () => {
    clearTimeout(timer);
    resolveDone();
  };

  try {
    if (config.consumerMode === 'eachMessage') {
      const lastCountedByPartition = new Map();
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          markStart();
          const before = validator.count;
          const more = validator.observe(message.value);
          if (validator.count > before) {
            lastCountedByPartition.set(partition, message.offset);
          }
          if (!more) {
            // Persist exactly the counted set: for each partition, the
            // offset AFTER the last counted message.
            const offsets = [...lastCountedByPartition.entries()].map(([p, offset]) => ({
              topic,
              partition: p,
              offset: (BigInt(offset) + 1n).toString(),
            }));
            if (offsets.length > 0) await consumer.commitOffsets(offsets);
            finish();
          }
        },
      });
    } else {
      await consumer.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
          markStart();
          // batch.messages all belong to batch.partition. Count one by
          // one and resolve only what was counted: if the target is hit
          // mid-batch, the rest of the batch stays unresolved.
          for (const message of batch.messages) {
            const more = validator.observe(message.value);
            resolveOffset(message.offset);
            if (!more) break;
          }
          await heartbeat();
          if (validator.count >= validator.target) {
            await commitOffsetsIfNecessary();
            finish();
          }
        },
      });
    }
    await done;
  } finally {
    clearTimeout(timer);
    try {
      await consumer.stop();
    } catch {
      // Already stopped or never fully started; disconnect below is enough.
    }
    await consumer.disconnect();
  }

  const wallMs = Number(process.hrtime.bigint() - start) / 1e6;
  const cpu = process.cpuUsage(cpu0);
  return {
    wallMs,
    cpuMs: (cpu.user + cpu.system) / 1000,
    rssDeltaMb: (process.memoryUsage().rss - rss0) / 1e6,
  };
}

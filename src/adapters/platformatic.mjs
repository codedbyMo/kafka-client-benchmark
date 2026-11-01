// @platformatic/kafka 2.14.0 adapter.

import { Producer, Consumer, serializersFrom, deserializersFrom } from '@platformatic/kafka';
import { readFileSync } from 'node:fs';

export const name = 'platformatic';
export const version = JSON.parse(
  readFileSync(new URL('../../node_modules/@platformatic/kafka/package.json', import.meta.url), 'utf8')
).version;

// Raw Buffer pass-through: the harness produces and validates Buffers, so
// no string/JSON (de)serialisation may sit between the client and the wire.
const bufferSerializers = serializersFrom((data) => data);
const bufferDeserializers = deserializersFrom((data) => data);

export function acksNote(config) {
  return `platformatic: acks is a native per-send setting with the same -1/0/1 semantics; using acks=${config.acks} exactly as configured.`;
}

export async function createProducer(config) {
  const producer = new Producer({
    clientId: 'bench',
    bootstrapBrokers: config.brokers,
    serializers: bufferSerializers,
  });
  return {
    async sendBatch(topic, chunk) {
      await producer.send({
        messages: chunk.map((m) => ({ topic, key: m.key, value: m.value })),
        acks: config.acks,
        compression: 'none',
      });
    },
    async close() {
      await producer.close();
    },
  };
}

// Fetch/count-only consumer, same contract as the KafkaJS adapter: timing
// and CPU/RSS baselines start at the first message, group setup excluded.
// The window ends when the target count is reached and the counted set's
// commit has completed; consumer.close() is teardown and sits outside
// the window. (Methodology correction, Oct 31, 2026: close time was
// previously included in wallMs.)
//
// Offset policy: autocommit is off and the stream runs in 'earliest' mode
// for the fresh group. The last counted message per partition is tracked
// during the run, and the counted set is committed once, at the target,
// by committing each partition's last counted message (message.commit()
// commits offset + 1 through the stream's commit machinery, while the
// stream is still live). Messages that were not counted are never
// committed. The stream is closed as soon as the target count is
// reached, so nothing past the target can be consumed or committed by
// this run. (Methodology correction, Oct 31, 2026: this adapter
// previously committed every counted record during the run, a heavier
// workload than the other adapters' single end-of-run commit; the
// policy is now aligned across all three adapters.)
export async function consumeOnce({ config, topic, groupId, validator }) {
  const consumer = new Consumer({
    groupId,
    clientId: 'bench',
    bootstrapBrokers: config.brokers,
    deserializers: bufferDeserializers,
  });

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

  const lastCountedByPartition = new Map();
  const consumeLoop = (async () => {
    const stream = await consumer.consume({
      topics: [topic],
      mode: 'earliest',
      autocommit: false,
    });
    for await (const message of stream) {
      markStart();
      const before = validator.count;
      const more = validator.observe(message.value);
      if (validator.count > before) {
        lastCountedByPartition.set(message.partition, message);
      }
      if (!more) {
        // Target reached: commit the counted set once, now, mirroring
        // the other adapters. No commits happen per record during the
        // measured window.
        for (const lastCounted of lastCountedByPartition.values()) {
          await lastCounted.commit();
        }
        break;
      }
    }
  })();

  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () =>
        reject(new Error(`Consumer timed out at ${validator.count}/${validator.target} messages`)),
      config.timeoutMs
    );
  });

  let end;
  let cpuAtEnd;
  let rssEnd;
  try {
    await Promise.race([consumeLoop, timeout]);
    // Target reached and the counted set committed inside the loop: the
    // window ends here, before the teardown in the finally block.
    end = process.hrtime.bigint();
    cpuAtEnd = process.cpuUsage(cpu0);
    rssEnd = process.memoryUsage().rss;
  } finally {
    clearTimeout(timer);
    await consumer.close(true).catch(() => {});
  }

  const wallMs = Number(end - start) / 1e6;
  return {
    wallMs,
    cpuMs: (cpuAtEnd.user + cpuAtEnd.system) / 1000,
    rssDeltaMb: (rssEnd - rss0) / 1e6,
  };
}

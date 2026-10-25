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
//
// Offset policy: autocommit is off and the stream runs in 'earliest' mode
// for the fresh group. Each counted message is committed explicitly with
// message.commit() right after it is counted; messages that were not
// counted are never committed. The stream is closed as soon as the target
// count is reached, so nothing past the target can be consumed or
// committed by this run.
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
        await message.commit();
      }
      if (!more) break;
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

  try {
    await Promise.race([consumeLoop, timeout]);
  } finally {
    clearTimeout(timer);
    await consumer.close(true).catch(() => {});
  }

  const wallMs = Number(process.hrtime.bigint() - start) / 1e6;
  const cpu = process.cpuUsage(cpu0);
  return {
    wallMs,
    cpuMs: (cpu.user + cpu.system) / 1000,
    rssDeltaMb: (process.memoryUsage().rss - rss0) / 1e6,
  };
}

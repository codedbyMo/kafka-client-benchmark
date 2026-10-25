// Topic management for the harness itself.
//
// This uses KafkaJS's admin client no matter which client is being
// benchmarked. That is deliberate: topic creation, deletion and offset
// inspection are test infrastructure, not part of the measurement, and
// using one admin implementation keeps the setup identical for every
// client under test.

import { Kafka } from 'kafkajs';

export function createAdmin(brokers) {
  const kafka = new Kafka({ clientId: 'bench-admin', brokers });
  return kafka.admin();
}

export async function ensureTopic(admin, topic, partitions, replicationFactor) {
  await admin.createTopics({
    topics: [{ topic, numPartitions: partitions, replicationFactor }],
    waitForLeaders: true,
  });
}

export async function removeTopic(admin, topic) {
  try {
    await admin.deleteTopics({ topics: [topic] });
  } catch {
    // Cleanup is best-effort; a leftover empty bench topic is harmless.
  }
}

// Total messages currently stored in a topic: sum of (high watermark) over
// all partitions. Used to prove a seeded topic holds exactly the dataset
// before a consumer run starts.
export async function topicMessageCount(admin, topic) {
  const offsets = await admin.fetchTopicOffsets(topic);
  return offsets.reduce((sum, p) => sum + Number(p.offset), 0);
}

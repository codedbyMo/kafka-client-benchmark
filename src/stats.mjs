// Run statistics.
//
// Definitions used everywhere in this harness (kept honest on purpose):
//   - Producer latencies are PER SEND CALL, not per message. One send() call
//     carries up to batchSize messages; dividing its latency by the batch
//     size would invent a per-message number nobody measured. They are
//     reported as sendCallP50/P95/P99Ms.
//   - The headline run is the run with the median throughput. Its latency
//     percentiles and CPU/RSS figures come from THAT run alone; they are
//     not pooled or averaged across runs, and the output says so.

export function percentile(sortedAscending, p) {
  if (sortedAscending.length === 0) return null;
  const idx = Math.min(sortedAscending.length - 1, Math.floor((p / 100) * sortedAscending.length));
  return sortedAscending[idx];
}

export function medianOf(values) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

export function summarizeProducerRun({ sendLatenciesMs, totalMessages, wallMs, cpuMs, rssDeltaMb }) {
  if (sendLatenciesMs.length === 0 || wallMs <= 0) {
    throw new Error('Cannot summarize a producer run with no samples or no elapsed time');
  }
  const sorted = [...sendLatenciesMs].sort((a, b) => a - b);
  return {
    messages: totalMessages,
    sendCalls: sendLatenciesMs.length,
    wallMs: Math.round(wallMs),
    messagesPerSec: Math.round(totalMessages / (wallMs / 1000)),
    sendCallP50Ms: round2(percentile(sorted, 50)),
    sendCallP95Ms: round2(percentile(sorted, 95)),
    sendCallP99Ms: round2(percentile(sorted, 99)),
    cpuMs: Math.round(cpuMs),
    rssDeltaMb: round1(rssDeltaMb),
  };
}

export function summarizeConsumerRun({ received, wallMs, cpuMs, rssDeltaMb }) {
  if (received === 0 || wallMs <= 0) {
    throw new Error('Cannot summarize a consumer run with no messages or no elapsed time');
  }
  return {
    messages: received,
    wallMs: Math.round(wallMs),
    messagesPerSec: Math.round(received / (wallMs / 1000)),
    cpuMs: Math.round(cpuMs),
    rssDeltaMb: round1(rssDeltaMb),
  };
}

// Pick the run with the median throughput and report it as the headline,
// keeping every other run visible alongside.
export function medianRun(runs) {
  if (runs.length === 0) return null;
  const sorted = [...runs].sort((a, b) => a.messagesPerSec - b.messagesPerSec);
  return sorted[Math.floor(sorted.length / 2)];
}

function round2(n) {
  return n === null ? null : Math.round(n * 100) / 100;
}
function round1(n) {
  return n === null ? null : Math.round(n * 10) / 10;
}

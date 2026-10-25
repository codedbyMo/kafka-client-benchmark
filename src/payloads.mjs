// Deterministic dataset generation.
//
// Every client benchmarked by this harness consumes the SAME bytes. The
// dataset is a pure function of (seed, count, payloadBytes): message i is
// generated from the seed and its own sequence number only, so datasets are
// identical across processes, machines and clients, and any single message
// can be regenerated and checked on its own.
//
// Message layout (value, exactly payloadBytes long):
//   bytes 0..3   sequence number, uint32 big-endian
//   bytes 4..7   seed, uint32 big-endian
//   bytes 8..end deterministic PRNG fill derived from (seed, seq)

export const DEFAULT_SEED = 0x5eed;

function mulberry32(initialState) {
  let state = initialState >>> 0;
  return function next() {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function generateMessage(seq, payloadBytes, seed = DEFAULT_SEED) {
  if (payloadBytes < 8) {
    throw new Error(`payloadBytes must be at least 8 to hold the header, got ${payloadBytes}`);
  }
  const value = Buffer.alloc(payloadBytes);
  value.writeUInt32BE(seq >>> 0, 0);
  value.writeUInt32BE(seed >>> 0, 4);
  // Per-message PRNG stream: fill depends only on (seed, seq), never on
  // generation order, so datasets are reproducible message by message.
  const next = mulberry32((seed ^ Math.imul(seq + 1, 0x9e3779b1)) >>> 0);
  for (let offset = 8; offset < payloadBytes; offset++) {
    value[offset] = Math.floor(next() * 256);
  }
  return { key: `key-${seq}`, value };
}

export function generateDataset(count, payloadBytes, seed = DEFAULT_SEED) {
  const messages = new Array(count);
  for (let seq = 0; seq < count; seq++) {
    messages[seq] = generateMessage(seq, payloadBytes, seed);
  }
  return messages;
}

export function readSeq(value) {
  return value.readUInt32BE(0);
}

// FNV-1a 64-bit over a buffer, returned as a BigInt. Used for an
// order-independent dataset checksum: the sum of per-message hashes is the
// same no matter which order partitions deliver the messages in.
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = 0xffffffffffffffffn;

export function messageHash(value) {
  let hash = FNV_OFFSET;
  for (const byte of value) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & MASK_64;
  }
  return hash;
}

export function datasetChecksum(messages) {
  let sum = 0n;
  for (const message of messages) {
    sum = (sum + messageHash(message.value)) & MASK_64;
  }
  return sum;
}

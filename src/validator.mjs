// Exact consumption validation.
//
// The consumer benchmark is only meaningful if the consumer read exactly the
// seeded dataset: every message, each exactly once, nothing else. Counting
// "messages >= target" is not enough (a batch can overshoot, a shared topic
// can replay old data, a redelivery can double-count). This validator checks
// the full set, not just the total:
//
//   - every sequence number in [0, target) seen exactly once
//   - no sequence number outside that range
//   - the order-independent checksum of everything consumed equals the
//     checksum of the seeded dataset
//
// Feed it messages one at a time with observe(seq, value). It returns false
// from observe() once the target count is reached, so the caller can stop
// mid-batch and must not count (or resolve offsets for) the rest.

import { datasetChecksum, messageHash, readSeq } from './payloads.mjs';

export class ConsumptionValidator {
  constructor(messages) {
    this.target = messages.length;
    this.expectedChecksum = datasetChecksum(messages);
    this.seen = new Set();
    this.count = 0;
    this.duplicates = 0;
    this.outOfRange = 0;
    this.checksum = 0n;
  }

  // Returns true while more messages are needed, false when the target has
  // been reached (including when this very message completed the set).
  observe(value) {
    if (this.count >= this.target) return false;
    const seq = readSeq(value);
    this.count++;
    if (seq < 0 || seq >= this.target) {
      this.outOfRange++;
    } else if (this.seen.has(seq)) {
      this.duplicates++;
    } else {
      this.seen.add(seq);
    }
    this.checksum = (this.checksum + messageHash(value)) & 0xffffffffffffffffn;
    return this.count < this.target;
  }

  result() {
    const missing = this.target - this.seen.size;
    const checksumOk = this.checksum === this.expectedChecksum;
    const ok =
      this.count === this.target &&
      this.seen.size === this.target &&
      this.duplicates === 0 &&
      this.outOfRange === 0 &&
      checksumOk;
    return {
      ok,
      count: this.count,
      unique: this.seen.size,
      missing,
      duplicates: this.duplicates,
      outOfRange: this.outOfRange,
      checksumOk,
    };
  }
}

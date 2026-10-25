// Dataset determinism check (no broker needed).
//
// Prints the fingerprints of the dataset this harness would seed. Run it
// twice, or on two machines: the checksums must match exactly, because the
// dataset is a pure function of (seed, count, payloadBytes).

import { createHash } from 'node:crypto';
import { loadConfig } from './config.mjs';
import { generateDataset, datasetChecksum } from './payloads.mjs';

const config = loadConfig();
const messages = generateDataset(config.messages, config.payloadBytes, config.seed);
const digest = (buf) => createHash('sha256').update(buf).digest('hex');

console.log(`messages:       ${messages.length}`);
console.log(`payloadBytes:   ${config.payloadBytes}`);
console.log(`seed:           ${config.seed}`);
console.log(`checksum (hex): ${datasetChecksum(messages).toString(16)}`);
console.log(`first message:  key=${messages[0].key} sha256=${digest(messages[0].value)}`);
console.log(`last message:   key=${messages[messages.length - 1].key} sha256=${digest(messages[messages.length - 1].value)}`);

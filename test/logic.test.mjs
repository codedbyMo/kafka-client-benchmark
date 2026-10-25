// Broker-free logic tests: dataset determinism, exact-count validation,
// and the statistics definitions. Run with: npm run test:logic

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { generateDataset, generateMessage, datasetChecksum } from '../src/payloads.mjs';
import { ConsumptionValidator } from '../src/validator.mjs';
import { percentile, medianOf, summarizeProducerRun, medianRun } from '../src/stats.mjs';
import { loadConfig } from '../src/config.mjs';

const COUNT = 2_000;
const BYTES = 128;

test('dataset generation is deterministic within a process', () => {
  const a = generateDataset(COUNT, BYTES, 42);
  const b = generateDataset(COUNT, BYTES, 42);
  assert.equal(datasetChecksum(a), datasetChecksum(b));
  assert.deepEqual(a[123], b[123]);
});

test('dataset generation is deterministic across processes', () => {
  const here = datasetChecksum(generateDataset(COUNT, BYTES, 42)).toString();
  const payloadsUrl = new URL('../src/payloads.mjs', import.meta.url).href;
  const childScript = `
    const m = await import(${JSON.stringify('PAYLOADS_URL')});
    const ds = m.generateDataset(${COUNT}, ${BYTES}, 42);
    console.log(m.datasetChecksum(ds).toString());`.replace('PAYLOADS_URL', payloadsUrl);
  const out = execFileSync(process.execPath, ['--input-type=module', '-e', childScript], {
    encoding: 'utf8',
  }).trim();
  assert.equal(out, here);
});

test('a single message regenerates identically out of order', () => {
  const dataset = generateDataset(COUNT, BYTES, 7);
  assert.deepEqual(generateMessage(999, BYTES, 7), dataset[999]);
});

test('different seeds produce different datasets', () => {
  assert.notEqual(
    datasetChecksum(generateDataset(100, BYTES, 1)),
    datasetChecksum(generateDataset(100, BYTES, 2))
  );
});

function shuffledCopy(messages) {
  const copy = [...messages];
  // Deterministic shuffle (Fisher-Yates with a fixed LCG) so the test is stable.
  let s = 12345;
  const rnd = () => ((s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

test('validator accepts the exact dataset in any delivery order', () => {
  const dataset = generateDataset(COUNT, BYTES, 9);
  const validator = new ConsumptionValidator(dataset);
  for (const message of shuffledCopy(dataset)) validator.observe(message.value);
  const result = validator.result();
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.equal(result.count, COUNT);
});

test('validator rejects a duplicate (double-counted) message', () => {
  const dataset = generateDataset(100, BYTES, 9);
  const validator = new ConsumptionValidator(dataset);
  for (const message of dataset.slice(0, 99)) validator.observe(message.value);
  validator.observe(dataset[0].value); // duplicate instead of message 99
  const result = validator.result();
  assert.equal(result.ok, false);
  assert.equal(result.duplicates, 1);
  assert.equal(result.missing, 1);
});

test('validator rejects a short read (missing messages)', () => {
  const dataset = generateDataset(100, BYTES, 9);
  const validator = new ConsumptionValidator(dataset);
  for (const message of dataset.slice(0, 90)) validator.observe(message.value);
  const result = validator.result();
  assert.equal(result.ok, false);
  assert.equal(result.missing, 10);
});

test('validator refuses to count past the target (no overshoot)', () => {
  const dataset = generateDataset(10, BYTES, 9);
  const validator = new ConsumptionValidator(dataset);
  for (const message of dataset) {
    assert.equal(validator.observe(message.value), validator.count < 10);
  }
  assert.equal(validator.observe(dataset[0].value), false);
  assert.equal(validator.count, 10);
  assert.equal(validator.result().ok, true);
});

test('validator rejects foreign data (wrong seed pollutes the set)', () => {
  const dataset = generateDataset(50, BYTES, 9);
  const foreign = generateDataset(50, BYTES, 10);
  const validator = new ConsumptionValidator(dataset);
  for (const message of dataset.slice(0, 49)) validator.observe(message.value);
  validator.observe(foreign[49].value);
  assert.equal(validator.result().checksumOk, false);
  assert.equal(validator.result().ok, false);
});

test('percentile and median definitions', () => {
  const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  assert.equal(percentile(sorted, 50), 6);
  assert.equal(percentile(sorted, 99), 10);
  assert.equal(percentile([], 50), null);
  assert.equal(medianOf([3, 1, 2]), 2);
  assert.equal(medianOf([4, 1, 3, 2]), 2.5);
});

test('producer summary refuses empty samples', () => {
  assert.throws(() =>
    summarizeProducerRun({ sendLatenciesMs: [], totalMessages: 0, wallMs: 0, cpuMs: 0, rssDeltaMb: 0 })
  );
});

test('medianRun picks the middle-throughput run', () => {
  const runs = [
    { messagesPerSec: 100, tag: 'slow' },
    { messagesPerSec: 300, tag: 'mid' },
    { messagesPerSec: 200, tag: 'fast-ish' },
  ];
  assert.equal(medianRun(runs).tag, 'fast-ish');
});

test('default config aligns durability at acks=-1', () => {
  const config = loadConfig({});
  assert.equal(config.acks, -1);
  assert.equal(config.messages, 20_000);
});

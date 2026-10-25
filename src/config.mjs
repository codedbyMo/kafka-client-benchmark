// Benchmark configuration. Every knob comes from the environment so runs
// are reproducible from a command line, and the full effective config is
// printed before anything is measured (see printConfig).

export function loadConfig(env = process.env) {
  const config = {
    brokers: (env.BROKERS ?? '127.0.0.1:9092').split(',').map((s) => s.trim()).filter(Boolean),
    brokerVersion: env.BROKER_VERSION ?? 'unknown (set BROKER_VERSION)',
    clients: (env.CLIENTS ?? 'kafkajs').split(',').map((s) => s.trim()).filter(Boolean),
    messages: int(env, 'MESSAGES', 20_000),
    payloadBytes: int(env, 'PAYLOAD_BYTES', 256),
    batchSize: int(env, 'BATCH_SIZE', 200),
    runs: int(env, 'RUNS', 3),
    // Durability is aligned across clients by default: -1 means "all
    // in-sync replicas" (KafkaJS acks=-1). Adapters map this to the closest
    // documented equivalent for their client and print any difference.
    acks: int(env, 'ACKS', -1),
    partitions: int(env, 'PARTITIONS', 3),
    replicationFactor: int(env, 'REPLICATION_FACTOR', 1),
    producerScenario: env.PRODUCER_SCENARIO ?? 'serial', // serial | bounded | both
    concurrency: int(env, 'CONCURRENCY', 10),
    consumerMode: env.CONSUMER_MODE ?? 'eachBatch', // kafkajs: eachBatch | eachMessage
    seed: int(env, 'SEED', 0x5eed),
    timeoutMs: int(env, 'TIMEOUT_MS', 120_000),
    keepTopics: env.KEEP_TOPICS === '1',
  };
  if (config.acks !== -1 && config.acks !== 0 && config.acks !== 1) {
    throw new Error(`ACKS must be -1, 0 or 1, got ${config.acks}`);
  }
  return config;
}

function int(env, name, fallback) {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) throw new Error(`${name} must be a number, got "${raw}"`);
  return value;
}

export function printConfig(config, clientName, clientVersion) {
  const lines = [
    '--- benchmark configuration ---',
    `client:          ${clientName} ${clientVersion}`,
    `node:            ${process.version}`,
    `broker:          ${config.brokerVersion} at ${config.brokers.join(',')}`,
    `messages:        ${config.messages} per run, ${config.runs} measured runs (+ warm-up)`,
    `payload:         ${config.payloadBytes} bytes/message, seed ${config.seed}`,
    `topic layout:    ${config.partitions} partitions, replication factor ${config.replicationFactor}, one fresh topic per client per run`,
    `producer acks:   ${config.acks} (-1 = all in-sync replicas)`,
    `producer shape:  scenario=${config.producerScenario}, batchSize=${config.batchSize} messages per send call` +
      (config.producerScenario !== 'serial' ? `, concurrency=${config.concurrency}` : ''),
    `consumer:        fresh group per run, fetch/count-only handler, mode=${config.consumerMode} (kafkajs) or client equivalent`,
    `compression:     none (client defaults overridden where the client allows)`,
    '-------------------------------',
  ];
  for (const line of lines) console.log(line);
}

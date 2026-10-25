// Adapter load check (no broker needed): proves every shipped adapter
// module imports cleanly and reports its pinned client version.
import * as kafkajs from './adapters/kafkajs.mjs';
import * as platformatic from './adapters/platformatic.mjs';
import * as confluent from './adapters/confluent.mjs';

for (const adapter of [kafkajs, platformatic, confluent]) {
  console.log(
    `${adapter.name}: version=${adapter.version} ` +
      `createProducer=${typeof adapter.createProducer} consumeOnce=${typeof adapter.consumeOnce}`
  );
}
console.log('All adapters loaded OK');

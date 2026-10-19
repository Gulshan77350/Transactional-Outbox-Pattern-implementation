/**
 * Moves dead-lettered (FAILED) outbox rows back to PENDING so the relay
 * retries them with a fresh attempt budget.
 *
 *   npm run outbox:requeue-failed                 every FAILED row
 *   npm run outbox:requeue-failed -- <id> <id>    only these event ids
 *
 * Uses MONGO_URI (defaults to the local outbox database).
 */
import { ObjectId } from 'mongodb';
import { loadConfig } from '../src/config.js';
import { connectMongo } from '../src/db/mongo.js';
import { createOutboxRepository } from '../src/outbox/outbox.repository.js';

const ids = process.argv.slice(2);
const invalid = ids.filter((id) => !ObjectId.isValid(id));
if (invalid.length > 0) {
  console.error(`not valid event ids: ${invalid.join(', ')}`);
  process.exit(2);
}

const { client, db } = await connectMongo(loadConfig().mongoUri);
try {
  const repo = createOutboxRepository(db);
  const requeued = await repo.requeueFailed(ids.length ? { ids: ids.map((id) => new ObjectId(id)) } : {});
  console.log(`requeued ${requeued} FAILED event(s) in ${db.databaseName}.outbox`);
  console.table(await repo.countByStatus());
} finally {
  await client.close();
}

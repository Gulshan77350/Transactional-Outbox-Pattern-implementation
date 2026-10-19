import { MongoMemoryReplSet } from 'mongodb-memory-server';

/**
 * One in-memory single-node replica set for the whole run. A replica set
 * (not a standalone mongod) is required, because the code under test uses
 * multi-document transactions. Each test file works in its own database.
 */
export default async function setup({ provide }) {
  const replSet = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: 'wiredTiger' } });
  provide('mongoUri', replSet.getUri());
  return async () => replSet.stop();
}

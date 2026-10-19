import { MongoClient } from 'mongodb';

/**
 * Connects and returns the client plus the database named in the URI path.
 * Transactions need a replica set, so we fail fast if we are talking to a
 * standalone mongod rather than surfacing an obscure error on the first write.
 */
export async function connectMongo(uri, { maxPoolSize = 100 } = {}) {
  const client = new MongoClient(uri, { maxPoolSize });
  await client.connect();

  const hello = await client.db('admin').command({ hello: 1 });
  if (!hello.setName) {
    await client.close();
    throw new Error(
      'MongoDB is not running as a replica set; multi-document transactions are unavailable. ' +
        'Start it with --replSet and run rs.initiate() (docker compose does this for you).',
    );
  }

  return { client, db: client.db() };
}

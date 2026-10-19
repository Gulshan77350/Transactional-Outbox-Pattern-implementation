import http from 'node:http';
import { once } from 'node:events';
import { io as connect } from 'socket.io-client';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { NoSubscribersError, createSocketHub, createSocketTransport } from '../../src/relay/socket-server.js';
import { Protocol } from '../../src/shared/protocol.js';
import { eventually, silentLogger } from '../helpers/db.js';

const envelope = { id: 'evt-1', type: 'OrderCreated', payload: {} };

describe('socket transport', () => {
  let server;
  let io;
  let transport;
  let url;
  const clients = [];

  async function subscriber(onEvent, types = ['OrderCreated']) {
    const socket = connect(url, { transports: ['websocket'] });
    clients.push(socket);
    socket.on(Protocol.EVENT, onEvent);
    await once(socket, 'connect');
    const ack = await socket.emitWithAck(Protocol.SUBSCRIBE, types);
    expect(ack).toEqual({ ok: true, types });
    return socket;
  }

  beforeAll(async () => {
    server = http.createServer();
    io = createSocketHub({ httpServer: server, logger: silentLogger });
    transport = createSocketTransport({ io, ackTimeoutMs: 200 });
    await new Promise((resolve) => server.listen(0, resolve));
    url = `http://localhost:${server.address().port}`;
  });
  afterEach(async () => {
    for (const socket of clients.splice(0)) socket.close();
    await eventually(() => transport.subscriberCount('OrderCreated') === 0);
  });
  afterAll(() => new Promise((resolve) => io.close(() => resolve())));

  it('refuses to report success when nobody is subscribed', async () => {
    await expect(transport.deliver(envelope)).rejects.toBeInstanceOf(NoSubscribersError);
  });

  it('only routes events to sockets subscribed to that type', async () => {
    await subscriber(() => {}, ['SomethingElse']);
    expect(transport.subscriberCount('OrderCreated')).toBe(0);
    await expect(transport.deliver(envelope)).rejects.toBeInstanceOf(NoSubscribersError);
  });

  it('resolves once every subscriber acks', async () => {
    const received = [];
    await subscriber((evt, ack) => {
      received.push(['a', evt.id]);
      ack({ ok: true });
    });
    await subscriber((evt, ack) => {
      received.push(['b', evt.id]);
      ack({ ok: true, duplicate: true });
    });

    const acks = await transport.deliver(envelope);
    expect(acks).toHaveLength(2);
    expect(received.sort()).toEqual([
      ['a', 'evt-1'],
      ['b', 'evt-1'],
    ]);
  });

  it('rejects when a consumer nacks', async () => {
    await subscriber((_evt, ack) => ack({ ok: false, error: 'db down' }));
    await expect(transport.deliver(envelope)).rejects.toThrow('consumer rejected event: db down');
  });

  it('rejects when a consumer does not ack in time', async () => {
    await subscriber(() => {});
    await expect(transport.deliver(envelope)).rejects.toThrow();
  });

  it('accepts any client when no auth token is configured', async () => {
    await subscriber((_evt, ack) => ack({ ok: true }));
    expect(transport.subscriberCount('OrderCreated')).toBe(1);
  });

  it('tolerates a malformed subscribe request', async () => {
    const socket = connect(url, { transports: ['websocket'] });
    clients.push(socket);
    await once(socket, 'connect');
    expect(await socket.emitWithAck(Protocol.SUBSCRIBE, 'not-an-array')).toEqual({ ok: true, types: [] });
    socket.emit(Protocol.SUBSCRIBE, ['OrderCreated']); // no ack callback
    await eventually(() => transport.subscriberCount('OrderCreated') === 1);
  });
});

describe('socket hub authentication', () => {
  let server;
  let io;
  let url;
  const sockets = [];

  beforeAll(async () => {
    server = http.createServer();
    io = createSocketHub({ httpServer: server, logger: silentLogger, authToken: 's3cret' });
    await new Promise((resolve) => server.listen(0, resolve));
    url = `http://localhost:${server.address().port}`;
  });
  afterEach(() => {
    for (const socket of sockets.splice(0)) socket.close();
  });
  afterAll(() => new Promise((resolve) => io.close(() => resolve())));

  const attempt = (auth) => {
    const socket = connect(url, { transports: ['websocket'], reconnection: false, auth });
    sockets.push(socket);
    return new Promise((resolve) => {
      socket.on('connect', () => resolve('connected'));
      socket.on('connect_error', (err) => resolve(err.message));
    });
  };

  it('rejects consumers without the token or with a wrong one', async () => {
    expect(await attempt(undefined)).toBe('unauthorized');
    expect(await attempt({ token: 'wrong' })).toBe('unauthorized');
    expect(await attempt({ token: 's3cret-but-longer' })).toBe('unauthorized');
  });

  it('accepts consumers presenting the token', async () => {
    expect(await attempt({ token: 's3cret' })).toBe('connected');
  });
});

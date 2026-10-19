import { Server } from 'socket.io';
import { Protocol, roomFor } from '../shared/protocol.js';

export class NoSubscribersError extends Error {
  constructor(eventType) {
    super(`no consumers subscribed to ${eventType}`);
    this.name = 'NoSubscribersError';
  }
}

export function createSocketHub({ httpServer, logger }) {
  const io = new Server(httpServer, { serveClient: false });

  io.on('connection', (socket) => {
    logger?.info({ socket: socket.id }, 'consumer connected');

    socket.on(Protocol.SUBSCRIBE, (eventTypes, ack) => {
      const types = Array.isArray(eventTypes) ? eventTypes.filter((t) => typeof t === 'string') : [];
      for (const type of types) socket.join(roomFor(type));
      logger?.info({ socket: socket.id, types }, 'consumer subscribed');
      if (typeof ack === 'function') ack({ ok: true, types });
    });

    socket.on('disconnect', (reason) => logger?.info({ socket: socket.id, reason }, 'consumer disconnected'));
  });

  return io;
}

/**
 * Delivers one envelope to every consumer subscribed to its type and resolves
 * only when all of them have acknowledged it. Any timeout, negative ack, or
 * absence of subscribers rejects, which the relay turns into a retry.
 */
export function createSocketTransport({ io, ackTimeoutMs }) {
  function subscriberCount(eventType) {
    return io.of('/').adapter.rooms.get(roomFor(eventType))?.size ?? 0;
  }

  async function deliver(envelope) {
    // A broadcast to an empty room "succeeds" with zero acks, which would
    // silently mark the event SENT. Treat it as a failed delivery instead.
    if (subscriberCount(envelope.type) === 0) throw new NoSubscribersError(envelope.type);

    const acks = await io.to(roomFor(envelope.type)).timeout(ackTimeoutMs).emitWithAck(Protocol.EVENT, envelope);
    const rejected = acks.find((ack) => !ack || ack.ok !== true);
    if (rejected) throw new Error(`consumer rejected event: ${rejected?.error ?? 'invalid ack'}`);
    return acks;
  }

  return { deliver, subscriberCount };
}

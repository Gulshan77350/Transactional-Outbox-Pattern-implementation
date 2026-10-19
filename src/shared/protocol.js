/** Socket.IO contract shared by the relay (server) and consumers (clients). */
export const Protocol = Object.freeze({
  /** client -> server: subscribe(eventTypes: string[], ack) */
  SUBSCRIBE: 'subscribe',
  /** server -> client: event(envelope, ack({ ok, duplicate?, error? })) */
  EVENT: 'outbox:event',
});

export const roomFor = (eventType) => `event:${eventType}`;

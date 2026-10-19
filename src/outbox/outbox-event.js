import { ObjectId } from 'mongodb';
import { OutboxStatus } from './event-types.js';

/** Builds a new outbox row. It is only ever inserted inside the producer's transaction. */
export function buildOutboxEvent({ eventType, aggregateType, aggregateId, payload, now = new Date() }) {
  return {
    _id: new ObjectId(),
    event_type: eventType,
    aggregate_type: aggregateType,
    aggregate_id: aggregateId,
    payload,
    status: OutboxStatus.PENDING,
    attempts: 0,
    claims: 0,
    next_attempt_at: now,
    locked_by: null,
    locked_until: null,
    last_error: null,
    created_at: now,
    sent_at: null,
  };
}

/** Wire format sent to consumers. The outbox _id doubles as the idempotency key. */
export function toEnvelope(row) {
  return {
    id: row._id.toHexString(),
    type: row.event_type,
    aggregate_type: row.aggregate_type,
    aggregate_id: String(row.aggregate_id),
    payload: row.payload,
    created_at: row.created_at.toISOString(),
    delivery_attempt: row.attempts + 1,
  };
}

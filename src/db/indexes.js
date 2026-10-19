export async function ensureOutboxIndexes(db) {
  await db.collection('outbox').createIndexes([
    // Serves the claim query: PENDING rows that are due, oldest first.
    // ESR order (equality, sort, range) so the oldest due row is found by
    // walking the index instead of sorting every due row in memory.
    { key: { status: 1, created_at: 1, next_attempt_at: 1 }, name: 'claim_due_pending' },
    // Serves lease recovery: PROCESSING rows whose lease has expired.
    { key: { status: 1, locked_until: 1 }, name: 'expired_leases' },
  ]);
}

export async function ensureOrderIndexes(db) {
  await db.collection('orders').createIndexes([
    { key: { customer_id: 1, created_at: -1 }, name: 'customer_orders' },
  ]);
}

export async function ensureConsumerIndexes(db) {
  await db.collection('billing_notifications').createIndexes([
    // Second line of defence behind processed_events: one notification per event.
    { key: { event_id: 1 }, name: 'one_notification_per_event', unique: true },
  ]);
}

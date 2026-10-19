export const EventTypes = Object.freeze({
  ORDER_CREATED: 'OrderCreated',
});

export const OutboxStatus = Object.freeze({
  PENDING: 'PENDING',
  PROCESSING: 'PROCESSING',
  SENT: 'SENT',
  FAILED: 'FAILED',
});

export class ValidationError extends Error {
  constructor(details) {
    super('Invalid order');
    this.name = 'ValidationError';
    this.details = details;
  }
}

const MAX_ITEMS = 50;

/**
 * Validates and normalises a create-order request body.
 * Money is kept in integer minor units (cents) to avoid floating point drift.
 */
export function parseCreateOrder(body) {
  const errors = [];
  const input = body && typeof body === 'object' ? body : {};

  if (typeof input.customer_id !== 'string' || input.customer_id.trim() === '') {
    errors.push('customer_id must be a non-empty string');
  }

  const currency = input.currency ?? 'USD';
  if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)) {
    errors.push('currency must be a 3-letter ISO code');
  }

  if (!Array.isArray(input.items) || input.items.length === 0 || input.items.length > MAX_ITEMS) {
    errors.push(`items must be an array of 1-${MAX_ITEMS} entries`);
  } else {
    input.items.forEach((item, i) => {
      if (!item || typeof item.sku !== 'string' || item.sku === '') {
        errors.push(`items[${i}].sku must be a non-empty string`);
      }
      if (!Number.isInteger(item?.quantity) || item.quantity <= 0) {
        errors.push(`items[${i}].quantity must be a positive integer`);
      }
      if (!Number.isInteger(item?.unit_price_cents) || item.unit_price_cents < 0) {
        errors.push(`items[${i}].unit_price_cents must be a non-negative integer`);
      }
    });
  }

  if (errors.length > 0) throw new ValidationError(errors);

  const items = input.items.map(({ sku, quantity, unit_price_cents }) => ({ sku, quantity, unit_price_cents }));
  return {
    customer_id: input.customer_id.trim(),
    currency,
    items,
    total_cents: items.reduce((sum, item) => sum + item.quantity * item.unit_price_cents, 0),
  };
}

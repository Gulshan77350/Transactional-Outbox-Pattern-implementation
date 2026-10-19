import { describe, expect, it } from 'vitest';
import { ValidationError, parseCreateOrder } from '../../src/orders/order.validation.js';
import { orderBody } from '../helpers/db.js';

describe('parseCreateOrder', () => {
  it('normalises a valid order and computes the total in cents', () => {
    const parsed = parseCreateOrder(orderBody({ customer_id: '  c-42 ' }));
    expect(parsed).toEqual({
      customer_id: 'c-42',
      currency: 'USD',
      items: [
        { sku: 'SKU-1', quantity: 2, unit_price_cents: 1250 },
        { sku: 'SKU-2', quantity: 1, unit_price_cents: 499 },
      ],
      total_cents: 2999,
    });
  });

  it('defaults the currency to USD and drops unknown item fields', () => {
    const parsed = parseCreateOrder({ customer_id: 'c', items: [{ sku: 'A', quantity: 1, unit_price_cents: 5, hack: true }] });
    expect(parsed.currency).toBe('USD');
    expect(parsed.items[0]).toEqual({ sku: 'A', quantity: 1, unit_price_cents: 5 });
  });

  it.each([
    ['a missing body', undefined, 'customer_id must be a non-empty string'],
    ['a blank customer', orderBody({ customer_id: '   ' }), 'customer_id must be a non-empty string'],
    ['a bad currency', orderBody({ currency: 'usd' }), 'currency must be a 3-letter ISO code'],
    ['no items', orderBody({ items: [] }), 'items must be an array of 1-50 entries'],
    ['too many items', orderBody({ items: Array(51).fill({ sku: 'A', quantity: 1, unit_price_cents: 1 }) }), 'items must be an array of 1-50 entries'],
    ['a missing sku', orderBody({ items: [{ quantity: 1, unit_price_cents: 1 }] }), 'items[0].sku must be a non-empty string'],
    ['a fractional quantity', orderBody({ items: [{ sku: 'A', quantity: 1.5, unit_price_cents: 1 }] }), 'items[0].quantity must be a positive integer'],
    ['a negative price', orderBody({ items: [{ sku: 'A', quantity: 1, unit_price_cents: -1 }] }), 'items[0].unit_price_cents must be a non-negative integer'],
    ['a null item', orderBody({ items: [null] }), 'items[0].sku must be a non-empty string'],
  ])('rejects %s', (_label, body, message) => {
    try {
      parseCreateOrder(body);
      expect.unreachable('expected a ValidationError');
    } catch (err) {
      expect(err).toBeInstanceOf(ValidationError);
      expect(err.details).toContain(message);
    }
  });
});

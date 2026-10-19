import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createApp } from '../../src/app.js';
import { createOrderService } from '../../src/orders/order.service.js';
import { openTestDb, orderBody, silentLogger } from '../helpers/db.js';

async function listen(app) {
  const server = app.listen(0);
  await new Promise((resolve) => server.once('listening', resolve));
  return { server, base: `http://localhost:${server.address().port}` };
}

const post = (base, body, raw = false) =>
  fetch(`${base}/orders`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: raw ? body : JSON.stringify(body) });

describe('HTTP API', () => {
  let ctx;
  let server;
  let base;

  beforeAll(async () => {
    ctx = await openTestDb('api');
    ({ server, base } = await listen(createApp({ orderService: createOrderService(ctx), logger: silentLogger })));
  });
  beforeEach(() => ctx.reset());
  afterAll(async () => {
    await new Promise((resolve) => server.close(resolve));
    await ctx.close();
  });

  it('GET /health', async () => {
    const res = await fetch(`${base}/health`);
    expect(await res.json()).toEqual({ status: 'ok' });
  });

  it('POST /orders creates the order and returns the outbox event id', async () => {
    const res = await post(base, orderBody());
    expect(res.status).toBe(201);
    const body = await res.json();

    expect(body.order).toMatchObject({ customer_id: 'customer-1', total_cents: 2999, status: 'CREATED' });
    expect(body.order._id).toBeUndefined();
    expect(body.event_id).toMatch(/^[0-9a-f]{24}$/);
    expect(await ctx.db.collection('outbox').countDocuments({ status: 'PENDING' })).toBe(1);

    const fetched = await fetch(`${base}/orders/${body.order.id}`);
    expect((await fetched.json()).order.id).toBe(body.order.id);
  });

  it('POST /orders rejects invalid input with details', async () => {
    const res = await post(base, { customer_id: 'c', items: [] });
    expect(res.status).toBe(400);
    expect((await res.json()).details).toContain('items must be an array of 1-50 entries');
  });

  it('POST /orders rejects malformed JSON', async () => {
    const res = await post(base, '{"customer_id":', true);
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: 'malformed JSON body' });
  });

  it('returns 404 for unknown orders and routes', async () => {
    expect((await fetch(`${base}/orders/0123456789abcdef01234567`)).status).toBe(404);
    expect((await fetch(`${base}/nope`)).status).toBe(404);
  });
});

describe('HTTP API error handling', () => {
  it('returns 500 and logs when the service fails unexpectedly', async () => {
    const logger = { error: vi.fn() };
    const orderService = { createOrder: vi.fn().mockRejectedValue(new Error('mongo down')) };
    const { server, base } = await listen(createApp({ orderService, logger }));

    const res = await post(base, orderBody());
    expect(res.status).toBe(500);
    expect(await res.json()).toEqual({ error: 'internal error' });
    expect(logger.error).toHaveBeenCalled();
    await new Promise((resolve) => server.close(resolve));
  });
});

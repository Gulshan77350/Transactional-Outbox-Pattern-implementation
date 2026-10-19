import { Router } from 'express';

function serializeOrder(order) {
  return { ...order, _id: undefined, id: order._id.toHexString() };
}

export function orderRoutes(orderService) {
  const router = Router();

  router.post('/', async (req, res) => {
    const { order, eventId, committedAtMs, txnMs } = await orderService.createOrder(req.body);
    res.status(201).json({
      order: serializeOrder(order),
      event_id: eventId.toHexString(),
      committed_at_ms: committedAtMs,
      txn_ms: txnMs,
    });
  });

  router.get('/:id', async (req, res) => {
    const order = await orderService.getOrder(req.params.id);
    if (!order) return res.status(404).json({ error: 'order not found' });
    res.json({ order: serializeOrder(order) });
  });

  return router;
}

import express from 'express';
import { orderRoutes } from './orders/order.routes.js';
import { ValidationError } from './orders/order.validation.js';

export function createApp({ orderService, logger }) {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '100kb' }));

  app.get('/health', (_req, res) => res.json({ status: 'ok' }));
  app.use('/orders', orderRoutes(orderService));

  app.use((_req, res) => res.status(404).json({ error: 'not found' }));

  // eslint-disable-next-line no-unused-vars
  app.use((err, _req, res, _next) => {
    if (err instanceof ValidationError) {
      return res.status(400).json({ error: err.message, details: err.details });
    }
    if (err.type === 'entity.parse.failed') {
      return res.status(400).json({ error: 'malformed JSON body' });
    }
    logger?.error({ err }, 'request failed');
    res.status(500).json({ error: 'internal error' });
  });

  return app;
}

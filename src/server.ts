import { Hono } from '@x/hono';

export function serveHono() {
  const app = new Hono();

  app.get('/', (c) => c.text('Hono!'));

  Deno.serve(app.fire);

  return app;
}

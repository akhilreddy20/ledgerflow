import { IdempotencyStore } from '../src/idempotency';

/** Minimal in-memory stand-in for the three Redis commands the store uses. */
class FakeRedis {
  data = new Map<string, string>();
  async set(k: string, v: string, _ex: string, _ttl: number, nx?: string) {
    if (nx === 'NX' && this.data.has(k)) return null;
    this.data.set(k, v);
    return 'OK';
  }
  async get(k: string) {
    return this.data.get(k) ?? null;
  }
  async del(k: string) {
    this.data.delete(k);
    return 1;
  }
}

describe('IdempotencyStore', () => {
  const store = () => new IdempotencyStore(new FakeRedis() as any, 'test');

  it('lets the first request through and blocks a concurrent duplicate', async () => {
    const s = store();
    expect(await s.begin('key-1', 'fp')).toEqual({ kind: 'new' });
    expect(await s.begin('key-1', 'fp')).toEqual({ kind: 'in_progress' });
  });

  it('replays the stored response after completion', async () => {
    const s = store();
    await s.begin('key-1', 'fp');
    await s.complete('key-1', 'fp', { status: 202, body: { id: 'p1' } });
    expect(await s.begin('key-1', 'fp')).toEqual({ kind: 'replay', response: { status: 202, body: { id: 'p1' } } });
  });

  it('rejects a key reused with a different body', async () => {
    const s = store();
    await s.begin('key-1', 'fp-a');
    await s.complete('key-1', 'fp-a', { status: 201, body: {} });
    expect(await s.begin('key-1', 'fp-b')).toEqual({ kind: 'mismatch' });
  });

  it('releases the key after a server error so the client can retry', async () => {
    const s = store();
    await s.begin('key-1', 'fp');
    await s.abort('key-1');
    expect(await s.begin('key-1', 'fp')).toEqual({ kind: 'new' });
  });
});

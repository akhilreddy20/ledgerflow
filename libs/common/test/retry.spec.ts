import { BusinessError, TransientError } from '../src/errors';
import { backoffDelay, retryWithBackoff } from '../src/retry';

describe('backoffDelay (full jitter)', () => {
  it('grows exponentially and is capped', () => {
    expect(backoffDelay(0, 100, 1000, () => 0.999)).toBe(99);
    expect(backoffDelay(3, 100, 1000, () => 0.999)).toBe(799);
    expect(backoffDelay(10, 100, 1000, () => 0.999)).toBe(999);
    expect(backoffDelay(5, 100, 1000, () => 0)).toBe(0);
  });
});

describe('retryWithBackoff', () => {
  const noSleep = { sleep: async () => undefined };

  it('retries transient errors until success', async () => {
    let calls = 0;
    const result = await retryWithBackoff(
      async () => {
        if (++calls < 3) throw new TransientError('flaky');
        return 'done';
      },
      { attempts: 5, baseDelayMs: 1, maxDelayMs: 1, ...noSleep },
    );
    expect(result).toBe('done');
    expect(calls).toBe(3);
  });

  it('never retries business errors', async () => {
    let calls = 0;
    await expect(
      retryWithBackoff(
        async () => {
          calls++;
          throw new BusinessError('INSUFFICIENT_FUNDS', 'no money');
        },
        { attempts: 5, baseDelayMs: 1, maxDelayMs: 1, ...noSleep },
      ),
    ).rejects.toThrow('no money');
    expect(calls).toBe(1);
  });

  it('gives up after the configured attempts', async () => {
    let calls = 0;
    await expect(
      retryWithBackoff(
        async () => {
          calls++;
          throw new TransientError('down');
        },
        { attempts: 4, baseDelayMs: 1, maxDelayMs: 1, ...noSleep },
      ),
    ).rejects.toThrow('down');
    expect(calls).toBe(4);
  });
});

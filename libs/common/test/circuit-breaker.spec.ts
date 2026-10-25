import { CircuitBreaker, CircuitOpenError } from '../src/circuit-breaker';

describe('CircuitBreaker', () => {
  let now = 0;
  const make = () =>
    new CircuitBreaker({
      name: 'test',
      failureThreshold: 3,
      windowMs: 1000,
      openDurationMs: 5000,
      halfOpenMaxCalls: 1,
      isFailure: (e) => !(e instanceof SyntaxError), // pretend SyntaxError is a "business" outcome
      now: () => now,
    });
  const fail = () => Promise.reject(new Error('boom'));
  const ok = () => Promise.resolve('ok');

  beforeEach(() => (now = 0));

  it('opens after the failure threshold and fails fast', async () => {
    const cb = make();
    for (let i = 0; i < 3; i++) await expect(cb.execute(fail)).rejects.toThrow('boom');
    expect(cb.currentState).toBe('OPEN');
    const spy = jest.fn(ok);
    await expect(cb.execute(spy)).rejects.toBeInstanceOf(CircuitOpenError);
    expect(spy).not.toHaveBeenCalled();
  });

  it('only counts failures inside the rolling window', async () => {
    const cb = make();
    await expect(cb.execute(fail)).rejects.toThrow();
    await expect(cb.execute(fail)).rejects.toThrow();
    now = 2000; // first two failures fall out of the window
    await expect(cb.execute(fail)).rejects.toThrow();
    expect(cb.currentState).toBe('CLOSED');
  });

  it('half-opens after the open duration and closes on a successful probe', async () => {
    const cb = make();
    for (let i = 0; i < 3; i++) await expect(cb.execute(fail)).rejects.toThrow();
    now = 5000;
    expect(cb.currentState).toBe('HALF_OPEN');
    await expect(cb.execute(ok)).resolves.toBe('ok');
    expect(cb.currentState).toBe('CLOSED');
  });

  it('re-opens when the half-open probe fails', async () => {
    const cb = make();
    for (let i = 0; i < 3; i++) await expect(cb.execute(fail)).rejects.toThrow();
    now = 5000;
    await expect(cb.execute(fail)).rejects.toThrow('boom');
    expect(cb.currentState).toBe('OPEN');
  });

  it('does not count business errors as failures', async () => {
    const cb = make();
    for (let i = 0; i < 5; i++) await expect(cb.execute(() => Promise.reject(new SyntaxError('declined')))).rejects.toThrow();
    expect(cb.currentState).toBe('CLOSED');
  });
});

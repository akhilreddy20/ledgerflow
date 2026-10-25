/** Dependency-injection tokens shared by every service. */
export const PG_POOL = Symbol('PG_POOL');
export const REDIS = Symbol('REDIS');
export const KAFKA = Symbol('KAFKA');
export const PRODUCER = Symbol('PRODUCER');
export const SERVICE_NAME = Symbol('SERVICE_NAME');

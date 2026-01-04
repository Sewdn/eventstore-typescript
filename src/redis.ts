// Redis distribution entry point
// Re-exports core functionality plus Redis store

// Export core functionality explicitly to avoid conflicts
export { MemoryEventStore } from './eventstore/stores/memory';
export { MemoryEventStreamNotifier } from './eventstore/notifiers';
export { createFilter, createQuery } from './eventstore/filter';
export * from './eventstore/types';

// Export Redis-specific stores and notifiers
export { RedisEventStore, RedisEventStoreOptions } from './eventstore/stores/redis';
export { RedisPubSubNotifier, RedisPubSubNotifierOptions } from './eventstore/notifiers/redis';



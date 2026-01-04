// Redis distribution entry point
// Re-exports core functionality plus Redis store

export * from './index'; // Core functionality
export { RedisEventStore, RedisEventStoreOptions } from './eventstore/stores/redis';



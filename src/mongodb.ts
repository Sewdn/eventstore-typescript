// MongoDB distribution entry point
// Re-exports core functionality plus MongoDB store

export * from './index'; // Core functionality
export { MongoEventStore, MongoEventStoreOptions } from './eventstore/stores/mongodb';



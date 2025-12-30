import {
  Event,
  EventStore,
  QueryResult,
  EventStreamNotifier,
  HandleEvents,
  EventSubscription,
  EventQuery,
  EventFilter,
} from '../../types';
import { compileQueryFilter, getEventTypesToQuery } from './query';
import {
  mapDocumentsToEvents,
  extractMaxSequenceNumber,
  prepareEventForStorage,
  EventDocument,
} from './transform';
import {
  SEQUENCE_COUNTER_KEY,
  getEventKey,
  getEventTypeIndexKey,
  parseSequenceNumberFromKey,
  getDatabaseNameFromConnectionString,
} from './schema';
import { createFilter, createQuery } from '../../filter';
import { MemoryEventStreamNotifier } from '../../notifiers';

import { createClient } from 'redis';

const NON_EXISTENT_EVENT_TYPE = '__NON_EXISTENT__' + Math.random().toString(36);

export interface RedisEventStoreOptions {
  connectionString?: string;
  database?: number;
  notifier?: EventStreamNotifier;
}

/**
 * Represents an implementation of an event store using Redis as the underlying database.
 * Provides functionality to append events, query events, and manage database initialization.
 * Additionally, it facilitates event subscriptions through an event stream notifier mechanism.
 */
export class RedisEventStore implements EventStore {
  private client: RedisClientType;
  private readonly database: number;
  private readonly notifier: EventStreamNotifier;

  constructor(options: RedisEventStoreOptions = {}) {
    const connectionString = options.connectionString || process.env.REDIS_URL || process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error(
        'eventstore-stores-redis-err02: Connection string missing. REDIS_URL or DATABASE_URL environment variable not set.'
      );
    }

    // Get database number from options, connection string, or default to 0
    this.database = options.database ?? getDatabaseNameFromConnectionString(connectionString);

    this.client = createClient({
      url: connectionString,
      database: this.database,
    });

    // This is the "Default" EventStreamNotifier, but allow override
    this.notifier = options.notifier ?? new MemoryEventStreamNotifier();
  }

  async query(filterCriteria: EventQuery): Promise<QueryResult>;
  async query(filterCriteria: EventFilter): Promise<QueryResult>;
  async query(filterCriteria: EventQuery | EventFilter): Promise<QueryResult> {
    try {
      // Ensure client is connected
      if (!this.client.isOpen) {
        await this.client.connect();
      }

      // If it's an EventFilter, wrap it in an EventQuery
      const eventQuery =
        'filters' in filterCriteria
          ? (filterCriteria as EventQuery)
          : createQuery(filterCriteria as EventFilter);

      // Get event types to query (for optimization)
      const eventTypesToQuery = getEventTypesToQuery(eventQuery);
      const filterFn = compileQueryFilter(eventQuery);

      // Fetch events
      const allEvents: EventDocument[] = [];

      if (eventTypesToQuery && eventTypesToQuery.length > 0) {
        // Query specific event types using index
        for (const eventType of eventTypesToQuery) {
          const indexKey = getEventTypeIndexKey(eventType);
          const sequenceNumbers = await this.client.sMembers(indexKey);
          
          for (const seqStr of sequenceNumbers) {
            const seqNum = parseInt(seqStr, 10);
            if (isNaN(seqNum)) continue;
            
            const eventKey = getEventKey(seqNum);
            const eventData = await this.client.get(eventKey);
            if (eventData) {
              const doc = JSON.parse(eventData) as EventDocument;
              if (filterFn(doc)) {
                allEvents.push(doc);
              }
            }
          }
        }
      } else {
        // Query all events
        const keys = await this.client.keys('eventstore:events:*');
        for (const key of keys) {
          const eventData = await this.client.get(key);
          if (eventData) {
            const doc = JSON.parse(eventData) as EventDocument;
            if (filterFn(doc)) {
              allEvents.push(doc);
            }
          }
        }
      }

      // Sort by sequence number
      allEvents.sort((a, b) => a.sequence_number - b.sequence_number);

      return {
        events: mapDocumentsToEvents(allEvents),
        maxSequenceNumber: extractMaxSequenceNumber(allEvents),
      };
    } catch (error) {
      throw new Error(`eventstore-stores-redis-err04: Query failed: ${error}`);
    }
  }

  async subscribe(handle: HandleEvents): Promise<EventSubscription> {
    return this.notifier.subscribe(handle);
  }

  async append(events: Event[]): Promise<void>;
  async append(events: Event[], filterCriteria: EventQuery, expectedMaxSequenceNumber: number): Promise<void>;
  async append(events: Event[], filterCriteria: EventFilter, expectedMaxSequenceNumber: number): Promise<void>;
  async append(
    events: Event[],
    filterCriteria?: EventQuery | EventFilter,
    expectedMaxSequenceNumber?: number
  ): Promise<void> {
    if (events.length === 0) return;

    // Ensure client is connected
    if (!this.client.isOpen) {
      await this.client.connect();
    }

    // Convert EventFilter to EventQuery if needed
    let eventQuery: EventQuery;
    if (filterCriteria === undefined) {
      eventQuery = createQuery(createFilter([NON_EXISTENT_EVENT_TYPE]));
      expectedMaxSequenceNumber = 0;
    } else if ('filters' in filterCriteria) {
      // It's an EventQuery
      eventQuery = filterCriteria;
      if (eventQuery.filters.length === 0) {
        eventQuery = createQuery(createFilter([NON_EXISTENT_EVENT_TYPE]));
        expectedMaxSequenceNumber = 0;
      }
    } else {
      // It's an EventFilter, wrap it in EventQuery
      eventQuery = createQuery(filterCriteria);
    }

    if (expectedMaxSequenceNumber === undefined) {
      throw new Error(
        'eventstore-stores-redis-err05: Expected max sequence number is required when a filter is provided!'
      );
    }

    try {
      // Use Redis transaction (MULTI/EXEC) for atomicity
      const multi = this.client.multi();

      // Get the current max sequence number for the context
      const filterFn = compileQueryFilter(eventQuery);
      const eventTypesToQuery = getEventTypesToQuery(eventQuery);

      let contextMaxSeq = 0;
      if (eventTypesToQuery && eventTypesToQuery.length > 0) {
        // Check max sequence for specific event types
        for (const eventType of eventTypesToQuery) {
          const indexKey = getEventTypeIndexKey(eventType);
          const sequenceNumbers = await this.client.sMembers(indexKey);
          
          for (const seqStr of sequenceNumbers) {
            const seqNum = parseInt(seqStr, 10);
            if (seqNum > contextMaxSeq) {
              const eventKey = getEventKey(seqNum);
              const eventData = await this.client.get(eventKey);
              if (eventData) {
                const doc = JSON.parse(eventData) as EventDocument;
                if (filterFn(doc)) {
                  contextMaxSeq = Math.max(contextMaxSeq, seqNum);
                }
              }
            }
          }
        }
      } else {
        // Check all events
        const keys = await this.client.keys('eventstore:events:*');
        for (const key of keys) {
          const seqNum = parseSequenceNumberFromKey(key);
          if (seqNum !== null && seqNum > contextMaxSeq) {
            const eventData = await this.client.get(key);
            if (eventData) {
              const doc = JSON.parse(eventData) as EventDocument;
              if (filterFn(doc)) {
                contextMaxSeq = Math.max(contextMaxSeq, seqNum);
              }
            }
          }
        }
      }

      // Verify optimistic locking
      if (contextMaxSeq !== expectedMaxSequenceNumber) {
        throw new Error(
          'eventstore-stores-redis-err06: Context changed: events were modified between query() and append()'
        );
      }

      // Get the next sequence number atomically
      const nextSequenceNumber = await this.client.incrBy(SEQUENCE_COUNTER_KEY, events.length);
      const startSequenceNumber = nextSequenceNumber - events.length + 1;

      // Prepare events for storage
      const now = new Date();
      const documentsToStore: EventDocument[] = [];

      for (let i = 0; i < events.length; i++) {
        const event = events[i];
        if (!event) continue; // Skip if undefined (shouldn't happen, but TypeScript safety)
        
        const sequenceNumber = startSequenceNumber + i;
        const doc = prepareEventForStorage(event, sequenceNumber, now);
        documentsToStore.push(doc);

        const eventKey = getEventKey(sequenceNumber);
        const indexKey = getEventTypeIndexKey(doc.event_type);

        // Store event
        multi.set(eventKey, JSON.stringify(doc));
        // Add to event type index
        multi.sAdd(indexKey, sequenceNumber.toString());
      }

      // Execute transaction
      await multi.exec();

      // Convert stored documents to EventRecord[] and notify subscribers
      const insertedEvents = mapDocumentsToEvents(documentsToStore);
      await this.notifier.notify(insertedEvents);
    } catch (error) {
      if (error instanceof Error && error.message.includes('Context changed')) {
        throw error;
      }
      throw new Error(`eventstore-stores-redis-err08: Append failed: ${error}`);
    }
  }

  async initializeDatabase(): Promise<void> {
    // Redis doesn't require explicit database creation
    // Just ensure connection is established
    if (!this.client.isOpen) {
      await this.client.connect();
    }
    
    // Initialize sequence counter if it doesn't exist
    const exists = await this.client.exists(SEQUENCE_COUNTER_KEY);
    if (!exists) {
      await this.client.set(SEQUENCE_COUNTER_KEY, '0');
    }
  }

  async close(): Promise<void> {
    await this.notifier.close();
    if (this.client.isOpen) {
      await this.client.quit();
    }
  }
}


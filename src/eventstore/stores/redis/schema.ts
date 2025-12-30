/**
 * Redis key patterns and schema definitions for event storage
 */

export const SEQUENCE_COUNTER_KEY = 'eventstore:sequence:counter';
export const EVENTS_KEY_PREFIX = 'eventstore:events:';
export const EVENT_KEY_PATTERN = `${EVENTS_KEY_PREFIX}*`;
export const EVENT_INDEX_TYPE_KEY = 'eventstore:index:type:';
export const EVENT_INDEX_TYPE_PATTERN = `${EVENT_INDEX_TYPE_KEY}*`;

/**
 * Gets the Redis key for a specific event by sequence number
 */
export function getEventKey(sequenceNumber: number): string {
  return `${EVENTS_KEY_PREFIX}${sequenceNumber}`;
}

/**
 * Gets the Redis key for event type index
 */
export function getEventTypeIndexKey(eventType: string): string {
  return `${EVENT_INDEX_TYPE_KEY}${eventType}`;
}

/**
 * Parses sequence number from event key
 */
export function parseSequenceNumberFromKey(key: string): number | null {
  const match = key.match(/^eventstore:events:(\d+)$/);
  return match ? parseInt(match[1], 10) : null;
}

/**
 * Gets database number from Redis connection string/URL
 */
export function getDatabaseNameFromConnectionString(connectionString: string): number {
  try {
    // Handle both URL format (redis://host:port?db=0) and simple format
    if (connectionString.includes('?')) {
      const url = new URL(connectionString);
      const dbParam = url.searchParams.get('db');
      return dbParam ? parseInt(dbParam, 10) : 0;
    }
    // If not a URL format, assume default database 0
    return 0;
  } catch (err) {
    // If not a URL, assume default database 0
    return 0;
  }
}


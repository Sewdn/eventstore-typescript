import { EventRecord, Event } from '../../types';

/**
 * Redis Stream entry structure returned by XRANGE
 */
export interface StreamEntry {
  id: string; // Stream ID in format "timestamp-sequence"
  message: Record<string, string>; // Field-value pairs
}

/**
 * Event document extracted from Redis Stream entry
 */
export interface EventDocument {
  streamId: string;
  sequence_number: number;
  occurred_at: string; // ISO date string
  event_type: string;
  payload: Record<string, unknown>;
}

/**
 * Converts a Redis Stream entry to an EventDocument
 */
export function streamEntryToEventDocument(entry: StreamEntry): EventDocument {
  const message = entry.message;
  
  return {
    streamId: entry.id,
    sequence_number: parseInt(message.sequence_number || '0', 10),
    occurred_at: message.occurred_at || new Date().toISOString(),
    event_type: message.event_type || '',
    payload: message.payload ? JSON.parse(message.payload) : {},
  };
}

/**
 * Converts a Redis-stored event document to an EventRecord
 */
export function deserializeEvent(doc: EventDocument): EventRecord {
  return {
    sequenceNumber: doc.sequence_number,
    timestamp: new Date(doc.occurred_at),
    eventType: doc.event_type,
    payload: doc.payload,
  };
}

/**
 * Converts Redis Stream entries to EventRecord array
 */
export function mapStreamEntriesToEvents(entries: StreamEntry[]): EventRecord[] {
  const docs = entries.map(streamEntryToEventDocument);
  return docs.map((doc) => deserializeEvent(doc));
}

/**
 * Converts EventDocuments to EventRecord array (for backward compatibility)
 */
export function mapDocumentsToEvents(docs: EventDocument[]): EventRecord[] {
  return docs.map((doc) => deserializeEvent(doc));
}

/**
 * Extracts the maximum sequence number from query results
 */
export function extractMaxSequenceNumber(docs: EventDocument[]): number {
  if (docs.length === 0) {
    return 0;
  }
  
  // Documents should be sorted by sequence_number ascending
  const lastDoc = docs[docs.length - 1];
  return lastDoc?.sequence_number || 0;
}

/**
 * Prepares events for Redis Stream storage as field-value pairs for XADD
 * Returns an object with field-value pairs (all values must be strings)
 */
export function prepareEventForStreamStorage(
  event: Event,
  sequenceNumber: number,
  occurredAt: Date
): Record<string, string> {
  return {
    sequence_number: sequenceNumber.toString(),
    occurred_at: occurredAt.toISOString(),
    event_type: event.eventType,
    payload: JSON.stringify(event.payload),
  };
}

/**
 * Prepares events for Redis storage (legacy function for backward compatibility)
 */
export function prepareEventForStorage(event: Event, sequenceNumber: number, occurredAt: Date): EventDocument {
  return {
    streamId: '', // Not used in legacy mode
    sequence_number: sequenceNumber,
    occurred_at: occurredAt.toISOString(),
    event_type: event.eventType,
    payload: event.payload,
  };
}


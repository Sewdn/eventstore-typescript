import { EventRecord, Event } from '../../types';

export interface EventDocument {
  sequence_number: number;
  occurred_at: string; // ISO date string
  event_type: string;
  payload: Record<string, unknown>;
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
 * Converts Redis documents to EventRecord array
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
 * Prepares events for Redis storage
 */
export function prepareEventForStorage(event: Event, sequenceNumber: number, occurredAt: Date): EventDocument {
  return {
    sequence_number: sequenceNumber,
    occurred_at: occurredAt.toISOString(),
    event_type: event.eventType,
    payload: event.payload,
  };
}


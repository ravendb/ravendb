package net.ravendb.client.listeners;

import net.ravendb.abstractions.json.linq.RavenJObject;

/**
 * Hook for users to provide additional logic for converting to / from
 * entities to document/metadata pairs.
 */
public interface IDocumentConversionListener {

  /**
   * Called when converting an entity to a document and metadata
   * @param key
   * @param entity
   * @param document
   * @param metadata
   */
  void entityToDocument(String key, Object entity, RavenJObject document, RavenJObject metadata);

  /**
   * Called when converting a document and metadata to an entity
   * @param key
   * @param entity
   * @param document
   * @param metadata
   */
  void documentToEntity(String key, Object entity, RavenJObject document, RavenJObject metadata);
}

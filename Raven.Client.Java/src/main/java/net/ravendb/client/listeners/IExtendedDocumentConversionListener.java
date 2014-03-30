package net.ravendb.client.listeners;

import net.ravendb.abstractions.json.linq.RavenJObject;

/**
 *  Extended hook for users to provide additional logic for converting to / from
 * entities to document/metadata pairs.
 */
public interface IExtendedDocumentConversionListener {
  /**
   * Called before converting an entity to a document and metadata
   * @param key
   * @param entity
   * @param metadata
   */
  void beforeConversionToDocument(String key, Object entity, RavenJObject metadata);

  /**
   * Called after having converted an entity to a document and metadata
   * @param key
   * @param entity
   * @param document
   * @param metadata
   */
  void afterConversionToDocument(String key, Object entity, RavenJObject document, RavenJObject metadata);

  /**
   * Called before converting a document and metadata to an entity
   * @param key
   * @param document
   * @param metadata
   */
  void beforeConversionToEntity(String key, RavenJObject document, RavenJObject metadata);

  /**
   * Called after having converted a document and metadata to an entity
   * @param key
   * @param document
   * @param metadata
   * @param entity
   */
  void afterConversionToEntity(String key, RavenJObject document, RavenJObject metadata, Object entity);
}

package net.ravendb.client.listeners;

import net.ravendb.abstractions.json.linq.RavenJObject;

/**
 * Hook for users to provide additional logic on store operations
 */
public interface IDocumentStoreListener {
  /**
   * Invoked before the store request is sent to the server.
   * @param key The key.
   * @param entityInstance The entity instance.
   * @param metadata The metadata.
   * @param original The original document that was loaded from the server
   * @return
   *
   * Whatever the entity instance was modified and requires us re-serialize it.
   * Returning true would force re-serialization of the entity, returning false would
   * mean that any changes to the entityInstance would be ignored in the current SaveChanges call.
   */
  boolean beforeStore(String key, Object entityInstance, RavenJObject metadata, RavenJObject original);

  /**
   * Invoked after the store request is sent to the server.
   * @param key The key.
   * @param entityInstance The entity instance.
   * @param metadata The metadata
   */
  void afterStore(String key, Object entityInstance, RavenJObject metadata);
}

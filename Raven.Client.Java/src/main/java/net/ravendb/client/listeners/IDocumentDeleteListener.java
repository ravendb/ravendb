package net.ravendb.client.listeners;

import net.ravendb.abstractions.json.linq.RavenJObject;

/**
 * Hook for users to provide additional logic on delete operations
 */
public interface IDocumentDeleteListener {

  /**
   * Invoked before the delete request is sent to the server.
   * @param key The key.
   * @param entityInstance The entity instance.
   * @param metadata The metadata.
   */
  public void beforeDelete(String key, Object entityInstance, RavenJObject metadata);
}

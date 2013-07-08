package raven.client.listeners;

import java.util.List;

import raven.abstractions.basic.Holder;
import raven.abstractions.data.JsonDocument;

/**
 * Hooks for users that allows you to handle document replication conflicts
 */
public interface IDocumentConflictListener {

  public boolean tryResolveConflict(String key, List<JsonDocument> results, Holder<JsonDocument> resolvedDocument);
}

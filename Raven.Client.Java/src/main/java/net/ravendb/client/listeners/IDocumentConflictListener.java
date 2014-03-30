package net.ravendb.client.listeners;

import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.JsonDocument;


/**
 * Hooks for users that allows you to handle document replication conflicts
 */
public interface IDocumentConflictListener {

  public boolean tryResolveConflict(String key, List<JsonDocument> results, Reference<JsonDocument> resolvedDocument);
}

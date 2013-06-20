package raven.client.listeners;

import java.util.List;

import raven.abstractions.basic.Holder;
import raven.abstractions.data.JsonDocument;

public class IDocumentConflictListener {

  public boolean tryResolveConflict(String key, List<JsonDocument> results, Holder<JsonDocument> resolvedDocument) {
    // TODO Auto-generated method stub
    return false;
  }
  //TODO: implement me
}

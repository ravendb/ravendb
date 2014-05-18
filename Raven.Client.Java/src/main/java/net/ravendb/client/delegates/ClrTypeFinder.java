package net.ravendb.client.delegates;

import net.ravendb.abstractions.json.linq.RavenJObject;


public interface ClrTypeFinder {
  public String find(String id, RavenJObject doc, RavenJObject metadata);
}

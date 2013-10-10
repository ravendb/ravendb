package raven.client.delegates;

import raven.abstractions.json.linq.RavenJObject;


public interface ClrTypeFinder {
  public String find(String id, RavenJObject doc, RavenJObject metadata);
}

package net.ravendb.client.delegates;

public interface PropertyNameFinder {
  public String find(Class< ? > indexedType, String indexedName, String path, String prop);
}

package net.ravendb.client.delegates;


public interface DocumentKeyFinder {
  public String find(Object id, Class< ? > type, Boolean allowNull);
}

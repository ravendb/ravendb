package net.ravendb.client.delegates;


public interface ClrTypeNameFinder {
  public String find(Class<?> clazz);
}

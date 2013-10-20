package net.ravendb.client.delegates;


public interface IdValuePartFinder {
  public String find(Object entity, String id);
}

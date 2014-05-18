package net.ravendb.client.delegates;


public interface RequestCachePolicy {
  public Boolean shouldCacheRequest(String url);
}

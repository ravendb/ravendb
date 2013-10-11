package raven.client.delegates;


public interface RequestCachePolicy {
  public Boolean shouldCacheRequest(String url);
}

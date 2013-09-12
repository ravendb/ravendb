package raven.client.util;

import raven.abstractions.closure.Action1;
import raven.client.changes.IDatabaseChanges;

//TODO: finish me
public class EvictItemsFromCacheBasedOnChanges implements AutoCloseable {

  public EvictItemsFromCacheBasedOnChanges(String databaseName, IDatabaseChanges changes, Action1<String> evictCacheOldItems) {
  }

  @Override
  public void close() throws Exception {

  }

}

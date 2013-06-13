package raven.client.util;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import raven.abstractions.closure.Action1;
import raven.client.connection.CachedRequest;
import raven.client.connection.profiling.ConcurrentLruSet;

public class SimpleCache implements AutoCloseable {

  private final ConcurrentLruSet<String> lruKeys;
  private final ConcurrentHashMap<String, CachedRequest> actualCache;
  private final ConcurrentHashMap<String, Date> lastWritePerDb = new ConcurrentHashMap<>();

  private AtomicInteger memoryPressureCounterOnSet = new AtomicInteger();
  private AtomicInteger memoryPressureCounterOnGet = new AtomicInteger();

  public SimpleCache(int maxNumberOfCacheEntries) {
    actualCache = new ConcurrentHashMap<>();
    lruKeys = new ConcurrentLruSet<>(maxNumberOfCacheEntries, new Action1<String>() {
      @Override
      public void apply(String key) {
        actualCache.remove(key);
      }
    });
  }

  private static long getAvailableMemory() {
    return Runtime.getRuntime().freeMemory() / 1024 / 1024;
  }

  public void set(String key, CachedRequest val) {
    if (memoryPressureCounterOnSet.incrementAndGet() % 25 == 0) {
      tryClearMemory();
    }
    actualCache.put(key, val);
    lruKeys.push(key);
  }

  private void tryClearMemory() {
    long availableMemory = getAvailableMemory();
    if (availableMemory != -1 && availableMemory < 1024) {
      lruKeys.clearHalf();
    }
  }

  public CachedRequest get(String key) {
    CachedRequest value = null;
    if (actualCache.containsKey(key)) {
      value = actualCache.get(key);
      lruKeys.push(key);
      if (memoryPressureCounterOnGet.incrementAndGet() % 1000 == 0) {
        tryClearMemory();
      }
    }
    if (value != null) {
      Date lastWrite;
      if (lastWritePerDb.containsKey(value.getDatabase())) {
        lastWrite = lastWritePerDb.get(value.getDatabase());
        if (value.getTime().before(lastWrite)) {
          value.setForceServerCheck(true);
        }
      }
    }

    return value;
  }

  public int getCurrentSize() {
    return actualCache.size();
  }

  @Override
  public void close() throws Exception {
    lruKeys.clear();
    actualCache.clear();
  }

  public void forceServerCheckOfCachedItemsForDatabase(String databaseName) {
    Date newTime = new Date();
    lastWritePerDb.put(databaseName, newTime);
  }

}

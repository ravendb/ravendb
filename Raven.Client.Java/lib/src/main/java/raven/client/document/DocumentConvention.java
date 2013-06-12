package raven.client.document;

import java.util.concurrent.atomic.AtomicInteger;

import raven.abstractions.closure.Function1;

public class DocumentConvention {
  private AtomicInteger requestCount = new AtomicInteger(0);

  private FailoverBehavior failoverBehavior;

  private Function1<String, Boolean> shouldCacheRequest;

  /**
   * @return the shouldCacheRequest
   */
  public Function1<String, Boolean> getShouldCacheRequest() {
    return shouldCacheRequest;
  }

  /**
   * @param shouldCacheRequest the shouldCacheRequest to set
   */
  public void setShouldCacheRequest(Function1<String, Boolean> shouldCacheRequest) {
    this.shouldCacheRequest = shouldCacheRequest;
  }

  /**
   * @return the failoverBehavior
   */
  public FailoverBehavior getFailoverBehavior() {
    return failoverBehavior;
  }

  /**
   * @param failoverBehavior the failoverBehavior to set
   */
  public void setFailoverBehavior(FailoverBehavior failoverBehavior) {
    this.failoverBehavior = failoverBehavior;
  }

  public int incrementRequestCount() {
    return requestCount.incrementAndGet();
  }

}

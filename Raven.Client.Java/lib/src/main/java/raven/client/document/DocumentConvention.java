package raven.client.document;

import java.util.concurrent.atomic.AtomicInteger;

public class DocumentConvention {
  private AtomicInteger requestCount = new AtomicInteger(0);

  private FailoverBehavior failoverBehavior;

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

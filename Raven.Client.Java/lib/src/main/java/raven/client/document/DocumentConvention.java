package raven.client.document;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;

import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;

//TODO: finish me
public class DocumentConvention {
  private AtomicInteger requestCount = new AtomicInteger(0);

  private FailoverBehavior failoverBehavior;

  private Function1<String, Boolean> shouldCacheRequest;

  private Function1<HttpResponse, Action1<HttpRequest>> handleForbiddenResponse;

  private Function1<HttpResponse, Action1<HttpRequest>> handleUnauthorizedResponse;


  /**
   * @return the handleUnauthorizedResponse
   */
  public Function1<HttpResponse, Action1<HttpRequest>> getHandleUnauthorizedResponse() {
    return handleUnauthorizedResponse;
  }

  /**
   * @param handleUnauthorizedResponse the handleUnauthorizedResponse to set
   */
  public void setHandleUnauthorizedResponse(Function1<HttpResponse, Action1<HttpRequest>> handleUnauthorizedResponse) {
    this.handleUnauthorizedResponse = handleUnauthorizedResponse;
  }

  /**
   * @return the handleForbiddenResponse
   */
  public Function1<HttpResponse, Action1<HttpRequest>> getHandleForbiddenResponse() {
    return handleForbiddenResponse;
  }

  /**
   * @param handleForbiddenResponse the handleForbiddenResponse to set
   */
  public void setHandleForbiddenResponse(Function1<HttpResponse, Action1<HttpRequest>> handleForbiddenResponse) {
    this.handleForbiddenResponse = handleForbiddenResponse;
  }

  public DocumentConvention() {
    //TODO:
    shouldCacheRequest = new Function1<String, Boolean>() {

      @Override
      public Boolean apply(String input) {
        return true;
      }
    };
  }

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

package net.ravendb.client.connection;

public class CachedRequestOp {
  private CachedRequest cachedRequest;
  private boolean skipServerCheck;

  public CachedRequestOp() {
    super();
  }

  public CachedRequestOp(CachedRequest cachedRequest, boolean skipServerCheck) {
    super();
    this.cachedRequest = cachedRequest;
    this.skipServerCheck = skipServerCheck;
  }

  /**
   * @return the cachedRequest
   */
  public CachedRequest getCachedRequest() {
    return cachedRequest;
  }

  /**
   * @param cachedRequest the cachedRequest to set
   */
  public void setCachedRequest(CachedRequest cachedRequest) {
    this.cachedRequest = cachedRequest;
  }

  /**
   * @return the skipServerCheck
   */
  public boolean isSkipServerCheck() {
    return skipServerCheck;
  }

  /**
   * @param skipServerCheck the skipServerCheck to set
   */
  public void setSkipServerCheck(boolean skipServerCheck) {
    this.skipServerCheck = skipServerCheck;
  }

}

package raven.client.connection;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import raven.abstractions.basic.EventArgs;
import raven.abstractions.basic.EventHandler;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.exceptions.ServerClientException;

//TODO: finish me
public class ReplicationInformer {

  protected static AtomicInteger readStripingBase = new AtomicInteger(0);

  public void updateReplicationInformationIfNeeded(ServerClient serverClient) {
    // TODO Auto-generated method stub

  }

  //TODO: impl me

  public int getReadStripingBase() {
    return readStripingBase.incrementAndGet();
  }

  public <T> T executeWithReplication(HttpMethods method, String url, int currentRequest, int currentReadStripingBase, Function1<String, T> operation) throws ServerClientException {
    return operation.apply(url);
    //TODO: implement me
  }

  public int getFailureCount(String url) {
    // TODO Auto-generated method stub
    return 0;
  }

  public Date getFailureLastCheck(String operationUrl) {
    //TODO:
    return null;
  }

  public static class FailoverStatusChangedEventArgs extends EventArgs {
    private Boolean failing;
    private String url;
    /**
     * @return the failing
     */
    public Boolean getFailing() {
      return failing;
    }
    /**
     * @param failing the failing to set
     */
    public void setFailing(Boolean failing) {
      this.failing = failing;
    }
    /**
     * @return the url
     */
    public String getUrl() {
      return url;
    }
    /**
     * @param url the url to set
     */
    public void setUrl(String url) {
      this.url = url;
    }

  }

  public void addFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    // TODO Auto-generated method stub

  }

  public void removeFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    // TODO Auto-generated method stub

  }

  public void forceCheck(String primaryUrl, boolean shouldForceCheck) {
    // TODO Auto-generated method stub

  }

}

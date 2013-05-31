package raven.client.connection;

import java.util.concurrent.atomic.AtomicInteger;

import raven.abstractions.closure.Function1;
import raven.client.json.lang.ServerClientException;

public class ReplicationInformer {

  protected static AtomicInteger readStripingBase = new AtomicInteger(0);

  public void updateReplicationInformationIfNeeded(ServerClient serverClient) {
    // TODO Auto-generated method stub

  }
  //TODO: impl me

  public int getReadStripingBase() {
    return readStripingBase.incrementAndGet();
  }

  public  <T> T executeWithReplication(HttpMethods method, String url, int currentRequest, int currentReadStripingBase, Function1<String, T> operation) throws ServerClientException {
    return operation.apply(url);
    //TODO: implement me
  }

  public int getFailureCount(String url) {
    // TODO Auto-generated method stub
    return 0;
  }


}

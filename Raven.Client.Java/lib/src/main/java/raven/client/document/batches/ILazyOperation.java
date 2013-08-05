package raven.client.document.batches;

import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.client.connection.IDatabaseCommands;

public interface ILazyOperation {
  public GetRequest createRequest();
  public Object getResult();
  public boolean isRequiresRetry();
  public void handleResponse(GetResponse response);
  public AutoCloseable enterContext();
  public Object executeEmbedded(IDatabaseCommands commands);
  public void handleEmbeddedResponse(Object result);

}
